-- /usr/lib/lua/luci/controller/admin/dashboard.lua
module("luci.controller.admin.dashboard", package.seeall)

-- Global logging toggle
local LOG_ENABLED = false

-- Localize commonly used globals
local io_open    = io.open
local os_time    = os.time
local tonumber   = tonumber
local tostring   = tostring
local math_floor = math.floor
local table_sort = table.sort
local os_clock   = os.clock

-- Optional libraries
local nixio_ok, nixio = pcall(require, "nixio")
local nixio_fs        = nixio_ok and nixio.fs or nil

local has_jsonc, jsonc = pcall(require, "luci.jsonc")
local has_sys,   sys   = pcall(require, "luci.sys")
local util             = require "luci.util"
local ubus             = require "ubus"

-- --------------------
-- Refresh TTLs (seconds)
-- --------------------
local REFRESH = {
  PING    = 2,      -- internet reachability + RTT
  TEMPS   = 5,      -- CPU + Wi-Fi temps
  CLIENTS = 20,     -- Wi-Fi clients + LAN ports + RAM
  UPTIME  = 10,     -- system uptime + PPPoE status/uptime + wan device
  VNSTAT  = 300,    -- daily/monthly traffic
  ADBLOCK = 300,    -- adblock status (5 min cache, shared across all clients)
  NETWORK = 43200,  -- public IP / ISP / ASN  (12 h)
  BINARY  = 3600    -- binary/package presence checks (1 h) — binaries don't appear/vanish at runtime
}

-- PPPoE interface (must match watchdog config)

local PING_HOSTS = { "1.1.1.1", "9.9.9.9" }

local TMPFS_OK = true

-- --------------------
-- Logging
-- --------------------
local function log_hit(msg)
  if LOG_ENABLED then util.perror("dashboard: " .. msg) end
end

-- --------------------
-- JSON cache helpers
-- --------------------
local function save_cache(path, obj)
  local json
  if has_jsonc then json = jsonc.stringify(obj)
  else return end
  local f = io_open(path, "w")
  if f then f:write(json); f:close() end
end

local function load_cache(path)
  local f = io_open(path, "r")
  if not f then return nil end
  local data = f:read("*a"); f:close()
  if has_jsonc  then return jsonc.parse(data) end
  return nil
end

-- --------------------
-- Low-level read helpers
-- All use nixio_fs when available — no fork, direct kernel read
-- --------------------
local function sysfs_read(path)
  -- Returns trimmed string or nil
  if nixio_fs and nixio_fs.readfile then
    local v = nixio_fs.readfile(path)
    return v and v:match("^%s*(.-)%s*$") or nil
  end
  local f = io_open(path, "r")
  if not f then return nil end
  local s = f:read("*l"); f:close()
  return s
end

local function sysfs_num(path)
  return tonumber(sysfs_read(path))
end

local function read_ts(path)
  return tonumber(sysfs_read(path)) or 0
end

local function write_ts(path, now)
  local f = io_open(path, "w")
  if f then f:write(tostring(now)); f:close() end
end

-- Directory listing: nixio_fs.dir avoids fork entirely
local function list_dir(path)
  if not path then return {} end
  if nixio_fs and nixio_fs.dir then
    local ok, iter = pcall(nixio_fs.dir, path)
    if ok and iter then
      local out = {}
      for e in iter do
        if e ~= "." and e ~= ".." then out[#out+1] = e end
      end
      return out
    end
  end
  -- fallback: popen (still cached, so runs rarely)
  local out, p = {}, io.popen("ls -1 " .. path .. " 2>/dev/null")
  if p then
    for line in p:lines() do out[#out+1] = line end
    p:close()
  end
  return out
end

local function file_exists(path)
  if not path then return false end
  if nixio_fs and nixio_fs.stat then return nixio_fs.stat(path) ~= nil end
  local f = io_open(path, "r")
  if not f then return false end
  f:close(); return true
end

-- Helper: read downtime timestamp written by watchdog
local function read_downtime_start(path)
  return tonumber(sysfs_read(path))
end

-- Clear all ts files (force refresh)
local function clear_all_ts()
  local files = {
    "/tmp/dashboard_ping_ts",
    "/tmp/dashboard_temps_ts",
    "/tmp/dashboard_clients_mem_ts",
    "/tmp/dashboard_uptimes_ts",
    "/tmp/dashboard_vnstat_all_ts",    -- unified multi-iface cache (current)
    "/tmp/dashboard_vnstat_all.json",
    "/tmp/dashboard_vnstat_daily_ts",  -- legacy per-iface caches (may still exist)
    "/tmp/dashboard_vnstat_monthly_ts",
    "/tmp/dashboard_network_ts",
    "/tmp/dashboard_adblock_ts",
  }
  for _, f in ipairs(files) do os.remove(f) end
end

-- --------------------
-- ① Ping  (2 s cache)
-- Uses sys.exec — no ubus equivalent for ICMP ping
-- wan_iface: the L3 device to bind ping to (e.g. "pppoe-wan" or "eth1" for DHCP).
--            Pass nil/empty to skip interface binding (ping goes via default route).
-- Returns: { status="online"|"offline", rtt_ms=<float>|nil }
-- --------------------
local function get_ping_status(now, wan_iface)
  local ts_f, dat_f = "/tmp/dashboard_ping_ts", "/tmp/dashboard_ping.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.PING then
    local c = load_cache(dat_f)
    if c then
      log_hit("ping HIT status=" .. c.status .. " rtt=" .. tostring(c.rtt_ms))
      return c
    end
  end

  local result = { status = "offline", rtt_ms = nil }

  -- Build -I flag only when we have a valid interface name
  local iface_flag = (wan_iface and wan_iface ~= "") and ("-I " .. wan_iface .. " ") or ""

  for _, host in ipairs(PING_HOSTS) do
    local out = sys.exec(
      "ping " .. iface_flag .. "-c1 -W2 " .. host .. " 2>/dev/null") or ""
    -- Handles both iputils and busybox RTT formats
    local rtt = out:match("min/avg/max[^=]*=%s*[%d.]+/([%d.]+)/")
    if rtt then
      result.status = "online"
      result.rtt_ms = tonumber(rtt)
      break
    end
  end

  write_ts(ts_f, now)
  save_cache(dat_f, result)
  log_hit("ping FETCH status=" .. result.status .. " rtt=" .. tostring(result.rtt_ms))
  return result
end

-- --------------------
-- ② Uptimes + PPPoE status + wan device  (10 s cache)
-- Uses conn:call() — zero fork overhead:
--   system.info              → system uptime
--   network.interface.wan    → pppoe up/uptime/device
-- --------------------
local function get_uptimes(now, conn)
  local ts_f, dat_f = "/tmp/dashboard_uptimes_ts", "/tmp/dashboard_uptimes.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.UPTIME then
    local c = load_cache(dat_f)
    if c then
      log_hit("uptimes HIT sys=" .. c.system .. " wan_uptime=" .. c.wan_uptime .. " status=" .. c.wan_status)
      return c
    end
  end

  -- System uptime via ubus system.info (no fork)
  local system = 0
  local sysinfo = conn:call("system", "info", {})
  if sysinfo and sysinfo.uptime then
    system = math_floor(tonumber(sysinfo.uptime) or 0)
  else
    -- fallback: /proc/uptime (still no fork, direct file read)
    local s = sysfs_read("/proc/uptime")
    system = math_floor(tonumber(s and s:match("^(%d+%.?%d*)")) or 0)
  end

  -- WAN status + uptime + both wan device names via ubus (no fork)
  --   wan_l3_device   = logical WAN interface (e.g. "pppoe-wan" for PPPoE, "eth1"/"wan" for DHCP)
  --   wan_phys_device = physical port        (e.g. "eth1") — used for TX/RX counters
  --
  -- With PPPoE software offloading (nf_flow_table / xt_FLOWOFFLOAD) the pppoe-wan
  -- counters stop counting offloaded flows after the first packet.  The physical
  -- ethernet port sees every byte regardless of offloading, and its rx/tx semantics
  -- are already correct (rx = download from ISP, tx = upload to ISP) — no swap needed.
  local wan_uptime    = 0
  local wan_status    = "down"
  local wan_type      = "unknown"   -- "pppoe" | "dhcp" | "static" | "unknown"
  local wan_l3_device = ""
  local wan_phys_device = ""  -- fallback; overwritten below when wan is up
  local wan = conn:call("network.interface.wan", "status", {})
  if wan then
    if wan.up == true then
      wan_status = "up"
      wan_uptime = tonumber(wan.uptime) or 0
    end
    -- Detect protocol: ubus exposes wan.proto ("pppoe", "dhcp", "static", …)
    local proto = (type(wan.proto) == "string") and wan.proto:lower() or ""
    if     proto == "pppoe"  then wan_type = "pppoe"
    elseif proto == "dhcp" or proto == "dhcpv6" then wan_type = "dhcp"
    elseif proto == "static" then wan_type = "static"
    elseif proto ~= ""       then wan_type = proto
    end
    wan_l3_device   = wan.l3_device or ""        -- e.g. pppoe-wan (PPPoE) or eth1 (DHCP)
    wan_phys_device = wan.device    or wan_l3_device  -- physical port e.g. eth1
  end

  local result = {
    system          = system,
    wan_uptime      = wan_uptime,
    wan_status      = wan_status,
    wan_type        = wan_type,
    wan_l3_device   = wan_l3_device,
    wan_phys_device = wan_phys_device
  }

  write_ts(ts_f, now)
  save_cache(dat_f, result)
  return result
end

-- --------------------
-- ③ Network device counters  (live — always fresh, it's just a ubus call)
-- Uses the PHYSICAL wan port (e.g. eth1) via conn:call("network.device","status").
-- The physical port counts every byte even when PPPoE software offloading is active;
-- rx = download (from ISP), tx = upload (to ISP) — semantics are already correct.
-- --------------------
local function get_net(conn, wan_phys_device)
  local dev = conn:call("network.device", "status", { name = wan_phys_device })
  if dev and dev.statistics then
    return dev.statistics.rx_bytes or "N/A",
           dev.statistics.tx_bytes or "N/A"
  end
  -- fallback: sysfs direct read (no fork)
  local rx = sysfs_num("/sys/class/net/" .. wan_phys_device .. "/statistics/rx_bytes")
  local tx = sysfs_num("/sys/class/net/" .. wan_phys_device .. "/statistics/tx_bytes")
  return rx or "N/A", tx or "N/A"
end

-- --------------------
-- ④ Temperatures  (5 s cache)
-- Uses sysfs_num/sysfs_read via nixio_fs — no fork
-- list_dir uses nixio_fs.dir — no fork

-- --------------------
-- Binary presence cache: vnstat
-- Caches the result of the binary check for REFRESH.BINARY seconds (1 h).
-- Avoids forking a shell on every 2-second poll just to run "which vnstat".
-- --------------------
local function get_vnstat_installed(now)
  local ts_f  = "/tmp/dashboard_vnstat_bin_ts"
  local dat_f = "/tmp/dashboard_vnstat_bin.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.BINARY then
    local c = load_cache(dat_f)
    if c and c.installed ~= nil then return c.installed end
  end

  -- sysfs_read is faster than sys.exec for existence checks when possible,
  -- but for PATH lookups we still need a shell. This runs once per hour.
  local bin = (sys.exec("command -v vnstat 2>/dev/null") or ""):match("%S+")
  local installed = (bin ~= nil and bin ~= "")
  write_ts(ts_f, now)
  save_cache(dat_f, { installed = installed })
  return installed
end

local function get_adblock(now)
  local ts_f, dat_f = "/tmp/dashboard_adblock_ts", "/tmp/dashboard_adblock.json"

  -- Serve from cache if still fresh and the entry has the installed field.
  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.ADBLOCK then
    local c = load_cache(dat_f)
    if c and c.installed ~= nil then return c end
  end

  -- Detect whether adblock is actually installed by checking for its binary.
  -- We do NOT rely on /tmp/run/adb_runtime.json alone because that file can
  -- linger after adblock is removed, which would make it look installed.
  local adb_bin = (sys.exec("command -v adblock 2>/dev/null || command -v /etc/init.d/adblock 2>/dev/null") or ""):match("%S+")
  local installed = (adb_bin ~= nil and adb_bin ~= "")

  if not installed then
    local result = { installed = false }
    write_ts(ts_f, now)
    save_cache(dat_f, result)
    return result
  end

  -- adblock is installed — try to read the runtime status file.
  -- Read raw bytes — do NOT use load_cache/jsonc.parse on this file.
  -- adb_runtime.json contains Unicode symbols (✘ ✔) that crash luci.jsonc.
  -- Simple pattern matching is immune and extracts exactly what we need.
  local f = io_open("/tmp/run/adb_runtime.json", "r")
  if not f then
    -- installed but not yet running / no status yet
    local result = { installed = true, status = "stopped", blocked_domains = 0 }
    write_ts(ts_f, now)
    save_cache(dat_f, result)
    return result
  end
  local raw = f:read("*a"); f:close()
  if not raw or raw == "" then
    local result = { installed = true, status = "stopped", blocked_domains = 0 }
    write_ts(ts_f, now)
    save_cache(dat_f, result)
    return result
  end

  local status      = (raw:match('"adblock_status"%s*:%s*"([^"]*)"') or ""):lower()
  local domains_str = raw:match('"blocked_domains"%s*:%s*"([^"]*)"')
                   or raw:match('"blocked_domains"%s*:%s*(%d+)')
                   or "0"
  local domains = tonumber((domains_str:gsub("[^%d]", ""))) or 0

  local result = { installed = true, status = status, blocked_domains = domains }
  write_ts(ts_f, now)
  save_cache(dat_f, result)
  return result
end

local function get_temps(now)
  local ts_f, dat_f = "/tmp/dashboard_temps_ts", "/tmp/dashboard_temps.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.TEMPS then
    local c = load_cache(dat_f)
    if c then
      log_hit("temps HIT cpu=" .. tostring(c.cpu) .. " wifi=" .. tostring(c.wifi))
      return c
    end
  end

  -- CPU temp: thermal_zone0 first, then hwmon coretemp/cpu
  local cpu_t   = "N/A"
  local cpu_raw = sysfs_num("/sys/class/thermal/thermal_zone0/temp")
  if cpu_raw then
    cpu_t = cpu_raw / 1000
  else
    for _, entry in ipairs(list_dir("/sys/class/hwmon")) do
      local base = "/sys/class/hwmon/" .. entry
      local name = sysfs_read(base .. "/name") or ""
      if name:match("coretemp") or name:match("cpu") then
        local v = sysfs_num(base .. "/temp1_input")
        if v then cpu_t = v / 1000; break end
      end
    end
  end

  -- Wi-Fi temps: average of non-CPU hwmon sensors
  local sum, count = 0, 0
  for _, entry in ipairs(list_dir("/sys/class/hwmon")) do
    local base = "/sys/class/hwmon/" .. entry
    local name = sysfs_read(base .. "/name") or ""
    if name ~= "" and not name:match("coretemp") and not name:match("cpu") then
      for _, fn in ipairs(list_dir(base)) do
        if fn:match("^temp%d+_input$") then
          local v = sysfs_num(base .. "/" .. fn)
          if v and v > 0 then sum = sum + v; count = count + 1 end
        end
      end
    end
  end
  local wifi_t = (count > 0) and (sum / count / 1000) or "N/A"

  local result = { cpu = cpu_t, wifi = wifi_t }
  write_ts(ts_f, now)
  save_cache(dat_f, result)
  return result
end

-- --------------------
-- ⑤ Wi-Fi clients + LAN ports + RAM  (20 s cache)
-- Wi-Fi: conn:call via hostapd (already ubus, no change)
-- LAN:   list_dir via nixio_fs.dir + sysfs_read — no fork
-- RAM:   /proc/meminfo direct read — no fork
-- --------------------
local function get_clients_and_mem(now, conn)
  local ts_f, dat_f = "/tmp/dashboard_clients_mem_ts", "/tmp/dashboard_clients_mem.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.CLIENTS then
    local c = load_cache(dat_f)
    if c then
      log_hit("clients HIT wifi=" .. c.wifi.total .. " lan=" .. c.lan.total)
      return c
    end
  end

  -- Wi-Fi clients via hostapd ubus objects
  -- popen only for object listing (no pure-lua ubus enumeration available);
  -- runs once per 20 s so fork cost is negligible
  local total_wifi, aps = 0, {}
  local f = io.popen("ubus list 2>/dev/null | grep '^hostapd\\.'")
  if f then
    for line in f:lines() do
      local iface = line:match("^hostapd%.(.+)$")
      if iface and iface ~= "" then
        local status = conn:call("hostapd." .. iface, "get_status", {})
        if status and status.ssid and status.freq then
          local fmhz = tonumber(status.freq) or 0
          local band = "unknown"
          if     fmhz >= 2400 and fmhz < 2500 then band = "2.4_Ghz"
          elseif fmhz >= 5000 and fmhz < 6000 then band = "5_Ghz"
          elseif fmhz >= 6000               then band = "6_Ghz" end

          local count  = 0
          local ok, res = pcall(conn.call, conn, "hostapd." .. iface, "get_clients", {})
          if ok and res and res.clients then
            for _ in pairs(res.clients) do count = count + 1 end
          end

          -- Signal quality via iwinfo ubus — same conn, no fork
          -- quality_max is typically 70 (nl80211); we normalise to 0-100 %
          local quality = nil
          local iw = conn:call("iwinfo", "info", { device = iface })
          if iw and iw.quality_max and iw.quality_max > 0 and iw.quality then
            quality = math_floor((iw.quality / iw.quality_max) * 100)
          end

          aps[#aps+1] = { iface=iface, essid=status.ssid, freq=fmhz, band=band, count=count, quality=quality }
          total_wifi  = total_wifi + count
        end
      end
    end
    f:close()
  end
  local wifi = { total = total_wifi, aps = aps }

  -- ── LAN ports — Bridge/AP detection (4-signal cascade) ──────────────────
  --
  -- Signal 1 (hard)  — FDB mac_count > 1
  --     Multiple distinct MACs behind one physical port.  Definitively a
  --     bridge, managed AP, or unmanaged switch.  No false positives.
  --
  -- Signal 2 (soft)  — OUI matches known router/AP vendor list
  --     The single MAC on the port belongs to a manufacturer that ships
  --     routers or APs.  On its own this just means "probably a network
  --     device", but combined with the absence of a routable IP (Signal 4)
  --     it is very strong evidence of an AP in bridge mode.
  --
  -- Signal 3 (soft)  — device IP appears as a routing-table gateway
  --     The kernel is actively forwarding packets through this device
  --     ( "ip route show" shows "via <ip>" ).  This means it is running NAT
  --     or acting as a next-hop router — it is not a plain end-device.
  --
  -- Signal 4 (soft)  — MAC present in FDB but no IP in ARP or DHCP leases
  --     An AP running in pure bridge mode does NOT request a DHCP lease
  --     for itself — it passes DHCP through to its own clients.  So the
  --     port will have exactly one MAC in the FDB, but that MAC will not
  --     appear in /proc/net/arp or /tmp/dhcp.leases.
  --     This is the primary detection path for "AP connected, zero clients".
  --     We always cross-check ARP *and* DHCP before concluding no-IP, to
  --     avoid false-positives on a newly connected PC whose lease hasn't
  --     propagated to the registry yet.
  --
  -- Any signal → ap_label set.  Label = "AP" when OUI is recognised,
  -- "Bridge" otherwise (conservative).  mac_count emitted = clients-behind
  -- (FDB count minus 1 for the AP's own uplink MAC; 0 when no clients yet).

  -- Known router/AP OUI prefixes (first 3 octets, lowercase hex)
  local ROUTER_OUIS = {
    -- TP-Link
    ["00:1d:0f"]=true,["14:cc:20"]=true,["50:c7:bf"]=true,["a0:f3:c1"]=true,
    ["98:da:c4"]=true,["b0:4e:26"]=true,["2c:55:d3"]=true,["f4:f2:6d"]=true,
    ["c4:e9:84"]=true,["d8:0d:17"]=true,["e8:de:27"]=true,["54:af:97"]=true,
    -- ASUS
    ["00:0c:6e"]=true,["04:d4:c4"]=true,["08:60:6e"]=true,["10:bf:48"]=true,
    ["14:da:e9"]=true,["1c:87:2c"]=true,["2c:4d:54"]=true,["2c:fd:a1"]=true,
    ["38:2c:4a"]=true,["50:46:5d"]=true,["54:a0:50"]=true,["74:d0:2b"]=true,
    ["ac:84:c6"]=true,["f0:2f:74"]=true,["b0:6e:bf"]=true,["3c:7c:3f"]=true,
    -- Netgear
    ["00:09:5b"]=true,["00:14:6c"]=true,["20:4e:7f"]=true,["28:c6:8e"]=true,
    ["2c:b0:5d"]=true,["c0:ff:d4"]=true,["9c:3d:cf"]=true,
    -- D-Link
    ["00:05:5d"]=true,["1c:7e:e5"]=true,["28:10:7b"]=true,["b0:c5:54"]=true,
    -- MikroTik
    ["00:0c:42"]=true,["18:fd:74"]=true,["2c:c8:1b"]=true,["48:8f:5a"]=true,
    ["64:d1:54"]=true,["6c:3b:6b"]=true,["74:4d:28"]=true,["b8:69:f4"]=true,
    ["cc:2d:e0"]=true,["d4:ca:6d"]=true,["dc:2c:6e"]=true,["e4:8d:8c"]=true,
    ["4c:5e:0c"]=true,["c4:ad:34"]=true,["08:55:31"]=true,
    -- Ubiquiti
    ["00:27:22"]=true,["04:18:d6"]=true,["18:e8:29"]=true,["24:a4:3c"]=true,
    ["44:d9:e7"]=true,["60:22:32"]=true,["68:72:51"]=true,["78:8a:20"]=true,
    ["80:2a:a8"]=true,["dc:9f:db"]=true,["f0:9f:c2"]=true,["fc:ec:da"]=true,
    ["00:15:6d"]=true,["04:92:26"]=true,["b4:fb:e4"]=true,
    -- GL.iNet
    ["94:83:c4"]=true,["e4:95:6e"]=true,["20:79:18"]=true,
    -- Huawei
    ["00:1e:10"]=true,["04:02:1f"]=true,["20:a6:80"]=true,["70:72:cf"]=true,
    -- Xiaomi/Redmi
    ["00:9e:c8"]=true,["0c:1a:f0"]=true,["28:6c:07"]=true,["34:ce:00"]=true,
    ["78:11:dc"]=true,["ac:c1:ee"]=true,["d4:97:0b"]=true,["f8:a4:5f"]=true,
    -- Tenda
    ["c8:3a:35"]=true,["d4:61:fe"]=true,["e4:d3:32"]=true,["f4:92:bf"]=true,
  }

  -- Signal 1: FDB — per-port MAC count and first seen MAC
  local fdb_count     = {}   -- port → int (total dynamic MACs)
  local fdb_first_mac = {}   -- port → first dynamic MAC (lowercase)
  if has_sys then
    local fdb_raw = sys.exec("bridge fdb show br br-lan 2>/dev/null") or ""
    local port_sets = {}
    for line in fdb_raw:gmatch("[^\r\n]+") do
      if not line:find("permanent") and not line:find("self") then
        local mac, dev = line:match("^(%x%x:%x%x:%x%x:%x%x:%x%x:%x%x)%s+dev%s+(%S+)")
        if mac and dev then
          mac = mac:lower()
          if not port_sets[dev] then port_sets[dev] = {} end
          if not port_sets[dev][mac] then
            port_sets[dev][mac] = true
            if not fdb_first_mac[dev] then fdb_first_mac[dev] = mac end
          end
        end
      end
    end
    for port, macs in pairs(port_sets) do
      local n = 0; for _ in pairs(macs) do n = n + 1 end
      fdb_count[port] = n
    end
  end

  -- Signal 3: routing-table gateway IPs  ("via <ip>" entries)
  local gateway_ips = {}
  if has_sys then
    local rt = sys.exec("ip route show 2>/dev/null") or ""
    for gw in rt:gmatch("%svia%s+(%S+)") do gateway_ips[gw] = true end
  end

  -- ARP/neighbour table: MAC → IP  (used by Signals 3 & 4)
  -- Skips FAILED/INCOMPLETE/PROBE states and fe80::/10 link-local IPv6.
  local arp_by_mac = {}
  if has_sys then
    local out = sys.exec("ip neigh show 2>/dev/null") or ""
    for line in out:gmatch("[^\r\n]+") do
      local ip  = line:match("^(%S+)")
      local mac = line:match("lladdr%s+(%x%x:%x%x:%x%x:%x%x:%x%x:%x%x)")
      if ip and mac then
        local is_ll = ip:sub(1,4):lower():match("^fe[89ab]") ~= nil
        if not is_ll then
          local st = line:match("%s(%u+)%s*$") or ""
          if st ~= "FAILED" and st ~= "INCOMPLETE" and st ~= "PROBE" then
            arp_by_mac[mac:lower()] = ip
          end
        end
      end
    end
  end

  -- DHCP leases: MAC → IP  (fallback for Signal 4)
  local dhcp_by_mac = {}
  do
    local f = io_open("/tmp/dhcp.leases", "r")
    if f then
      for line in f:lines() do
        local parts = {}
        for tok in line:gmatch("%S+") do parts[#parts+1] = tok end
        if #parts >= 3 and parts[2] and parts[3] then
          dhcp_by_mac[parts[2]:lower()] = parts[3]
        end
      end
      f:close()
    end
  end

  local brif_path = "/sys/class/net/br-lan/brif"
  local ports, total_lan, total_physical = {}, 0, 0

  for _, iface in ipairs(list_dir(brif_path)) do
    if not (file_exists("/sys/class/net/" .. iface .. "/wireless")
         or file_exists("/sys/class/net/" .. iface .. "/phy80211")
         or iface:match("^wl") or iface:match("^wlan")) then
      total_physical = total_physical + 1
      local carrier = sysfs_read("/sys/class/net/" .. iface .. "/carrier")
      local oper    = sysfs_read("/sys/class/net/" .. iface .. "/operstate")
      local active  = (carrier == "1") or (oper and oper:match("^up") ~= nil)
      local spd_raw = sysfs_num("/sys/class/net/" .. iface .. "/speed")
      local speed   = (spd_raw and spd_raw > 0) and spd_raw or nil
      if active then total_lan = total_lan + 1 end

      local n_macs    = fdb_count[iface] or 0
      local first_mac = fdb_first_mac[iface]          -- may be nil if no FDB entry
      local oui       = first_mac and first_mac:sub(1, 8) or ""
      local known_oui = first_mac and ROUTER_OUIS[oui] or false

      -- Resolve the best IP for the primary MAC (for Signals 3 & 4)
      local port_ip = (first_mac and (arp_by_mac[first_mac] or dhcp_by_mac[first_mac])) or ""

      -- Run cascade — stop at first positive signal
      local is_bridge    = false
      local signal_fired = nil

      if n_macs > 1 then                                    -- Signal 1
        is_bridge = true; signal_fired = 1
      elseif known_oui then                                 -- Signal 2
        is_bridge = true; signal_fired = 2
      elseif port_ip ~= "" and gateway_ips[port_ip] then   -- Signal 3
        is_bridge = true; signal_fired = 3
      elseif first_mac and port_ip == "" then               -- Signal 4
        is_bridge = true; signal_fired = 4
      end

      -- Label: "AP" when OUI is recognised, "Bridge" otherwise
      local ap_label = is_bridge and (known_oui and "AP" or "Bridge") or nil

      -- Clients behind the AP: FDB count minus the AP's own uplink MAC.
      -- Zero when no clients yet (Signals 2/3/4 with n_macs <= 1).
      local client_count = (is_bridge and n_macs > 1) and (n_macs - 1) or 0

      ports[#ports+1] = {
        name      = iface,
        active    = active,
        speed     = speed,
        mac_count = client_count,
        ap_label  = ap_label,
      }
    end
  end

  table_sort(ports, function(a, b) return a.name < b.name end)
  local lan = { total = total_lan, total_physical = total_physical, ports = ports }

  -- RAM: /proc/meminfo line-by-line, early exit
  local mem_total, mem_avail
  local mf = io_open("/proc/meminfo", "r")
  if mf then
    for line in mf:lines() do
      if not mem_total then mem_total = tonumber(line:match("MemTotal:%s+(%d+)")) end
      if not mem_avail then mem_avail = tonumber(line:match("MemAvailable:%s+(%d+)")) end
      if mem_total and mem_avail then break end
    end
    mf:close()
  end
  local mem = { total = mem_total or 0, avail = mem_avail or 0 }

  local result = { wifi = wifi, lan = lan, mem = mem }
  write_ts(ts_f, now)
  save_cache(dat_f, result)
  return result
end

-- --------------------
-- ⑥ CPU stats  (live — /proc/stat, single read, no fork)
-- --------------------
local function get_cpu_and_cores(now)
  local s = ""
  local f = io_open("/proc/stat", "r")
  if f then s = f:read("*a"); f:close() end

  local agg, cores = {}, {}
  for line in s:gmatch("[^\r\n]+") do
    if line:match("^cpu%s+") then
      local u,n,su,i,io_w,irq,soft,steal =
        line:match("^cpu%s+(%d+)%s+(%d+)%s+(%d+)%s+(%d+)%s*(%d*)%s*(%d*)%s*(%d*)%s*(%d*)")
      agg = {
        usr=tonumber(u) or 0, nice=tonumber(n) or 0, sys=tonumber(su) or 0,
        idle=tonumber(i) or 0, iowait=tonumber(io_w) or 0,
        irq=tonumber(irq) or 0, softirq=tonumber(soft) or 0,
        steal=tonumber(steal) or 0, ts=now
      }
    else
      local id,u,n,su,i,io_w,irq,soft,steal =
        line:match("^cpu(%d+)%s+(%d+)%s+(%d+)%s+(%d+)%s+(%d+)%s*(%d*)%s*(%d*)%s*(%d*)%s*(%d*)")
      if id then
        cores[#cores+1] = {
          id=tonumber(id),
          usr=tonumber(u) or 0, nice=tonumber(n) or 0, sys=tonumber(su) or 0,
          idle=tonumber(i) or 0, iowait=tonumber(io_w) or 0,
          irq=tonumber(irq) or 0, softirq=tonumber(soft) or 0,
          steal=tonumber(steal) or 0, ts=now
        }
      end
    end
  end
  -- Sanity check: only send per-core breakdown when >1 core present.
  -- Single-core systems expose only "cpu" in /proc/stat with no "cpu0" line,
  -- or exactly one core line — either way per-core bars add no value.
  local out_cores = (#cores > 1) and cores or nil
  return { agg=agg, cores=out_cores }
end

-- --------------------
-- ⑦ vnstat  (300 s cache — sys.exec unavoidable)
-- --------------------

-- Format raw bytes into a human-readable string.
local function fmt_bytes(n)
  n = tonumber(n) or 0
  if     n >= 1099511627776 then return string.format("%.2f TB", n / 1099511627776)
  elseif n >= 1073741824    then return string.format("%.2f GB", n / 1073741824)
  elseif n >= 1048576       then return string.format("%.2f MB", n / 1048576)
  elseif n >= 1024          then return string.format("%.2f KB", n / 1024)
  else                           return string.format("%d B",    n) end
end

-- Normalise unit strings produced by vnstat --oneline (legacy path).
local function normalize_units(str)
  if not str or type(str) ~= "string" then return str end
  for k, v in pairs({ KiB="KB", MiB="MB", GiB="GB", TiB="TB",
                      kib="KB", mib="MB", gib="GB", tib="TB" }) do
    str = str:gsub(k, v)
  end
  return str:gsub("(%d[%d%.]*)%s*([KMGT]?B)", "%1 %2")
end

-- Build a map of kernel-device-name → human label using ubus network metadata.
-- e.g. { eth1="WAN · eth1", ["br-lan"]="LAN · br-lan", ["pppoe-wan"]="WAN · pppoe-wan" }
local function get_iface_labels(conn)
  local labels = {}
  if not conn then return labels end
  local ok, dump = pcall(conn.call, conn, "network", "interface_dump", {})
  if not ok or not dump or type(dump.interface) ~= "table" then return labels end
  for _, entry in ipairs(dump.interface) do
    local logical = type(entry.interface) == "string" and entry.interface or ""
    local device  = type(entry.device)    == "string" and entry.device    or ""
    local l3dev   = type(entry.l3_device) == "string" and entry.l3_device or ""
    -- Skip loopback and unnamed entries; they're not useful in a bandwidth UI
    if logical ~= "" and logical ~= "loopback" then
      local lbl = logical:upper()
      if device ~= "" and not labels[device] then
        labels[device] = lbl .. " \xc2\xb7 " .. device  -- "WAN · eth1"
      end
      if l3dev ~= "" and l3dev ~= device and not labels[l3dev] then
        labels[l3dev] = lbl .. " \xc2\xb7 " .. l3dev    -- "WAN · pppoe-wan"
      end
    end
  end
  return labels
end

-- Return today's/this-month's entry from a vnstat traffic array.
-- use_id=true  → jsonversion 1: id==1 means current period
-- use_id=false → jsonversion 2: ignore id entirely, match by date (entries have id but it's NOT sequential)
local function pick_current(entries, is_monthly, use_id)
  if type(entries) ~= "table" or #entries == 0 then return nil end
  if use_id then
    for _, e in ipairs(entries) do
      if e.id == 1 then return e end
    end
    return nil
  end
  -- jsonversion 2: match by today's actual date, scan newest-first
  local t = os.date("*t")
  for i = #entries, 1, -1 do
    local e = entries[i]
    if type(e.date) == "table" then
      if is_monthly then
        if e.date.year == t.year and e.date.month == t.month then return e end
      else
        if e.date.year == t.year and e.date.month == t.month and e.date.day == t.day then return e end
      end
    end
  end
  return entries[#entries]  -- fallback: last element
end

-- Extract bytes from a vnstat rx/tx field.
-- vnstat 2.0-2.8: field is a plain number  → rx = 12345
-- vnstat 2.9+:    field is an object       → rx = { bytes=12345, packets=67 }
local function rxb(v)
  if type(v) == "number" then return v end
  if type(v) == "table"  then return tonumber(v.bytes) or 0 end
  return tonumber(v) or 0
end
-- Everything (interface list, labels, usage) is stored in one unified 5-minute cache.
-- Primary:  vnstat --json   (vnstat ≥ 2.x — one exec, authoritative interface list)
--   jsonversion 1 (vnstat 2.0-2.5): traffic arrays named "days" / "months", entries have "id"
--   jsonversion 2 (vnstat 2.6+):    traffic arrays named "day"  / "month",  no "id", recent-first
-- Fallback: --dbiflist + --oneline per interface (vnstat 1.x)
local function get_all_vnstat_data(now, preferred_iface, conn)
  local ts_f  = "/tmp/dashboard_vnstat_all_ts"
  local dat_f = "/tmp/dashboard_vnstat_all.json"

  -- 5-minute unified cache covers interface list, labels, and all usage data
  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.VNSTAT then
    local c = load_cache(dat_f)
    if c then log_hit("vnstat all HIT"); return c end
  end

  local iface_labels = get_iface_labels(conn)
  local result       = { ifaces = {}, data = {} }

  -- ── Primary path: vnstat --json (2.x) ──────────────────────────────────
  local json_out = sys.exec("vnstat --json 2>/dev/null") or ""
  local parsed   = (json_out ~= "") and (has_jsonc and jsonc.parse(json_out)) or nil

  if parsed and type(parsed.interfaces) == "table" and #parsed.interfaces > 0 then
    log_hit("vnstat --json OK, " .. #parsed.interfaces .. " iface(s)")

    -- jsonversion "1" uses id==1 for current period.
    -- jsonversion "2" has id fields but they are NOT sequential — use date matching only.
    local use_id = (tostring(parsed.jsonversion or "2") == "1")

    -- Reorder: preferred (WAN physical) comes first if present
    local ordered, seen = {}, {}
    if preferred_iface and preferred_iface ~= "" then
      for _, idata in ipairs(parsed.interfaces) do
        if idata.name == preferred_iface then
          ordered[#ordered+1] = idata
          seen[preferred_iface] = true
          break
        end
      end
    end
    for _, idata in ipairs(parsed.interfaces) do
      if not seen[idata.name] then
        ordered[#ordered+1] = idata
        seen[idata.name] = true
      end
    end

    for _, idata in ipairs(ordered) do
      local name = type(idata.name) == "string" and idata.name or ""
      if name ~= "" then
        local tr = (type(idata.traffic) == "table") and idata.traffic or {}

        -- Handle both jsonversion 1 ("days"/"months") and jsonversion 2 ("day"/"month")
        local day_arr = tr.day   or tr.days
        local mon_arr = tr.month or tr.months

        local day_e = pick_current(day_arr, false, use_id)
        local d_rx  = day_e and rxb(day_e.rx) or 0
        local d_tx  = day_e and rxb(day_e.tx) or 0

        local mon_e = pick_current(mon_arr, true, use_id)
        local m_rx  = mon_e and rxb(mon_e.rx) or 0
        local m_tx  = mon_e and rxb(mon_e.tx) or 0

        result.ifaces[#result.ifaces+1] = {
          name  = name,
          label = iface_labels[name] or name,
        }
        result.data[name] = {
          daily   = { rx=fmt_bytes(d_rx), tx=fmt_bytes(d_tx), total=fmt_bytes(d_rx+d_tx) },
          monthly = { rx=fmt_bytes(m_rx), tx=fmt_bytes(m_tx), total=fmt_bytes(m_rx+m_tx) },
        }
      end
    end

  else
    -- ── Fallback path: vnstat 1.x / --json unavailable ─────────────────
    log_hit("vnstat --json unavailable, falling back to --oneline")

    local seen, candidates = {}, {}
    -- Only add preferred if it actually produces output (avoids phantom entries)
    if preferred_iface and preferred_iface ~= "" then
      local test = sys.exec("vnstat --oneline -i " .. preferred_iface .. " 2>/dev/null") or ""
      if test ~= "" and not test:match("^Error") then
        candidates[#candidates+1] = preferred_iface
        seen[preferred_iface] = true
      end
    end
    local db_out = sys.exec("vnstat --dbiflist 2>/dev/null") or ""
    for iface in db_out:gmatch("%S+") do
      if not seen[iface] then
        candidates[#candidates+1] = iface
        seen[iface] = true
      end
    end

    for _, iface in ipairs(candidates) do
      local out = sys.exec("vnstat --oneline -i " .. iface .. " 2>/dev/null") or ""
      local daily   = { rx="N/A", tx="N/A", total="N/A" }
      local monthly = { rx="N/A", tx="N/A", total="N/A" }

      if out ~= "" and not out:match("^Error") then
        -- Format: 1;IFACE;YYYY-MM-DD;day_rx;day_tx;day_total;YYYY-MM;mon_rx;mon_tx;mon_total;alltime
        --         [1]  [2]    [3]      [4]    [5]    [6]       [7]    [8]    [9]    [10]      [11]
        local parts = {}
        for field in out:gmatch("[^;]+") do parts[#parts+1] = field end
        if #parts >= 6 then
          daily = {
            rx    = normalize_units(parts[4] or "N/A"),
            tx    = normalize_units(parts[5] or "N/A"),
            total = normalize_units(parts[6] or "N/A"),
          }
        end
        if #parts >= 10 then
          monthly = {
            rx    = normalize_units(parts[8]  or "N/A"),
            tx    = normalize_units(parts[9]  or "N/A"),
            total = normalize_units(parts[10] or "N/A"),
          }
        end
      end

      result.ifaces[#result.ifaces+1] = {
        name  = iface,
        label = iface_labels[iface] or iface,
      }
      result.data[iface] = { daily = daily, monthly = monthly }
    end
  end

  -- Cache the full result (interface list + labels + usage) for REFRESH.VNSTAT seconds
  if #result.ifaces > 0 then
    write_ts(ts_f, now)
    save_cache(dat_f, result)
  end
  return result
end

-- --------------------
-- ⑧ Public IP / ISP / ASN  (12 h cache)
-- Fetches only when online; serves stale cache when offline.
-- wan_iface: L3 device to bind curl to. Empty/nil = no --interface flag (DHCP/default route).
-- sys.exec(curl) — no ubus equivalent
-- --------------------
local function get_network_info(now, inet_status, wan_iface)
  local ts_f, dat_f = "/tmp/dashboard_network_ts", "/tmp/dashboard_network.json"

  local prev = read_ts(ts_f)
  if prev > 0 and (now - prev) <= REFRESH.NETWORK then
    local c = load_cache(dat_f)
    if c then
      log_hit("network HIT ip=" .. tostring(c.public_ipv4))
      return c
    end
  end

  local ip, isp, asn, ipv6 = "-", "-", "", "-"

  -- Build --interface flag only when we have a live L3 device
  local iface_flag = (wan_iface and wan_iface ~= "") and ("--interface " .. wan_iface .. " ") or ""

  if inet_status == "online" then
    -- Primary: ifconfig.co/json
    local out = sys.exec(
      "curl " .. iface_flag ..
      "-s --fail --max-time 8 https://ifconfig.co/json 2>/dev/null") or ""
    if out ~= "" then
      local obj = has_jsonc and jsonc.parse(out)
      if obj then
        ip  = obj.ip      or "-"
        asn = obj.asn     or ""
        isp = obj.asn_org or "-"
      end
    end

    -- Fallback IPv4: ipify
    if ip == "-" then
      local out2 = sys.exec(
        "curl " .. iface_flag ..
        "-s --fail --max-time 8 'https://api.ipify.org?format=json' 2>/dev/null") or ""
      local obj = out2 ~= "" and has_jsonc and jsonc.parse(out2)
      if obj then ip = obj.ip or "-" end
    end

    -- Fallback ISP/ASN: ipinfo.io
    if isp == "-" or asn == "" then
      local out3 = sys.exec(
        "curl " .. iface_flag ..
        "-s --fail --max-time 8 https://ipinfo.io/json 2>/dev/null") or ""
      local obj = out3 ~= "" and has_jsonc and jsonc.parse(out3)
      if obj and obj.org and obj.org ~= "" then
        asn = obj.org:match("^(%S+)") or ""
        isp = obj.org:match("^%S+%s+(.+)$") or "-"
      end
    end

    -- IPv6
    local out4 = sys.exec(
      "curl -6 " .. iface_flag ..
      "-s --fail --max-time 8 https://api64.ipify.org 2>/dev/null") or ""
    ipv6 = (out4 ~= "") and out4:match("^%S+") or "-"

    -- Only persist when we got a real IP
    if ip ~= "-" then
      local payload = { public_ipv4=ip, public_ipv6=ipv6, isp_name=isp, isp_asn=asn }
      write_ts(ts_f, now)
      save_cache(dat_f, payload)
      log_hit("network FETCH ip=" .. ip .. " isp=" .. isp .. " asn=" .. asn)
    end
  else
    -- Offline: return stale without touching the timestamp
    local stale = load_cache(dat_f)
    if stale then
      ip   = stale.public_ipv4 or "-"
      ipv6 = stale.public_ipv6 or "-"
      isp  = stale.isp_name    or "-"
      asn  = stale.isp_asn     or ""
    end
  end

  return { public_ipv4=ip, public_ipv6=ipv6, isp_name=isp, isp_asn=asn }
end

-- --------------------
-- Routes
-- --------------------
function action_force_refresh()
  clear_all_ts()
  local http = require "luci.http"
  http.prepare_content("application/json")
  http.write_json({ ok=true, message="timestamps cleared" })
end

function action_adblock_refresh()
  local http = require "luci.http"
  -- Only delete the adblock cache files — leave all other caches intact
  os.remove("/tmp/dashboard_adblock_ts")
  os.remove("/tmp/dashboard_adblock.json")
  http.prepare_content("application/json")
  http.write_json({ ok=true })
end

function index()
  entry({"admin", "realtime"}, firstchild(), _("Realtime Monitor"), 1).index = true
  entry({"admin", "realtime", "dashboard"}, template("admin/dashboard"), _("Dashboard"), 1).index = false
  entry({"admin", "status", "dashboard"}, call("action_dashboard")).dependent = false
  entry({"admin", "status", "dashboard", "force"}, call("action_force_refresh")).dependent = false
  entry({"admin", "status", "dashboard", "adblock_refresh"}, call("action_adblock_refresh")).dependent = false
end

-- --------------------
-- Main action
-- One ubus connection shared across all ubus-capable functions.
-- --------------------
function action_dashboard()
  local http = require "luci.http"

  local ok, err = pcall(function()
  local now  = os_time()

  -- Open a single ubus connection for this request
  local conn = ubus.connect()

  -- ① Uptimes + WAN status/type + device names  (10 s cache, uses conn)
  --   Must run FIRST so wan_l3_device is available for ping and curl.
  local uptimes = get_uptimes(now, conn)

  -- The L3 device to bind ping/curl to.
  -- For PPPoE: "pppoe-wan" (or whatever the PPP logical iface is)
  -- For DHCP:  the physical/logical WAN iface (e.g. "eth1", "wan")
  -- Empty string = no binding (falls back to default route)
  local wan_l3_device   = uptimes.wan_l3_device
  local wan_phys_device = uptimes.wan_phys_device

  -- ② Ping: determines inet_status  (2 s cache)
  local ping        = get_ping_status(now, wan_l3_device)
  local inet_status = ping.status

  -- ③ IP / ISP / ASN  (12 h cache, curl, only when online)
  local netinfo = get_network_info(now, inet_status, wan_l3_device)

  -- ④ Wi-Fi clients + LAN + RAM  (20 s cache, uses conn for hostapd)
  local clients_mem = get_clients_and_mem(now, conn)

  -- ⑤ Temperatures  (5 s cache, sysfs only)
  local temps = get_temps(now)

  -- ⑥ CPU stats  (live, /proc/stat)
  local cpu_stats = get_cpu_and_cores(now)

  -- ⑦ vnstat  (300 s unified cache, CLI)
  -- Binary detection is cached for 1 h — no shell fork on every poll tick.
  local vnstat_installed = get_vnstat_installed(now)
  local vn_all = vnstat_installed
    and get_all_vnstat_data(now, wan_phys_device, conn)
    or  { ifaces = {}, data = {} }

  -- ⑧ Network counters  (live, ubus network.device on physical WAN port)
  -- wan_phys_device (e.g. eth1) bypasses PPPoE software offloading blind spot.
  -- wan_l3_device   (e.g. pppoe-wan or eth1) reported as iface name for display only.
  local rx, tx = get_net(conn, wan_phys_device)

  -- Done with ubus
  if conn then conn:close() end

  -- Assemble output
  local out = {
    cpu       = cpu_stats.agg,
    cpu_temp  = temps.cpu,
    wifi_temp = temps.wifi,
    mem       = clients_mem.mem,
    wifi      = clients_mem.wifi,
    lan       = clients_mem.lan,
    vnstat    = { installed = vnstat_installed, ifaces = vn_all.ifaces, data = vn_all.data },
    adblock   = get_adblock(now),
    network   = {
      status               = inet_status,
      ping_rtt_ms          = ping.rtt_ms,
      public_ipv4          = netinfo.public_ipv4,
      public_ipv6          = netinfo.public_ipv6,
      isp_name             = netinfo.isp_name,
      isp_asn              = netinfo.isp_asn,
      -- Generic WAN fields (work for PPPoE, DHCP, static, …)
      wan_type             = uptimes.wan_type,
      wan_status           = uptimes.wan_status,
      wan_uptime_seconds   = uptimes.wan_uptime,
    },
    system_uptime_seconds = uptimes.system,
    net       = { iface = wan_l3_device, rx = rx, tx = tx },
    tmpfs_ok  = TMPFS_OK,
  }

  -- Only include per-core data when multi-core; keeps JSON clean for single-core devices
  if cpu_stats.cores then out.cpu_cores = cpu_stats.cores end

  -- Downtime counter: timestamp written by watchdog to /tmp/wan_down_time (any proto)
  local wan_down_start = read_downtime_start("/tmp/wan_down_time")
  if wan_down_start and out.network.wan_status == "down" then
    out.network.wan_downtime_seconds = now - wan_down_start
  end

  local inet_start = read_downtime_start("/tmp/internet_down_time")
  if inet_start and out.network.status == "offline" then
    out.network.downtime_seconds = now - inet_start
  end

  http.prepare_content("application/json")
  http.write_json(out)
  end) -- end pcall

  if not ok then
    http.prepare_content("application/json")
    http.write_json({ error = tostring(err) })
  end
end