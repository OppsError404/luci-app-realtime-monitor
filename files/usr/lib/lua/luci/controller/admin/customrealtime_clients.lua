-- /usr/lib/lua/luci/controller/admin/customrealtime_clients.lua
module("luci.controller.admin.customrealtime_clients", package.seeall)

local jsonc = require "luci.jsonc"
local http  = require "luci.http"
local ubus  = require "ubus"
local nixio = require "nixio"

-- All persistent artifacts kept in /tmp to avoid flash wear
local prev_file       = "/tmp/realtime_clients_prev.json"
local prev_ts_file    = "/tmp/realtime_clients_prev_ts"
local dhcp_leases     = "/tmp/dhcp.leases"
local bridge_name     = "br-lan"

-- ── Identity registry ─────────────────────────────────────────────────────────
-- Stores everything that CANNOT change while a client is connected:
--   wifi_clients : mac → { ip, hostname, iface, ap_key }
--   lan_clients  : mac → { ip, hostname, port }
--   aps          : list of { key, iface, essid, freq, band }
--   lan_ports    : list of port names
--   lan_alias    : port → display label
-- Written only when a new client appears (reg_dirty = true).
-- An entry is removed immediately when its MAC is absent from a poll.
-- Metrics (rx/tx, signal) are NEVER stored here — they come from sysfs/ubus live.
local registry_file = "/tmp/realtime_clients_registry.json"

-- ── Shared cross-process response cache ──────────────────────────────────────
-- One process generates per cycle; all concurrent tabs read this file.
-- A nixio flock ensures only one process regenerates at a time.
local resp_cache_file = "/tmp/realtime_clients_response.json"
local resp_cache_ts   = "/tmp/realtime_clients_response.ts"
local resp_lock_file  = "/tmp/realtime_clients_response.lock"

-- ── Bridge client liveness ───────────────────────────────────────────────────
-- Per-MAC state: { last_active_ts, last_ping_ts, ping_fail_count, ip }
--   last_active_ts  : last poll where nft delta >= BRIDGE_ACTIVE_BYTES
--   last_ping_ts    : last time a background ping was fired
--   ping_fail_count : consecutive failed bg pings (0 = ok, never hide below 2)
--
-- Per poll, per client:
--   delta >= BRIDGE_ACTIVE_BYTES → show immediately, no ping needed
--   idle < BRIDGE_IDLE_SECS      → show, no ping needed
--   idle >= BRIDGE_IDLE_SECS     → fire bg ping if due; hide only if fail_count >= 2
--                                   fire new background ping if interval elapsed
--
-- Background ping writes result atomically to /tmp/bpr_<hex>.txt
-- Lua reads it next poll — zero blocking on the request thread.
local bridge_state_file   = "/tmp/realtime_clients_bridge_state.json"
local BRIDGE_ACTIVE_BYTES = 1024  -- bytes per poll to be considered active
local BRIDGE_IDLE_SECS    = 30    -- idle seconds before ping-based liveness
local BRIDGE_PING_SECS    = 15    -- how often to re-fire the background ping

-- start with sensible defaults; overwritten from registry at runtime
local lan_ports = { "lan1", "lan2", "lan3", "lan4" }
local lan_alias = { lan1 = "LAN 1", lan2 = "LAN 2", lan3 = "LAN 3", lan4 = "LAN 4" }

-- Resolve binaries once at module load
local function exists(p) local f = io.open(p, "r") if f then f:close(); return true end; return false end
local function resolve_bins()
  local b = {}
  b.bridge = exists("/usr/sbin/bridge") and "/usr/sbin/bridge"
          or (exists("/sbin/bridge")    and "/sbin/bridge"
          or (exists("/bin/bridge")     and "/bin/bridge" or "bridge"))
  b.ip = exists("/sbin/ip") and "/sbin/ip" or (exists("/bin/ip") and "/bin/ip" or "ip")
  b.ping = exists("/bin/ping") and "/bin/ping"
        or (exists("/usr/bin/ping") and "/usr/bin/ping" or "ping")
  return b
end
local bins = resolve_bins()

-- Localized stdlib
local fmt       = string.format
local tonumber  = tonumber
local ipairs    = ipairs
local pairs     = pairs
local os_time   = os.time
local os_clock  = os.clock
local io_open   = io.open
local os_rename = os.rename

-- ── nftables bridge MAC rate ──────────────────────────────────────────────────
local NFT_TABLE     = "bridge"
local NFT_TNAME     = "brcct"
local CHAIN_FWD     = "brcct_fwd"
local CHAIN_IN      = "brcct_in"
local CHAIN_OUT     = "brcct_out"
local nft_prev_file = "/tmp/realtime_clients_nft_prev.json"

-- ── IO helpers ────────────────────────────────────────────────────────────────
local function readfile(path)
  local f = io_open(path, "r")
  if not f then return nil end
  local d = f:read("*a"); f:close(); return d
end

local function atomic_write_tmp(path, data)
  local tmp = path .. ".tmp"
  local f = io_open(tmp, "w+")
  if not f then return false end
  f:write(data); f:close()
  return os_rename(tmp, path)
end

local function run(cmd)
  local fp = io.popen(cmd, "r")
  if not fp then return "" end
  local out = fp:read("*a") or ""; fp:close(); return out
end

local pat_fdb = "^(%x%x:%x%x:%x%x:%x%x:%x%x:%x%x)%s+dev%s+(%S+)%s+.*master%s+(%S+)"
local function split_lines(s)
  local t = {}
  for line in (s or ""):gmatch("[^\r\n]+") do t[#t+1] = line end
  return t
end

-- ── Known router/AP OUI prefixes (first 3 octets, lowercase) ─────────────────
-- Used as a soft hint only — not conclusive on its own.
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
  ["00:09:5b"]=true,["00:14:6c"]=true,["00:1b:2f"]=true,["00:22:3f"]=true,
  ["20:4e:7f"]=true,["28:c6:8e"]=true,["2c:b0:5d"]=true,["30:46:9a"]=true,
  ["3c:37:86"]=true,["a0:04:60"]=true,["c0:ff:d4"]=true,["9c:3d:cf"]=true,
  -- D-Link
  ["00:05:5d"]=true,["00:0d:88"]=true,["00:0f:3d"]=true,["00:11:95"]=true,
  ["00:17:9a"]=true,["00:1c:f0"]=true,["00:1e:58"]=true,["00:21:91"]=true,
  ["00:22:b0"]=true,["1c:7e:e5"]=true,["28:10:7b"]=true,["b0:c5:54"]=true,
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
  -- Huawei
  ["00:1e:10"]=true,["04:02:1f"]=true,["04:75:03"]=true,["10:1b:54"]=true,
  ["14:a5:1a"]=true,["20:a6:80"]=true,["24:69:a5"]=true,["2c:ab:00"]=true,
  ["30:d1:7e"]=true,["54:89:98"]=true,["70:72:cf"]=true,["9c:74:1a"]=true,
  -- Xiaomi / Redmi
  ["00:9e:c8"]=true,["0c:1a:f0"]=true,["18:59:36"]=true,["28:6c:07"]=true,
  ["34:ce:00"]=true,["50:64:2b"]=true,["58:44:98"]=true,["64:09:80"]=true,
  ["78:11:dc"]=true,["8c:be:be"]=true,["ac:c1:ee"]=true,["d4:97:0b"]=true,
  ["f8:a4:5f"]=true,["fc:64:ba"]=true,
  -- GL.iNet
  ["94:83:c4"]=true,["e4:95:6e"]=true,["20:79:18"]=true,
  -- Linksys / Cisco home
  ["00:0c:e5"]=true,["00:0f:66"]=true,["00:13:10"]=true,["00:14:bf"]=true,
  ["00:16:b6"]=true,["00:18:39"]=true,["00:1a:70"]=true,["00:1c:10"]=true,
  ["00:1d:7e"]=true,["00:21:29"]=true,["00:22:6b"]=true,["00:25:45"]=true,
  ["20:aa:4b"]=true,["48:f8:b3"]=true,["c0:c1:c0"]=true,
  -- ZTE
  ["00:19:c6"]=true,["0c:72:2c"]=true,["1c:1b:0d"]=true,["28:ed:e0"]=true,
  ["34:79:16"]=true,["3c:9a:b3"]=true,["40:f0:1b"]=true,["60:de:44"]=true,
  ["7c:b7:33"]=true,["8c:a6:df"]=true,["bc:76:70"]=true,["c8:50:e9"]=true,
  -- Tenda
  ["00:0a:eb"]=true,["c8:3a:35"]=true,["d4:61:fe"]=true,["e4:d3:32"]=true,
  ["f4:92:bf"]=true,
}


-- ══════════════════════════════════════════════════════════════════════════════
-- nftables helpers — bridge family, MAC counters, 3 hooks
--   fwd_tx / in_tx  : bytes leaving the client  (upload)
--   fwd_rx / out_rx : bytes arriving at client   (download)
-- ══════════════════════════════════════════════════════════════════════════════
local function run_e(cmd)
  local fp = io.popen(cmd .. " 2>&1", "r"); if not fp then return "" end
  local o = fp:read("*a") or ""; fp:close(); return o
end

local function nft_file(lines)
  local path = "/tmp/.brcct_crt.nft"
  local f = io_open(path, "w"); if not f then return end
  for _, l in ipairs(lines) do f:write(l .. "\n") end
  f:close(); run_e("nft -f " .. path)
end

local function nft_ensure_setup()
  run_e(fmt("nft add table %s %s", NFT_TABLE, NFT_TNAME))
  nft_file({
    fmt("add chain %s %s %s { type filter hook forward priority -200; policy accept; }", NFT_TABLE, NFT_TNAME, CHAIN_FWD),
    fmt("add chain %s %s %s { type filter hook input   priority -200; policy accept; }", NFT_TABLE, NFT_TNAME, CHAIN_IN),
    fmt("add chain %s %s %s { type filter hook output  priority -200; policy accept; }", NFT_TABLE, NFT_TNAME, CHAIN_OUT),
  })
end

local function nft_list_all()
  local out = ""
  for _, ch in ipairs({ CHAIN_FWD, CHAIN_IN, CHAIN_OUT }) do
    out = out .. run_e(fmt("nft list chain %s %s %s", NFT_TABLE, NFT_TNAME, ch))
  end
  return out
end

local function nft_ensure_mac(mac, listing)
  local tag   = mac:gsub(":", "_")
  local lines = {}
  local rules = {
    { chain=CHAIN_FWD, field="ether saddr", comment="fwd_tx:"..tag },
    { chain=CHAIN_FWD, field="ether daddr", comment="fwd_rx:"..tag },
    { chain=CHAIN_IN,  field="ether saddr", comment="in_tx:"..tag  },
    { chain=CHAIN_OUT, field="ether daddr", comment="out_rx:"..tag },
  }
  for _, r in ipairs(rules) do
    if not listing:find(r.comment, 1, true) then
      lines[#lines+1] = fmt('add rule %s %s %s %s %s counter comment "%s"',
                            NFT_TABLE, NFT_TNAME, r.chain, r.field, mac, r.comment)
    end
  end
  if #lines > 0 then nft_file(lines) end
end

local function nft_remove_stale(all_macs)
  for _, ch in ipairs({ CHAIN_FWD, CHAIN_IN, CHAIN_OUT }) do
    local out = run_e(fmt("nft -a list chain %s %s %s", NFT_TABLE, NFT_TNAME, ch))
    for _, line in ipairs(split_lines(out)) do
      local comment = line:match('comment "([^"]+)"')
      if comment then
        local _, tag = comment:match("^(%w+_[tr][x]):(.+)$")
        if tag then
          local mac = tag:gsub("_", ":")
          if not all_macs[mac] then
            local h = line:match("# handle (%d+)")
            if h then run_e(fmt("nft delete rule %s %s %s handle %s", NFT_TABLE, NFT_TNAME, ch, h)) end
          end
        end
      end
    end
  end
end

local function nft_read_counters()
  local counters = {}
  for _, ch in ipairs({ CHAIN_FWD, CHAIN_IN, CHAIN_OUT }) do
    local out = run_e(fmt("nft list chain %s %s %s", NFT_TABLE, NFT_TNAME, ch))
    for _, line in ipairs(split_lines(out)) do
      local bytes   = line:match("bytes (%d+)")
      local comment = line:match('comment "([^"]+)"')
      if bytes and comment then
        local dir, tag = comment:match("^(%w+_[tr][x]):(.+)$")
        if dir and tag then
          local mac = tag:gsub("_", ":")
          if not counters[mac] then counters[mac] = { tx=0, rx=0 } end
          local b = tonumber(bytes) or 0
          if dir:find("tx") then counters[mac].tx = counters[mac].tx + b
                             else counters[mac].rx = counters[mac].rx + b end
        end
      end
    end
  end
  return counters
end

local function load_nft_prev()
  local raw = readfile(nft_prev_file)
  if not raw or raw == "" then return { _ts=0, macs={} } end
  local ok, t = pcall(jsonc.parse, raw)
  if not ok or type(t) ~= "table" then return { _ts=0, macs={} } end
  t.macs = t.macs or {}; t._ts = t._ts or 0; return t
end

local function save_nft_prev(tbl)
  local ok, s = pcall(jsonc.stringify, tbl)
  if ok then atomic_write_tmp(nft_prev_file, s) end
end

-- ── Bridge ping-gate helpers ─────────────────────────────────────────────────


-- ── Bridge state helpers ─────────────────────────────────────────────────────
local function load_bridge_state()
  local raw = readfile(bridge_state_file)
  if not raw or raw == "" then return {} end
  local ok, t = pcall(jsonc.parse, raw)
  return (ok and type(t) == "table") and t or {}
end

local function save_bridge_state(tbl)
  local ok, s = pcall(jsonc.stringify, tbl)
  if ok then atomic_write_tmp(bridge_state_file, s) end
end

-- Background ping helpers — never block the request thread.
-- A result file /tmp/bpr_HEXIP.txt is written atomically by a detached shell job.
-- Contents: "1" = alive, "0" = dead. Missing = job still running or not yet fired.

local function bpr_file(ip)
  local a,b,c,d = ip:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")
  if not a then return nil end
  return fmt("/tmp/bpr_%02x%02x%02x%02x.txt",
    tonumber(a), tonumber(b), tonumber(c), tonumber(d))
end

-- Fire a background ping; result appears in bpr_file on next poll.
local function bg_ping_fire(ip)
  local rf = bpr_file(ip)
  if not rf then return end
  -- Shell: run ping, write "1" or "0" to .tmp atomically, then rename.
  -- The outer & detaches fully so Lua returns instantly.
  os.execute(fmt(
    "sh -c 'if %s -c 5 -W 1 -q %s >/dev/null 2>&1; then echo 1 > %s.tmp; else echo 0 > %s.tmp; fi; mv %s.tmp %s' >/dev/null 2>&1 &",
    bins.ping, ip, rf, rf, rf, rf))
end

-- Read last completed ping result. Returns true=alive, false=dead, nil=unknown.
local function bg_ping_read(ip)
  local rf = bpr_file(ip)
  if not rf then return nil end
  local raw = readfile(rf)
  if not raw or raw == "" then return nil end
  return raw:find("1") and true or false
end

-- Delete result file (on purge)
local function bg_ping_clear(ip)
  local rf = bpr_file(ip)
  if rf then os.execute("rm -f " .. rf .. " " .. rf .. ".tmp 2>/dev/null") end
end

-- ── IPv6 link-local detection ─────────────────────────────────────────────────
-- RFC 4291 §2.5.6: link-local unicast prefix = fe80::/10.
-- The prefix covers fe80–febf (top 10 bits = 1111111010).
-- Matching the nibble range [89ab] after "fe" is correct and router-agnostic.
local function is_link_local_ipv6(ip)
  if not ip or not ip:find(":", 1, true) then return false end
  return ip:sub(1, 4):lower():match("^fe[89ab]") ~= nil
end

-- ── ARP table parser ──────────────────────────────────────────────────────────
-- Returns { ip→mac, mac→ip } from /proc/net/arp
local function get_arp_table()
  -- Use 'ip neigh show' for state-aware neighbour entries.
  -- Exclude states that indicate the client is gone: FAILED, INCOMPLETE, PROBE.
  -- Accept REACHABLE, STALE, DELAY and anything unknown (permissive fallback).
  -- Format per line: <ip> dev <iface> lladdr <mac> [<STATE>]
  local t = {}
  local out = run(bins.ip .. " neigh show 2>/dev/null")
  for _, line in ipairs(split_lines(out)) do
    local ip  = line:match("^(%S+)%s+")
    local mac = line:match("lladdr%s+(%x%x:%x%x:%x%x:%x%x:%x%x:%x%x)")
    if ip and mac then
      -- Skip link-local IPv6 (fe80::/10): not routable, causes false sub_client
      -- entries when the same MAC has both a real IPv4 and a fe80:: NDP entry.
      if not is_link_local_ipv6(ip) then
        local state = line:match("%s(%u+)%s*$") or ""
        if state ~= "FAILED" and state ~= "INCOMPLETE" and state ~= "PROBE" then
          mac = mac:lower()
          t[ip]  = mac
          t[mac] = ip
        end
      end
    end
  end
  return t
end

-- ── Gateway IP set ────────────────────────────────────────────────────────────
-- Returns set of IPs that appear as 'via <ip>' in the routing table.
-- A LAN client whose IP is in this set is forwarding traffic → NAT router.
local function get_gateway_ips()
  local gws = {}
  local out = run(bins.ip .. " route show 2>/dev/null")
  for line in out:gmatch("[^\r\n]+") do
    local gw = line:match("%svia%s+(%S+)")
    if gw then gws[gw] = true end
  end
  return gws
end

-- ── Formatting ────────────────────────────────────────────────────────────────
local function fmt_bytes(b)
  b = b or 0
  if b >= 1073741824 then return fmt("%.2f GB", b/1073741824) end
  if b >= 1048576    then return fmt("%.2f MB", b/1048576)    end
  if b >= 1024       then return fmt("%.1f KB", b/1024)       end
  return fmt("%d B", b)
end

-- ── Shared response cache helpers ────────────────────────────────────────────
local function resp_cache_age()
  local ts = tonumber((readfile(resp_cache_ts) or ""):match("^%s*(%d+)")) or 0
  return os_time() - ts
end

local function resp_cache_write(json_str)
  if atomic_write_tmp(resp_cache_file, json_str) then
    atomic_write_tmp(resp_cache_ts, tostring(os_time()))
  end
end

local function lock_acquire()
  local f = nixio.open(resp_lock_file, "w+", "0600")
  if not f then return nil end
  if f:lock("tlock") then return f end
  f:close(); return nil
end

local function lock_release(f)
  if f then f:lock("ulock"); f:close() end
end

-- ── Identity registry ─────────────────────────────────────────────────────────
local function load_registry()
  local raw = readfile(registry_file)
  if not raw or raw == "" then
    return { wifi_clients={}, lan_clients={}, aps={}, lan_ports={}, lan_alias={} }
  end
  local ok, t = pcall(jsonc.parse, raw)
  if not ok or type(t) ~= "table" then
    return { wifi_clients={}, lan_clients={}, aps={}, lan_ports={}, lan_alias={} }
  end
  t.wifi_clients = t.wifi_clients or {}
  t.lan_clients  = t.lan_clients  or {}
  t.aps          = t.aps          or {}
  t.lan_ports    = t.lan_ports    or {}
  t.lan_alias    = t.lan_alias    or {}
  return t
end

local function save_registry(reg)
  local ok, s = pcall(jsonc.stringify, reg)
  if ok then atomic_write_tmp(registry_file, s) end
end

-- ── DHCP lease parser — called ONLY for brand-new MACs ───────────────────────
-- No TTL, no in-memory cache across requests. The result is stored in the
-- registry on the same write, so this will not run again for the same MAC
-- for the entire duration of that client's connection.
-- _leases_cache is reset to nil at the start of each generation so that
-- leases are re-read at most once per generation (handles multiple new MACs
-- appearing in the same poll without re-reading the file for each one).
local _leases_cache = nil
local function ensure_leases()
  if _leases_cache then return _leases_cache end
  local mac2name, mac2ip = {}, {}
  local data = readfile(dhcp_leases) or ""
  for line in data:gmatch("[^\r\n]+") do
    local parts = {}
    for p in line:gmatch("%S+") do parts[#parts+1] = p end
    if #parts >= 4 then
      local mac  = parts[2]
      local ip   = parts[3]
      local host = parts[4]
      if mac and ip then mac2ip[mac:lower()] = ip end
      if mac and host and host ~= "*" then mac2name[mac:lower()] = host end
    end
  end
  _leases_cache = { mac2name=mac2name, mac2ip=mac2ip }
  return _leases_cache
end

-- ── prev snapshot helpers ─────────────────────────────────────────────────────
local function load_prev()
  local content = readfile(prev_file)
  if not content or content == "" then return {} end
  local ok, p = pcall(jsonc.parse, content)
  return (ok and type(p) == "table") and p or {}
end

local function guarded_atomic_write_prev(prev_tbl)
  local ok, s = pcall(jsonc.stringify, prev_tbl)
  if not ok then return false end
  local now = os_time()
  local last_ts = tonumber((readfile(prev_ts_file) or ""):match("^%s*(%d+)")) or 0
  if (now - last_ts) < 1 then
    if (now % 3) ~= 0 then return false end
  end
  local existing = readfile(prev_file) or ""
  if existing == s then
    atomic_write_tmp(prev_ts_file, tostring(now)); return true
  end
  local wrote = atomic_write_tmp(prev_file, s)
  if wrote then atomic_write_tmp(prev_ts_file, tostring(now)) end
  return wrote
end

-- ── Per-request sysfs cache ───────────────────────────────────────────────────
local function SysfsCache()
  local C = {}
  return { read_once = function(path)
    if C[path] ~= nil then return C[path] end
    local v = readfile(path)
    if v and v:sub(-1) == "\n" then v = v:sub(1,-2) end
    C[path] = v; return v
  end}
end

-- ── AP discovery — only on first run (registry.aps empty) ────────────────────
-- Once stored in the registry this is never called again unless the registry
-- is wiped or a new AP appears mid-session (handled by re-running if aps={}).
local function discover_aps(conn)
  local aps = {}
  local f = io.popen("ubus list | grep '^hostapd\\.'")
  if not f then return aps end
  for line in f:lines() do
    local iface = line:match("^hostapd%.(.+)$")
    if iface and iface ~= "" then
      local status = conn:call(line, "get_status", {})
      if status and status.ssid and status.freq then
        local fmhz = tonumber(status.freq) or 0
        local band = "unknown"
        if     fmhz >= 2400 and fmhz < 2500 then band = "2.4_Ghz"
        elseif fmhz >= 5000 and fmhz < 6000 then band = "5_Ghz"
        elseif fmhz >= 6000               then band = "6_Ghz" end
        aps[#aps+1] = {
          key=fmt("(%s, %s)", status.ssid, band), iface=iface,
          essid=status.ssid, freq=fmhz, band=band
        }
      end
    end
  end
  f:close()
  return aps
end

-- ── LAN port discovery — only on first run (registry.lan_ports empty) ────────
local function discover_lan_ports(br)
  local ports, aliases = {}, {}
  local ls = run("ls -1 /sys/class/net 2>/dev/null")
  if ls ~= "" then
    for dev in ls:gmatch("[^\r\n]+") do
      if dev ~= br then
        local master = (run("readlink -f /sys/class/net/"..dev.."/master 2>/dev/null") or ""):match("([^/]+)$")
        if master == br then
          ports[#ports+1] = dev
          aliases[dev] = dev:match("^lan(%d+)$") and ("LAN "..dev:match("^lan(%d+)$")) or dev
        end
      end
    end
  end
  if #ports == 0 then
    ports   = {"lan1","lan2","lan3","lan4"}
    aliases = {lan1="LAN 1",lan2="LAN 2",lan3="LAN 3",lan4="LAN 4"}
  end
  local seen, out_p, out_a = {}, {}, {}
  for _, p in ipairs(ports) do
    if not seen[p] then seen[p]=true; out_p[#out_p+1]=p; out_a[p]=aliases[p] end
  end
  return out_p, out_a
end

-- ── Wired FDB: always fresh per poll (needed for connect/disconnect detection) ─
-- Returns:
--   map           : mac → port  (first seen mac wins, filters permanent/self)
--   port_mac_count: port → number of distinct dynamic MACs on that port
--                   > 1 definitively indicates a bridge, AP, or switch
local function get_fdb_port_map(br)
  local fdb_raw = run(fmt("%s fdb show br %s 2>/dev/null", bins.bridge, br))
  local map            = {}   -- mac → port
  local port_macs      = {}   -- port → set of macs
  local port_macs_list = {}   -- port → ordered list of macs
  for _, line in ipairs(split_lines(fdb_raw)) do
    if not line:find("permanent") and not line:find("self") then
      local mac, dev, master = line:match(pat_fdb)
      if mac and dev and master == br and dev:match("^lan%d+$") then
        mac = mac:lower()
        if not map[mac] then map[mac] = dev end
        if not port_macs[dev] then port_macs[dev] = {}; port_macs_list[dev] = {} end
        if not port_macs[dev][mac] then
          port_macs[dev][mac] = true
          port_macs_list[dev][#port_macs_list[dev]+1] = mac
        end
      end
    end
  end
  local port_mac_count = {}
  for port, macs in pairs(port_macs) do
    local n = 0
    for _ in pairs(macs) do n = n + 1 end
    port_mac_count[port] = n
  end
  return map, port_mac_count, port_macs_list
end

-- ── Wireless metrics: always fresh per poll ───────────────────────────────────
-- Returns mac → { signal_pct, raw_rx, raw_tx } for clients with signal > 0.
-- Identity (ip/hostname) is NOT looked up here.
local function get_hostapd_metrics(iface, conn)
  local ok, res = pcall(function() return conn:call("hostapd."..iface, "get_clients", {}) end)
  if not ok or not res or not res.clients then return {} end
  local metrics = {}
  for mac, c in pairs(res.clients) do
    local sig = tonumber(c.signal) or -100
    local pct = math.floor(((sig + 100) / 60) * 100 + 0.5)
    if pct < 0 then pct = 0 elseif pct > 100 then pct = 100 end
    if pct > 0 then
      metrics[mac:lower()] = {
        signal_pct = pct,
        raw_rx = tonumber(c.bytes and c.bytes.tx or 0) or 0,  -- intentional swap preserved
        raw_tx = tonumber(c.bytes and c.bytes.rx or 0) or 0
      }
    end
  end
  return metrics
end

-- ── LuCI registration ─────────────────────────────────────────────────────────
function index()
  entry({"admin","realtime","client"}, template("admin/customrealtime_clients"), _("Client Monitor"), 50).index = false
  entry({"admin","customrealtime_clients","data"}, call("get_data"), nil).leaf = true
end

-- ─────────────────────────────────────────────────────────────────────────────
function get_data()
  http.prepare_content("application/json")

  local params  = http.formvalue() or {}
  local refresh = tonumber(params.refresh) or 3
  if refresh ~= 1 and refresh ~= 2 and refresh ~= 3 and refresh ~= 5 and refresh ~= 10 then refresh = 2 end
  local force   = params.force == "1"   -- force=1 bypasses cache entirely

  -- ── Shared response cache: fast path ─────────────────────────────────────
  local cache_max_age = math.max(0.5, refresh - 1)
  local age = resp_cache_age()
  if not force and age <= cache_max_age then
    local cached = readfile(resp_cache_file)
    if cached and cached ~= "" then http.write(cached); return end
  end

  -- ── Concurrency gate ─────────────────────────────────────────────────────
  local lock_f = lock_acquire()
  if not lock_f then
    nixio.nanosleep(0, 200000000)
    local cached = readfile(resp_cache_file)
    http.write((cached and cached ~= "") and cached or "{}"); return
  end

  -- Double-check after winning lock (skip for force refresh)
  age = resp_cache_age()
  if not force and age <= cache_max_age then
    local cached = readfile(resp_cache_file)
    if cached and cached ~= "" then lock_release(lock_f); http.write(cached); return end
  end

  -- ── Full generation ───────────────────────────────────────────────────────
  local conn = ubus.connect()
  if not conn then lock_release(lock_f); http.write("{}"); return end

  local now       = os_time()
  local t0        = os_clock()
  local sc        = SysfsCache()
  local prev      = load_prev()
  local reg       = load_registry()
  local reg_dirty = false
  _leases_cache   = nil  -- reset one-shot lease cache for this generation

  -- ── AP list: discover once, then serve from registry forever ─────────────
  if #reg.aps == 0 then
    reg.aps   = discover_aps(conn)
    reg_dirty = true
  end

  -- ── LAN port list: discover once, then serve from registry forever ────────
  if #reg.lan_ports == 0 then
    reg.lan_ports, reg.lan_alias = discover_lan_ports(bridge_name)
    reg_dirty = true
  end
  lan_ports = reg.lan_ports
  lan_alias = reg.lan_alias

  local clients          = { lan = {} }
  local initial_prev_ts  = tonumber((readfile(prev_ts_file) or ""):match("^%s*(%d+)")) or 0

  -- ── Device-type detection data (collected once per generation) ─────────────
  -- Declared here (before the WiFi loop) so the ARP table is available for
  -- static-IP resolution in both the wireless and wired client sections.
  local arp_table   = get_arp_table()
  local gateway_ips = get_gateway_ips()

  -- ══ Wireless clients ══════════════════════════════════════════════════════
  for _, ap in ipairs(reg.aps) do
    -- Skip APs whose interface is not currently up (disabled / brought down).
    -- This prevents a 'no clients' card appearing for a disabled AP.
    local ap_oper = sc.read_once("/sys/class/net/"..ap.iface.."/operstate")
    if not ap_oper or not ap_oper:match("^up") then
      -- Evict any stale registry entries belonging to this AP
      for mac, entry in pairs(reg.wifi_clients) do
        if entry.iface == ap.iface then
          reg.wifi_clients[mac] = nil
          reg_dirty = true
        end
      end
    else
      local ap_key = ap.key
      clients[ap_key] = { clients={}, totals={ raw_rx=0, raw_tx=0 } }

      -- Fetch live metrics (signal + bytes) — this is ALL that changes per poll
      local metrics   = get_hostapd_metrics(ap.iface, conn)
      local seen_wifi = {}

      for mac, m in pairs(metrics) do
        seen_wifi[mac] = true

        -- Identity: read from registry; resolve from leases ONLY if this is a new MAC
        local id = reg.wifi_clients[mac]
        if not id then
          local leases = ensure_leases()
          id = {
            ip       = leases.mac2ip[mac]   or "",
            hostname = leases.mac2name[mac] or "",
            iface    = ap.iface,
            ap_key   = ap_key
          }
          reg.wifi_clients[mac] = id
          reg_dirty = true
        end

        -- Static-IP clients never appear in dhcp.leases so id.ip may be "".
        -- On every poll where ip is still empty, try the live ARP/neighbour
        -- table first (catches static IPs immediately), then fall back to
        -- DHCP leases as a secondary check (handles the race where leases
        -- lags a freshly-connected dynamic client).
        if id.ip == "" then
          local arp_ip = arp_table[mac]
          if arp_ip and not is_link_local_ipv6(arp_ip) then
            id.ip     = arp_ip
            reg_dirty = true
          else
            local leases2  = ensure_leases()
            local lease_ip = leases2.mac2ip[mac]
            if lease_ip and lease_ip ~= "" then
              id.ip     = lease_ip
              reg_dirty = true
            end
          end
        end

        local bucket = clients[ap_key].clients
        bucket[#bucket+1] = {
          mac        = mac,
          ip         = id.ip,
          hostname   = id.hostname,
          rx_str     = fmt_bytes(m.raw_rx),
          tx_str     = fmt_bytes(m.raw_tx),
          signal_pct = m.signal_pct,
          raw_rx     = m.raw_rx,
          raw_tx     = m.raw_tx
        }

        local tot = clients[ap_key].totals
        tot.raw_rx = tot.raw_rx + m.raw_rx
        tot.raw_tx = tot.raw_tx + m.raw_tx
        prev[ap.iface.."_"..mac] = { rx=m.raw_tx, tx=m.raw_rx, ts=now }
      end

      local tot = clients[ap_key].totals
      tot.rx_str = fmt_bytes(tot.raw_rx)
      tot.tx_str = fmt_bytes(tot.raw_tx)

      -- Immediately remove clients from registry that left this AP
      for mac, entry in pairs(reg.wifi_clients) do
        if entry.iface == ap.iface and not seen_wifi[mac] then
          reg.wifi_clients[mac] = nil
          reg_dirty = true
        end
      end
    end
  end

  -- ══ Wired clients ═════════════════════════════════════════════════════════
  -- FDB is always read fresh: needed to detect connects and disconnects.
  -- For MACs already in the registry no lease parsing is done.
  local fdb_map, port_mac_count, port_macs_list = get_fdb_port_map(bridge_name)

  -- Register any new wired MACs seen in FDB
  for mac, port in pairs(fdb_map) do
    if not reg.lan_clients[mac] then
      local leases = ensure_leases()
      reg.lan_clients[mac] = {
        ip       = leases.mac2ip[mac]   or "",
        hostname = leases.mac2name[mac] or "",
        port     = port
      }
      reg_dirty = true
    end
  end

  -- Immediately evict wired MACs that are no longer in FDB
  for mac in pairs(reg.lan_clients) do
    if not fdb_map[mac] then
      reg.lan_clients[mac] = nil
      reg_dirty = true
    end
  end

  -- Build port → mac lookup from registry
  local port2mac = {}
  for mac, entry in pairs(reg.lan_clients) do
    port2mac[entry.port] = mac
  end

  -- RFC 4291 §2.5.6: link-local unicast = fe80::/10
  -- The prefix covers fe80 – febf (top 10 bits = 1111111010).
  -- We match the hex range rather than a single hard-coded "fe80:" prefix so
  -- this works on any router regardless of the specific link-local address used.

  -- ── Early nft counter snapshot ────────────────────────────────────────────
  -- Read nftables bridge counters HERE, at the same moment as the sysfs port
  -- counters below, so both sources reflect the same point in time.
  -- Rule management (ensure_setup/mac/remove_stale) still runs in Step 3 after
  -- bridge_mac_set is built; only the counter READ is moved earlier.
  local nft_prev     = load_nft_prev()
  local nft_now      = os_time()
  local nft_dt       = nft_now - (nft_prev._ts or 0)
  local nft_valid    = nft_dt > 0 and nft_dt <= 60
  local new_nft_prev = { _ts=nft_now, macs={} }
  -- Snapshot counters now (before any significant Lua work adds latency).
  -- If the nft table doesn't exist yet cur_nft will simply be empty ({}).
  local cur_nft = nft_read_counters()

  -- Per-port entries: identity from registry, bytes from sysfs (live)
  local lan_sum_rx, lan_sum_tx = 0, 0
  for _, p in ipairs(lan_ports) do
    local oper    = sc.read_once("/sys/class/net/"..p.."/operstate")
    local carrier = sc.read_once("/sys/class/net/"..p.."/carrier")
    if (oper and oper:match("^up")) or (carrier == "1") then
      local rx = tonumber(sc.read_once("/sys/class/net/"..p.."/statistics/rx_bytes")) or 0
      local tx = tonumber(sc.read_once("/sys/class/net/"..p.."/statistics/tx_bytes")) or 0

      local mac      = port2mac[p] or ""
      local id       = mac ~= "" and reg.lan_clients[mac] or nil
      local hostname = (id and id.hostname ~= "") and id.hostname or (lan_alias[p] or p)

      -- ── Device-type hints ──────────────────────────────────────────────────
      -- Collect zero or more non-exclusive hints; frontend decides how to render.
      local hints     = {}
      local mcount    = port_mac_count[p] or (mac ~= "" and 1 or 0)

      -- Hard evidence: multiple MACs on one physical port = bridge / AP / switch
      if mcount > 1 then
        hints[#hints+1] = "bridge"
      end

      -- Soft hint 1: MAC OUI matches a known router/AP vendor
      local ip_addr = id and id.ip or ""
      if mac ~= "" then
        local oui = mac:sub(1,8):lower()
        if ROUTER_OUIS[oui] then
          hints[#hints+1] = "router_oui"
        end
      end

      -- Soft hint 2: device IP appears as a 'via' gateway in the routing table
      -- (means the kernel is actively forwarding packets through it → NAT router)
      if ip_addr ~= "" and gateway_ips[ip_addr] then
        hints[#hints+1] = "gateway"
      end

      -- Always freshen ip_addr from the live ARP/neighbour table for single-MAC
      -- ports (mcount <= 1). This makes static-IP clients visible immediately:
      -- they never appear in dhcp.leases, so the registry may have ip="" or a
      -- stale DHCP address.  ARP is authoritative for what the kernel currently
      -- knows about that MAC, so we use it whenever it has a routable answer.
      if mac ~= "" and mcount <= 1 then
        local arp_ip = arp_table[mac]
        if arp_ip and not is_link_local_ipv6(arp_ip) and arp_ip ~= ip_addr then
          ip_addr = arp_ip
          if id then id.ip = ip_addr end
          reg_dirty = true
        end
      end

      -- Soft hint 3: MAC present but no IP → treat as bridge/AP (device hasn't
      -- obtained a DHCP lease on this segment, typical of APs in bridge mode).
      -- IMPORTANT: also check the live ARP table and DHCP leases before concluding
      -- there is no IP — the registry may simply not have been updated yet for a
      -- client that already has a lease (e.g. a wired PC that just connected).
      if mac ~= "" and ip_addr == "" and mcount <= 1 then
        -- Cross-check ARP table (live kernel neighbour cache)
        local arp_ip = arp_table[mac]
        if arp_ip and not is_link_local_ipv6(arp_ip) then
          -- Device has a routable IP in ARP — update ip_addr so it propagates
          -- to the emitted client record and suppresses the bridge hint.
          ip_addr = arp_ip
          -- Also backfill the registry so the next poll finds it immediately.
          if id then id.ip = ip_addr end
          reg_dirty = true
        else
          -- ARP had nothing; fall back to DHCP leases
          local leases = ensure_leases()
          local lease_ip = leases.mac2ip[mac]
          if lease_ip and lease_ip ~= "" then
            ip_addr = lease_ip
            if id then id.ip = ip_addr end
            reg_dirty = true
          end
        end
        -- Only emit the bridge hint if we still have no IP after all checks
        if ip_addr == "" then
          local already_bridge = false
          for _, h in ipairs(hints) do if h == "bridge" then already_bridge = true; break end end
          if not already_bridge then
            hints[#hints+1] = "bridge"
          end
        end
      end

      -- Soft hint 4: no MAC, no IP, and link speed ≤ 10 Mbps →
      -- Ethernet negotiation failure: cable fault, disabled port, or 10Base-T half-duplex.
      if mac == "" and ip_addr == "" then
        local speed_raw = sc.read_once("/sys/class/net/"..p.."/speed") or ""
        local speed_num = tonumber(speed_raw:match("^%s*(%d+)")) or 0
        if speed_num > 0 and speed_num <= 10 then
          hints[#hints+1] = "eth_issue"
        end
      end

      clients.lan[#clients.lan+1] = {
        port        = p,
        port_label  = lan_alias[p] or p,
        hostname    = hostname,
        mac         = mac,
        ip          = ip_addr,
        rx_str      = fmt_bytes(tx),   -- intentional swap preserved
        tx_str      = fmt_bytes(rx),
        raw_rx      = rx,
        raw_tx      = tx,
        mac_count   = mcount,
        device_hints = hints,
        sub_clients  = (function()
          -- Only populate for confirmed bridge ports (mcount > 1)
          if mcount <= 1 then return nil end
          local leases = ensure_leases()
          local list = {}
          local all_macs = port_macs_list[p] or {}
          for _, m in ipairs(all_macs) do
            -- Prefer ARP-table IPv4; fall back to registry IP.
            local arp_ip = arp_table[m]
            local entry  = reg.lan_clients[m]
            local use_ip =
              (arp_ip and not is_link_local_ipv6(arp_ip))                         and arp_ip
              or (entry and entry.ip ~= "" and not is_link_local_ipv6(entry.ip))  and entry.ip
              or nil
            if use_ip then
              local sub_host = (entry and entry.hostname ~= "") and entry.hostname
                               or (leases.mac2name[m] or "")
              list[#list+1] = { mac=m, ip=use_ip, hostname=sub_host }
            end
          end
          return list
        end)(),
        -- The AP/bridge hardware MAC: the FDB MAC that has no routable IP.
        -- Used by the frontend to know which MAC to exclude from the sub-client list
        -- (so the AP itself doesn't appear as one of its own downstream devices).
        -- This is distinct from c.mac which may be any registered client on the port.
        bridge_ap_mac = (function()
          local all_macs = port_macs_list[p] or {}
          for _, m in ipairs(all_macs) do
            local has_ip = arp_table[m]
                           or (reg.lan_clients[m] and reg.lan_clients[m].ip ~= "")
            if not has_ip then return m end
          end
          return nil  -- all MACs have IPs — no clear AP MAC, JS will use c.mac
        end)(),
      }

      lan_sum_rx = lan_sum_rx + tx
      lan_sum_tx = lan_sum_tx + rx
      prev["lan_"..p] = { rx=rx, tx=tx, ts=now }
    end
  end

  clients.lan_totals = {
    rx_str = fmt_bytes(lan_sum_rx),
    tx_str = fmt_bytes(lan_sum_tx),
    raw_rx = lan_sum_rx,
    raw_tx = lan_sum_tx
  }

  -- ── Interface rates ──────────────────────────────────────────────────────────
  -- Enumerate every interface in /sys/class/net, read rx/tx byte counters,
  -- compute per-second rates from the prev snapshot, and emit as iface_rates.
  -- Each entry: { name, label, rx_bytes, tx_bytes, rx_rate, tx_rate,
  --               rx_str, tx_str, rx_rate_str, tx_rate_str, is_wan, is_ap,
  --               operstate }
  -- AP interfaces carry ssid/band so the frontend can show a human label.
  -- WAN detection: interface named "wan", "eth0", "pppoe-wan", etc. that is
  --   NOT a bridge member and NOT a LAN/AP interface.
  do
    local ap_ifaces = {}
    for _, ap in ipairs(reg.aps) do ap_ifaces[ap.iface] = ap end

    -- Per-SSID accumulator for (smart) combined entries
    -- ssid_acc[essid] = { rx_b, tx_b, rx_rate, tx_rate, count }
    local ssid_acc = {}

    local lan_iface_set = {}
    for _, p in ipairs(lan_ports) do lan_iface_set[p] = true end
    lan_iface_set[bridge_name] = true

    -- ── WAN interface discovery ───────────────────────────────────────────────
    -- Strategy (in priority order):
    --   1. Ask UCI for the WAN network's ifname/device  → physical WAN port
    --   2. Ask ip route for the default-route interface → active WAN port
    --   3. Fall back to any iface named "wan*" or "pppoe*"
    --
    -- Rule: show at most TWO WAN entries:
    --   a) the highest-level logical/virtual interface (pppoe-wan > wan > eth0)
    --   b) the physical port underneath it ONLY if it differs from (a)
    --
    -- LAN ports: only show if operstate == "up" (cable inserted)
    -- APs:       only show if operstate == "up"

    -- Get UCI WAN device (the physical port, e.g. eth0 or eth1)
    local uci_wan_dev  = (run("uci -q get network.wan.device 2>/dev/null")  or ""):gsub("%s+","")
    local uci_wan_dev2 = (run("uci -q get network.wan.ifname 2>/dev/null")  or ""):gsub("%s+","")
    local uci_wan_phys = (uci_wan_dev ~= "" and uci_wan_dev) or
                         (uci_wan_dev2 ~= "" and uci_wan_dev2) or nil

    -- Get default-route interface as a cross-check
    local route_iface = (run("ip route show default 2>/dev/null") or ""):match("dev%s+(%S+)") or ""

    -- Collect all /sys/class/net interfaces
    -- NOTE: tunnel/VPN interfaces (tun, wg, ppp, etc.) report operstate="unknown"
    -- even when fully active. We treat "unknown" as "up" when the interface has
    -- IFF_UP + IFF_RUNNING set in /sys/class/net/<iface>/flags (hex bitmask).
    local raw = {}
    local ls_out = run("ls -1 /sys/class/net 2>/dev/null")
    for iface in ls_out:gmatch("[^%c]+") do
      if iface ~= "lo" then
        local base  = "/sys/class/net/" .. iface
        local oper  = (readfile(base .. "/operstate") or ""):gsub("%s+","")
        -- Tunnel/VPN/PPP ifaces report operstate="unknown" even when active.
        -- IFF_RUNNING is not set for point-to-point links (WireGuard, PPPoE).
        -- IFF_UP alone is sufficient: if the admin brought it up, show it.
        -- ifb* are internal kernel QoS mirrors — exclude them even if UP.
        if oper == "unknown" then
          local flags_hex = (readfile(base .. "/flags") or "0x0"):gsub("%s+","")
          local flags = tonumber(flags_hex) or 0
          local IFF_UP_BIT = 0x1
          local is_up = math.floor(flags / IFF_UP_BIT) % 2 == 1
          if is_up and not iface:find("^ifb") then
            oper = "up"
          else
            oper = "down"
          end
        end
        local rx_b  = tonumber(readfile(base .. "/statistics/rx_bytes") or "") or 0
        local tx_b  = tonumber(readfile(base .. "/statistics/tx_bytes") or "") or 0
        raw[iface]  = { oper=oper, rx_b=rx_b, tx_b=tx_b }
      end
    end

    -- Build the allowed WAN set (max 2: logical + physical)
    -- Priority: pppoe* > wan* > route_iface > uci_wan_phys
    local wan_logical  = nil   -- highest-level (pppoe-wan, wan)
    local wan_physical = nil   -- underlying physical (eth0, eth1)

    -- Step 1: find the active logical WAN (pppoe* or wan*)
    local candidates = {}
    for iface in pairs(raw) do
      if iface:find("pppoe") then candidates[#candidates+1] = { name=iface, pri=0 }
      elseif iface:find("^wan") then candidates[#candidates+1] = { name=iface, pri=1 }
      end
    end
    table.sort(candidates, function(a,b) return a.pri < b.pri end)
    for _, c in ipairs(candidates) do
      if raw[c.name] and raw[c.name].oper == "up" then
        wan_logical = c.name; break
      end
    end
    -- If none up, take first candidate regardless
    if not wan_logical and #candidates > 0 then
      wan_logical = candidates[1].name
    end

    -- Step 2: physical WAN port from UCI or default route
    if uci_wan_phys and uci_wan_phys ~= "" and uci_wan_phys ~= wan_logical then
      wan_physical = uci_wan_phys
    elseif route_iface ~= "" and route_iface ~= wan_logical then
      -- Only use route_iface as physical if it looks like an ethernet port
      if route_iface:find("^eth") or route_iface:find("^eno") or route_iface:find("^enp") then
        wan_physical = route_iface
      end
    end

    -- The allowed WAN set
    local wan_allowed = {}
    if wan_logical  then wan_allowed[wan_logical]  = true end
    if wan_physical then wan_allowed[wan_physical] = true end

    -- Accumulator for combined LAN entry
    local lan_acc  = { rx_b=0, tx_b=0, rx_rate=0, tx_rate=0, active=false }
    -- Accumulator for combined WiFi entry
    local wifi_acc = { rx_b=0, tx_b=0, rx_rate=0, tx_rate=0, active=false }

    local iface_list = {}
    for iface, d in pairs(raw) do
      local oper        = d.oper
      local rx_b        = d.rx_b
      local tx_b        = d.tx_b
      local ap_info     = ap_ifaces[iface]
      local is_lan_port = lan_iface_set[iface] == true
      local is_bridge   = (iface == bridge_name)
      local is_wan      = wan_allowed[iface] == true
      local is_other    = not is_wan and not is_lan_port and not ap_info and not is_bridge

      -- ── Inclusion filters ─────────────────────────────────────────────────
      local _noise = iface:find("^dummy") or iface:find("^veth")
                  or iface:find("^docker") or iface:find("^virbr")
                  or iface:find("^ifb")    or iface:find("^teql")
                  or iface:find("^sit%d")  or iface:find("^gre%d")
                  or iface:find("^ip6tnl")
      local _skip = is_bridge or (_noise ~= nil) or oper ~= "up"

      if not _skip then do
        local prev_k  = "ifr_" .. iface
        local prev_e  = prev[prev_k]
        local dt      = prev_e and (now - (prev_e.ts or 0)) or 0
        local rx_rate = (prev_e and dt > 0) and math.max(0, rx_b - prev_e.rx) / dt or 0
        local tx_rate = (prev_e and dt > 0) and math.max(0, tx_b - prev_e.tx) / dt or 0
        prev[prev_k]  = { rx=rx_b, tx=tx_b, ts=now }

        -- ── Rate perspective correction ───────────────────────────────────
        -- The kernel counts from the router's perspective:
        --   rx = bytes the router received FROM the client/network
        --   tx = bytes the router sent   TO   the client/network
        -- For LAN ports and APs the user expects client perspective:
        --   Download (Rx) = what the client downloaded  = router tx
        --   Upload   (Tx) = what the client uploaded    = router rx
        -- WAN and VPN/other are already in the correct perspective
        -- (router rx = internet download, router tx = internet upload).
        local show_rx_rate, show_tx_rate
        local show_rx_b,    show_tx_b
        if is_lan_port or ap_info then
          show_rx_rate = tx_rate   -- client download  = router tx
          show_tx_rate = rx_rate   -- client upload    = router rx
          show_rx_b    = tx_b
          show_tx_b    = rx_b
        else
          show_rx_rate = rx_rate
          show_tx_rate = tx_rate
          show_rx_b    = rx_b
          show_tx_b    = tx_b
        end

        -- ── LAN ports: accumulate into one combined entry, skip individual ──
        if is_lan_port then
          lan_acc.rx_b    = lan_acc.rx_b    + show_rx_b
          lan_acc.tx_b    = lan_acc.tx_b    + show_tx_b
          lan_acc.rx_rate = lan_acc.rx_rate + show_rx_rate
          lan_acc.tx_rate = lan_acc.tx_rate + show_tx_rate
          lan_acc.active  = true
        else
          -- Accumulate AP ifaces into wireless combined
          if ap_info then
            wifi_acc.rx_b    = wifi_acc.rx_b    + show_rx_b
            wifi_acc.tx_b    = wifi_acc.tx_b    + show_tx_b
            wifi_acc.rx_rate = wifi_acc.rx_rate + show_rx_rate
            wifi_acc.tx_rate = wifi_acc.tx_rate + show_tx_rate
            wifi_acc.active  = true
          end
          local label
          if ap_info then
            local essid = ap_info.essid or iface
            -- Always label with band; smart entry emitted separately
            label = fmt("%s (%s)", essid,
                        ap_info.band == "2.4_Ghz" and "2.4 GHz"
                        or ap_info.band == "5_Ghz" and "5 GHz"
                        or ap_info.band == "6_Ghz" and "6 GHz"
                        or ap_info.band or "WiFi")
            -- Accumulate into per-SSID smart entry
            local sa = ssid_acc[essid]
            if not sa then
              sa = { rx_b=0, tx_b=0, rx_rate=0, tx_rate=0, count=0 }
              ssid_acc[essid] = sa
            end
            sa.rx_b    = sa.rx_b    + show_rx_b
            sa.tx_b    = sa.tx_b    + show_tx_b
            sa.rx_rate = sa.rx_rate + show_rx_rate
            sa.tx_rate = sa.tx_rate + show_tx_rate
            sa.count   = sa.count   + 1
          elseif is_wan then
            local wtype = iface:find("pppoe") and "PPPoE"
                       or (iface == uci_wan_phys or iface == wan_physical) and "Physical"
                       or "WAN"
            label = fmt("%s (%s)", wtype, iface)
          elseif is_other then
            local vtype = (iface:find("^wg") or iface:find("^nordlynx") or iface:find("^mullvad") or iface:find("^proton")) and "WireGuard"
                       or (iface:find("^tun") or iface:find("^tap") or iface:find("^ovpn")) and "OpenVPN"
                       or (iface:find("^ipsec") or iface:find("^l2tp") or iface:find("^pptp")) and "VPN"
                       or nil
            label = vtype and fmt("%s (%s)", vtype, iface) or iface
          else
            label = iface
          end

          iface_list[#iface_list+1] = {
            name         = iface,
            label        = label,
            operstate    = oper,
            rx_bytes     = math.floor(show_rx_b),
            tx_bytes     = math.floor(show_tx_b),
            rx_rate      = math.floor(show_rx_rate),
            tx_rate      = math.floor(show_tx_rate),
            rx_rate_str  = fmt_bytes(math.floor(show_rx_rate)) .. "/s",
            tx_rate_str  = fmt_bytes(math.floor(show_tx_rate)) .. "/s",
            rx_str       = fmt_bytes(math.floor(show_rx_b)),
            tx_str       = fmt_bytes(math.floor(show_tx_b)),
            is_wan       = is_wan,
            is_ap        = ap_info ~= nil,
            is_bridge    = false,
            is_lan_port  = false,
            is_other     = is_other,
          }
        end
      end end -- if not _skip
    end

    -- ── Emit combined LAN entry ───────────────────────────────────────────
    if lan_acc.active then
      iface_list[#iface_list+1] = {
        name         = "__lan_combined__",
        label        = "LAN Ports (combined)",
        operstate    = "up",
        rx_bytes     = math.floor(lan_acc.rx_b),
        tx_bytes     = math.floor(lan_acc.tx_b),
        rx_rate      = math.floor(lan_acc.rx_rate),
        tx_rate      = math.floor(lan_acc.tx_rate),
        rx_rate_str  = fmt_bytes(math.floor(lan_acc.rx_rate)) .. "/s",
        tx_rate_str  = fmt_bytes(math.floor(lan_acc.tx_rate)) .. "/s",
        rx_str       = fmt_bytes(math.floor(lan_acc.rx_b)),
        tx_str       = fmt_bytes(math.floor(lan_acc.tx_b)),
        is_wan       = false,
        is_ap        = false,
        is_bridge    = false,
        is_lan_port  = true,
        is_other     = false,
      }
    end

    -- ── Emit per-SSID (smart) entries for multi-band SSIDs ────────────────────────
    for essid, sa in pairs(ssid_acc) do
      if sa.count > 1 then
        iface_list[#iface_list+1] = {
          name             = "__smart_" .. essid .. "__",
          label            = fmt("%s (smart)", essid),
          operstate        = "up",
          rx_bytes         = math.floor(sa.rx_b),
          tx_bytes         = math.floor(sa.tx_b),
          rx_rate          = math.floor(sa.rx_rate),
          tx_rate          = math.floor(sa.tx_rate),
          rx_rate_str      = fmt_bytes(math.floor(sa.rx_rate)) .. "/s",
          tx_rate_str      = fmt_bytes(math.floor(sa.tx_rate)) .. "/s",
          rx_str           = fmt_bytes(math.floor(sa.rx_b)),
          tx_str           = fmt_bytes(math.floor(sa.tx_b)),
          is_wan           = false,
          is_ap            = false,
          is_smart         = true,
          is_smart_essid   = essid,
          is_wifi_combined = false,
          is_bridge        = false,
          is_lan_port      = false,
          is_other         = false,
        }
      end
    end

    -- ── Emit combined WiFi entry ───────────────────────────────────────────────────
    if wifi_acc.active then
      iface_list[#iface_list+1] = {
        name             = "__wifi_combined__",
        label            = "Wireless (combined)",
        operstate        = "up",
        rx_bytes         = math.floor(wifi_acc.rx_b),
        tx_bytes         = math.floor(wifi_acc.tx_b),
        rx_rate          = math.floor(wifi_acc.rx_rate),
        tx_rate          = math.floor(wifi_acc.tx_rate),
        rx_rate_str      = fmt_bytes(math.floor(wifi_acc.rx_rate)) .. "/s",
        tx_rate_str      = fmt_bytes(math.floor(wifi_acc.tx_rate)) .. "/s",
        rx_str           = fmt_bytes(math.floor(wifi_acc.rx_b)),
        tx_str           = fmt_bytes(math.floor(wifi_acc.tx_b)),
        is_wan           = false,
        is_ap            = false,
        is_wifi_combined = true,
        is_bridge        = false,
        is_lan_port      = false,
        is_other         = false,
      }
    end

    -- Sort: WAN first, then APs, then wifi combined, then bridge, then LAN ports, then rest
    table.sort(iface_list, function(a, b)
      local function rank(x)
        if x.is_wan           then return 0 end
        if x.is_ap            then return 1 end
        if x.is_smart         then return 2 end
        if x.is_wifi_combined then return 3 end
        if x.is_bridge        then return 4 end
        if x.is_lan_port      then return 5 end
        if x.is_other         then return 6 end
        return 7
      end
      local ra, rb = rank(a), rank(b)
      if ra ~= rb then return ra < rb end
      -- Smart entries sort immediately after their SSID siblings
      local a_essid = a.is_smart and a.is_smart_essid or a.label
      local b_essid = b.is_smart and b.is_smart_essid or b.label
      if a_essid ~= b_essid then return a_essid < b_essid end
      -- Smart entry always last within its SSID group
      if a.is_smart ~= b.is_smart then return b.is_smart end
      return a.name < b.name
    end)

    clients.iface_rates = iface_list
  end

  -- ── Persist registry (stage 1: before bridge section) ──────────────────────
  -- Note: bridge section may also mutate reg.lan_clients (IP wipe/restore).
  -- A second save is done after the bridge section for those mutations.
  if reg_dirty then save_registry(reg); reg_dirty = false end

  -- ── Persist prev snapshot ─────────────────────────────────────────────────
  local latest_prev_ts = tonumber((readfile(prev_ts_file) or ""):match("^%s*(%d+)")) or 0
  if latest_prev_ts == initial_prev_ts then
    guarded_atomic_write_prev(prev)
  else
    atomic_write_tmp(prev_ts_file, tostring(now))
  end

  if conn and conn.close then conn:close() end

  local t1 = os_clock()

  -- ══ Bridge / AP sub-clients — nftables MAC per-client rates ══════════════
  --
  -- Visibility rule per client per poll (NO blocking):
  --   delta >= BRIDGE_ACTIVE_BYTES → show  (provably active)
  --   idle  <  BRIDGE_IDLE_SECS   → show  (recently active)
  --   idle  >= BRIDGE_IDLE_SECS   → show only if last bg ping succeeded
  --                                  fire new bg ping if BRIDGE_PING_SECS elapsed
  --
  -- Background ping result from previous poll is read instantly (file read).
  -- New ping is fired with & — returns immediately, result ready next poll.
  -- Request thread never waits for ping. No state machines, no ARP tricks.
  -- ──────────────────────────────────────────────────────────────────────────

  local br_state = load_bridge_state()
  -- br_state[mac] = { last_active_ts, last_ping_ts, ping_fail_count, ip }

  local bridge_mac_set = {}
  local bridge_by_port = {}
  local seen_macs      = {}

  for _, lc in ipairs(clients.lan) do
    if (lc.mac_count or 0) > 1 and lc.sub_clients and #lc.sub_clients > 0 then
      local pe = { port_label=lc.port_label or lc.port, clients={} }

      for _, sc in ipairs(lc.sub_clients) do
        if sc.ip and sc.ip ~= "" then
          local mac = sc.mac
          seen_macs[mac] = true

          local st = br_state[mac]
          if not st then
            -- First time seeing this MAC: assume alive, fire first ping immediately
            st = { last_active_ts=now, last_ping_ts=0, ping_fail_count=0, ip=sc.ip }
            br_state[mac] = st
            bg_ping_fire(sc.ip)
            st.last_ping_ts = now
          end
          st.ip = sc.ip

          -- nft byte delta
          local cur_c     = cur_nft[mac]
          local prv_c     = (nft_prev.macs or {})[mac]
          local cur_total = cur_c and ((cur_c.rx or 0) + (cur_c.tx or 0)) or 0
          local prv_total = prv_c and ((prv_c.rx or 0) + (prv_c.tx or 0)) or 0
          local delta     = cur_total - prv_total

          if delta >= BRIDGE_ACTIVE_BYTES then
            -- Actively transferring: mark active, no ping needed
            st.last_active_ts  = now
            st.ping_fail_count = 0   -- traffic proves liveness
          end

          local idle_secs = now - (st.last_active_ts or now)

          if idle_secs >= BRIDGE_IDLE_SECS then
            -- Client is idle: read latest background ping result (non-blocking)
            local result = bg_ping_read(sc.ip)
            if result == true then
              st.ping_fail_count = 0
              st.last_active_ts  = now   -- soft keepalive
            elseif result == false then
              st.ping_fail_count = math.min((st.ping_fail_count or 0) + 1, 99)
            end
            -- result == nil: job still running, leave fail_count unchanged
            -- Fire a new background ping if interval has elapsed
            local since_ping = now - (st.last_ping_ts or 0)
            if since_ping >= BRIDGE_PING_SECS then
              bg_ping_fire(sc.ip)
              st.last_ping_ts = now
            end
          end

          -- Hide only after 2 consecutive failures (1 miss = sleeping device, not dead)
          local show = (idle_secs < BRIDGE_IDLE_SECS) or ((st.ping_fail_count or 0) < 2)

          if show then
            bridge_mac_set[mac] = true
            pe.clients[#pe.clients+1] = { mac=mac, ip=sc.ip, hostname=sc.hostname or "" }
          end

          -- Save nft totals for next-poll delta
          if cur_c then
            new_nft_prev.macs[mac] = { rx=cur_c.rx or 0, tx=cur_c.tx or 0 }
          end
        end
      end

      if #pe.clients > 0 then
        bridge_by_port[lc.port] = pe
      end
    end
  end

  -- Purge state for MACs no longer in any sub_clients
  for mac, st in pairs(br_state) do
    if not seen_macs[mac] then
      bg_ping_clear(st.ip)
      br_state[mac] = nil
    end
  end

  -- Build output
  clients.bridge_clients = {}

  if next(bridge_mac_set) then
    nft_ensure_setup()
    local listing = nft_list_all()
    for mac in pairs(bridge_mac_set) do nft_ensure_mac(mac, listing) end
    nft_remove_stale(bridge_mac_set)

    for port, pe in pairs(bridge_by_port) do
      local band_clients = {}
      local sum_rx, sum_tx = 0, 0

      for _, sc in ipairs(pe.clients) do
        local mac   = sc.mac
        local cur_c = cur_nft[mac]
        local raw_rx = cur_c and cur_c.rx or 0
        local raw_tx = cur_c and cur_c.tx or 0
        sum_rx = sum_rx + raw_rx
        sum_tx = sum_tx + raw_tx
        band_clients[#band_clients+1] = {
          mac=mac, ip=sc.ip, hostname=sc.hostname,
          raw_rx=raw_rx, raw_tx=raw_tx,
          rx_str=fmt_bytes(raw_rx), tx_str=fmt_bytes(raw_tx),
          signal_pct=0, is_bridge_client=true, port_label=pe.port_label,
        }
      end

      clients.bridge_clients[#clients.bridge_clients+1] = {
        key=fmt("Bridge AP (%s)", pe.port_label),
        port=port, port_label=pe.port_label,
        clients=band_clients,
        totals={ raw_rx=sum_rx, raw_tx=sum_tx,
                 rx_str=fmt_bytes(sum_rx), tx_str=fmt_bytes(sum_tx) },
      }
    end
  end

  -- Emit empty card for bridge ports with no alive clients
  local function has_hint(hints, h)
    if not hints then return false end
    for _, v in ipairs(hints) do if v == h then return true end end
    return false
  end
  for _, lc in ipairs(clients.lan) do
    if (lc.mac_count or 0) > 1 or has_hint(lc.device_hints, "bridge") then
      local port_label = lc.port_label or lc.port
      local key        = fmt("Bridge AP (%s)", port_label)
      local already    = false
      for _, bc in ipairs(clients.bridge_clients) do
        if bc.key == key then already = true; break end
      end
      if not already then
        clients.bridge_clients[#clients.bridge_clients+1] = {
          key=key, port=lc.port, port_label=port_label,
          clients={}, totals={ raw_rx=0, raw_tx=0, rx_str="0 B", tx_str="0 B" },
        }
      end
    end
  end

  save_nft_prev(new_nft_prev)
  save_bridge_state(br_state)

  clients._runtime_s = tonumber(fmt("%.3f", t1 - t0))
  clients._ts = now   -- server-side generation timestamp; JS uses this to skip rate updates on duplicate cached payloads

  local json_out = jsonc.stringify(clients)
  resp_cache_write(json_out)
  lock_release(lock_f)
  http.write(json_out)
end