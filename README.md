# luci-app-realtime-monitor

A realtime monitoring dashboard for OpenWrt LuCI.

## Features
- Live CPU, memory, and network stats
- Realtime client monitor dashboard

## Requirements
- OpenWrt with LuCI
- ip-bridge (installed automatically on first boot)

## Installation
Download the latest `.apk` from releases and install:
```sh
apk add --allow-untrusted luci-app-realtime-monitor-1.6-r1.apk
```

## Build from source
```sh
git clone https://github.com/yourusername/luci-app-realtime-monitor
cp -r luci-app-realtime-monitor /path/to/openwrt-sdk/package/
make package/luci-app-realtime-monitor/compile V=s
```
