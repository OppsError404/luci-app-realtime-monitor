# luci-app-realtime-monitor

A realtime monitoring dashboard for OpenWrt LuCI.

## Features
- Live CPU, memory, and network stats
- Realtime clients usage monitoring

## Requirements
- OpenWrt with LuCI
- ip-bridge / required for lan clients monitoring (installed automatically on first installation)
- vnstat2 for usage data on dashboard

## Installation

Method 1: ( Recommended )
```sh
wget --no-check-certificate -O /tmp/luci-app-realtime-monitor.apk \
    https://github.com/OppsError404/luci-app-realtime-monitor/releases/download/v1.7/luci-app-realtime-monitor-1.7-r1.apk \
    && apk add --allow-untrusted /tmp/luci-app-realtime-monitor.apk \
    && rm /tmp/luci-app-realtime-monitor.apk \
    && apk update \
    && apk add ip-bridge \
    || echo "Something failed, check manually."
```

Method 2: 

Manually download the latest `.apk` from releases and install:
```sh
apk add --allow-untrusted luci-app-realtime-monitor-1.7-r1.apk
```
