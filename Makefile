include $(TOPDIR)/rules.mk

PKG_NAME:=luci-app-realtime-monitor
PKG_VERSION:=1.6
PKG_RELEASE:=1

include $(INCLUDE_DIR)/package.mk

define Package/luci-app-realtime-monitor
  SECTION:=luci
  CATEGORY:=LuCI
  SUBMENU:=3. Applications
  TITLE:=Realtime Monitor and Dashboard
  DEPENDS:=+luci-base +luci-mod-admin-full
  PKGARCH:=all
endef

define Package/luci-app-realtime-monitor/description
  A realtime monitoring dashboard for OpenWrt LuCI.
  Provides live system stats including CPU, memory, network, and more.
endef

define Build/Compile
endef

define Package/luci-app-realtime-monitor/preinst
#!/bin/sh
rm -f /usr/lib/lua/luci/controller/admin/dashboard.lua
rm -f /usr/lib/lua/luci/view/admin/dashboard.htm
exit 0
endef

define Package/luci-app-realtime-monitor/install
	$(INSTALL_DIR) $(1)/usr/lib/lua/luci/controller/admin
	$(INSTALL_DIR) $(1)/usr/lib/lua/luci/view/admin
	$(CP) ./files/usr/lib/lua/luci/controller/admin/* $(1)/usr/lib/lua/luci/controller/admin/
	$(CP) ./files/usr/lib/lua/luci/view/admin/* $(1)/usr/lib/lua/luci/view/admin/
endef

define Package/luci-app-realtime-monitor/postinst
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0

if ! apk info 2>/dev/null | grep -q "^ip-bridge$$"; then
    echo "" >&2
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
    echo "!!  WARN: ip-bridge package is not installed  !!" >&2
    echo "!!  A post script will attempt to install it. !!" >&2
    echo "!!  If it fails, run: apk add ip-bridge       !!" >&2
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
    echo "" >&2

    setsid /bin/sh -c '
        sleep 2
        if apk update && apk add ip-bridge; then
            logger -t luci-realtime-monitor "ip-bridge installed successfully."
        else
            logger -t luci-realtime-monitor "Could not install ip-bridge. Run apk add ip-bridge manually when online."
        fi
    ' >/dev/null 2>&1 &
fi

rm -f /tmp/luci-indexcache
/etc/init.d/rpcd restart
/etc/init.d/uhttpd restart

exit 0
endef

define Package/luci-app-realtime-monitor/postrm
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0

rm -f /tmp/luci-indexcache
/etc/init.d/rpcd restart
/etc/init.d/uhttpd restart

exit 0
endef

$(eval $(call BuildPackage,luci-app-realtime-monitor))
