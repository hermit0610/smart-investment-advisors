# -*- coding: utf-8 -*-
"""Proxy auto-detect tool - reads Windows system proxy settings"""
import sys

print("=" * 50)
print("  Proxy Detection Tool")
print("=" * 50)

try:
    import winreg
    key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                         r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
    proxy_enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
    if proxy_enabled:
        proxy_server, _ = winreg.QueryValueEx(key, "ProxyServer")
        winreg.CloseKey(key)
        print(f"\n  System proxy ENABLED")
        print(f"  Address: {proxy_server}")
        # Parse
        p = proxy_server.split(";")[0].strip()
        if "=" in p:
            p = p.split("=", 1)[1]
        if not p.startswith("http"):
            p = "http://" + p
        print(f"  Parsed:  {p}")
        print(f"\n  All good! The app will auto-detect this proxy.")
    else:
        winreg.CloseKey(key)
        print("\n  System proxy DISABLED.")
        print("  Turn on 'System Proxy' in your VPN app (Clash/V2Ray/SS).")
        print("  Or manually set proxy in config.json: {\"proxy\": \"http://127.0.0.1:xxxx\"}")
except ImportError:
    print("\n  Not on Windows - check HTTPS_PROXY environment variable.")
except Exception as e:
    print(f"\n  Error reading system proxy: {e}")

print()

