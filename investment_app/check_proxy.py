# -*- coding: utf-8 -*-
"""Proxy auto-detect tool - finds your VPN proxy port"""
import socket, sys

COMMON_PORTS = [7890, 1080, 10809, 8888, 1087, 8118, 9090, 3128, 8080]

print("=" * 50)
print("  Proxy Detection Tool")
print("=" * 50)
print("\nChecking common VPN proxy ports on localhost...\n")

found = []
for port in COMMON_PORTS:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        result = s.connect_ex(("127.0.0.1", port))
        s.close()
        if result == 0:
            proxy_type = ""
            if port == 7890: proxy_type = "(likely Clash)"
            elif port == 1080: proxy_type = "(likely Shadowsocks)"
            elif port == 10809: proxy_type = "(likely V2Ray/Xray)"
            elif port == 8888: proxy_type = "(HTTP proxy)"
            print(f"  OPEN: 127.0.0.1:{port} {proxy_type}")
            found.append(port)
    except Exception:
        pass

if not found:
    print("  No common proxy ports found on localhost.")
    print("\n  Make sure your VPN/proxy software is RUNNING.")
    print("  If using Clash: enable 'System Proxy' or 'TUN Mode'")
    print("  If using V2Ray: check your inbound port settings")

if found:
    p = found[0]
    is_http = p in [7890, 8888, 8080, 3128, 8118]
    print(f"\n  Recommended config.json setting:")
    if is_http:
        print(f'  {{"proxy": "http://127.0.0.1:{p}"}}')
    else:
        print(f'  {{"proxy": "socks5://127.0.0.1:{p}"}}')
    print(f"\n  Or set in terminal before running app:")
    print(f"  set HTTPS_PROXY=http://127.0.0.1:{p}")

print()
