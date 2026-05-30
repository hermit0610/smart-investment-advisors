"""OKX API standalone test"""
import requests, json, time, hmac, base64

with open("investment_app/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

KEY = cfg["okx_api_key"].strip()
SECRET = cfg["okx_api_secret"].strip()
PASSPHRASE = cfg["okx_api_passphrase"].strip()
PROXY = cfg.get("proxy", "").strip()

proxies = {"http": PROXY, "https": PROXY} if PROXY else None
print(f"Proxy: {PROXY if PROXY else 'none'}")

print("\n1. Getting OKX server time...")
try:
    r = requests.get("https://www.okx.com/api/v5/public/time", timeout=5, proxies=proxies)
    ts = r.json()["data"][0]["ts"]
    print(f"   OKX time: {ts}")
except Exception as e:
    print(f"   FAILED: {e}")
    exit()

method = "GET"
path = "/api/v5/account/balance"
msg = f"{ts}{method}{path}"
sign = base64.b64encode(hmac.new(SECRET.encode("utf-8"), msg.encode("utf-8"), "sha256").digest()).decode("utf-8")

headers = {
    "OK-ACCESS-KEY": KEY,
    "OK-ACCESS-SIGN": sign,
    "OK-ACCESS-TIMESTAMP": ts,
    "OK-ACCESS-PASSPHRASE": PASSPHRASE,
}

print(f"\n2. Calling OKX balance API...")
try:
    r = requests.get("https://www.okx.com/api/v5/account/balance", headers=headers, timeout=5, proxies=proxies)
    result = r.json()
    print(f"   Code: {result.get('code')}  Msg: {result.get('msg')}")
    if result.get("code") == "0":
        print("   SUCCESS!")
    else:
        print(f"   Full: {json.dumps(result, indent=2)}")
except Exception as e:
    print(f"   ERROR: {e}")
