#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Smart Investment Advisor v2 - Optimized
Fast data: Binance API + Multi-source fallback
"""

import json, time, requests, threading, os, socket
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify
import numpy as np
import pandas as pd

app = Flask(__name__)

# ============================================================
# Fast Data Fetcher - Multi-source with auto proxy detection
# ============================================================

# All known exchange API endpoints - tried in order
EXCHANGE_ENDPOINTS = [
    # Binance (main + mirrors)
    ("binance", "https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT"),
    ("binance", "https://api1.binance.com/api/v3/ticker/24hr?symbol={sym}USDT"),
    ("binance", "https://api2.binance.com/api/v3/ticker/24hr?symbol={sym}USDT"),
    ("binance", "https://api3.binance.com/api/v3/ticker/24hr?symbol={sym}USDT"),
    ("binance", "https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"),
    # OKX
    ("okx", "https://www.okx.com/api/v5/market/ticker?instId={sym}-USDT"),
    # Bybit
    ("bybit", "https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}USDT"),
    # MEXC
    ("mexc", "https://api.mexc.com/api/v3/ticker/24hr?symbol={sym}USDT"),
    # KuCoin
    ("kucoin", "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}-USDT"),
    # Gate.io
    ("gate", "https://api.gate.io/api2/1/ticker/{sym_lower}_usdt"),
    # CoinGecko (free, no API key)
    ("coingecko", "https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true"),
]

REQUEST_TIMEOUT = 2.0

_price_cache = {}
_cache_ttl = 10
_klines_cache = {}

_network_ok = True
_network_checked_at = 0
_network_retry_interval = 60
_network_lock = threading.Lock()
_proxy_cache = None
_proxy_test_time = 0

# CoinGecko ID mapping
CG_IDS = {
    "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin",
    "XRP":"ripple","DOGE":"dogecoin","ADA":"cardano","AVAX":"avalanche-2",
    "DOT":"polkadot","LINK":"chainlink","MATIC":"matic-network",
    "UNI":"uniswap","ATOM":"cosmos","LTC":"litecoin"
}

# ---- Smart Proxy Auto-Detection ----
COMMON_PROXY_PORTS = [7890, 10809, 1080, 7891, 8118, 8888, 8080, 3128, 1087, 9090]

def _test_proxy(proxy_url, test_url="https://api.binance.com/api/v3/ping", timeout=2):
    """Test if a proxy actually works by making a request through it"""
    try:
        proxies = {"http": proxy_url, "https": proxy_url}
        r = requests.get(test_url, timeout=timeout, proxies=proxies)
        return r.status_code == 200
    except Exception:
        return False

def _auto_discover_proxy():
    """Auto-discover working proxy from common ports and environment"""
    global _proxy_cache, _proxy_test_time

    # Return cached proxy if tested recently
    if _proxy_cache is not None and time.time() - _proxy_test_time < 300:
        return _proxy_cache

    # 1. Check environment variables
    for v in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY"]:
        val = os.environ.get(v, "")
        if val and val.startswith("http"):
            if _test_proxy(val):
                _proxy_cache = {"http": val, "https": val}
                _proxy_test_time = time.time()
                print(f"Proxy found via env {v}: {val}")
                return _proxy_cache

    # 2. Check config file
    try:
        with open("config.json", "r") as f:
            cfg = json.load(f)
        proxy = cfg.get("proxy", "")
        if proxy and proxy.startswith("http"):
            if _test_proxy(proxy):
                _proxy_cache = {"http": proxy, "https": proxy}
                _proxy_test_time = time.time()
                print(f"Proxy found via config.json: {proxy}")
                return _proxy_cache
    except Exception:
        pass

    # 3. Auto-scan common proxy ports on localhost
    for port in COMMON_PROXY_PORTS:
        proxy_url = f"http://127.0.0.1:{port}"
        # Quick socket check first
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.3)
        result = sock.connect_ex(("127.0.0.1", port))
        sock.close()
        if result == 0:
            # Port is open - test if it's actually a proxy
            if _test_proxy(proxy_url):
                _proxy_cache = {"http": proxy_url, "https": proxy_url}
                _proxy_test_time = time.time()
                print(f"Proxy auto-detected on port {port}: {proxy_url}")
                return _proxy_cache

    _proxy_test_time = time.time()
    return None

# Proxy: only use if explicitly set in config.json
# Data fetching uses system proxy automatically (no explicit config needed)
# OKX trading uses this proxy for authenticated API calls
PROXY = None
try:
    cfg_path_proxy = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    with open(cfg_path_proxy, "r", encoding="utf-8") as f_proxy:
        cfg_proxy = json.load(f_proxy)
    pv = cfg_proxy.get("proxy", "").strip()
    if pv and pv.startswith("http"):
        PROXY = {"http": pv, "https": pv}
        print(f"OKX proxy: {pv}")
except: pass

if not PROXY:
    print("OKX proxy not configured. Data fetching uses system proxy.")

def _try_fetch_json(url, timeout=REQUEST_TIMEOUT, use_proxy=True):
    """Fetch JSON from URL"""
    try:
        proxies = PROXY if (PROXY and use_proxy) else None
        r = requests.get(url, timeout=timeout, proxies=proxies)
        if r.status_code == 200:
            return r.json()
    except: pass
    return None

DEMO_MODE = False

def _check_network():
    """Network check - tried with proxy if configured"""
    global _network_ok, _network_checked_at, DEMO_MODE
    now = time.time()
    if now - _network_checked_at < _network_retry_interval:
        return _network_ok
    with _network_lock:
        if now - _network_checked_at < _network_retry_interval:
            return _network_ok
        result_holder = [None]
        def _try(u):
            try:
                proxies = PROXY if PROXY else None
                r = requests.get(u, timeout=2, proxies=proxies)
                if r.status_code == 200 and result_holder[0] is None:
                    result_holder[0] = True
            except: pass
        urls = ["https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
                "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT"]
        threads = [threading.Thread(target=_try, args=(u,), daemon=True) for u in urls]
        for t in threads: t.start()
        for t in threads: t.join(timeout=3)
        _network_checked_at = time.time()
        _network_ok = result_holder[0] is not None
        DEMO_MODE = not _network_ok
        return _network_ok

CRYPTO_LIST = ["BTC","ETH","SOL","BNB","XRP","DOGE","ADA","AVAX","DOT","LINK","MATIC","UNI","ATOM","LTC"]

# ---- Multi-source price fetcher ----
def _parse_binance_ticker(data, sym):
    """Parse Binance 24hr ticker response"""
    return {
        "price": float(data["lastPrice"]),
        "change_pct": round(float(data["priceChangePercent"]), 2),
        "name": sym, "currency": "USDT", "source": "binance"
    }

def _parse_okx_ticker(data, sym):
    """Parse OKX ticker response"""
    try:
        d = data["data"][0]
        return {
            "price": float(d["last"]),
            "change_pct": round(float(d.get("change24h", "0")) * 100, 2),
            "name": sym, "currency": "USDT", "source": "okx"
        }
    except: return None

def _parse_bybit_ticker(data, sym):
    """Parse Bybit ticker response"""
    try:
        d = data["result"]["list"][0]
        return {
            "price": float(d["lastPrice"]),
            "change_pct": round(float(d.get("price24hPcnt", "0")) * 100, 2),
            "name": sym, "currency": "USDT", "source": "bybit"
        }
    except: return None

def _parse_mexc_ticker(data, sym):
    """Parse MEXC ticker response"""
    return {
        "price": float(data["lastPrice"]),
        "change_pct": round(float(data.get("priceChangePercent", 0)), 2),
        "name": sym, "currency": "USDT", "source": "mexc"
    }

def _parse_kucoin_ticker(data, sym):
    """Parse KuCoin ticker response"""
    try:
        d = data["data"]
        return {
            "price": float(d["price"]),
            "change_pct": round(float(d.get("changeRate", "0")) * 100, 2),
            "name": sym, "currency": "USDT", "source": "kucoin"
        }
    except: return None

def _parse_gate_ticker(data, sym):
    """Parse Gate.io ticker response"""
    return {
        "price": float(data["last"]),
        "change_pct": round(float(data.get("percentChange", 0)), 2),
        "name": sym, "currency": "USDT", "source": "gate"
    }

def _parse_coingecko_price(data, sym):
    """Parse CoinGecko simple price response"""
    cg_id = CG_IDS.get(sym)
    if cg_id and cg_id in data:
        pdata = data[cg_id]
        return {
            "price": float(pdata.get("usd", 0)),
            "change_pct": round(float(pdata.get("usd_24h_change", 0)), 2),
            "name": sym, "currency": "USDT", "source": "coingecko"
        }
    return None

def _get_preferred_exchange():
    """Read preferred exchange order from config"""
    try:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        order = cfg.get("exchange_order", [])
        if order:
            return order
    except: pass
    # Default: OKX first (better for China users), then Binance, then others
    return ["okx", "binance", "bybit", "mexc", "coingecko", "kucoin", "gate"]

def _race_fetch_price(symbol):
    """Race multiple exchange APIs for price - return first successful result"""
    sym = symbol.upper().replace("/USDT","").replace("-USD","")
    sym_lower = sym.lower()
    cg_id = CG_IDS.get(sym)

    # Source 1: Binance
    binance_data = _try_fetch_json(
        f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT"
    )
    if binance_data and "lastPrice" in binance_data:
        return _parse_binance_ticker(binance_data, sym)

    # Source 2: OKX
    okx_data = _try_fetch_json(
        f"https://www.okx.com/api/v5/market/ticker?instId={sym}-USDT"
    )
    if okx_data and okx_data.get("code") == "0":
        result = _parse_okx_ticker(okx_data, sym)
        if result: return result

    # Source 3: Bybit
    bybit_data = _try_fetch_json(
        f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}USDT"
    )
    if bybit_data and bybit_data.get("retCode") == 0:
        result = _parse_bybit_ticker(bybit_data, sym)
        if result: return result

    # Source 4: CoinGecko
    if cg_id:
        cg_data = _try_fetch_json(
            f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true"
        )
        if cg_data and cg_id in cg_data:
            result = _parse_coingecko_price(cg_data, sym)
            if result: return result

    # Source 5: KuCoin
    kc_data = _try_fetch_json(
        f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}-USDT"
    )
    if kc_data and kc_data.get("code") == "200000":
        result = _parse_kucoin_ticker(kc_data, sym)
        if result: return result

    # Source 6: Gate.io
    gate_data = _try_fetch_json(
        f"https://api.gate.io/api2/1/ticker/{sym_lower}_usdt"
    )
    if gate_data and "last" in gate_data:
        return _parse_gate_ticker(gate_data, sym)

    # Last resort: Yahoo Finance
    try:
        import yfinance as yf
        t = yf.Ticker(f"{sym}-USD")
        info = t.info
        price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
        if price and price > 0:
            return {
                "price": round(price, 2),
                "change_pct": round(info.get("regularMarketChangePercent", 0) or 0, 2),
                "name": info.get("shortName", sym), "currency": "USD", "source": "yahoo"
            }
    except Exception:
        pass

    return None

# Demo data
DEMO_PRICES = {
    "BTC": 87000, "ETH": 3200, "SOL": 145, "BNB": 610,
    "XRP": 0.52, "DOGE": 0.12, "ADA": 0.45, "AVAX": 35,
    "DOT": 7.5, "LINK": 15, "MATIC": 0.75, "UNI": 8.5,
    "ATOM": 9, "LTC": 80
}

def get_price(symbol):
    """Get price - race multiple sources, fallback to demo"""
    if _check_network():
        result = _race_fetch_price(symbol)
        if result and result.get("price", 0) > 0:
            return result

    # Demo fallback
    sym = symbol.upper().replace("/USDT","").replace("-USD","")
    if sym in DEMO_PRICES:
        import random
        random.seed(int(time.time()/60) + hash(sym))
        price = DEMO_PRICES[sym] * (1 + random.uniform(-0.03, 0.03))
        return {"price": round(price, 2), "change_pct": round(random.uniform(-3, 3), 2),
                "name": sym, "currency": "USDT", "demo": True}
    raise ValueError(f"Cannot fetch price for {symbol}")

def get_klines(symbol, interval="1d", limit=200):
    """Get klines - try Binance, then Yahoo, fallback to demo"""
    if _check_network():
        sym = symbol.upper().replace("/USDT","").replace("-USD","")
        interval_map = {"1d": "1d", "1h": "1h", "1wk": "1w", "4h": "4h"}
        bi = interval_map.get(interval, interval)

        # Try Binance
        for base_url in ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]:
            data = _try_fetch_json(f"{base_url}/api/v3/klines?symbol={sym}USDT&interval={bi}&limit={limit}", timeout=3)
            if data and len(data) > 10:
                df = pd.DataFrame(data, columns=[
                    "timestamp","open","high","low","close","volume",
                    "close_time","quote_vol","trades","taker_buy_base",
                    "taker_buy_quote","ignore"
                ])
                for col in ["open","high","low","close","volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("timestamp", inplace=True)
                df = df[["open","high","low","close","volume"]].dropna()
                return df

        # Yahoo Finance fallback
        try:
            import yfinance as yf
            ticker = yf.Ticker(f"{sym}-USD")
            df = ticker.history(period="6mo", interval=interval)
            df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}, inplace=True)
            if not df.empty:
                return df
        except Exception:
            pass

    # Demo data fallback
    sym = symbol.upper().replace("/USDT","").replace("-USD","")
    if sym in DEMO_PRICES:
        import random
        random.seed(hash(sym) % 10000)
        np.random.seed(hash(sym) % 10000)
        base = DEMO_PRICES[sym]
        dates = pd.date_range(end=datetime.now(), periods=limit, freq="D")
        trend = np.linspace(0, base * 0.1, limit)
        cycles = base * 0.05 * np.sin(np.linspace(0, 6*np.pi, limit))
        noise = np.random.normal(0, base * 0.01, limit)
        close = base - base * 0.05 + trend + cycles + noise
        close = np.maximum(close, base * 0.5)
        high = close + np.abs(np.random.normal(0, base * 0.015, limit))
        low = close - np.abs(np.random.normal(0, base * 0.015, limit))
        open_p = low + np.random.random(limit) * (high - low)
        volume = np.random.normal(base * 100, base * 30, limit)
        volume = np.maximum(volume, 100)
        df = pd.DataFrame({
            "open": open_p, "high": high, "low": low,
            "close": close, "volume": volume
        }, index=dates)
        return df

    raise ValueError(f"Cannot fetch klines for {symbol}")

# ============================================================
# Batch fetch - concurrent for speed
# ============================================================

def batch_prices(symbols):
    """Fetch multiple prices concurrently"""
    results = {}
    def _fetch(sym):
        try:
            results[sym] = get_price(sym)
        except Exception:
            results[sym] = {"error": "unavailable"}

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_fetch, symbols))

    return dict(sorted(results.items(), key=lambda x: CRYPTO_LIST.index(x[0]) if x[0] in CRYPTO_LIST else 99))

print(f"Data module loaded. Exchanges: {', '.join(_get_preferred_exchange())}")


# ============================================================
# Analysis Engines
# ============================================================

def ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

def analyze_macd(df):
    close = df["close"]
    ml = ema(close, 12) - ema(close, 26)
    sl = ema(ml, 9)
    hist = ml - sl
    c, p = float(ml.iloc[-1]), float(sl.iloc[-1])
    h, hp = float(hist.iloc[-1]), float(hist.iloc[-2])
    golden = float(ml.iloc[-2]) <= float(sl.iloc[-2]) and c > p
    dead = float(ml.iloc[-2]) >= float(sl.iloc[-2]) and c < p
    trend = "bullish" if c > p else "bearish"
    divergence = None
    if len(df) >= 20:
        pr, mr = df["close"].iloc[-20:], ml.iloc[-20:]
        if pr.iloc[-1] >= pr.max() and mr.iloc[-1] < mr.max() * 0.9:
            divergence = "bearish"
        elif pr.iloc[-1] <= pr.min() and mr.iloc[-1] > mr.min() * 1.1:
            divergence = "bullish"
    strength = 50.0
    details = []
    if golden:
        strength += 30
        details.append({"factor": "golden_cross", "impact": 30, "desc_en": "MACD golden cross detected (DIF crossed above DEA)", "desc_zh": "MACD金叉形成（DIF上穿DEA）"})
    if dead:
        strength -= 30
        details.append({"factor": "dead_cross", "impact": -30, "desc_en": "MACD dead cross detected (DIF crossed below DEA)", "desc_zh": "MACD死叉形成（DIF下穿DEA）"})
    if h > 0 and h > hp:
        strength += 15
        details.append({"factor": "histogram_rising", "impact": 15, "desc_en": "MACD histogram rising (momentum increasing)", "desc_zh": "MACD柱状图上升（动能增强）"})
    elif h < 0 and h < hp:
        strength -= 15
        details.append({"factor": "histogram_falling", "impact": -15, "desc_en": "MACD histogram falling (momentum weakening)", "desc_zh": "MACD柱状图下降（动能减弱）"})
    if divergence == "bullish":
        strength += 20
        details.append({"factor": "bullish_divergence", "impact": 20, "desc_en": "Bullish divergence: price making lower lows but MACD making higher lows", "desc_zh": "底背离：价格创新低但MACD走高，看涨信号"})
    elif divergence == "bearish":
        strength -= 20
        details.append({"factor": "bearish_divergence", "impact": -20, "desc_en": "Bearish divergence: price making higher highs but MACD making lower highs", "desc_zh": "顶背离：价格创新高但MACD走低，看跌信号"})
    if c > 0 and p > 0:
        strength += 5
        details.append({"factor": "positive_zone", "impact": 5, "desc_en": "Both DIF and DEA above zero line (bullish zone)", "desc_zh": "DIF与DEA均位于零轴上方（多头区域）"})
    elif c < 0 and p < 0:
        strength -= 5
        details.append({"factor": "negative_zone", "impact": -5, "desc_en": "Both DIF and DEA below zero line (bearish zone)", "desc_zh": "DIF与DEA均位于零轴下方（空头区域）"})
    if not details:
        details.append({"factor": "neutral", "impact": 0, "desc_en": "No strong MACD signal", "desc_zh": "MACD无明显信号"})
    strength = max(0, min(100, strength))
    return {"macd_line":round(c,6),"signal_line":round(p,6),"histogram":round(h,6),
            "trend":trend,"golden_cross":golden,"dead_cross":dead,
            "divergence":divergence,"strength":round(strength,1),
            "score":round(strength,1),  # Alias for consistency
            "score_details": details,
            "macd_history":[round(x,6)for x in ml.iloc[-100:].tolist()],
            "signal_history":[round(x,6)for x in sl.iloc[-100:].tolist()],
            "histogram_history":[round(x,6)for x in hist.iloc[-100:].tolist()]}

def analyze_volume(df):
    cv = float(df["volume"].iloc[-1])
    ma = float(df["volume"].rolling(20).mean().iloc[-1])
    ratio = cv / ma if ma > 0 else 1.0
    spike = ratio >= 2.0
    recent = df["volume"].iloc[-14:]
    if len(recent) >= 6:
        h = len(recent)//2
        a1, a2 = float(recent.iloc[:h].mean()), float(recent.iloc[-h:].mean())
        vt = "increasing" if a2>a1*1.15 else ("decreasing" if a2<a1*0.85 else "stable")
    else:
        vt = "stable"
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i-1]: obv.append(obv[-1]+df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i-1]: obv.append(obv[-1]-df["volume"].iloc[i])
        else: obv.append(obv[-1])
    obv_s = pd.Series(obv, index=df.index)
    n = min(10, len(obv_s))
    slope = np.polyfit(range(n), obv_s.iloc[-n:], 1)[0]
    ot = "rising" if slope>0 else ("falling" if slope<0 else "flat")
    pc = (float(df["close"].iloc[-1])-float(df["close"].iloc[-2]))/float(df["close"].iloc[-2])
    score = 50.0
    details = []
    if pc>0 and spike:
        score += 25
        details.append({"factor": "price_up_volume_spike", "impact": 25, "desc_en": f"Price up +{pc*100:.1f}% with volume spike ({ratio:.1f}x avg) - strong bullish confirmation", "desc_zh": f"价格上涨{pc*100:.1f}%且放量({ratio:.1f}倍均量) - 强势看涨确认"})
    elif pc>0 and vt=="increasing":
        score += 15
        details.append({"factor": "price_up_volume_rising", "impact": 15, "desc_en": f"Price up +{pc*100:.1f}% with increasing volume trend", "desc_zh": f"价格上涨{pc*100:.1f}%且成交量趋势增加"})
    elif pc<0 and spike:
        score -= 25
        details.append({"factor": "price_down_volume_spike", "impact": -25, "desc_en": f"Price down {pc*100:.1f}% with volume spike ({ratio:.1f}x avg) - panic selling", "desc_zh": f"价格下跌{abs(pc*100):.1f}%且放量({ratio:.1f}倍均量) - 恐慌性抛售"})
    elif pc<0 and vt=="decreasing":
        score -= 10
        details.append({"factor": "price_down_volume_falling", "impact": -10, "desc_en": f"Price down {pc*100:.1f}% with decreasing volume trend", "desc_zh": f"价格下跌{abs(pc*100):.1f}%且成交量趋势减少"})
    if ot=="rising":
        score += 10
        details.append({"factor": "obv_rising", "impact": 10, "desc_en": "OBV (On-Balance Volume) rising - accumulation detected", "desc_zh": "OBV能量潮上升 - 检测到资金流入"})
    elif ot=="falling":
        score -= 10
        details.append({"factor": "obv_falling", "impact": -10, "desc_en": "OBV (On-Balance Volume) falling - distribution detected", "desc_zh": "OBV能量潮下降 - 检测到资金流出"})
    if not details:
        details.append({"factor": "neutral", "impact": 0, "desc_en": "No significant volume signal", "desc_zh": "成交量无明显信号"})
    score = max(0, min(100, score))
    return {"current_volume":round(cv,1),"volume_ma":round(ma,1),"volume_ratio":round(ratio,2),
            "trend":vt,"is_spike":spike,"obv_trend":ot,"score":round(score,1),
            "score_details": details,
            "volume_history":[round(x,1)for x in df["volume"].iloc[-100:].tolist()]}

def analyze_fib(df):
    sub = df.iloc[-50:]
    high = float(sub["high"].max())
    low = float(sub["low"].min())
    cp = float(df["close"].iloc[-1])
    diff = high - low
    is_up = cp > (high+low)/2
    ret_levels = [0,0.236,0.382,0.5,0.618,0.786,1.0]
    ext_levels = [1.272,1.414,1.618,2.0,2.618]
    ret = {str(l): round(high-diff*l if is_up else low+diff*l, 4) for l in ret_levels}
    ext = {str(l): round(high+diff*(l-1.0) if is_up else low-diff*(l-1.0), 4) for l in ext_levels}
    levels = sorted(ret.values())
    ns = (0,0); nr = (0,0)
    for p in levels:
        if p <= cp: ns = (round(p/high,4) if high else 0, p)
    for p in levels:
        if p >= cp: nr = (round(p/high,4) if high else 0, p); break
    for p in sorted(ext.values()):
        if p > cp: nr = (nr[0] or p, p); break
    ds = (cp-ns[1])/cp if ns[1] else 1
    dr = (nr[1]-cp)/cp if nr[1] else 1
    pos = "near_support" if ds<0.02 else ("near_resistance" if dr<0.02 else "mid_range")
    score = 50.0
    details = []
    f618 = ret.get("0.618",0)
    golden_hit = False
    if f618 and abs(cp-f618)/cp < 0.03:
        if pos=="near_support":
            score += 20
            details.append({"factor": "golden_618_support", "impact": 20, "desc_en": f"Price near 0.618 golden ratio support (${f618:,.2f}) - ideal entry zone", "desc_zh": f"价格接近0.618黄金分割支撑位(${f618:,.2f}) - 理想入场区域"})
            golden_hit = True
        elif pos=="near_resistance":
            score -= 15
            details.append({"factor": "golden_618_resistance", "impact": -15, "desc_en": f"Price near 0.618 golden ratio resistance (${f618:,.2f}) - caution zone", "desc_zh": f"价格接近0.618黄金分割阻力位(${f618:,.2f}) - 注意风险"})
    for k in ["0.382","0.5","0.618"]:
        if ret.get(k,0) and abs(cp-ret[k])/cp < 0.015 and not golden_hit:
            score += 10
            details.append({"factor": f"near_retrace_{k}", "impact": 10, "desc_en": f"Price near {k} retracement level (${ret[k]:,.2f}) - potential bounce zone", "desc_zh": f"价格接近{k}回撤位(${ret[k]:,.2f}) - 潜在反弹区域"})
            break
    if pos=="near_support" and not details:
        details.append({"factor": "near_support", "impact": 5, "desc_en": f"Price near support level ${ns[1]:,.2f}", "desc_zh": f"价格接近支撑位${ns[1]:,.2f}"})
        score += 5
    elif pos=="near_resistance" and not details:
        details.append({"factor": "near_resistance", "impact": -5, "desc_en": f"Price near resistance level ${nr[1]:,.2f}", "desc_zh": f"价格接近阻力位${nr[1]:,.2f}"})
        score -= 5
    if not details:
        details.append({"factor": "mid_range", "impact": 0, "desc_en": f"Price in mid-range (support: ${ns[1]:,.2f}, resistance: ${nr[1]:,.2f})", "desc_zh": f"价格处于中段区域（支撑:${ns[1]:,.2f}，阻力:${nr[1]:,.2f}）"})
    score = max(0, min(100, score))
    return {"high":round(high,4),"low":round(low,4),"current":round(cp,4),
            "retracement":ret,"extension":ext,"support":[ns[0],ns[1]],
            "resistance":[nr[0],nr[1]],"position":pos,"score":round(score,1),
            "score_details": details}

WEIGHTS = {"macd":0.35,"volume":0.35,"fibonacci":0.30}
CONTRACT_WEIGHTS = {"macd":0.25,"rsi":0.20,"volume":0.20,"bollinger":0.20,"fibonacci":0.15}

# ============================================================
# New Indicators: RSI, Bollinger Bands, ATR
# ============================================================

def analyze_rsi(df, period=14):
    """RSI analysis with score"""
    close = df["close"]
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    current = float(rsi.iloc[-1])
    prev = float(rsi.iloc[-2]) if len(rsi) >= 2 else current

    score = 50.0
    details = []
    # RSI scoring for LONG direction
    if current < 30:
        score += 25
        details.append({"factor": "rsi_oversold", "impact_l": 25, "impact_s": -25,
                        "desc_en": f"RSI={current:.0f} oversold (<30) - bounce likely",
                        "desc_zh": f"RSI={current:.0f} 超卖区(<30) - 反弹概率高"})
    elif current < 40:
        score += 15
        details.append({"factor": "rsi_near_oversold", "impact_l": 15, "impact_s": 0,
                        "desc_en": f"RSI={current:.0f} near oversold - potential reversal zone",
                        "desc_zh": f"RSI={current:.0f} 接近超卖区 - 潜在反转区域"})
    elif current > 70:
        score -= 25
        details.append({"factor": "rsi_overbought", "impact_l": -25, "impact_s": 25,
                        "desc_en": f"RSI={current:.0f} overbought (>70) - pullback likely",
                        "desc_zh": f"RSI={current:.0f} 超买区(>70) - 回调概率高"})
    elif current > 60:
        score -= 10
        details.append({"factor": "rsi_near_overbought", "impact_l": -10, "impact_s": 0,
                        "desc_en": f"RSI={current:.0f} near overbought - caution",
                        "desc_zh": f"RSI={current:.0f} 接近超买区 - 注意风险"})
    else:
        details.append({"factor": "rsi_neutral", "impact_l": 0, "impact_s": 0,
                        "desc_en": f"RSI={current:.0f} neutral zone",
                        "desc_zh": f"RSI={current:.0f} 中性区域"})

    # RSI divergence check
    if len(df) >= 20:
        pr, ri = df["close"].iloc[-20:], rsi.iloc[-20:]
        if pr.iloc[-1] <= pr.min() and ri.iloc[-1] > ri.min() * 1.05:
            score += 15
            details.append({"factor": "rsi_bullish_div", "impact_l": 15, "impact_s": 0,
                            "desc_en": "RSI bullish divergence detected",
                            "desc_zh": "RSI底背离 - 价格新低但RSI走高"})
        elif pr.iloc[-1] >= pr.max() and ri.iloc[-1] < ri.max() * 0.95:
            score -= 15
            details.append({"factor": "rsi_bearish_div", "impact_l": -15, "impact_s": 0,
                            "desc_en": "RSI bearish divergence detected",
                            "desc_zh": "RSI顶背离 - 价格新高但RSI走低"})

    score = max(0, min(100, score))
    return {"rsi": round(current, 1), "score": round(score, 1), "score_details": details,
            "rsi_history": [round(x, 1) for x in rsi.iloc[-100:].tolist()]}


def analyze_bb(df, period=20, std=2):
    """Bollinger Bands analysis"""
    close = df["close"]
    mid = close.rolling(period).mean()
    std_dev = close.rolling(period).std()
    upper = mid + std * std_dev
    lower = mid - std * std_dev
    cp = float(close.iloc[-1])
    up = float(upper.iloc[-1])
    lo = float(lower.iloc[-1])
    mi = float(mid.iloc[-1])
    bw = (up - lo) / mi if mi > 0 else 0  # Bandwidth
    pct_b = (cp - lo) / (up - lo) if (up - lo) > 0 else 0.5

    score = 50.0
    details = []
    if pct_b < 0.1:
        score += 20
        details.append({"factor": "bb_lower_touch", "impact_l": 20, "impact_s": -20,
                        "desc_en": f"Price near lower BB (${lo:,.2f}) - oversold bounce zone",
                        "desc_zh": f"价格触及布林下轨(${lo:,.2f}) - 超卖反弹区域"})
    elif pct_b < 0.25:
        score += 10
        details.append({"factor": "bb_near_lower", "impact_l": 10, "impact_s": 0,
                        "desc_en": f"Price near lower BB range - potential support",
                        "desc_zh": f"价格接近布林下轨 - 潜在支撑"})
    elif pct_b > 0.9:
        score -= 20
        details.append({"factor": "bb_upper_touch", "impact_l": -20, "impact_s": 20,
                        "desc_en": f"Price near upper BB (${up:,.2f}) - overbought pullback zone",
                        "desc_zh": f"价格触及布林上轨(${up:,.2f}) - 超买回调区域"})
    elif pct_b > 0.75:
        score -= 10
        details.append({"factor": "bb_near_upper", "impact_l": -10, "impact_s": 0,
                        "desc_en": f"Price near upper BB range - potential resistance",
                        "desc_zh": f"价格接近布林上轨 - 潜在阻力"})

    # Squeeze detection
    if bw < 0.05:
        details.append({"factor": "bb_squeeze", "impact_l": 5, "impact_s": 5,
                        "desc_en": "BB squeeze - volatility contraction, breakout imminent",
                        "desc_zh": "布林带收窄 - 波动率收缩，即将突破"})
        score += 5

    score = max(0, min(100, score))
    return {"upper": round(up, 4), "middle": round(mi, 4), "lower": round(lo, 4),
            "pct_b": round(pct_b, 4), "bandwidth": round(bw, 4),
            "score": round(score, 1), "score_details": details}


def analyze_atr(df, period=14):
    """ATR - Average True Range for volatility-based stops"""
    high, low, close = df["high"], df["low"], df["close"]
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    current_atr = float(atr.iloc[-1])
    current_price = float(close.iloc[-1])
    atr_pct = current_atr / current_price if current_price > 0 else 0
    return {"atr": round(current_atr, 4), "atr_pct": round(atr_pct * 100, 2),
            "volatility": "high" if atr_pct > 0.03 else ("medium" if atr_pct > 0.015 else "low")}


# ============================================================
# Contract / Perpetual Analysis
# ============================================================

def contract_analysis(symbol, df, strategy=None):
    """Full contract analysis with LONG/SHORT dual-direction scoring"""
    if strategy is None:
        strategy = {}
    macd = analyze_macd(df)
    rsi = analyze_rsi(df)
    vol = analyze_volume(df)
    bb = analyze_bb(df)
    fib = analyze_fib(df)
    atr = analyze_atr(df)

    cp = float(df["close"].iloc[-1])

    # Calculate LONG score (bullish bias)
    long_score = (macd["strength"] * CONTRACT_WEIGHTS["macd"] +
                  rsi["score"] * CONTRACT_WEIGHTS["rsi"] +
                  vol["score"] * CONTRACT_WEIGHTS["volume"] +
                  bb["score"] * CONTRACT_WEIGHTS["bollinger"] +
                  fib["score"] * CONTRACT_WEIGHTS["fibonacci"])

    # SHORT score = 100 - long_score (inverse logic)
    short_score = 100 - long_score

    long_score = max(0, min(100, long_score))
    short_score = max(0, min(100, short_score))

    # Determine best direction
    if long_score >= 65:
        direction = "long"
        confidence = "strong" if long_score >= 80 else "normal"
    elif short_score >= 65:
        direction = "short"
        confidence = "strong" if short_score >= 80 else "normal"
    elif long_score >= 55:
        direction = "long"
        confidence = "weak"
    elif short_score >= 55:
        direction = "short"
        confidence = "weak"
    else:
        direction = "hold"
        confidence = "none"

    # ================================================================
    # Professional SL/TP: user strategy + market data + fees
    # ================================================================

    # User strategy overrides
    user_support = float(strategy.get("support", 0) or 0)
    user_resistance = float(strategy.get("resistance", 0) or 0)
    user_max_loss = float(strategy.get("max_loss", 2) or 2)
    user_target_rr = float(strategy.get("target_rr", 2) or 2)
    user_hold_periods = int(strategy.get("hold_periods", 24) or 24)
    user_leverage = int(strategy.get("leverage", 3) or 3)

    # Trading costs (OKX taker fees)
    TAKER_FEE = 0.0005          # 0.05% per trade
    TOTAL_FEE = TAKER_FEE * 2   # Open + Close = 0.10%
    FUNDING_RATE_EST = 0.0001   # Estimated funding rate per 8h (0.01%)

    entry_price = cp

    # ---- SL Calculation Priority ----
    # 1. User-provided support/resistance (best)
    # 2. Fibonacci levels (good)
    # 3. Volatility-based % (fallback)
    if direction == "long":
        if user_support > 0 and user_support < cp:
            stop_loss = round(user_support * 0.997, 4)  # Just under user support
            sl_source = "user_support"
        elif fib["support"][1] and fib["support"][1] < cp:
            stop_loss = round(fib["support"][1] * 0.995, 4)
            sl_source = "fib_support"
        else:
            sl_pct = user_max_loss / 100.0
            stop_loss = round(cp * (1 - sl_pct), 4)
            sl_source = f"{user_max_loss}%_max_loss"
    elif direction == "short":
        if user_resistance > 0 and user_resistance > cp:
            stop_loss = round(user_resistance * 1.003, 4)
            sl_source = "user_resistance"
        elif fib["resistance"][1] and fib["resistance"][1] > cp:
            stop_loss = round(fib["resistance"][1] * 1.005, 4)
            sl_source = "fib_resistance"
        else:
            sl_pct = user_max_loss / 100.0
            stop_loss = round(cp * (1 + sl_pct), 4)
            sl_source = f"{user_max_loss}%_max_loss"
    else:
        stop_loss = None
        sl_source = "none"

    # ---- TP Calculation ----
    if direction != "hold" and stop_loss:
        if direction == "long":
            sl_dist = (cp - stop_loss) / cp
            tp1_dist = sl_dist * user_target_rr
            take_profit_1 = round(cp * (1 + tp1_dist), 4)
            take_profit_2 = round(cp * (1 + tp1_dist * 1.8), 4)
            # If user resistance is above TP1, use it as TP2
            if user_resistance > take_profit_1 and user_resistance < take_profit_2:
                take_profit_2 = round(user_resistance, 4)
            elif user_resistance > take_profit_1:
                take_profit_1 = round(user_resistance * 0.995, 4)  # Just below resistance
        else:
            sl_dist = (stop_loss - cp) / cp
            tp1_dist = sl_dist * user_target_rr
            take_profit_1 = round(cp * (1 - tp1_dist), 4)
            take_profit_2 = round(cp * (1 - tp1_dist * 1.8), 4)
            if user_support < take_profit_1 and user_support > take_profit_2:
                take_profit_2 = round(user_support, 4)
            elif user_support < take_profit_1:
                take_profit_1 = round(user_support * 1.005, 4)
    else:
        take_profit_1 = None
        take_profit_2 = None

    # Actual SL% for display
    if stop_loss and direction != "hold":
        sl_display_pct = round(abs(cp - stop_loss) / cp * 100, 2)
        if direction == "long":
            tp1_display_pct = round((take_profit_1 - cp) / cp * 100, 2) if take_profit_1 else 0
        else:
            tp1_display_pct = round((cp - take_profit_1) / cp * 100, 2) if take_profit_1 else 0
    else:
        sl_display_pct = user_max_loss
        tp1_display_pct = sl_display_pct * user_target_rr

    # Fee-adjusted profitability
    pos_size = 100.0
    if stop_loss and take_profit_1 and direction != "hold":
        if direction == "long":
            sl_dist_pct = (cp - stop_loss) / cp
            tp_dist_pct = (take_profit_1 - cp) / cp
        else:
            sl_dist_pct = (stop_loss - cp) / cp
            tp_dist_pct = (cp - take_profit_1) / cp
        risk_loss = round(pos_size * sl_dist_pct, 2)
        fee_cost = round(pos_size * TOTAL_FEE, 2)
        funding_cost = round(pos_size * FUNDING_RATE_EST * user_hold_periods, 2)
        total_cost = round(fee_cost + funding_cost, 2)
        gross_profit = round(pos_size * tp_dist_pct, 2)
        net_profit = round(gross_profit - total_cost, 2)
        net_rr = round(net_profit / (risk_loss + total_cost), 2) if (risk_loss + total_cost) > 0 else 0
        be_move_pct = round(total_cost / pos_size * 100, 4)
    else:
        risk_loss = 0; fee_cost = 0; funding_cost = 0; total_cost = 0
        net_profit = 0; net_rr = 0; be_move_pct = 0

    # Raw R:R
    if stop_loss and take_profit_1 and direction != "hold":
        if direction == "long":
            raw_rr = round((take_profit_1 - cp) / (cp - stop_loss), 2) if cp > stop_loss else 0
        else:
            raw_rr = round((cp - take_profit_1) / (stop_loss - cp), 2) if stop_loss > cp else 0
    else:
        raw_rr = 0

    # Viability check
    viable = net_rr >= 1.2 and direction != "hold"

    # Signal description
    signal_map = {
        ("long", "strong"): ("strong_long", "强烈做多", "STRONG LONG"),
        ("long", "normal"): ("long", "做多", "LONG"),
        ("long", "weak"): ("weak_long", "弱做多", "WEAK LONG"),
        ("short", "strong"): ("strong_short", "强烈做空", "STRONG SHORT"),
        ("short", "normal"): ("short", "做空", "SHORT"),
        ("short", "weak"): ("weak_short", "弱做空", "WEAK SHORT"),
    }
    sig_key = (direction, confidence)
    sig = signal_map.get(sig_key, ("hold", "观望", "HOLD"))

    # Warnings
    warnings = []
    if macd["divergence"]:
        div_type_w = "底背离(看涨)" if macd["divergence"] == "bullish" else "顶背离(看跌)"
        warnings.append(f"MACD {div_type_w}")
    if atr["volatility"] == "high":
        warnings.append(f"高波动率(ATR={atr['atr_pct']}%)，建议使用较低杠杆(1-3x)")
    if direction == "long" and rsi["rsi"] > 70:
        warnings.append(f"RSI={rsi['rsi']:.0f}超买，做多追高风险较大")
    if direction == "short" and rsi["rsi"] < 30:
        warnings.append(f"RSI={rsi['rsi']:.0f}超卖，做空追低风险较大")
    if bb["bandwidth"] < 0.03:
        warnings.append("布林带极度收窄，可能出现剧烈突破，建议降低仓位")
    if direction != "hold" and not viable:
        warnings.append(f"扣除手续费({fee_cost}$)+资金费({funding_cost}$)后盈亏比仅1:{net_rr}，不满足最低要求")
    if total_cost > 0:
        warnings.append(f"交易成本预估：手续费${fee_cost:.2f}(0.1%) + 资金费率${funding_cost:.2f}({user_hold_periods}周期估算) = ${total_cost:.2f}，需价格波动>{be_move_pct}%才能盈利")

    closes = [round(x, 4) for x in df["close"].iloc[-200:].tolist()]
    dates = [str(x.date()) for x in df.index[-200:]]

    return {
        "symbol": symbol,
        "price": cp,
        "direction": direction,
        "confidence": confidence,
        "signal": sig[0],
        "signal_cn": sig[1],
        "signal_en": sig[2],
        "long_score": round(long_score, 1),
        "short_score": round(short_score, 1),
        "best_score": round(max(long_score, short_score), 1),
        "viable": viable,  # Whether the trade is viable after fees
        "indicators": {
            "macd": macd,
            "rsi": rsi,
            "volume": vol,
            "bollinger": bb,
            "fibonacci": fib,
            "atr": atr
        },
        "entry": {
            "price": entry_price,
            "direction": direction,
            "stop_loss": stop_loss,
            "stop_loss_pct": sl_display_pct,
            "stop_loss_source": sl_source,
            "take_profit_1": take_profit_1,
            "take_profit_1_pct": tp1_display_pct,
            "take_profit_2": take_profit_2,
            "take_profit_2_pct": round(tp1_display_pct * 1.8, 2),
            "risk_reward": raw_rr,
            "net_rr": net_rr,
            "user_support": user_support,
            "user_resistance": user_resistance,
        },
        "costs": {
            "taker_fee_pct": 0.05,
            "total_fee_pct": round(TOTAL_FEE * 100, 2),
            "funding_rate_est": round(FUNDING_RATE_EST * 100, 3),
            "funding_periods": user_hold_periods,
            "total_fee_usd": fee_cost,
            "total_funding_usd": funding_cost,
            "total_cost_usd": total_cost,
            "breakeven_move_pct": be_move_pct,
            "net_profit_usd": net_profit,
            "risk_loss_usd": round(risk_loss, 2),
        },
        "warnings": warnings,
        "chart": {"dates": dates, "prices": closes,
                  "rsi": rsi.get("rsi_history", []),
                  "bb_upper": [round(bb["upper"], 4)] * len(closes) if len(closes) > 0 else [],
                  "bb_lower": [round(bb["lower"], 4)] * len(closes) if len(closes) > 0 else []}
    }


def full_analysis(symbol, df):
    """Complete analysis combining all 3 indicators"""
    macd = analyze_macd(df)
    vol = analyze_volume(df)
    fib = analyze_fib(df)
    score = macd["strength"] * WEIGHTS["macd"] + \
            vol["score"] * WEIGHTS["volume"] + \
            fib["score"] * WEIGHTS["fibonacci"]

    ema20 = float(df["close"].ewm(span=20).mean().iloc[-1])
    ema50 = float(df["close"].ewm(span=50).mean().iloc[-1]) if len(df) >= 50 else ema20
    if ema20 > ema50 * 1.02:
        trend = "uptrend"
    elif ema20 < ema50 * 0.98:
        trend = "downtrend"
    else:
        trend = "sideways"

    if score >= 80:
        signal = "strong_buy"
    elif score >= 65:
        signal = "buy"
    elif score >= 55:
        signal = "weak_buy"
    elif score >= 45:
        signal = "hold"
    elif score >= 35:
        signal = "weak_sell"
    elif score >= 20:
        signal = "sell"
    else:
        signal = "strong_sell"

    if macd["divergence"] == "bullish" and "sell" in signal:
        signal = "buy"
        score += 10
    elif macd["divergence"] == "bearish" and "buy" in signal:
        signal = "sell"

    score = min(100, max(0, score))

    warnings = []
    if macd["divergence"]:
        div_type = "bottom" if macd["divergence"] == "bullish" else "top"
        warnings.append(f"MACD {div_type} divergence detected")
    if vol["is_spike"]:
        pc = (float(df["close"].iloc[-1]) - float(df["close"].iloc[-2])) / float(df["close"].iloc[-2])
        if pc > 0.05:
            warnings.append(f"Volume spike ({vol['volume_ratio']:.1f}x) - watch for distribution")
        elif pc < -0.05:
            warnings.append(f"Volume spike ({vol['volume_ratio']:.1f}x) - panic selling")
    if fib["position"] == "near_resistance":
        warnings.append(f"Near fib resistance at {fib['resistance'][1]}")
    elif fib["position"] == "near_support":
        warnings.append(f"Near fib support at {fib['support'][1]}")

    cp = fib["current"]
    entry = None
    exit_plan = None
    if "buy" in signal:
        sl = round(fib["support"][1] * 0.97, 4) if fib["support"][1] else None
        t1 = round(fib["resistance"][1], 4) if fib["resistance"][1] else None
        t2 = round(fib["extension"].get("1.618", 0), 4) or None
        entry = {
            "zone": f"{fib['support'][1]} - {cp}",
            "stop_loss": sl,
            "target_1": t1,
            "target_2": t2,
            "position_pct": 30 if score > 70 else 15
        }
    elif "sell" in signal:
        sl = round(fib["resistance"][1] * 1.03, 4) if fib["resistance"][1] else None
        t = round(fib["support"][1], 4) if fib["support"][1] else None
        exit_plan = {"stop_loss": sl, "target": t}

    closes = [round(x, 4) for x in df["close"].iloc[-200:].tolist()]
    dates = [str(x.date()) for x in df.index[-200:]]

    return {
        "symbol": symbol, "price": cp, "score": round(score, 1),
        "signal": signal, "trend": trend,
        "macd": macd, "volume": vol, "fibonacci": fib,
        "warnings": warnings, "entry": entry, "exit_plan": exit_plan,
        "chart": {"dates": dates, "prices": closes}
    }


# ============================================================
# Investment Plan Generator
# ============================================================

def _build_reason(a, action="buy"):
    """Build detailed reasoning string for a recommendation"""
    macd = a.get("macd", {})
    vol = a.get("volume", {})
    fib = a.get("fibonacci", {})
    score = a.get("score", 50)
    signal = a.get("signal", "hold")
    trend = a.get("trend", "sideways")
    warnings = a.get("warnings", [])

    signal_map = {
        "strong_buy": "强烈买入/Strong Buy", "buy": "买入/Buy",
        "weak_buy": "弱买入/Weak Buy", "hold": "持有/Hold",
        "weak_sell": "弱卖出/Weak Sell", "sell": "卖出/Sell",
        "strong_sell": "强烈卖出/Strong Sell"
    }
    trend_map = {
        "uptrend": "上升趋势/Uptrend", "downtrend": "下降趋势/Downtrend",
        "sideways": "横盘/Sideways"
    }

    parts = []

    # 1. Overall score and signal
    parts.append(f"【综合评分】{score:.0f}/100，信号：{signal_map.get(signal, signal)}")
    parts.append(f"【趋势判断】{trend_map.get(trend, trend)}")

    # 2. MACD details
    macd_details = macd.get("score_details", [])
    if macd_details:
        reason_items = []
        for d in macd_details:
            if d["impact"] != 0:
                sign = "+" if d["impact"] > 0 else ""
                reason_items.append(f"{d['desc_zh']}（{sign}{d['impact']}分）")
        if reason_items:
            parts.append(f"【MACD分析】(得分{macd.get('strength', 50):.0f}) {'; '.join(reason_items)}")
        else:
            parts.append(f"【MACD分析】(得分{macd.get('strength', 50):.0f}) 无明显信号")
    else:
        parts.append(f"【MACD分析】(得分{macd.get('strength', 50):.0f})")

    # 3. Volume details
    vol_details = vol.get("score_details", [])
    if vol_details:
        reason_items = []
        for d in vol_details:
            if d["impact"] != 0:
                sign = "+" if d["impact"] > 0 else ""
                reason_items.append(f"{d['desc_zh']}（{sign}{d['impact']}分）")
        if reason_items:
            parts.append(f"【量能分析】(得分{vol.get('score', 50):.0f}) {'; '.join(reason_items)}")
        else:
            parts.append(f"【量能分析】(得分{vol.get('score', 50):.0f}) 无明显信号")
    else:
        parts.append(f"【量能分析】(得分{vol.get('score', 50):.0f})")

    # 4. Fibonacci details
    fib_details = fib.get("score_details", [])
    if fib_details:
        reason_items = []
        for d in fib_details:
            if d["impact"] != 0:
                sign = "+" if d["impact"] > 0 else ""
                reason_items.append(f"{d['desc_zh']}（{sign}{d['impact']}分）")
        if reason_items:
            parts.append(f"【斐波那契分析】(得分{fib.get('score', 50):.0f}) {'; '.join(reason_items)}")
        else:
            parts.append(f"【斐波那契分析】(得分{fib.get('score', 50):.0f}) 无明显信号")
    else:
        parts.append(f"【斐波那契分析】(得分{fib.get('score', 50):.0f})")

    # 5. Entry/Exit specific details
    if action == "buy" and a.get("entry"):
        e = a["entry"]
        parts.append(f"【入场策略】入场区间：{e.get('zone', 'N/A')}，止损：{e.get('stop_loss', 'N/A')}，目标1：{e.get('target_1', 'N/A')}，目标2：{e.get('target_2', 'N/A')}，建议仓位：{e.get('position_pct', 'N/A')}%")
    elif action == "sell" and a.get("exit_plan"):
        e = a["exit_plan"]
        parts.append(f"【离场策略】止损：{e.get('stop_loss', 'N/A')}，目标：{e.get('target', 'N/A')}")

    # 6. Risk warnings
    if warnings:
        parts.append(f"【风险提示】{'；'.join(warnings)}")

    return "\n".join(parts)


def generate_plan(portfolio, analyses):
    """Generate investment plan from portfolio + analyses"""
    total = float(portfolio.get("total_assets", 10000))
    cash = float(portfolio.get("cash", total * 0.3))
    risk = portfolio.get("risk_level", "medium")
    holdings = portfolio.get("holdings", [])

    risk_map = {"low": 0.15, "medium": 0.30, "high": 0.50}
    max_pct = risk_map.get(risk, 0.30)
    investable = min(cash, total * max_pct)

    risk_label = {"low": "保守/Conservative", "medium": "平衡/Balanced", "high": "激进/Aggressive"}

    buy_list = sorted(
        [a for a in analyses if "buy" in a.get("signal", "")],
        key=lambda x: -x["score"]
    )
    sell_list = sorted(
        [a for a in analyses if "sell" in a.get("signal", "")],
        key=lambda x: x["score"]
    )

    plan = {"action": "hold", "allocations": [], "total_invest": 0,
            "reserve": round(cash, 2), "risk_level": risk,
            "risk_label": risk_label.get(risk, risk)}

    # Check existing holdings for sell signals
    for item in holdings:
        sym = item.get("symbol", "").upper()
        for a in sell_list:
            if a["symbol"].upper() == sym:
                reason = _build_reason(a, "sell")
                signal_cn_map = {"strong_sell":"强烈卖出","sell":"卖出","weak_sell":"弱卖出"}
                plan["allocations"].append({
                    "symbol": sym, "action": "sell",
                    "reason": reason,
                    "reason_short": f"评分{a['score']:.0f}，信号{signal_cn_map.get(a.get('signal','sell'), '卖出')}",
                    "score": a["score"], "signal": a["signal"],
                    "amount": item.get("amount", 0)
                })

    # Allocate to top buy signals
    if buy_list:
        top = buy_list[:min(3, len(buy_list))]
        total_score = sum(p["score"] for p in top)
        remaining = investable
        for p in top:
            weight = p["score"] / total_score if total_score > 0 else 1.0 / len(top)
            alloc = round(remaining * weight, 2)
            reason = _build_reason(p, "buy")
            plan["allocations"].append({
                "symbol": p["symbol"], "action": "buy",
                "alloc_amount": alloc, "entry_price": p["price"],
                "stop_loss": p.get("entry", {}).get("stop_loss") if p.get("entry") else None,
                "position_pct": p.get("entry", {}).get("position_pct", 15) if p.get("entry") else 15,
                "score": p["score"], "signal": p["signal"],
                "reason": reason,
                "reason_short": f"综合{p['score']:.0f}分 MACD{p['macd']['strength']:.0f} 量能{p['volume']['score']:.0f} 斐波{p['fibonacci']['score']:.0f}"
            })
            plan["total_invest"] += alloc
            remaining -= alloc

    plan["total_invest"] = round(plan["total_invest"], 2)
    plan["reserve"] = round(cash - plan["total_invest"], 2)

    # Market outlook
    avg_score = sum(a["score"] for a in analyses) / max(len(analyses), 1)
    up = sum(1 for a in analyses if a["trend"] == "uptrend")
    down = sum(1 for a in analyses if a["trend"] == "downtrend")
    if up > len(analyses) * 0.6:
        outlook = "bullish"
        outlook_cn = "看涨"
    elif down > len(analyses) * 0.6:
        outlook = "bearish"
        outlook_cn = "看跌"
    else:
        outlook = "mixed"
        outlook_cn = "震荡"

    plan["outlook"] = outlook
    plan["outlook_cn"] = outlook_cn
    plan["avg_score"] = round(avg_score, 1)
    plan["uptrend_count"] = up
    plan["downtrend_count"] = down

    # Build rich summary
    summary_parts = []
    summary_parts.append(f"市场展望：{outlook_cn}（{up}个币种上升趋势，{down}个下降趋势，综合评分{avg_score:.0f}/100）")
    summary_parts.append(f"投资风格：{risk_label.get(risk, risk)}，最大投入比例{max_pct*100:.0f}%，可用资金${investable:,.0f}")

    buys = [a for a in plan["allocations"] if a.get("action") == "buy"]
    sells = [a for a in plan["allocations"] if a.get("action") == "sell"]

    if buys:
        coins = "、".join([a["symbol"] for a in buys])
        summary_parts.append(f"建议买入：{coins}，总计投入${plan['total_invest']:,.0f}")
    if sells:
        coins = "、".join([a["symbol"] for a in sells])
        summary_parts.append(f"建议卖出：{coins}，信号偏空，建议减仓规避风险")
    if not buys and not sells:
        summary_parts.append("当前无明显买卖信号，建议观望等待更明确的机会")

    summary_parts.append(f"保留${plan['reserve']:,.0f}现金备用（{(plan['reserve']/total*100):.1f}%）")

    plan["summary"] = "；".join(summary_parts) + "。"

    return plan


# ============================================================
# Contract Investment Plan Generator
# ============================================================

def _build_contract_reason(a):
    """Build detailed reasoning for contract recommendation"""
    ind = a.get("indicators", {})
    macd = ind.get("macd", {})
    rsi = ind.get("rsi", {})
    vol = ind.get("volume", {})
    bb = ind.get("bollinger", {})
    fib = ind.get("fibonacci", {})
    atr = ind.get("atr", {})

    direction = a.get("direction", "hold")
    dir_cn = {"long": "做多", "short": "做空", "hold": "观望"}.get(direction, direction)
    conf_cn = {"strong": "强信号", "normal": "正常", "weak": "弱信号", "none": "无"}.get(a.get("confidence", "none"), "")
    entry = a.get("entry", {})

    parts = []
    parts.append(f"【方向判断】{dir_cn}（{conf_cn}），做多评分{a['long_score']:.0f}/做空评分{a['short_score']:.0f}")

    # Each indicator contribution
    macd_d = macd.get("score_details", [])
    if macd_d:
        items = [f"{d['desc_zh']}" for d in macd_d if d.get("impact_l", 0) != 0]
        if items: parts.append(f"【MACD】{'；'.join(items)}")

    rsi_d = rsi.get("score_details", [])
    if rsi_d:
        items = [f"{d['desc_zh']}" for d in rsi_d if d.get("impact_l", 0) != 0]
        if items: parts.append(f"【RSI】(rsi={rsi.get('rsi',0):.0f}) {'；'.join(items)}")

    vol_d = vol.get("score_details", [])
    if vol_d:
        items = [f"{d['desc_zh']}" for d in vol_d if d.get("impact_l", 0) != 0]
        if items: parts.append(f"【量能】{'；'.join(items)}")

    bb_d = bb.get("score_details", [])
    if bb_d:
        items = [f"{d['desc_zh']}" for d in bb_d if d.get("impact_l", 0) != 0]
        if items: parts.append(f"【布林带】{'；'.join(items)}")

    fib_d = fib.get("score_details", [])
    if fib_d:
        items = [f"{d['desc_zh']}" for d in fib_d if d.get("impact_l", 0) != 0]
        if items: parts.append(f"【斐波那契】{'；'.join(items)}")

    # Entry plan if not hold
    if direction != "hold" and entry:
        parts.append(f"【入场计划】入场${entry.get('price', 0):,.2f}，止损${entry.get('stop_loss', 0):,.2f}，止盈1 ${entry.get('take_profit_1', 0):,.2f}，止盈2 ${entry.get('take_profit_2', 0):,.2f}，盈亏比1:{entry.get('risk_reward', 0)}")
        parts.append(f"【风控】ATR={atr.get('atr', 0):.2f}，波动率{atr.get('volatility', 'medium')}，止损={entry.get('sl_atr_mult', 0)}×ATR")

    # Warnings
    warnings = a.get("warnings", [])
    if warnings:
        parts.append(f"【风险提示】{'；'.join(warnings)}")

    return "\n".join(parts)


def generate_contract_plan(portfolio, analyses):
    """Generate contract/perpetual investment plan with long/short directions"""
    total = float(portfolio.get("total_assets", 10000))
    cash = float(portfolio.get("cash", total * 0.3))
    risk = portfolio.get("risk_level", "medium")
    holdings = portfolio.get("holdings", [])

    # Contract risk: more conservative
    risk_map_contract = {"low": 0.05, "medium": 0.10, "high": 0.20}
    max_pct = risk_map_contract.get(risk, 0.10)
    investable = min(cash, total * max_pct)

    risk_label = {"low": "保守/Conservative(低杠杆)", "medium": "平衡/Balanced(中杠杆)", "high": "激进/Aggressive(高杠杆)"}
    leverage_map = {"low": "1-3x", "medium": "3-5x", "high": "5-10x"}

    # Separate long and short candidates
    long_list = sorted(
        [a for a in analyses if a.get("direction") == "long"],
        key=lambda x: -x["long_score"]
    )
    short_list = sorted(
        [a for a in analyses if a.get("direction") == "short"],
        key=lambda x: -x["short_score"]
    )

    plan = {"action": "hold", "allocations": [], "total_invest": 0,
            "reserve": round(cash, 2), "risk_level": risk,
            "risk_label": risk_label.get(risk, risk),
            "leverage_suggestion": leverage_map.get(risk, "3-5x"),
            "mode": "contract"}

    # Check holdings for close signals
    for item in holdings:
        sym = item.get("symbol", "").upper()
        for a in short_list:
            if a["symbol"].upper() == sym:
                reason = _build_contract_reason(a)
                plan["allocations"].append({
                    "symbol": sym, "action": "close_long",
                    "action_label": "平多/做空",
                    "reason": reason,
                    "score": a["best_score"],
                    "long_score": a["long_score"],
                    "short_score": a["short_score"],
                    "direction": "short",
                    "amount": item.get("amount", 0),
                    "entry": a.get("entry")
                })

    # Allocate to top long signals
    if long_list:
        remaining = investable * 0.6  # 60% to long
        top = long_list[:min(2, len(long_list))]
        total_ls = sum(p["long_score"] for p in top)
        for p in top:
            weight = p["long_score"] / total_ls if total_ls > 0 else 1.0 / len(top)
            alloc = round(remaining * weight, 2)
            reason = _build_contract_reason(p)
            plan["allocations"].append({
                "symbol": p["symbol"], "action": "long",
                "action_label": "做多 LONG",
                "alloc_amount": alloc, "entry_price": p["price"],
                "stop_loss": p.get("entry", {}).get("stop_loss"),
                "take_profit_1": p.get("entry", {}).get("take_profit_1"),
                "take_profit_2": p.get("entry", {}).get("take_profit_2"),
                "risk_reward": p.get("entry", {}).get("risk_reward", 0),
                "score": p["best_score"],
                "long_score": p["long_score"],
                "short_score": p["short_score"],
                "direction": "long",
                "reason": reason,
                "reason_short": f"做多{p['long_score']:.0f}分 MACD{p['indicators']['macd']['strength']:.0f} RSI{p['indicators']['rsi']['rsi']:.0f}"
            })
            plan["total_invest"] += alloc
            remaining -= alloc

    # Allocate to top short signals
    if short_list:
        remaining = investable * 0.4  # 40% to short
        top = short_list[:min(2, len(short_list))]
        total_ss = sum(p["short_score"] for p in top)
        for p in top:
            weight = p["short_score"] / total_ss if total_ss > 0 else 1.0 / len(top)
            alloc = round(remaining * weight, 2)
            reason = _build_contract_reason(p)
            plan["allocations"].append({
                "symbol": p["symbol"], "action": "short",
                "action_label": "做空 SHORT",
                "alloc_amount": alloc, "entry_price": p["price"],
                "stop_loss": p.get("entry", {}).get("stop_loss"),
                "take_profit_1": p.get("entry", {}).get("take_profit_1"),
                "take_profit_2": p.get("entry", {}).get("take_profit_2"),
                "risk_reward": p.get("entry", {}).get("risk_reward", 0),
                "score": p["best_score"],
                "long_score": p["long_score"],
                "short_score": p["short_score"],
                "direction": "short",
                "reason": reason,
                "reason_short": f"做空{p['short_score']:.0f}分 MACD{p['indicators']['macd']['strength']:.0f} RSI{p['indicators']['rsi']['rsi']:.0f}"
            })
            plan["total_invest"] += alloc
            remaining -= alloc

    plan["total_invest"] = round(plan["total_invest"], 2)
    plan["reserve"] = round(cash - plan["total_invest"], 2)

    # Market outlook from contract perspective
    avg_long = sum(a["long_score"] for a in analyses) / max(len(analyses), 1)
    avg_short = sum(a["short_score"] for a in analyses) / max(len(analyses), 1)
    long_count = len(long_list)
    short_count = len(short_list)

    if avg_long > avg_short + 10:
        outlook = "bullish"
        outlook_cn = "偏多"
    elif avg_short > avg_long + 10:
        outlook = "bearish"
        outlook_cn = "偏空"
    else:
        outlook = "mixed"
        outlook_cn = "震荡"

    plan["outlook"] = outlook
    plan["outlook_cn"] = outlook_cn
    plan["avg_long_score"] = round(avg_long, 1)
    plan["avg_short_score"] = round(avg_short, 1)
    plan["long_candidates"] = long_count
    plan["short_candidates"] = short_count

    # Build summary
    summary_parts = []
    summary_parts.append(f"合约市场展望：{outlook_cn}（做多均分{avg_long:.0f}，做空均分{avg_short:.0f}，{long_count}个做多候选，{short_count}个做空候选）")
    summary_parts.append(f"风险等级：{risk_label.get(risk, risk)}，建议杠杆{leverage_map.get(risk, '3-5x')}，最大投入{max_pct*100:.0f}%（${investable:,.0f}）")

    long_alloc = [a for a in plan["allocations"] if a.get("action") == "long"]
    short_alloc = [a for a in plan["allocations"] if a.get("action") == "short"]
    close_alloc = [a for a in plan["allocations"] if a.get("action") == "close_long"]

    if long_alloc:
        coins = "、".join([a["symbol"] for a in long_alloc])
        summary_parts.append(f"建议做多：{coins}")
    if short_alloc:
        coins = "、".join([a["symbol"] for a in short_alloc])
        summary_parts.append(f"建议做空：{coins}")
    if close_alloc:
        coins = "、".join([a["symbol"] for a in close_alloc])
        summary_parts.append(f"建议平多/反手：{coins}")
    if not long_alloc and not short_alloc:
        summary_parts.append("当前无明显合约信号，建议观望等待方向明确")

    summary_parts.append(f"保留${plan['reserve']:,.0f}作为保证金备用")

    plan["summary"] = "；".join(summary_parts) + "。"

    return plan


# ============================================================
# Flask Routes
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    proxy_info = list(PROXY.values())[0] if PROXY else "none"
    preferred = _get_preferred_exchange()
    return jsonify({
        "demo": DEMO_MODE,
        "network": "ok" if _network_ok else "offline",
        "proxy": proxy_info,
        "preferred_exchanges": preferred,
        "message": "Live data" if _network_ok else "Demo mode - checking network..."
    })


@app.route("/api/proxy", methods=["POST"])
def api_set_proxy():
    """Set proxy from web UI"""
    global PROXY
    try:
        data = request.get_json()
        proxy_url = data.get("proxy", "").strip()
        if proxy_url:
            # Test the proxy
            try:
                proxies = {"http": proxy_url, "https": proxy_url}
                r = requests.get("https://api.binance.com/api/v3/ping", timeout=3, proxies=proxies)
                if r.status_code == 200:
                    PROXY = {"http": proxy_url, "https": proxy_url}
                    # Save to config
                    try:
                        with open("config.json", "r") as f:
                            cfg = json.load(f)
                        cfg["proxy"] = proxy_url
                        with open("config.json", "w") as f:
                            json.dump(cfg, f, indent=4, ensure_ascii=False)
                    except: pass
                    # Reset network cache
                    global _network_checked_at
                    _network_checked_at = 0
                    return jsonify({"success": True, "message": f"Proxy set: {proxy_url}"})
                else:
                    return jsonify({"success": False, "message": f"Proxy connected but returned HTTP {r.status_code}"})
            except Exception as e:
                return jsonify({"success": False, "message": f"Proxy test failed: {str(e)}"})
        else:
            # Clear proxy
            PROXY = None
            try:
                with open("config.json", "r") as f:
                    cfg = json.load(f)
                cfg["proxy"] = ""
                with open("config.json", "w") as f:
                    json.dump(cfg, f, indent=4, ensure_ascii=False)
            except: pass
            return jsonify({"success": True, "message": "Proxy cleared"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route("/api/coins")
def api_coins():
    return jsonify(CRYPTO_LIST)


@app.route("/api/quote/<symbol>")
def api_quote(symbol):
    try:
        return jsonify(get_price(symbol))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/batch_quotes")
def api_batch_quotes():
    syms = request.args.get("symbols", "").split(",")
    syms = [s.strip() for s in syms if s.strip()][:20]
    if not syms:
        syms = CRYPTO_LIST[:14]
    try:
        results = batch_prices(syms)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze/<symbol>")
def api_analyze(symbol):
    try:
        interval = request.args.get("interval", "1d")
        df = get_klines(symbol, interval=interval, limit=200)
        if df is None or df.empty:
            return jsonify({"error": f"No data for {symbol}"}), 500
        result = full_analysis(symbol, df)
        try:
            info = get_price(symbol)
            result["name"] = info.get("name", symbol)
            result["change_pct"] = info.get("change_pct", 0)
        except Exception:
            result["name"] = symbol
            result["change_pct"] = 0
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/contract/<symbol>")
def api_contract(symbol):
    """Contract/perpetual analysis with LONG/SHORT dual-direction scoring"""
    try:
        interval = request.args.get("interval", "1h")
        strategy = request.args.get("strategy", None)
        if strategy:
            strategy = json.loads(strategy) if isinstance(strategy, str) else strategy
        df = get_klines(symbol, interval=interval, limit=200)
        if df is None or df.empty:
            return jsonify({"error": f"No data for {symbol}"}), 500
        result = contract_analysis(symbol, df, strategy)
        try:
            info = get_price(symbol)
            result["name"] = info.get("name", symbol)
            result["change_pct"] = info.get("change_pct", 0)
        except Exception:
            result["name"] = symbol
            result["change_pct"] = 0
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/plan", methods=["POST"])
def api_plan():
    try:
        data = request.get_json()
        portfolio = data.get("portfolio", {})
        symbols = data.get("symbols", CRYPTO_LIST[:10])
        mode = data.get("mode", "spot")
        strategy = data.get("strategy", {})

        analyses = []
        def _analyze_one(sym):
            try:
                interval = "1h" if mode == "contract" else "1d"
                df = get_klines(sym, interval=interval, limit=150)
                if df is not None and not df.empty:
                    if mode == "contract":
                        result = contract_analysis(sym, df, strategy)
                    else:
                        result = full_analysis(sym, df)
                    try:
                        info = get_price(sym)
                        result["name"] = info.get("name", sym)
                        result["change_pct"] = info.get("change_pct", 0)
                    except Exception:
                        result["name"] = sym
                        result["change_pct"] = 0
                    return result
            except Exception:
                pass
            return None

        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(_analyze_one, s): s for s in symbols}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    analyses.append(result)

        if mode == "contract":
            analyses.sort(key=lambda x: -x["best_score"])
            plan = generate_contract_plan(portfolio, analyses)
        else:
            analyses.sort(key=lambda x: -x["score"])
            plan = generate_plan(portfolio, analyses)
        return jsonify({"plan": plan, "analyses": analyses, "mode": mode})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# OKX Trading Module - Execute investment plans on OKX
# ============================================================

import hmac as _hmac
import base64 as _b64
import urllib.parse as _urlparse

OKX_REST = "https://www.okx.com"

def _load_okx_credentials():
    """Load OKX API credentials from config"""
    try:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        key = cfg.get("okx_api_key", "").strip()
        secret = cfg.get("okx_api_secret", "").strip()
        passphrase = cfg.get("okx_api_passphrase", "").strip()
        if key and secret and passphrase:
            return key, secret, passphrase
    except Exception:
        pass
    return None, None, None

def _okx_make_request(method, path, params=None):
    """Make a signed OKX API request - self-contained signing logic"""
    key, secret, passphrase = _load_okx_credentials()
    if not key:
        return {"error": "No API credentials configured"}

    # 1. Sync time from OKX
    try:
        r = requests.get(f"{OKX_REST}/api/v5/public/time", timeout=3,
                        proxies=PROXY if PROXY else None)
        server_ts_ms = int(r.json()["data"][0]["ts"])
    except:
        server_ts_ms = int(time.time() * 1000)

    # 2. Build request
    url = f"{OKX_REST}{path}"
    if params and method == "GET":
        url += "?" + _urlparse.urlencode(params)

    body_str = json.dumps(params) if params and method == "POST" else ""

    # 3. Sign
    sign_msg = f"{server_ts_ms}{method}{path}"
    if body_str:
        sign_msg += body_str
    signature = _b64.b64encode(
        _hmac.new(secret.encode("utf-8"), sign_msg.encode("utf-8"), "sha256").digest()
    ).decode("utf-8")

    # 4. Headers
    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": str(server_ts_ms),
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # 5. Execute
    try:
        req_kwargs = {"headers": headers, "timeout": 5}
        if PROXY:
            req_kwargs["proxies"] = PROXY
        if method == "GET":
            resp = requests.get(url, **req_kwargs)
        else:
            resp = requests.post(url, data=body_str, **req_kwargs)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


# ---- Account & Balance ----

def okx_get_balance():
    """Get account balance"""
    result = _okx_make_request("GET", "/api/v5/account/balance")
    if result.get("code") == "0":
        details = result.get("data", [{}])[0].get("details", [])
        balances = []
        for d in details:
            eq = float(d.get("eqUsd", 0) or d.get("eq", 0) or 0)
            if eq > 1:
                balances.append({
                    "ccy": d.get("ccy", ""),
                    "cash": d.get("cashBal", "0"),
                    "frozen": d.get("frozenBal", "0"),
                    "equity_usd": round(eq, 2),
                    "avail": d.get("availBal", "0")
                })
        return {"balances": balances, "total_usd": round(sum(b["equity_usd"] for b in balances), 2)}
    return {"error": result.get("msg", "Unknown error")}


def okx_get_positions(inst_type="SWAP"):
    """Get open positions"""
    result = _okx_make_request("GET", "/api/v5/account/positions", {"instType": inst_type})
    if result.get("code") == "0":
        positions = []
        for p in result.get("data", []):
            positions.append({
                "instId": p.get("instId"),
                "posSide": p.get("posSide"),
                "pos": p.get("pos"),
                "avgPx": p.get("avgPx"),
                "markPx": p.get("markPx"),
                "upl": p.get("upl"),
                "uplPct": p.get("uplRatio"),
                "lever": p.get("lever"),
                "margin": p.get("margin"),
                "liqPx": p.get("liqPx"),
            })
        return {"positions": positions}
    return {"error": result.get("msg", "Unknown error")}


# ---- Spot Trading ----

def okx_place_spot_buy(symbol, amount_usd):
    """Place spot market buy order"""
    instId = f"{symbol.upper()}-USDT"
    body = {
        "instId": instId,
        "tdMode": "cash",
        "side": "buy",
        "ordType": "market",
        "sz": str(round(amount_usd, 2))
    }
    result = _okx_make_request("POST", "/api/v5/trade/order", body)
    if result.get("code") == "0":
        return {"success": True, "orderId": result["data"][0]["ordId"], "msg": result["data"][0].get("sMsg", "")}
    return {"success": False, "error": result.get("msg", "Unknown error")}


def okx_place_spot_sell(symbol, amount):
    """Place spot market sell order (by quantity)"""
    instId = f"{symbol.upper()}-USDT"
    body = {
        "instId": instId,
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "sz": str(amount)
    }
    result = _okx_make_request("POST", "/api/v5/trade/order", body)
    if result.get("code") == "0":
        return {"success": True, "orderId": result["data"][0]["ordId"], "msg": result["data"][0].get("sMsg", "")}
    return {"success": False, "error": result.get("msg", "Unknown error")}


# ---- Contract / Perpetual Trading ----

def okx_set_leverage(symbol, leverage, pos_side="long"):
    """Set leverage for perpetual contract"""
    instId = f"{symbol.upper()}-USDT-SWAP"
    # Map position side
    mgnMode = "isolated"
    lever = min(max(int(leverage), 1), 125)
    body = {
        "instId": instId,
        "lever": str(lever),
        "mgnMode": mgnMode
    }
    # Set leverage for specific side
    if pos_side in ("long", "short"):
        body["posSide"] = pos_side

    result = _okx_make_request("POST", "/api/v5/account/set-leverage", body)
    if result.get("code") == "0":
        return {"success": True, "leverage": lever, "mgnMode": mgnMode}
    return {"success": False, "error": result.get("msg", "Already set" if result.get("code") == "51000" else "Unknown error")}


def okx_place_contract_order(symbol, side, amount_usd, leverage=5, sl_price=None, tp_price=None):
    """
    Place perpetual contract order with SL/TP

    Args:
        symbol: e.g. "BTC"
        side: "long" or "short"
        amount_usd: position size in USD
        leverage: leverage multiplier
        sl_price: stop loss price (optional)
        tp_price: take profit price (optional)
    """
    instId = f"{symbol.upper()}-USDT-SWAP"
    posSide = "long" if side == "long" else "short"
    tradeSide = "buy" if side == "long" else "sell"

    # 1. Set leverage
    lev_result = okx_set_leverage(symbol, leverage, posSide)

    # 2. Calculate contract size
    # Get current price for sz calculation
    try:
        price_data = get_price(symbol)
        current_price = price_data["price"]
    except:
        current_price = 0
    if current_price <= 0:
        return {"success": False, "error": "Cannot get current price"}

    # sz = amount_usd * leverage / current_price (contracts)
    # Or use sz = amount_usd for USDT-margined contracts (simpler)
    sz = round(amount_usd, 2)

    body = {
        "instId": instId,
        "tdMode": "isolated",
        "side": tradeSide,
        "posSide": posSide,
        "ordType": "market",
        "sz": str(sz),
    }

    result = _okx_make_request("POST", "/api/v5/trade/order", body)
    if result.get("code") != "0":
        return {"success": False, "error": result.get("msg", "Order failed")}

    order_id = result["data"][0]["ordId"]

    # 3. Place SL/TP if provided (as algo orders)
    sl_result = None
    tp_result = None
    if sl_price:
        sl_side = "sell" if side == "long" else "buy"
        sl_body = {
            "instId": instId,
            "tdMode": "isolated",
            "side": sl_side,
            "posSide": posSide,
            "ordType": "conditional",
            "sz": str(sz),
            "tpTriggerPx": "",
            "tpOrdPx": "",
            "slTriggerPx": str(round(sl_price, 2)),
            "slOrdPx": str(round(sl_price * 0.999 if side == "long" else sl_price * 1.001, 2)),
        }
        sl_result = _okx_make_request("POST", "/api/v5/trade/order-algo", sl_body)

    if tp_price:
        tp_side = "sell" if side == "long" else "buy"
        tp_body = {
            "instId": instId,
            "tdMode": "isolated",
            "side": tp_side,
            "posSide": posSide,
            "ordType": "conditional",
            "sz": str(sz),
            "tpTriggerPx": str(round(tp_price, 2)),
            "tpOrdPx": str(round(tp_price * 0.999 if side == "long" else tp_price * 1.001, 2)),
            "slTriggerPx": "",
            "slOrdPx": "",
        }
        tp_result = _okx_make_request("POST", "/api/v5/trade/order-algo", tp_body)

    return {
        "success": True,
        "orderId": order_id,
        "symbol": symbol,
        "side": side,
        "amount": amount_usd,
        "leverage": leverage,
        "entry_price": current_price,
        "sl": sl_price,
        "tp": tp_price,
        "sl_order": sl_result.get("data", [{}])[0].get("algoId") if sl_result and sl_result.get("code") == "0" else None,
        "tp_order": tp_result.get("data", [{}])[0].get("algoId") if tp_result and tp_result.get("code") == "0" else None,
        "msg": result["data"][0].get("sMsg", "")
    }


# ============================================================
# Trading API Endpoints
# ============================================================

@app.route("/api/okx/check")
def api_okx_check():
    """Check if OKX API credentials are configured and working"""
    key, secret, passphrase = _load_okx_credentials()
    if not key:
        return jsonify({"configured": False, "valid": False, "message": "Set okx_api_key/secret/passphrase in config.json"})

    import hmac as _h, base64 as _b

    # Step 1: Test basic connectivity
    try:
        r_time = requests.get(f"{OKX_REST}/api/v5/public/time", timeout=5,
                              proxies=PROXY if PROXY else None)
        server_time = r_time.json().get("data", [{}])[0].get("ts", "0")
    except Exception as e:
        return jsonify({"configured": True, "valid": False, "error": f"Cannot reach OKX: {str(e)}"})

    # Step 2: Sign and request
    ts = server_time
    msg = f"{ts}GET/api/v5/account/balance"
    sign = _b.b64encode(_h.new(secret.encode("utf-8"), msg.encode("utf-8"), "sha256").digest()).decode("utf-8")

    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }

    try:
        r_bal = requests.get(f"{OKX_REST}/api/v5/account/balance", headers=headers, timeout=5,
                             proxies=PROXY if PROXY else None)
        result = r_bal.json()
        return jsonify({
            "configured": True,
            "valid": result.get("code") == "0",
            "code": result.get("code"),
            "msg": result.get("msg"),
            "debug_ts": ts,
            "debug_used_proxy": bool(PROXY),
            "balance": result.get("data") if result.get("code") == "0" else None
        })
    except Exception as e:
        return jsonify({"configured": True, "valid": False, "error": f"Request failed: {str(e)}"})


@app.route("/api/okx/balance")
def api_okx_balance():
    """Get OKX account balance"""
    result = okx_get_balance()
    return jsonify(result)


@app.route("/api/okx/positions")
def api_okx_positions():
    """Get OKX open positions"""
    inst_type = request.args.get("type", "SWAP")
    result = okx_get_positions(inst_type)
    return jsonify(result)


@app.route("/api/okx/execute_spot", methods=["POST"])
def api_okx_execute_spot():
    """Execute spot plan orders"""
    try:
        data = request.get_json()
        allocations = data.get("allocations", [])
        results = []
        for a in allocations:
            symbol = a.get("symbol", "")
            action = a.get("action", "")
            amount = a.get("alloc_amount") or a.get("amount", 0)
            if not symbol or amount <= 0:
                continue
            if action == "buy":
                res = okx_place_spot_buy(symbol, amount)
            elif action == "sell":
                res = okx_place_spot_sell(symbol, amount)
            else:
                continue
            res["symbol"] = symbol
            res["action"] = action
            results.append(res)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/okx/execute_contract", methods=["POST"])
def api_okx_execute_contract():
    """Execute contract plan orders with SL/TP"""
    try:
        data = request.get_json()
        allocations = data.get("allocations", [])
        leverage = data.get("leverage", 5)
        results = []
        for a in allocations:
            symbol = a.get("symbol", "")
            direction = a.get("direction", "")
            amount = a.get("alloc_amount", 0)
            sl = a.get("stop_loss")
            tp = a.get("take_profit_1")
            if not symbol or amount <= 0 or direction not in ("long", "short"):
                continue
            res = okx_place_contract_order(
                symbol=symbol,
                side=direction,
                amount_usd=float(amount),
                leverage=int(leverage),
                sl_price=float(sl) if sl else None,
                tp_price=float(tp) if tp else None
            )
            results.append(res)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    import webbrowser
    print("=" * 55)
    print("  Smart Investment Advisor v3")
    print("  Multi-source: OKX(欧易) / Binance / Bybit / MEXC")
    print("=" * 55)
    pref = _get_preferred_exchange()
    print(f"  Exchange priority: {' → '.join(pref)}")
    if PROXY:
        print(f"  Proxy: {list(PROXY.values())[0]}")
    else:
        print(f"  ! No proxy. Configure proxy in config.json")
        print(f"  ! Common ports: 7890(Clash) 10809(V2Ray) 1080(SS)")
    print(f"  Config: edit config.json to change exchange order or proxy")
    print(f"\n  Opening http://127.0.0.1:5000 ...")
    # Auto-open browser after a short delay
    threading.Timer(1.5, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(debug=False, host="127.0.0.1", port=5000, threaded=True)
