# Smart Investment Advisor 🚀

Multi-exchange crypto analysis tool with MACD/RSI/BB/Fibonacci indicators and OKX trading integration.

智能多交易所加密货币分析工具，支持 MACD/RSI/布林带/斐波那契等技术指标，集成 OKX 交易功能。

---

## 📥 How to Use / 如何使用

### Method 1: Download & Run (Easy / 简单 - No Python needed / 无需安装 Python)

1. Go to [Releases](../../releases) page
2. Download `SmartInvestmentAdvisor.zip`
3. Extract the zip file
4. Double-click `启动投资顾问.bat`
5. Open browser → `http://127.0.0.1:5000`

> ⚠️ First time: copy `config.example.json` to `config.json` and edit if needed.

---

### Method 2: Run from Source (For Developers / 开发者)

```bash
git clone https://github.com/hermit0610/smart-investment-advisors.git
cd smart-investment-advisors/investment_app
pip install -r requirements.txt
copy config.example.json config.json   # Edit config if needed
python app.py
```

Then open `http://127.0.0.1:5000`

---

## ⚙️ Configuration / 配置

Edit `config.json`:

| Key | Description |
|-----|-------------|
| `proxy` | Proxy URL (auto-detected if empty / 留空自动检测) |
| `exchange_order` | Exchange priority order / 交易所优先级 |
| `okx_api_key` | OKX API Key (optional, for trading / 可选，用于交易) |

---

## ✨ Features / 功能

- 📊 **Multi-exchange data** - Binance, OKX, Bybit, MEXC, KuCoin, Gate, CoinGecko
- 📈 **Technical indicators** - MACD, RSI, Bollinger Bands, Fibonacci
- 🔌 **Auto proxy detection** - Works with Clash, V2Ray, Shadowsocks
- 💹 **OKX trading** - Place orders directly (optional API key)
- 🎯 **Real-time signals** - Buy/Sell recommendations

---

## 🖥 Requirements / 环境要求

- Windows 10/11 (64-bit)
- Or: Python 3.10+ with pip
