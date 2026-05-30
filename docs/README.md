# Smart Investment Advisor

**Multi-exchange crypto analysis tool** with MACD/RSI/Bollinger Bands/Fibonacci indicators.

## Live Demo

Visit: **https://hermit0610.github.io/smart-investment-advisors**

## Features

- Multi-exchange data - Binance + CoinGecko APIs (CORS-friendly)
- Technical indicators - MACD, RSI, Bollinger Bands, Fibonacci, ATR
- Spot analysis - Buy/Sell signals with weighted scoring
- Contract analysis - LONG/SHORT dual-direction scoring with SL/TP
- Investment plan - Auto-generate portfolio allocation plan
- Interactive charts - Plotly.js price, MACD, RSI charts
- Demo mode - Works offline with realistic demo data
- Bilingual - EN/CN toggle

## How to Use

1. Open https://hermit0610.github.io/smart-investment-advisors
2. Dashboard - Real-time prices for 14 crypto pairs
3. Analysis - Pick a coin + interval, get full technical analysis
4. Contract - LONG/SHORT signals with entry/exit plans
5. Plan - Configure assets and generate investment plan

## Data Sources

| Source | Type | API Key |
|--------|------|--------|
| Binance | Primary (prices, klines) | None |
| CoinGecko | Fallback (prices) | None |
| Demo | Offline fallback | None |

## Local Development

```
git clone https://github.com/hermit0610/smart-investment-advisors.git
cd smart-investment-advisors
rem Just open index.html in your browser
```

Or use any static server:
```
python -m http.server 8080
```


## Deploy to GitHub Pages

1. Fork this repo
2. Go to Settings > Pages
3. Source: Deploy from a branch
4. Branch: main, folder: /docs (or / root)
5. Save. Your site is live!

## Disclaimer

Not financial advice. DYOR.

## License

MIT
