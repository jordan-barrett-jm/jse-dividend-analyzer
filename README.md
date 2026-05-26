# JSE Dividend Analysis

A web app for analysing dividend history, consistency, and yield across stocks listed on the Jamaica Stock Exchange.

## Features

- **Consistency Rankings** — every listed stock ranked by how reliably it has paid dividends over the last 5 years
- **Yield at Date** — trailing 12-month dividend yield for any historical date, ranked highest to lowest
- **Stock Detail** — share price chart, dividend payment history, and a point-in-time yield chart (monthly or quarterly cadence)
- **Compare** — side-by-side price and dividend charts for multiple stocks
- **Live pipeline** — refresh data from the UI; incremental by default (only fetches what's missing), with a force re-scrape option

## Data Sources

| Data | Source |
|------|--------|
| Dividend history | JamStockEx company pages (`jamstockex.com`) |
| Weekly closing prices | JSE trade quotes (`jamstockex.com/trading/trade-quotes/`) |

Prices are collected weekly (Fridays). Dividend and price data go back up to 5 years.

## Setup

```bash
# 1. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create the data directory (populated on first pipeline run)
mkdir -p data
```

## Running

```bash
# Start the web app
venv/bin/uvicorn app:app --port 8000 --reload

# Open in browser
open http://localhost:8000
```

On first launch, click **Refresh Data** to run the pipeline and populate `data/dividends.csv` and `data/prices.csv`. Subsequent runs are incremental — only missing dates are fetched.

## Running the Pipeline Standalone

```bash
# Incremental (default) — skips already-fetched weeks
venv/bin/python pipeline.py

# Force full re-scrape
venv/bin/python -c "import asyncio; from pipeline import run_pipeline; asyncio.run(run_pipeline(print, force=True))"
```

## Project Structure

```
├── pipeline.py        # Data collection: JSE dividends + weekly prices
├── app.py             # FastAPI backend + analysis logic
├── static/
│   └── index.html     # Single-page dashboard (Tailwind + Chart.js via CDN)
├── data/              # Generated — gitignored
│   ├── dividends.csv
│   └── prices.csv
└── legacy/            # Superseded scripts kept for reference
```

## API Reference

| Endpoint | Description |
|----------|-------------|
| `GET /api/consistency` | All stocks ranked by dividend consistency (`?years=5&top=100`) |
| `GET /api/yield` | Yield rankings at a date (`?date=YYYY-MM-DD&min_payments=2&top=100`) |
| `GET /api/stock/{symbol}` | Full dividend and price history for a symbol |
| `GET /api/stock/{symbol}/yield-history` | Point-in-time yield series (`?cadence=monthly\|quarterly`) |
| `GET /api/stocks` | All symbols with metadata |
| `GET /api/data-info` | Row counts and date ranges for loaded data |
| `POST /api/run` | Trigger pipeline (`?force=true` to re-scrape everything) |
| `GET /api/run/status` | Pipeline running state and log tail |
