# JSE Dividend Analysis — Claude Guide

## What This Is

A FastAPI web app that scrapes JSE dividend and price data and presents it as an interactive dashboard. The three files that matter are `pipeline.py`, `app.py`, and `static/index.html`. Everything else is either generated data or legacy.

## Running the App

```bash
# Start the server (already running? check: pgrep -f "uvicorn app:app")
venv/bin/uvicorn app:app --port 8000

# The app is at http://localhost:8000
# Use .claude/launch.json for the preview panel (may not work due to sandbox)
```

If the server is already running from a previous session, kill it first:
```bash
kill $(pgrep -f "uvicorn app:app")
```

## Architecture

### `pipeline.py`
Two independent scrapers run concurrently via `asyncio.gather`:

1. **Dividends** — fetches all listed companies from JamStockEx (2 markets), then scrapes each company's dividend table. Outputs `data/dividends.csv`.

2. **Prices** — fetches weekly closing prices (Fridays only) from the JSE trade quotes page:
   `https://www.jamstockex.com/trading/trade-quotes/?market=50&date=YYYY-MM-DD`
   Returns ~129 symbols per date. Incremental: skips dates already in `data/prices.csv`. Outputs `data/prices.csv`.

Key behaviours:
- `run_pipeline(force=False)` — incremental, skips cached data
- `run_pipeline(force=True)` — full re-scrape from scratch
- Semaphore of 3 for price fetches + 0.4–0.9s delay per request — JSE blocks aggressive scrapers (403)
- `data/prices.csv` is the price cache; `data/dividends.csv` is always fully re-scraped

### `app.py`
FastAPI app. Key analysis functions:

- `compute_consistency(divs, years=5)` — scores each stock by how many of the last N years had at least one dividend payment
- `compute_yield_at_date(divs, prices, as_of, trailing_months=12)` — trailing 12m dividends / price, for all stocks at a given date
- `get_stock_yield_history(symbol, cadence)` — resamples price data monthly or quarterly, computes yield at each point
- `_price_near(prices_df, symbol, date, window_days=30)` — finds the most recent price within 30 days of a target date

All API responses pass through `_clean()` to replace NaN/inf with None before JSON serialisation.

### `static/index.html`
Single-page app, no build step. Uses Tailwind CSS and Chart.js from CDN.

Three tabs:
- **Consistency Rankings** — searchable/sortable table, consistency score bar
- **Yield at Date** — date picker defaults to `_priceLastDate` (latest available price); warns if date exceeds price data range
- **Compare** — multi-select stock comparison

Stock detail modal opens on any row click and shows:
1. Trailing 12m yield over time (line chart, monthly/quarterly toggle)
2. Share price (line chart)
3. Dividend payments (bar chart)
4. Full payment history table

Pipeline status is polled every 3s when running; the Logs panel shows the live tail.

## Data Files

| File | Description |
|------|-------------|
| `data/dividends.csv` | `company_name, symbol, sector, market, record_date, ex_date, payment_date, dividend_amount` |
| `data/prices.csv` | `date, symbol, price` — weekly Fridays, ~129 symbols, up to 5 years |

Both files are gitignored. On a fresh clone, run the pipeline to generate them.

## Known Quirks

- **JSE rate limiting**: The trade quotes endpoint returns 403 if hit too fast. The current semaphore (3) and per-request delay (~0.6s) avoid this for incremental runs. If a force re-scrape hits 403s mid-way, just re-run — cached dates are skipped automatically.
- **Weekend/holiday dates**: JSE returns empty pages for non-trading days. These are silently skipped; they are not added to the cache so they will be retried on the next run.
- **NaN in analysis**: Some stocks have irregular dividend data. All API response dicts are cleaned with `_clean()` before returning.
- **Price window**: `_price_near` looks back 30 days from the target date. Stocks with no price in that window are excluded from yield calculations.

## Legacy Scripts

These files predate the current pipeline and are superseded:

| File | What it was |
|------|-------------|
| `scrape_dividends.py` | Sync version of dividend scraper, merged into `pipeline.py` |
| `Get Historical Prices (mayberry).py` | Scraped Mayberry weekly trade sheets; replaced by JSE trade quotes |
| `Get Historical Prices.ipynb` | Notebook version of Mayberry scraper |
| `jse_quarterly_cadence_scraper.py` | Scraped quarterly report posts from JSE |
| `classify_report_titles.py` | Used OpenAI to classify report titles as financial statements (requires `OPENAI_API_KEY`) |

## Adding a New Analysis

1. Add a computation function to `app.py` (follow the pattern of `compute_consistency`)
2. Add a FastAPI route
3. Add a tab or section to `static/index.html`
4. No restart needed if using `--reload`; otherwise `kill` and restart uvicorn
