"""
JSE Dividend Analysis – FastAPI web app
Run: venv/bin/uvicorn app:app --reload --port 8000
"""

import asyncio
import json
import math
import re
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = Path(__file__).parent / "data"
DIVIDENDS_CSV = DATA_DIR / "dividends.csv"
PRICES_CSV = DATA_DIR / "prices.csv"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="JSE Dividend Analysis")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── pipeline state ─────────────────────────────────────────────────────────────

_pipeline_state: dict[str, Any] = {"running": False, "log": [], "last_run": None, "result": None}


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    _pipeline_state["log"].append(entry)
    print(entry)


async def _run_pipeline_bg(force: bool = False):
    _pipeline_state["running"] = True
    _pipeline_state["log"] = []
    _pipeline_state["result"] = None
    try:
        from pipeline import run_pipeline
        result = await run_pipeline(_log, force=force)
        _pipeline_state["result"] = result
        _pipeline_state["last_run"] = datetime.now().isoformat()
    except Exception as e:
        _log(f"ERROR: {e}")
    finally:
        _pipeline_state["running"] = False
        _invalidate_cache()


# ── data loading & caching ─────────────────────────────────────────────────────

_cache: dict[str, Any] = {}


def _invalidate_cache():
    _cache.clear()


# ── FX rates ───────────────────────────────────────────────────────────────────

_fx_cache: dict = {}   # {"rates": {...}, "fetched_date": "YYYY-MM-DD"}

def _to_jmd(amount: float, currency: str) -> float:
    """Convert an amount in any currency to JMD using cached daily FX rates."""
    if not currency or currency == "JMD":
        return amount
    today_str = date.today().isoformat()
    if _fx_cache.get("fetched_date") != today_str:
        try:
            with urllib.request.urlopen(
                "https://open.er-api.com/v6/latest/JMD", timeout=5
            ) as resp:
                data = json.loads(resp.read())
            _fx_cache["rates"] = data.get("rates", {})
            _fx_cache["fetched_date"] = today_str
        except Exception:
            pass   # if fetch fails, fall back to whatever is cached (or 1:1)
    rate = _fx_cache.get("rates", {}).get(currency)
    if rate and rate > 0:
        # rate = units of `currency` per 1 JMD  →  JMD = amount / rate
        return amount / rate
    return amount   # unknown currency — return as-is


def _clean(obj):
    """Recursively replace nan/inf floats with None for JSON safety."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


def _load_dividends() -> pd.DataFrame:
    if "dividends" in _cache:
        return _cache["dividends"]
    if not DIVIDENDS_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(DIVIDENDS_CSV)
    # Parse payment_date as the authoritative date; fallback to record_date
    for col in ["payment_date", "record_date", "ex_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["dividend_amount"] = pd.to_numeric(df["dividend_amount"], errors="coerce")
    df = df.dropna(subset=["dividend_amount", "payment_date"])
    if "currency" not in df.columns:
        df["currency"] = "JMD"
    # Pre-convert all amounts to JMD
    df["dividend_amount_jmd"] = df.apply(
        lambda r: _to_jmd(r["dividend_amount"], r.get("currency", "JMD")), axis=1
    )
    _cache["dividends"] = df
    return df


def _load_prices() -> pd.DataFrame:
    if "prices" in _cache:
        return _cache["prices"]
    if not PRICES_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(PRICES_CSV, parse_dates=["date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    _cache["prices"] = df
    return df


def _price_near(prices_df: pd.DataFrame, symbol: str, target_date: date, window_days=30) -> float | None:
    """Find the most recent price for symbol on or before target_date within window_days."""
    td = pd.Timestamp(target_date)
    sub = prices_df[
        (prices_df["symbol"] == symbol) &
        (prices_df["date"] <= td) &
        (prices_df["date"] >= td - pd.Timedelta(days=window_days))
    ]
    if sub.empty:
        return None
    return float(sub.loc[sub["date"].idxmax(), "price"])


# ── analysis ───────────────────────────────────────────────────────────────────

def compute_consistency(dividends_df: pd.DataFrame, years: int = 5) -> list[dict]:
    if dividends_df.empty:
        return []
    cutoff = pd.Timestamp(date.today()) - pd.DateOffset(years=years)
    recent = dividends_df[dividends_df["payment_date"] >= cutoff].copy()

    results = []
    for symbol, grp in dividends_df.groupby("symbol"):
        meta = grp.iloc[0]
        recent_grp = recent[recent["symbol"] == symbol]

        # Years paying dividends in last `years` years
        years_paying = recent_grp["payment_date"].dt.year.nunique() if not recent_grp.empty else 0
        consistency_score = round(years_paying / years, 2)

        # Payments per year (over whole history)
        total_payments = len(grp)
        history_years = max(
            (grp["payment_date"].max() - grp["payment_date"].min()).days / 365.25, 1
        )
        avg_payments_per_year = round(total_payments / history_years, 1)

        # Trailing 12-month dividends
        t12_cutoff = pd.Timestamp(date.today()) - pd.DateOffset(months=12)
        t12 = recent_grp[recent_grp["payment_date"] >= t12_cutoff]["dividend_amount_jmd"].sum()

        results.append({
            "symbol": symbol,
            "company_name": meta.get("company_name", ""),
            "sector": meta.get("sector", ""),
            "market": meta.get("market", ""),
            "years_paying": years_paying,
            "consistency_score": consistency_score,
            "total_payments": total_payments,
            "avg_payments_per_year": avg_payments_per_year,
            "trailing_12m_dividends": round(float(t12), 4),
            "first_dividend": grp["payment_date"].min().date().isoformat(),
            "latest_dividend": grp["payment_date"].max().date().isoformat(),
        })

    results.sort(key=lambda x: (-x["consistency_score"], -x["avg_payments_per_year"]))
    return results


def compute_yield_at_date(
    dividends_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    as_of: date,
    trailing_months: int = 12,
    min_payments: int = 1,
) -> list[dict]:
    if dividends_df.empty or prices_df.empty:
        return []

    as_of_ts = pd.Timestamp(as_of)
    window_start = as_of_ts - pd.DateOffset(months=trailing_months)

    trailing = dividends_df[
        (dividends_df["payment_date"] <= as_of_ts) &
        (dividends_df["payment_date"] >= window_start)
    ]

    results = []
    for symbol, grp in trailing.groupby("symbol"):
        if len(grp) < min_payments:
            continue
        price = _price_near(prices_df, symbol, as_of)
        if price is None or price <= 0:
            continue
        total_div = float(grp["dividend_amount_jmd"].sum())
        yld = round(total_div / price * 100, 2)
        meta = grp.iloc[0]
        results.append({
            "symbol": symbol,
            "company_name": meta.get("company_name", ""),
            "sector": meta.get("sector", ""),
            "market": meta.get("market", ""),
            "price_at_date": round(price, 2),
            "trailing_dividends": round(total_div, 4),
            "dividend_yield_pct": yld,
            "payment_count": len(grp),
        })

    results.sort(key=lambda x: -x["dividend_yield_pct"])
    return results


def get_stock_yield_history(symbol: str, cadence: str = "monthly") -> list[dict]:
    divs = _load_dividends()
    prices = _load_prices()
    if divs.empty or prices.empty:
        return []

    stock_divs = divs[divs["symbol"] == symbol]
    stock_prices = prices[prices["symbol"] == symbol].sort_values("date")
    if stock_prices.empty or stock_divs.empty:
        return []

    # Pick the last available price date within each month or quarter
    freq = "ME" if cadence == "monthly" else "QE"
    stock_prices = stock_prices.set_index("date")
    period_prices = stock_prices["price"].resample(freq).last().dropna()

    results = []
    for period_end, price in period_prices.items():
        if price <= 0:
            continue
        window_start = period_end - pd.DateOffset(months=12)
        trailing = float(
            stock_divs[
                (stock_divs["payment_date"] <= period_end) &
                (stock_divs["payment_date"] >= window_start)
            ]["dividend_amount_jmd"].sum()
        )
        results.append({
            "date": period_end.date().isoformat(),
            "price": round(float(price), 2),
            "trailing_12m_dividends": round(trailing, 4),
            "yield_pct": round(trailing / price * 100, 2) if trailing > 0 else 0,
        })

    return results


def get_stock_detail(symbol: str) -> dict:
    divs = _load_dividends()
    prices = _load_prices()

    stock_divs = divs[divs["symbol"] == symbol].copy() if not divs.empty else pd.DataFrame()
    stock_prices = prices[prices["symbol"] == symbol].copy() if not prices.empty else pd.DataFrame()

    dividend_history = []
    if not stock_divs.empty:
        stock_divs_sorted = stock_divs.sort_values("payment_date")
        for _, row in stock_divs_sorted.iterrows():
            dividend_history.append({
                "payment_date": row["payment_date"].date().isoformat(),
                "ex_date": row["ex_date"].date().isoformat() if pd.notna(row.get("ex_date")) else None,
                "record_date": row["record_date"].date().isoformat() if pd.notna(row.get("record_date")) else None,
                "dividend_amount": float(row["dividend_amount"]),
            })

    price_history = []
    if not stock_prices.empty:
        stock_prices_sorted = stock_prices.sort_values("date")
        for _, row in stock_prices_sorted.iterrows():
            price_history.append({
                "date": row["date"].date().isoformat(),
                "price": float(row["price"]),
            })

    meta = {}
    if not stock_divs.empty:
        r = stock_divs.iloc[0]
        meta = {
            "company_name": r.get("company_name", ""),
            "sector": r.get("sector", ""),
            "market": r.get("market", ""),
        }

    return {
        "symbol": symbol,
        **meta,
        "dividend_history": dividend_history,
        "price_history": price_history,
    }


def get_all_symbols() -> list[dict]:
    divs = _load_dividends()
    if divs.empty:
        return []
    result = []
    for symbol, grp in divs.groupby("symbol"):
        r = grp.iloc[0]
        result.append({
            "symbol": symbol,
            "company_name": r.get("company_name", ""),
            "sector": r.get("sector", ""),
            "market": r.get("market", ""),
        })
    return sorted(result, key=lambda x: x["symbol"])


# ── routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = STATIC_DIR / "index.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/api/consistency")
async def api_consistency(years: int = 5, top: int = 100):
    divs = _load_dividends()
    data = compute_consistency(divs, years=years)
    return _clean({"data": data[:top], "total": len(data)})


@app.get("/api/yield")
async def api_yield(
    date: str = None,
    trailing_months: int = 12,
    min_payments: int = 1,
    top: int = 100,
):
    as_of = datetime.strptime(date, "%Y-%m-%d").date() if date else datetime.now().date()
    divs = _load_dividends()
    prices = _load_prices()
    data = compute_yield_at_date(divs, prices, as_of, trailing_months, min_payments)
    return _clean({"data": data[:top], "as_of": as_of.isoformat(), "total": len(data)})


@app.get("/api/stock/{symbol}/yield-history")
async def api_stock_yield_history(symbol: str, cadence: str = "monthly"):
    if cadence not in ("monthly", "quarterly"):
        cadence = "monthly"
    data = get_stock_yield_history(symbol.upper(), cadence)
    return _clean({"symbol": symbol.upper(), "cadence": cadence, "data": data})


@app.get("/api/stock/{symbol}")
async def api_stock(symbol: str):
    detail = get_stock_detail(symbol.upper())
    if not detail["dividend_history"] and not detail["price_history"]:
        raise HTTPException(status_code=404, detail=f"No data for symbol {symbol}")
    return _clean(detail)


@app.get("/api/stocks")
async def api_stocks():
    return _clean({"data": get_all_symbols()})


@app.get("/api/data-info")
async def api_data_info():
    divs = _load_dividends()
    prices = _load_prices()
    div_last = prices_last = None
    if not divs.empty:
        div_last = divs["payment_date"].max().date().isoformat()
    if not prices.empty:
        prices_last = prices["date"].max().date().isoformat()
    return {
        "dividends_count": len(divs),
        "prices_count": len(prices),
        "dividend_last_date": div_last,
        "price_last_date": prices_last,
        "dividends_csv_exists": DIVIDENDS_CSV.exists(),
        "prices_csv_exists": PRICES_CSV.exists(),
    }


@app.post("/api/run")
async def api_run(background_tasks: BackgroundTasks, force: bool = False):
    if _pipeline_state["running"]:
        return {"status": "already_running"}
    background_tasks.add_task(_run_pipeline_bg, force)
    return {"status": "started", "force": force}


@app.get("/api/run/status")
async def api_run_status():
    return {
        "running": _pipeline_state["running"],
        "log": _pipeline_state["log"][-200:],
        "last_run": _pipeline_state["last_run"],
        "result": _pipeline_state["result"],
    }
