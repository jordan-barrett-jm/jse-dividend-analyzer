"""
JSE Dividend & Price Pipeline
- Dividends: scraped from JamStockEx per listed company
- Prices:    fetched from JSE Investor JSON API (72 months per symbol)
Outputs: data/dividends.csv, data/prices.csv
"""
from __future__ import annotations

import asyncio
import aiohttp
import csv
import json
import random
import re
import time
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DIVIDENDS_CSV = DATA_DIR / "dividends.csv"
PRICES_CSV    = DATA_DIR / "prices.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.jamstockex.com/trading/trade-quotes/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

JSE_MARKETS = [
    ("https://www.jamstockex.com/listings/listed-companies/?market=31", "Main Market"),
    ("https://www.jamstockex.com/listings/listed-companies/?market=22", "Junior Market"),
]

JSE_QUOTES_URL = "https://www.jamstockex.com/trading/trade-quotes/?market=50&date={date}"
PRICE_YEARS_BACK = 5


# ── shared fetch ───────────────────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str, *, retries: int = 3) -> str | None:
    for attempt in range(1, retries + 1):
        await asyncio.sleep(random.uniform(0.1, 0.4))
        try:
            async with session.get(url, headers=HEADERS, timeout=ClientTimeout(total=25)) as r:
                if r.status == 200:
                    return await r.text()
                return None
        except Exception:
            if attempt < retries:
                await asyncio.sleep(1.2 * attempt + random.random())
    return None


def clean(text: str) -> str:
    return " ".join(text.split()) if isinstance(text, str) else ""


# ── company listings ───────────────────────────────────────────────────────────

def _parse_listings(html: str, market: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    companies = []
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        anchor = cells[0].find("a")
        if not anchor:
            continue
        href = anchor.get("href", "")
        url = f"https://www.jamstockex.com{href}" if href.startswith("/") else href
        symbol = clean(cells[1].get_text())
        if not symbol:
            continue
        sector = clean(cells[4].get_text()) if len(cells) > 4 else ""
        companies.append({
            "company_name": clean(anchor.get_text()),
            "symbol": symbol,
            "sector": sector,
            "market": market,
            "company_url": url,
        })
    return companies


async def get_companies(session: aiohttp.ClientSession, status_cb=None) -> list[dict]:
    companies = []
    for url, market in JSE_MARKETS:
        html = await fetch(session, url)
        if html:
            batch = _parse_listings(html, market)
            companies.extend(batch)
            if status_cb:
                status_cb(f"Listings: {market} — {len(batch)} companies")
    # Deduplicate by symbol (first occurrence wins)
    seen: set[str] = set()
    unique = [c for c in companies if c["symbol"] not in seen and not seen.add(c["symbol"])]
    return unique


# ── dividends ──────────────────────────────────────────────────────────────────

_CURRENCY_RE = re.compile(r"\b([A-Z]{3})\b")

def _parse_dividend_cell(raw: str) -> tuple[float | None, str]:
    """Return (amount, currency_code) from a raw dividend cell like '\\n TTD 0.5200\\n'."""
    currency = "JMD"
    m = _CURRENCY_RE.search(raw)
    if m:
        currency = m.group(1)
    cleaned = re.sub(r"[^\d.]", " ", raw).split()
    amount = next((float(p) for p in reversed(cleaned) if _is_float(p)), None)
    return amount, currency


def _parse_dividends(html: str, company: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = next(
        (t for t in soup.find_all("table")
         if (h := t.find("thead")) and "Record Date" in h.get_text() and "Dividend" in h.get_text()),
        None,
    )
    if not table:
        return []

    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue
        amount, currency = _parse_dividend_cell(cells[4].get_text())
        if amount is None:
            continue
        rows.append({
            "company_name":    company["company_name"],
            "symbol":          company["symbol"],
            "sector":          company["sector"],
            "market":          company["market"],
            "record_date":     clean(cells[0].get_text()),
            "ex_date":         clean(cells[2].get_text()),
            "payment_date":    clean(cells[3].get_text()),
            "dividend_amount": amount,
            "currency":        currency,
        })
    return rows


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


async def scrape_dividends(
    session: aiohttp.ClientSession,
    companies: list[dict],
    status_cb=None,
) -> list[dict]:
    sem = asyncio.Semaphore(8)
    all_dividends: list[dict] = []

    async def worker(company):
        async with sem:
            html = await fetch(session, company["company_url"])
        if html:
            divs = _parse_dividends(html, company)
            all_dividends.extend(divs)
            if status_cb and divs:
                status_cb(f"Dividends: {company['symbol']} ({len(divs)})")

    await asyncio.gather(*(worker(c) for c in companies))
    return all_dividends


# ── prices (JSE trade quotes) ──────────────────────────────────────────────────

def _parse_trade_quotes(html: str, trade_date: str) -> list[dict]:
    """Extract (symbol, closing_price) from a JSE trade quotes page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    seen: set[str] = set()
    for table in soup.find_all("table"):
        head = table.find("thead")
        if not head:
            continue
        headers = [th.get_text(strip=True) for th in head.find_all("th")]
        if "Symbol" not in headers or not any("ClosingPrice" in h for h in headers):
            continue
        sym_idx = headers.index("Symbol")
        price_idx = next(i for i, h in enumerate(headers) if "ClosingPrice" in h)
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) <= max(sym_idx, price_idx):
                continue
            symbol = cells[sym_idx].get_text(strip=True)
            if not symbol or symbol in seen:
                continue
            price_txt = cells[price_idx].get_text(strip=True).replace(",", "")
            try:
                price = float(price_txt)
                if price > 0:
                    rows.append({"date": trade_date, "symbol": symbol, "price": price})
                    seen.add(symbol)
            except ValueError:
                pass
    return rows


async def scrape_prices(
    session: aiohttp.ClientSession,
    status_cb=None,
    force: bool = False,
) -> list[dict]:
    # Load existing rows; build set of dates already cached
    existing: list[dict] = []
    cached_dates: set[str] = set()
    if not force and PRICES_CSV.exists():
        with open(PRICES_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing.append({"date": row["date"], "symbol": row["symbol"], "price": float(row["price"])})
                cached_dates.add(row["date"])
        if status_cb:
            status_cb(f"Prices cache: {len(existing)} rows across {len(cached_dates)} dates loaded")

    # One Friday per week for the past PRICE_YEARS_BACK years
    today = date.today()
    start = today - timedelta(days=365 * PRICE_YEARS_BACK)
    # Roll start back to the nearest Friday
    start -= timedelta(days=(start.weekday() - 4) % 7)
    all_dates = []
    d = start
    while d <= today:
        all_dates.append(d.isoformat())
        d += timedelta(weeks=1)

    to_fetch = [d for d in all_dates if d not in cached_dates]
    if status_cb:
        status_cb(
            f"Prices: {len(cached_dates)} dates cached, "
            f"fetching {len(to_fetch)} missing trading days from JSE"
        )

    sem = asyncio.Semaphore(3)   # stay polite — JSE blocks aggressive scrapers
    lock = asyncio.Lock()
    new_rows: list[dict] = []
    completed = 0
    found = 0

    async def fetch_date(trade_date: str):
        nonlocal completed, found
        url = JSE_QUOTES_URL.format(date=trade_date)
        async with sem:
            await asyncio.sleep(random.uniform(0.4, 0.9))  # extra courtesy delay
            html = await fetch(session, url)
        async with lock:
            completed += 1
            if html:
                rows = _parse_trade_quotes(html, trade_date)
                if rows:
                    found += 1
                    new_rows.extend(rows)
                    if status_cb:
                        status_cb(
                            f"Prices: {trade_date} ✓ ({len(rows)} symbols) "
                            f"[{completed}/{len(to_fetch)}, {found} trading days found]"
                        )
                    return
            if status_cb and completed % 20 == 0:
                status_cb(f"Prices: {completed}/{len(to_fetch)} checked, {found} trading days found")

    await asyncio.gather(*(fetch_date(d) for d in to_fetch))

    if status_cb:
        status_cb(f"Prices: {len(new_rows)} new rows from {found} trading days")

    # Merge + deduplicate
    seen_keys: set[tuple] = set()
    merged: list[dict] = []
    for r in existing + new_rows:
        key = (r["date"], r["symbol"])
        if key not in seen_keys:
            seen_keys.add(key)
            merged.append(r)

    merged.sort(key=lambda x: x["date"])
    return merged


# ── pipeline entry point ───────────────────────────────────────────────────────

async def fetch_fx_rates(session: aiohttp.ClientSession) -> dict[str, float]:
    """Fetch today's rates relative to JMD from open.er-api.com.
    Returns dict where rates[CCY] = units of CCY per 1 JMD."""
    try:
        text = await fetch(session, "https://open.er-api.com/v6/latest/JMD")
        if text:
            data = json.loads(text)
            return data.get("rates", {})
    except Exception:
        pass
    return {}


def _convert_to_jmd(amount: float, currency: str, rates: dict[str, float]) -> float:
    """Convert amount in any currency to JMD. Falls back to original if rate unknown."""
    if not currency or currency == "JMD":
        return amount
    rate = rates.get(currency)
    if rate and rate > 0:
        return amount / rate   # rates[CCY] = CCY per 1 JMD → JMD = amount / rate
    return amount


async def run_pipeline(status_cb=None, force: bool = False):
    started = time.time()
    if status_cb:
        status_cb(f"Pipeline started {'(force re-scrape)' if force else '(incremental)'}")

    async with aiohttp.ClientSession() as session:
        # Listings first — both scrapers need the symbol/company list
        companies = await get_companies(session, status_cb)

        dividends, prices, fx_rates = await asyncio.gather(
            scrape_dividends(session, companies, status_cb),
            scrape_prices(session, status_cb, force=force),
            fetch_fx_rates(session),
        )

    non_jmd = {r["currency"] for r in dividends if r.get("currency", "JMD") != "JMD"}
    if status_cb:
        status_cb(f"FX: converting {non_jmd or 'none'} → JMD")

    for r in dividends:
        r["dividend_amount_jmd"] = round(
            _convert_to_jmd(r["dividend_amount"], r.get("currency", "JMD"), fx_rates), 6
        )

    div_fields = ["company_name", "symbol", "sector", "market",
                  "record_date", "ex_date", "payment_date",
                  "dividend_amount", "currency", "dividend_amount_jmd"]
    with open(DIVIDENDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=div_fields)
        w.writeheader()
        w.writerows(dividends)

    with open(PRICES_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date", "symbol", "price"])
        w.writeheader()
        w.writerows(prices)

    elapsed = time.time() - started
    msg = f"Done: {len(dividends)} dividend records, {len(prices)} price rows in {elapsed:.1f}s"
    if status_cb:
        status_cb(msg)
    print(msg)
    return {"dividends": len(dividends), "prices": len(prices), "elapsed": elapsed}


if __name__ == "__main__":
    asyncio.run(run_pipeline(print))
