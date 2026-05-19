"""
live_data_fetcher.py
--------------------
Unified module for fetching live financial data from external APIs.

Sources:
  - Finnhub   → real-time stock quotes + market/company news
  - Alpha Vantage → crypto exchange rates + index (ETF proxy) quotes
  - Tradestie → Reddit & StockTwits social sentiment

Used by:
  - Kafka producers (stock_producer, rss_producer, social_producer)
  - Batch pipeline in --source live mode
  - Webapp fallback routes
"""

import hashlib
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    FINNHUB_API_KEY, FINNHUB_BASE_URL,
    ALPHA_VANTAGE_API_KEY, ALPHA_VANTAGE_BASE_URL,
    ALPHA_VANTAGE_CALL_DELAY_SEC,
    FINNHUB_TICKERS, ALPHA_CRYPTO, ALPHA_INDICES,
    CRYPTO_SYMBOL_MAP, INDEX_SYMBOL_MAP,
    FINNHUB_NEWS_CATEGORIES, ALL_TICKERS,
)

# ApeWisdom API (free, no key required) — Reddit stock mentions & sentiment
APEWISDOM_BASE_URL = "https://apewisdom.io/api/v1.0"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_session = requests.Session()
_session.headers.update({"User-Agent": "FinIntelEngine/2.0"})

_last_av_call = 0.0  # timestamp of last Alpha Vantage call


def _throttle_alpha_vantage():
    """Block until enough time has passed since the last Alpha Vantage call."""
    global _last_av_call
    elapsed = time.time() - _last_av_call
    if elapsed < ALPHA_VANTAGE_CALL_DELAY_SEC:
        time.sleep(ALPHA_VANTAGE_CALL_DELAY_SEC - elapsed)
    _last_av_call = time.time()


def _safe_get(url: str, params: dict | None = None, timeout: int = 15) -> dict | list | None:
    """GET with error handling. Returns parsed JSON or None."""
    try:
        resp = _session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.error("HTTP error for %s: %s", url, exc)
        return None
    except ValueError:
        logger.error("Invalid JSON from %s", url)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# STOCK QUOTES — Finnhub
# ═══════════════════════════════════════════════════════════════════════════

def fetch_finnhub_quote(symbol: str) -> dict | None:
    """
    Fetch a real-time quote for a single stock from Finnhub.
    Returns dict with {ticker, open, high, low, close, volume, bar_time, ingested_at}
    or None on failure.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/quote",
        params={"symbol": symbol, "token": FINNHUB_API_KEY},
    )
    if not data or data.get("c") is None or data.get("c") == 0:
        logger.warning("No Finnhub quote data for %s", symbol)
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "ticker":      symbol,
        "open":        round(float(data.get("o", 0)), 4),
        "high":        round(float(data.get("h", 0)), 4),
        "low":         round(float(data.get("l", 0)), 4),
        "close":       round(float(data.get("c", 0)), 4),
        "prev_close":  round(float(data.get("pc", 0)), 4),
        "change_pct":  round(float(data.get("dp", 0)), 4),
        "volume":      0,  # Finnhub /quote doesn't return volume
        "bar_time":    datetime.fromtimestamp(data.get("t", 0), tz=timezone.utc).isoformat() if data.get("t") else now_iso,
        "ingested_at": now_iso,
    }


def fetch_all_stock_quotes() -> list[dict]:
    """Fetch Finnhub quotes for all configured stock tickers."""
    results = []
    for symbol in FINNHUB_TICKERS:
        bar = fetch_finnhub_quote(symbol)
        if bar:
            results.append(bar)
        time.sleep(1)  # ~1 call/s stays well within 60/min
    return results


# ═══════════════════════════════════════════════════════════════════════════
# CRYPTO QUOTES — Alpha Vantage
# ═══════════════════════════════════════════════════════════════════════════

def fetch_alpha_crypto_quote(project_symbol: str) -> dict | None:
    """
    Fetch a real-time crypto exchange rate from Alpha Vantage.
    project_symbol: e.g. 'BTC-USD'
    """
    pair = CRYPTO_SYMBOL_MAP.get(project_symbol)
    if not pair:
        logger.warning("Unknown crypto symbol: %s", project_symbol)
        return None

    from_cur, to_cur = pair
    _throttle_alpha_vantage()

    data = _safe_get(ALPHA_VANTAGE_BASE_URL, params={
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": from_cur,
        "to_currency": to_cur,
        "apikey": ALPHA_VANTAGE_API_KEY,
    })
    if not data:
        return None

    rate_info = data.get("Realtime Currency Exchange Rate", {})
    if not rate_info:
        logger.warning("No Alpha Vantage crypto data for %s (rate limit?)", project_symbol)
        return None

    price = float(rate_info.get("5. Exchange Rate", 0))
    bid   = float(rate_info.get("8. Bid Price", price))
    ask   = float(rate_info.get("9. Ask Price", price))
    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "ticker":      project_symbol,
        "open":        round(price, 4),
        "high":        round(max(price, ask), 4),
        "low":         round(min(price, bid), 4),
        "close":       round(price, 4),
        "prev_close":  0,
        "change_pct":  0,
        "volume":      0,
        "bar_time":    rate_info.get("6. Last Refreshed", now_iso),
        "ingested_at": now_iso,
    }


def fetch_all_crypto_quotes() -> list[dict]:
    """Fetch Alpha Vantage quotes for all configured crypto pairs."""
    results = []
    for sym in ALPHA_CRYPTO:
        bar = fetch_alpha_crypto_quote(sym)
        if bar:
            results.append(bar)
    return results


# ═══════════════════════════════════════════════════════════════════════════
# INDEX QUOTES — Alpha Vantage (ETF proxies: SPY, QQQ)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_alpha_index_quote(project_symbol: str) -> dict | None:
    """
    Fetch an index price via its ETF proxy from Alpha Vantage GLOBAL_QUOTE.
    project_symbol: e.g. '^GSPC'
    """
    etf = INDEX_SYMBOL_MAP.get(project_symbol)
    if not etf:
        logger.warning("Unknown index symbol: %s", project_symbol)
        return None

    _throttle_alpha_vantage()

    data = _safe_get(ALPHA_VANTAGE_BASE_URL, params={
        "function": "GLOBAL_QUOTE",
        "symbol": etf,
        "apikey": ALPHA_VANTAGE_API_KEY,
    })
    if not data:
        return None

    gq = data.get("Global Quote", {})
    if not gq:
        logger.warning("No Alpha Vantage index data for %s/%s (rate limit?)", project_symbol, etf)
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "ticker":      project_symbol,
        "open":        round(float(gq.get("02. open", 0)), 4),
        "high":        round(float(gq.get("03. high", 0)), 4),
        "low":         round(float(gq.get("04. low", 0)), 4),
        "close":       round(float(gq.get("05. price", 0)), 4),
        "prev_close":  round(float(gq.get("08. previous close", 0)), 4),
        "change_pct":  round(float(gq.get("10. change percent", "0").replace("%", "")), 4),
        "volume":      int(float(gq.get("06. volume", 0))),
        "bar_time":    gq.get("07. latest trading day", now_iso),
        "ingested_at": now_iso,
    }


def fetch_all_index_quotes() -> list[dict]:
    """Fetch Alpha Vantage quotes for all configured indices (via ETF proxies)."""
    results = []
    for sym in ALPHA_INDICES:
        bar = fetch_alpha_index_quote(sym)
        if bar:
            results.append(bar)
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ALL PRICES — combined
# ═══════════════════════════════════════════════════════════════════════════

def fetch_all_prices() -> list[dict]:
    """Fetch live prices from all sources (stocks + crypto + indices)."""
    bars = []
    bars.extend(fetch_all_stock_quotes())
    bars.extend(fetch_all_crypto_quotes())
    bars.extend(fetch_all_index_quotes())
    return bars


def fetch_all_prices_as_snapshot() -> dict:
    """Return prices as {ticker: {close, change_pct, volume, ...}} dict for the webapp."""
    bars = fetch_all_prices()
    snapshot = {}
    for b in bars:
        snapshot[b["ticker"]] = {
            "close":      b["close"],
            "change_pct": b["change_pct"],
            "volume":     b["volume"],
            "high":       b["high"],
            "low":        b["low"],
            "open":       b["open"],
        }
    return snapshot


# ═══════════════════════════════════════════════════════════════════════════
# NEWS — Finnhub
# ═══════════════════════════════════════════════════════════════════════════

def _article_id(url: str) -> str:
    """Stable hash for deduplication."""
    return hashlib.md5(url.encode()).hexdigest()


def fetch_finnhub_market_news(category: str = "general", min_id: int = 0) -> list[dict]:
    """
    Fetch general market news from Finnhub.
    Returns list of article dicts in the project schema.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/news",
        params={"category": category, "minId": min_id, "token": FINNHUB_API_KEY},
    )
    if not data or not isinstance(data, list):
        return []

    articles = []
    for item in data:
        url = item.get("url", "")
        if not url:
            continue
        ts = item.get("datetime", 0)
        published = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else datetime.now(timezone.utc).isoformat()

        articles.append({
            "id":          _article_id(url),
            "source":      item.get("source", "Finnhub"),
            "title":       item.get("headline", ""),
            "summary":     item.get("summary", ""),
            "url":         url,
            "published":   published,
            "category":    item.get("category", category),
            "image":       item.get("image", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    return articles


def fetch_finnhub_company_news(symbol: str, days_back: int = 7) -> list[dict]:
    """
    Fetch company-specific news from Finnhub for a single ticker.
    """
    today = datetime.now(timezone.utc).date()
    from_date = (today - timedelta(days=days_back)).isoformat()
    to_date = today.isoformat()

    data = _safe_get(
        f"{FINNHUB_BASE_URL}/company-news",
        params={
            "symbol": symbol,
            "from": from_date,
            "to": to_date,
            "token": FINNHUB_API_KEY,
        },
    )
    if not data or not isinstance(data, list):
        return []

    articles = []
    for item in data:
        url = item.get("url", "")
        if not url:
            continue
        ts = item.get("datetime", 0)
        published = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else datetime.now(timezone.utc).isoformat()

        articles.append({
            "id":          _article_id(url),
            "source":      item.get("source", "Finnhub"),
            "title":       item.get("headline", ""),
            "summary":     item.get("summary", ""),
            "url":         url,
            "published":   published,
            "tickers":     [symbol],
            "category":    item.get("category", "company"),
            "image":       item.get("image", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    return articles


def fetch_all_news(days_back: int = 7) -> list[dict]:
    """
    Fetch all news: market-wide + per-ticker company news.
    Deduplicates by article URL hash.
    """
    seen_ids: set[str] = set()
    all_articles: list[dict] = []

    # Market-wide news
    for cat in FINNHUB_NEWS_CATEGORIES:
        for article in fetch_finnhub_market_news(category=cat):
            if article["id"] not in seen_ids:
                seen_ids.add(article["id"])
                all_articles.append(article)
        time.sleep(0.5)

    # Company news per stock ticker
    for symbol in FINNHUB_TICKERS:
        for article in fetch_finnhub_company_news(symbol, days_back=days_back):
            if article["id"] not in seen_ids:
                seen_ids.add(article["id"])
                all_articles.append(article)
        time.sleep(0.5)

    # Sort by published descending
    all_articles.sort(key=lambda a: a["published"], reverse=True)
    return all_articles


# ═══════════════════════════════════════════════════════════════════════════
# SOCIAL SENTIMENT — ApeWisdom (Reddit mentions from WallStreetBets & others)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_apewisdom_stocks() -> list[dict]:
    """
    Fetch top Reddit-mentioned stocks from ApeWisdom API.
    Returns list of social post dicts in the project schema.
    Source: r/wallstreetbets, r/stocks, r/investing combined.
    """
    data = _safe_get(f"{APEWISDOM_BASE_URL}/filter/all-stocks/")
    if not data or not isinstance(data, dict):
        return []

    results = data.get("results", [])
    if not results:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()
    posts = []
    for item in results:
        ticker = item.get("ticker", "")
        mentions = item.get("mentions", 0)
        upvotes = item.get("upvotes", 0)
        rank = item.get("rank", 0)
        rank_24h_ago = item.get("rank_24h_ago", 0)
        mentions_24h_ago = item.get("mentions_24h_ago", 0)

        # Determine sentiment from momentum (rising mentions = bullish)
        if mentions_24h_ago and mentions_24h_ago > 0:
            momentum = (mentions - mentions_24h_ago) / mentions_24h_ago
        else:
            momentum = 0

        if momentum > 0.2:
            sentiment_hint = "bullish"
        elif momentum < -0.2:
            sentiment_hint = "bearish"
        else:
            sentiment_hint = "neutral"

        name = item.get("name", ticker)
        post_id = hashlib.md5(f"reddit-{ticker}-{now_iso}".encode()).hexdigest()
        posts.append({
            "id":              post_id,
            "platform":        "Reddit (ApeWisdom)",
            "ticker":          ticker,
            "text":            f"{name} ({ticker}) — {mentions} mentions, {upvotes} upvotes on Reddit (rank #{rank})",
            "sentiment_hint":  sentiment_hint,
            "sentiment_score": round(momentum, 4),
            "mentions":        mentions,
            "upvotes":         upvotes,
            "rank":            rank,
            "likes":           upvotes,
            "reposts":         0,
            "timestamp":       now_iso,
            "ingested_at":     now_iso,
        })
    return posts


def fetch_apewisdom_crypto() -> list[dict]:
    """
    Fetch top Reddit-mentioned crypto from ApeWisdom API.
    Source: r/CryptoCurrency, r/Bitcoin, etc.
    """
    data = _safe_get(f"{APEWISDOM_BASE_URL}/filter/all-crypto/")
    if not data or not isinstance(data, dict):
        return []

    results = data.get("results", [])
    if not results:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()
    posts = []
    for item in results:
        ticker = item.get("ticker", "")
        mentions = item.get("mentions", 0)
        upvotes = item.get("upvotes", 0)
        rank = item.get("rank", 0)
        mentions_24h_ago = item.get("mentions_24h_ago", 0)

        if mentions_24h_ago and mentions_24h_ago > 0:
            momentum = (mentions - mentions_24h_ago) / mentions_24h_ago
        else:
            momentum = 0

        if momentum > 0.2:
            sentiment_hint = "bullish"
        elif momentum < -0.2:
            sentiment_hint = "bearish"
        else:
            sentiment_hint = "neutral"

        name = item.get("name", ticker)
        post_id = hashlib.md5(f"reddit-crypto-{ticker}-{now_iso}".encode()).hexdigest()
        posts.append({
            "id":              post_id,
            "platform":        "Reddit Crypto (ApeWisdom)",
            "ticker":          ticker,
            "text":            f"{name} ({ticker}) — {mentions} mentions, {upvotes} upvotes on Reddit crypto subs (rank #{rank})",
            "sentiment_hint":  sentiment_hint,
            "sentiment_score": round(momentum, 4),
            "mentions":        mentions,
            "upvotes":         upvotes,
            "rank":            rank,
            "likes":           upvotes,
            "reposts":         0,
            "timestamp":       now_iso,
            "ingested_at":     now_iso,
        })
    return posts


# ═══════════════════════════════════════════════════════════════════════════
# STOCK LOOKUP — Finnhub (search, profile, financials, candles, peers)
# ═══════════════════════════════════════════════════════════════════════════

def search_symbols(query: str) -> list[dict]:
    """
    Search for stock symbols via Finnhub /search endpoint.
    Returns list of {symbol, description, type} dicts.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/search",
        params={"q": query, "token": FINNHUB_API_KEY},
    )
    if not data or not isinstance(data, dict):
        return []
    results = data.get("result", [])
    return [
        {
            "symbol":      r.get("symbol", ""),
            "description": r.get("description", ""),
            "type":        r.get("type", ""),
        }
        for r in results[:15]
    ]


def fetch_company_profile(symbol: str) -> dict | None:
    """
    Fetch company profile from Finnhub /stock/profile2.
    Returns dict with name, logo, exchange, industry, etc. or None.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/stock/profile2",
        params={"symbol": symbol, "token": FINNHUB_API_KEY},
    )
    if not data or not data.get("name"):
        return None
    return {
        "name":            data.get("name", ""),
        "ticker":          data.get("ticker", symbol),
        "logo":            data.get("logo", ""),
        "exchange":        data.get("exchange", ""),
        "industry":        data.get("finnhubIndustry", ""),
        "country":         data.get("country", ""),
        "currency":        data.get("currency", ""),
        "marketCap":       data.get("marketCapitalization", 0),
        "shareOutstanding": data.get("shareOutstanding", 0),
        "weburl":          data.get("weburl", ""),
        "ipo":             data.get("ipo", ""),
    }


def fetch_company_financials(symbol: str) -> dict | None:
    """
    Fetch key financial metrics from Finnhub /stock/metric?metric=all.
    Returns a curated dict of useful metrics or None.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/stock/metric",
        params={"symbol": symbol, "metric": "all", "token": FINNHUB_API_KEY},
    )
    if not data or not isinstance(data, dict):
        return None
    m = data.get("metric", {})
    if not m:
        return None
    return {
        "peRatio":          m.get("peNormalizedAnnual"),
        "eps":              m.get("epsNormalizedAnnual"),
        "beta":             m.get("beta"),
        "dividendYield":    m.get("dividendYieldIndicatedAnnual"),
        "52WeekHigh":       m.get("52WeekHigh"),
        "52WeekLow":        m.get("52WeekLow"),
        "10DayAvgVolume":   m.get("10DayAverageTradingVolume"),
        "marketCap":        m.get("marketCapitalization"),
        "revenueGrowth":    m.get("revenueGrowthQuarterlyYoy"),
        "epsGrowth":        m.get("epsGrowthQuarterlyYoy"),
    }


def fetch_stock_candles(symbol: str, resolution: str = "D",
                        from_ts: int = 0, to_ts: int = 0) -> dict | None:
    """
    Fetch OHLCV candle data from Alpha Vantage TIME_SERIES_DAILY.
    Returns dict with {t, o, h, l, c, v} arrays for chart compatibility.
    from_ts / to_ts are used to filter the date range from the full response.
    """
    from datetime import datetime

    data = _safe_get(
        ALPHA_VANTAGE_BASE_URL,
        params={
            "function":   "TIME_SERIES_DAILY",
            "symbol":     symbol,
            "outputsize": "compact",
            "apikey":     ALPHA_VANTAGE_API_KEY,
        },
    )
    if not data or "Time Series (Daily)" not in data:
        return None

    ts_data = data["Time Series (Daily)"]
    # Sort dates ascending
    sorted_dates = sorted(ts_data.keys())

    # Apply date filters if provided
    if from_ts:
        from_date = datetime.utcfromtimestamp(from_ts).strftime("%Y-%m-%d")
        sorted_dates = [d for d in sorted_dates if d >= from_date]
    if to_ts:
        to_date = datetime.utcfromtimestamp(to_ts).strftime("%Y-%m-%d")
        sorted_dates = [d for d in sorted_dates if d <= to_date]

    if not sorted_dates:
        return None

    result = {"t": [], "o": [], "h": [], "l": [], "c": [], "v": []}
    for date_str in sorted_dates:
        entry = ts_data[date_str]
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        result["t"].append(int(dt.timestamp()))
        result["o"].append(float(entry.get("1. open", 0)))
        result["h"].append(float(entry.get("2. high", 0)))
        result["l"].append(float(entry.get("3. low", 0)))
        result["c"].append(float(entry.get("4. close", 0)))
        result["v"].append(int(float(entry.get("5. volume", 0))))

    return result


def fetch_stock_peers(symbol: str) -> list[str]:
    """
    Fetch industry peer tickers from Finnhub /stock/peers.
    Returns list of ticker strings.
    """
    data = _safe_get(
        f"{FINNHUB_BASE_URL}/stock/peers",
        params={"symbol": symbol, "token": FINNHUB_API_KEY},
    )
    if not data or not isinstance(data, list):
        return []
    # Remove the queried symbol itself and limit to 8 peers
    return [s for s in data if s != symbol][:8]


def fetch_all_social_sentiment() -> list[dict]:
    """
    Fetch social sentiment from ApeWisdom (Reddit stock + crypto mentions).
    Filters to our tracked tickers when possible.
    """
    posts = []

    stock_posts = fetch_apewisdom_stocks()
    posts.extend(stock_posts)
    time.sleep(2)

    crypto_posts = fetch_apewisdom_crypto()
    posts.extend(crypto_posts)

    # Filter to our tracked tickers (optional — keep all for broader coverage)
    tracked = set(ALL_TICKERS)
    filtered = [p for p in posts if p["ticker"] in tracked]

    # If filtering removes everything, return all posts for visibility
    return filtered if filtered else posts
