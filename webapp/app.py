"""
app.py
------
Flask web application for the Real-Time Financial News Intelligence Engine.

Week 1: skeleton routes with placeholder data.
Week 2: routes wired to PySpark-processed JSON/SQLite data.
Week 3+: RAG + multi-agent pipeline.

Run:
    python webapp/app.py
  or
    flask --app webapp/app run --debug
"""

import json
import sys
import os
from datetime import datetime, timezone

from flask import Flask, render_template, jsonify, request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import FLASK_HOST, FLASK_PORT, FLASK_DEBUG, ALL_TICKERS, ASSETS

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Data access helpers — read from processed files, fall back to placeholders
# ---------------------------------------------------------------------------

def _load_processed_articles():
    """Load articles from processed JSON, fallback to live Finnhub API."""
    try:
        from storage.json_writer import read_articles
        articles = read_articles()
        if articles:
            return articles
    except Exception:
        pass
    # Fallback: fetch live from Finnhub
    try:
        from ingestion.live_data_fetcher import fetch_all_news
        return fetch_all_news(days_back=3)
    except Exception:
        return []


def _load_processed_prices():
    """Load prices from processed JSON, fallback to live Finnhub/Alpha Vantage."""
    try:
        from storage.json_writer import read_prices
        prices = read_prices()
        if prices:
            return prices
    except Exception:
        pass
    # Fallback: fetch live from Finnhub + Alpha Vantage
    try:
        from ingestion.live_data_fetcher import fetch_all_prices_as_snapshot
        return fetch_all_prices_as_snapshot()
    except Exception:
        return {}


def _load_processed_aggregates():
    """Load sentiment aggregates from processed JSON."""
    try:
        from storage.json_writer import read_aggregates
        return read_aggregates()
    except Exception:
        return {}


def _has_processed_data():
    """Check if pipeline output exists."""
    try:
        from config.settings import PROCESSED_ARTICLES_PATH
        return os.path.exists(PROCESSED_ARTICLES_PATH)
    except Exception:
        return False



# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Main dashboard page."""
    return render_template(
        "index.html",
        tickers=ALL_TICKERS,
        assets=ASSETS,
        now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        has_processed_data=_has_processed_data(),
    )


@app.route("/api/news")
def api_news():
    """
    Return recent news articles.
    Query params:
        limit  (int, default 10)
        source (str, optional filter)
        sentiment (positive|negative|neutral, optional filter)
    """
    limit     = int(request.args.get("limit", 10))
    source    = request.args.get("source")
    sentiment = request.args.get("sentiment")

    results = _load_processed_articles()
    if source:
        results = [a for a in results if source.lower() in a.get("source", "").lower()]
    if sentiment:
        results = [a for a in results if a.get("sentiment") == sentiment]

    return jsonify({"count": len(results[:limit]), "articles": results[:limit]})


@app.route("/api/prices")
def api_prices():
    """
    Return latest price snapshot for all tickers.
    Query params:
        ticker (str, optional — return single ticker)
    """
    prices = _load_processed_prices()
    ticker = request.args.get("ticker")
    if ticker:
        data = prices.get(ticker.upper())
        if data is None:
            return jsonify({"error": f"Ticker '{ticker}' not found"}), 404
        return jsonify({ticker.upper(): data})
    return jsonify(prices)


@app.route("/api/sentiment-trend")
def api_sentiment_trend():
    """Return 7-day sentiment trend data for Chart.js."""
    aggregates = _load_processed_aggregates()
    trend = aggregates.get("by_time", [])
    return jsonify(trend)


@app.route("/api/sentiment-by-source")
def api_sentiment_by_source():
    """Return sentiment breakdown by news source."""
    aggregates = _load_processed_aggregates()
    by_source = aggregates.get("by_source", [])
    return jsonify(by_source)


@app.route("/api/sentiment-by-asset")
def api_sentiment_by_asset():
    """Return sentiment breakdown by ticker/asset."""
    aggregates = _load_processed_aggregates()
    by_asset = aggregates.get("by_asset", [])
    return jsonify(by_asset)


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """
    Accept a user question and return a placeholder answer.
    Week 3+: this will call the RAG + multi-agent pipeline.

    Request body: {"question": "Why is TSLA dropping?"}
    Response:     {"question": "...", "answer": "...", "agents": [...], "sources": [...]}
    """
    body = request.get_json(force=True, silent=True) or {}
    question = body.get("question", "").strip()

    if not question:
        return jsonify({"error": "No question provided"}), 400

    # Placeholder response — will be replaced by RAG agent in Week 3
    answer = (
        f"[Week 2 Placeholder] You asked: \"{question}\". "
        "The RAG + multi-agent pipeline has not been wired up yet. "
        "This route will return real AI-generated answers from Week 3 onwards."
    )

    return jsonify({
        "question": question,
        "answer":   answer,
        "agents": [
            {"name": "Market Analyst",    "response": "— pending Week 3 —"},
            {"name": "Risk Manager",      "response": "— pending Week 3 —"},
            {"name": "Portfolio Advisor", "response": "— pending Week 3 —"},
        ],
        "sources": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/risk-alerts")
def api_risk_alerts():
    """Return current risk alert signals derived from sentiment + price data."""
    level = request.args.get("level")
    alerts = _generate_risk_alerts()
    if level:
        alerts = [a for a in alerts if a["level"] == level]
    return jsonify({"count": len(alerts), "alerts": alerts})


def _generate_risk_alerts():
    """Generate risk alerts from processed data or return placeholders."""
    articles = _load_processed_articles()
    prices = _load_processed_prices()

    if not articles or not _has_processed_data():
        return []

    alerts = []
    alert_id = 1

    # Detect tickers with high negative sentiment
    ticker_neg_counts = {}
    for a in articles:
        if a.get("sentiment") == "negative":
            tickers = a.get("tickers", [])
            if isinstance(tickers, str):
                try:
                    tickers = json.loads(tickers)
                except (json.JSONDecodeError, TypeError):
                    tickers = []
            for t in tickers:
                ticker_neg_counts[t] = ticker_neg_counts.get(t, 0) + 1

    # Generate alerts for tickers with multiple negative articles
    for ticker, neg_count in sorted(ticker_neg_counts.items(), key=lambda x: -x[1]):
        if neg_count >= 3:
            level = "high"
        elif neg_count >= 2:
            level = "medium"
        else:
            continue

        price_data = prices.get(ticker, {})
        change = price_data.get("change_pct", 0)

        alerts.append({
            "id": f"r{alert_id:03d}",
            "level": level,
            "ticker": ticker,
            "signal": "Negative sentiment spike",
            "detail": f"{neg_count} negative articles detected. Price change: {change:+.2f}%",
            "time": "Recent",
        })
        alert_id += 1

    # Detect price volatility alerts
    for ticker, data in prices.items():
        vol = data.get("volatility")
        if vol and vol > 5.0:
            alerts.append({
                "id": f"r{alert_id:03d}",
                "level": "medium",
                "ticker": ticker,
                "signal": "High volatility detected",
                "detail": f"Rolling volatility: {vol:.2f}",
                "time": "Recent",
            })
            alert_id += 1

    return alerts


@app.route("/api/live-prices")
def api_live_prices():
    """Fetch prices directly from Finnhub + Alpha Vantage (bypasses pipeline)."""
    try:
        from ingestion.live_data_fetcher import fetch_all_prices_as_snapshot
        snapshot = fetch_all_prices_as_snapshot()
        return jsonify(snapshot)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/live-news")
def api_live_news():
    """Fetch news directly from Finnhub (bypasses pipeline)."""
    limit = int(request.args.get("limit", 20))
    try:
        from ingestion.live_data_fetcher import fetch_all_news
        articles = fetch_all_news(days_back=3)
        return jsonify({"count": len(articles[:limit]), "articles": articles[:limit]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/social-sentiment")
def api_social_sentiment():
    """Fetch social sentiment from Tradestie (Reddit + StockTwits)."""
    try:
        from ingestion.live_data_fetcher import fetch_all_social_sentiment
        posts = fetch_all_social_sentiment()
        return jsonify({"count": len(posts), "posts": posts})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/search")
def api_search():
    """Search for stock symbols via Finnhub."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    try:
        from ingestion.live_data_fetcher import search_symbols
        results = search_symbols(query)
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stock/<symbol>")
def api_stock_detail(symbol):
    """Fetch combined quote + company profile + financials for a symbol."""
    symbol = symbol.upper()
    try:
        from ingestion.live_data_fetcher import (
            fetch_finnhub_quote, fetch_company_profile, fetch_company_financials,
        )
        quote = fetch_finnhub_quote(symbol) or {}
        profile = fetch_company_profile(symbol) or {}
        financials = fetch_company_financials(symbol) or {}
        return jsonify({
            "quote": quote,
            "profile": profile,
            "financials": financials,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stock/<symbol>/candles")
def api_stock_candles(symbol):
    """Fetch OHLCV candle data for charting."""
    symbol = symbol.upper()
    resolution = request.args.get("resolution", "D")
    from_ts = int(request.args.get("from", 0))
    to_ts = int(request.args.get("to", 0))
    try:
        from ingestion.live_data_fetcher import fetch_stock_candles
        data = fetch_stock_candles(symbol, resolution, from_ts, to_ts)
        if not data:
            return jsonify({"error": "No candle data available"}), 404
        return jsonify(data)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stock/<symbol>/news")
def api_stock_news(symbol):
    """Fetch recent company news for a specific ticker."""
    symbol = symbol.upper()
    limit = int(request.args.get("limit", 15))
    try:
        from ingestion.live_data_fetcher import fetch_finnhub_company_news
        articles = fetch_finnhub_company_news(symbol, days_back=7)
        return jsonify({"count": len(articles[:limit]), "articles": articles[:limit]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stock/<symbol>/peers")
def api_stock_peers(symbol):
    """Fetch peer tickers with their quotes."""
    symbol = symbol.upper()
    try:
        from ingestion.live_data_fetcher import fetch_stock_peers, fetch_finnhub_quote
        import time as _time
        peers = fetch_stock_peers(symbol)
        peer_data = []
        for p in peers[:6]:
            q = fetch_finnhub_quote(p)
            if q:
                peer_data.append(q)
            _time.sleep(0.3)
        return jsonify(peer_data)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stock/<symbol>/social")
def api_stock_social(symbol):
    """Fetch Reddit social sentiment for a specific ticker."""
    symbol = symbol.upper()
    try:
        from ingestion.live_data_fetcher import fetch_apewisdom_stocks
        posts = fetch_apewisdom_stocks()
        match = [p for p in posts if p.get("ticker", "").upper() == symbol]
        if match:
            return jsonify(match[0])
        return jsonify({"ticker": symbol, "mentions": 0, "rank": None, "sentiment_hint": "unknown"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/status")
def api_status():
    """Health-check endpoint."""
    return jsonify({
        "status":    "ok",
        "version":   "week2",
        "pipeline_data": _has_processed_data(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "topics":    ["news-feed", "social-posts", "stock-prices"],
        "tickers":   ALL_TICKERS,
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)
