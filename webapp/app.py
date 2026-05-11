"""
app.py
------
Flask web application for the Real-Time Financial News Intelligence Engine.

Week 1: skeleton routes with placeholder data.
Week 2+: routes will be wired to live Kafka/PySpark data and the RAG agent.

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
# Placeholder data (replaced in Week 2 with live Kafka consumer reads)
# ---------------------------------------------------------------------------
PLACEHOLDER_NEWS = [
    {
        "id": "n001", "source": "Reuters Business",
        "title": "Fed signals possible rate hold amid strong jobs data",
        "summary": "Federal Reserve officials indicated they may hold interest rates steady as the labour market remains resilient despite inflation pressures.",
        "url": "https://reuters.com/placeholder", "published": "2025-05-11T08:30:00+00:00", "sentiment": "neutral",
    },
    {
        "id": "n002", "source": "CNBC Top News",
        "title": "NVIDIA shares surge after record quarterly earnings beat",
        "summary": "NVIDIA Corporation reported quarterly revenue well above analyst estimates, driven by surging demand for AI accelerator chips across data centres.",
        "url": "https://cnbc.com/placeholder", "published": "2025-05-11T07:15:00+00:00", "sentiment": "positive",
    },
    {
        "id": "n003", "source": "MarketWatch",
        "title": "Tesla faces renewed regulatory scrutiny in Europe",
        "summary": "European regulators announced an investigation into Tesla's Autopilot system following a series of accidents on German autobahns.",
        "url": "https://marketwatch.com/placeholder", "published": "2025-05-11T06:00:00+00:00", "sentiment": "negative",
    },
    {
        "id": "n004", "source": "Yahoo Finance",
        "title": "Apple unveils new AI-powered developer tools at WWDC",
        "summary": "Apple's annual developer conference showcased deep on-device AI integration, positioning the company to compete directly with Google and Microsoft.",
        "url": "https://finance.yahoo.com/placeholder", "published": "2025-05-11T05:45:00+00:00", "sentiment": "positive",
    },
    {
        "id": "n005", "source": "Seeking Alpha",
        "title": "Bitcoin breaks $63K resistance — analysts eye $70K next",
        "summary": "Bitcoin cleared a key technical resistance level overnight, with on-chain data showing large wallet accumulation and a sharp drop in exchange outflows.",
        "url": "https://seekingalpha.com/placeholder", "published": "2025-05-11T04:20:00+00:00", "sentiment": "positive",
    },
    {
        "id": "n006", "source": "Investing.com",
        "title": "Amazon logistics costs weigh on Q1 operating margin",
        "summary": "Amazon's first-quarter results showed AWS revenue growth but higher-than-expected fulfilment costs compressed margins in the North America retail segment.",
        "url": "https://investing.com/placeholder", "published": "2025-05-11T03:10:00+00:00", "sentiment": "negative",
    },
    {
        "id": "n007", "source": "Reuters Business",
        "title": "S&P 500 posts third consecutive weekly gain on tech rally",
        "summary": "The index advanced 0.9% for the week, led by semiconductor and AI-linked stocks, as investors rotated back into growth names on cooling Treasury yields.",
        "url": "https://reuters.com/placeholder2", "published": "2025-05-11T02:00:00+00:00", "sentiment": "positive",
    },
    {
        "id": "n008", "source": "CNBC Top News",
        "title": "Microsoft Azure outage disrupts enterprise customers across EMEA",
        "summary": "A multi-hour Azure cloud outage affected thousands of enterprise customers in Europe and the Middle East, raising reliability concerns among institutional investors.",
        "url": "https://cnbc.com/placeholder2", "published": "2025-05-11T01:30:00+00:00", "sentiment": "negative",
    },
    {
        "id": "n009", "source": "MarketWatch",
        "title": "Ethereum ETF net inflows hit monthly record",
        "summary": "Spot Ethereum ETFs recorded their highest single-month net inflow since approval, with BlackRock and Fidelity products leading the tally.",
        "url": "https://marketwatch.com/placeholder2", "published": "2025-05-10T22:45:00+00:00", "sentiment": "positive",
    },
    {
        "id": "n010", "source": "Seeking Alpha",
        "title": "Alphabet ad revenue growth slows amid AI search disruption fears",
        "summary": "Google's parent company beat earnings but guidance disappointed as analysts flagged structural risk from AI-powered search alternatives eroding core ad click volume.",
        "url": "https://seekingalpha.com/placeholder2", "published": "2025-05-10T20:00:00+00:00", "sentiment": "neutral",
    },
]

PLACEHOLDER_PRICES = {
    "AAPL":    {"close": 189.75, "change_pct": 0.42,  "volume": 52_340_000},
    "TSLA":    {"close": 172.30, "change_pct": -1.12, "volume": 89_120_000},
    "MSFT":    {"close": 415.20, "change_pct": 0.68,  "volume": 23_540_000},
    "GOOGL":   {"close": 175.10, "change_pct": 0.25,  "volume": 19_800_000},
    "AMZN":    {"close": 194.80, "change_pct": -0.33, "volume": 31_250_000},
    "NVDA":    {"close": 924.60, "change_pct": 3.21,  "volume": 44_100_000},
    "^GSPC":   {"close": 5309.50,"change_pct": 0.15,  "volume": 0},
    "^IXIC":   {"close": 16780.20,"change_pct": 0.30, "volume": 0},
    "BTC-USD": {"close": 62_400.00,"change_pct": 1.05,"volume": 0},
    "ETH-USD": {"close": 3_020.00, "change_pct": 0.80,"volume": 0},
}

PLACEHOLDER_SENTIMENT_TREND = [
    {"date": "2025-05-05", "positive": 42, "neutral": 31, "negative": 27},
    {"date": "2025-05-06", "positive": 38, "neutral": 35, "negative": 27},
    {"date": "2025-05-07", "positive": 45, "neutral": 29, "negative": 26},
    {"date": "2025-05-08", "positive": 50, "neutral": 28, "negative": 22},
    {"date": "2025-05-09", "positive": 44, "neutral": 33, "negative": 23},
    {"date": "2025-05-10", "positive": 39, "neutral": 30, "negative": 31},
    {"date": "2025-05-11", "positive": 47, "neutral": 28, "negative": 25},
]

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

    results = PLACEHOLDER_NEWS
    if source:
        results = [a for a in results if source.lower() in a["source"].lower()]
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
    ticker = request.args.get("ticker")
    if ticker:
        data = PLACEHOLDER_PRICES.get(ticker.upper())
        if data is None:
            return jsonify({"error": f"Ticker '{ticker}' not found"}), 404
        return jsonify({ticker.upper(): data})
    return jsonify(PLACEHOLDER_PRICES)


@app.route("/api/sentiment-trend")
def api_sentiment_trend():
    """Return 7-day sentiment trend data for Chart.js."""
    return jsonify(PLACEHOLDER_SENTIMENT_TREND)


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
        f"[Week 1 Placeholder] You asked: \"{question}\". "
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


PLACEHOLDER_RISK_ALERTS = [
    {"id": "r001", "level": "high",   "ticker": "TSLA",    "signal": "Negative sentiment spike",  "detail": "85% negative coverage in last 2 hours across 14 sources.",      "time": "10 min ago"},
    {"id": "r002", "level": "high",   "ticker": "AMZN",    "signal": "Earnings miss detected",    "detail": "Operating margin below consensus — 3 analyst downgrades filed.",   "time": "32 min ago"},
    {"id": "r003", "level": "medium", "ticker": "MSFT",    "signal": "Cloud outage reported",      "detail": "Azure EMEA outage may impact short-term revenue recognition.",       "time": "1 hr ago"},
    {"id": "r004", "level": "medium", "ticker": "BTC-USD", "signal": "Volatility alert",           "detail": "30-day realised volatility exceeded 60% threshold.",               "time": "2 hr ago"},
    {"id": "r005", "level": "low",    "ticker": "GOOGL",   "signal": "Guidance below estimate",   "detail": "Q2 guidance 2.1% below consensus — watch ad revenue trend.",       "time": "3 hr ago"},
]


@app.route("/api/risk-alerts")
def api_risk_alerts():
    """Return current risk alert signals."""
    level = request.args.get("level")
    results = PLACEHOLDER_RISK_ALERTS
    if level:
        results = [r for r in results if r["level"] == level]
    return jsonify({"count": len(results), "alerts": results})


@app.route("/api/status")
def api_status():
    """Health-check endpoint."""
    return jsonify({
        "status":    "ok",
        "version":   "week1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "topics":    ["news-feed", "social-posts", "stock-prices"],
        "tickers":   ALL_TICKERS,
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)
