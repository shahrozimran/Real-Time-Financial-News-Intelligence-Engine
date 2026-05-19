"""
sqlite_writer.py
----------------
SQLite persistence for processed analytics data.

Tables:
  - articles: processed articles with sentiment
  - sentiment_aggregates: pre-computed aggregations
  - price_bars: historical OHLCV data with analytics
  - price_analytics: latest price snapshot
"""

import sqlite3
import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import SQLITE_DB_PATH

logger = logging.getLogger(__name__)


def _get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating the DB file if needed."""
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_tables():
    """Create tables if they don't exist."""
    conn = _get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS articles (
                id TEXT PRIMARY KEY,
                source TEXT,
                title TEXT,
                summary TEXT,
                clean_title TEXT,
                clean_summary TEXT,
                url TEXT,
                published TEXT,
                tickers TEXT,
                sentiment TEXT,
                sentiment_score REAL,
                processed_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sentiment_aggregates (
                agg_type TEXT,
                agg_key TEXT,
                positive INTEGER DEFAULT 0,
                negative INTEGER DEFAULT 0,
                neutral INTEGER DEFAULT 0,
                total INTEGER DEFAULT 0,
                avg_score REAL DEFAULT 0.0,
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (agg_type, agg_key)
            );

            CREATE TABLE IF NOT EXISTS price_bars (
                ticker TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                change_pct REAL,
                ma_5 REAL,
                ma_20 REAL,
                volatility REAL,
                daily_change_pct REAL,
                volume_anomaly INTEGER DEFAULT 0,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS price_analytics (
                ticker TEXT PRIMARY KEY,
                close REAL,
                change_pct REAL,
                volume INTEGER,
                ma_5 REAL,
                ma_20 REAL,
                volatility REAL,
                high REAL,
                low REAL,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS social_posts (
                id TEXT PRIMARY KEY,
                platform TEXT,
                ticker TEXT,
                text TEXT,
                sentiment TEXT,
                sentiment_score REAL,
                mentions INTEGER DEFAULT 0,
                upvotes INTEGER DEFAULT 0,
                rank INTEGER DEFAULT 0,
                timestamp TEXT,
                processed_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_articles_sentiment ON articles(sentiment);
            CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
            CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published);
            CREATE INDEX IF NOT EXISTS idx_price_bars_ticker ON price_bars(ticker);
            CREATE INDEX IF NOT EXISTS idx_social_ticker ON social_posts(ticker);
            CREATE INDEX IF NOT EXISTS idx_social_sentiment ON social_posts(sentiment);
        """)
        conn.commit()
        logger.info("SQLite tables initialised at %s", SQLITE_DB_PATH)
    finally:
        conn.close()


def upsert_articles(articles: list):
    """Insert or update processed articles."""
    if not articles:
        return
    conn = _get_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO articles
               (id, source, title, summary, clean_title, clean_summary,
                url, published, tickers, sentiment, sentiment_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    a["id"], a.get("source", ""), a.get("title", ""),
                    a.get("summary", ""), a.get("clean_title", ""),
                    a.get("clean_summary", ""), a.get("url", ""),
                    a.get("published", ""),
                    json.dumps(a.get("tickers", [])),
                    a.get("sentiment", "neutral"),
                    a.get("sentiment_score", 0.0),
                )
                for a in articles
            ],
        )
        conn.commit()
        logger.info("Upserted %d articles to SQLite", len(articles))
    finally:
        conn.close()


def upsert_sentiment_aggregates(agg_type: str, aggregates: list):
    """
    Insert or update sentiment aggregates.

    Args:
        agg_type: 'by_source', 'by_asset', 'by_time'
        aggregates: list of dicts with keys matching columns
    """
    if not aggregates:
        return
    conn = _get_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO sentiment_aggregates
               (agg_type, agg_key, positive, negative, neutral, total, avg_score)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    agg_type,
                    a.get("key", a.get("source", a.get("ticker", a.get("date", "unknown")))),
                    a.get("positive", 0),
                    a.get("negative", 0),
                    a.get("neutral", 0),
                    a.get("total", 0),
                    a.get("avg_score", 0.0),
                )
                for a in aggregates
            ],
        )
        conn.commit()
        logger.info("Upserted %d %s aggregates", len(aggregates), agg_type)
    finally:
        conn.close()


def upsert_price_bars(bars: list):
    """Insert or update price bar data."""
    if not bars:
        return
    conn = _get_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO price_bars
               (ticker, date, open, high, low, close, volume, change_pct,
                ma_5, ma_20, volatility, daily_change_pct, volume_anomaly)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    b["ticker"], b["date"], b.get("open", 0), b.get("high", 0),
                    b.get("low", 0), b["close"], b.get("volume", 0),
                    b.get("change_pct", 0), b.get("ma_5"), b.get("ma_20"),
                    b.get("volatility"), b.get("daily_change_pct", 0),
                    int(b.get("volume_anomaly", False)),
                )
                for b in bars
            ],
        )
        conn.commit()
        logger.info("Upserted %d price bars", len(bars))
    finally:
        conn.close()


def upsert_price_snapshot(snapshot: dict):
    """Insert or update latest price analytics."""
    if not snapshot:
        return
    conn = _get_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO price_analytics
               (ticker, close, change_pct, volume, ma_5, ma_20, volatility, high, low)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    ticker, data["close"], data.get("change_pct", 0),
                    data.get("volume", 0), data.get("ma_5"),
                    data.get("ma_20"), data.get("volatility"),
                    data.get("high"), data.get("low"),
                )
                for ticker, data in snapshot.items()
            ],
        )
        conn.commit()
        logger.info("Upserted price snapshot for %d tickers", len(snapshot))
    finally:
        conn.close()


def upsert_social_posts(posts: list):
    """Insert or update social media posts with sentiment."""
    if not posts:
        return
    conn = _get_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO social_posts
               (id, platform, ticker, text, sentiment, sentiment_score,
                mentions, upvotes, rank, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    p["id"],
                    p.get("platform", ""),
                    p.get("ticker", ""),
                    p.get("text", ""),
                    p.get("sentiment", "neutral"),
                    float(p.get("sentiment_score", 0.0)),
                    int(p.get("mentions", 0)),
                    int(p.get("upvotes", 0)),
                    int(p.get("rank", 0)),
                    p.get("timestamp", ""),
                )
                for p in posts
            ],
        )
        conn.commit()
        logger.info("Upserted %d social posts to SQLite", len(posts))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query helpers (used by Flask routes)
# ---------------------------------------------------------------------------

def query_sentiment_trend() -> list:
    """Get daily sentiment trend for chart."""
    conn = _get_connection()
    try:
        rows = conn.execute(
            """SELECT agg_key as date, positive, neutral, negative
               FROM sentiment_aggregates
               WHERE agg_type = 'by_time'
               ORDER BY agg_key
               LIMIT 7"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_sentiment_by_source() -> list:
    """Get sentiment by source."""
    conn = _get_connection()
    try:
        rows = conn.execute(
            """SELECT agg_key as source, positive, negative, neutral, total, avg_score
               FROM sentiment_aggregates
               WHERE agg_type = 'by_source'
               ORDER BY total DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_sentiment_by_asset() -> list:
    """Get sentiment by asset/ticker."""
    conn = _get_connection()
    try:
        rows = conn.execute(
            """SELECT agg_key as ticker, positive, negative, neutral, total, avg_score
               FROM sentiment_aggregates
               WHERE agg_type = 'by_asset'
               ORDER BY total DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_social_posts(limit: int = 50, ticker: str = None, sentiment: str = None) -> list:
    """Query social posts from SQLite."""
    conn = _get_connection()
    try:
        query = "SELECT * FROM social_posts WHERE 1=1"
        params = []
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker.upper())
        if sentiment:
            query += " AND sentiment = ?"
            params.append(sentiment)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_articles(limit: int = 20, source: str = None, sentiment: str = None) -> list:
    """Query processed articles from SQLite."""
    conn = _get_connection()
    try:
        query = "SELECT * FROM articles WHERE 1=1"
        params = []
        if source:
            query += " AND source LIKE ?"
            params.append(f"%{source}%")
        if sentiment:
            query += " AND sentiment = ?"
            params.append(sentiment)
        query += " ORDER BY published DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("tickers"), str):
                try:
                    d["tickers"] = json.loads(d["tickers"])
                except (json.JSONDecodeError, TypeError):
                    d["tickers"] = []
            result.append(d)
        return result
    finally:
        conn.close()
