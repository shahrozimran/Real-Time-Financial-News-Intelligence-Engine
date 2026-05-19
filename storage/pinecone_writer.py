"""
pinecone_writer.py
------------------
Embed and upsert ALL financial data into Pinecone vector database.

Three namespaces in one index:
  - articles  : news articles with sentiment (text-embedding-3-small)
  - prices    : OHLCV price bars as text summaries
  - sentiment : aggregated sentiment stats by asset / source / time

All embedding via OpenRouter (text-embedding-3-small, 1536-dim).
"""

import hashlib
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    PINECONE_API_KEY, PINECONE_INDEX, PINECONE_CLOUD,
    PINECONE_REGION, PINECONE_DIMENSION,
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL, EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pinecone client (lazy singleton)
# ---------------------------------------------------------------------------
_pc = None
_index = None


def _get_index():
    """Return (and lazily create) the Pinecone index."""
    global _pc, _index
    if _index is not None:
        return _index

    from pinecone import Pinecone, ServerlessSpec

    _pc = Pinecone(api_key=PINECONE_API_KEY)

    existing = [idx.name for idx in _pc.list_indexes()]
    if PINECONE_INDEX not in existing:
        logger.info("Creating Pinecone index '%s' (%d-dim cosine)...", PINECONE_INDEX, PINECONE_DIMENSION)
        _pc.create_index(
            name=PINECONE_INDEX,
            dimension=PINECONE_DIMENSION,
            metric="cosine",
            spec=ServerlessSpec(cloud=PINECONE_CLOUD, region=PINECONE_REGION),
        )
        # Wait for index to be ready
        while not _pc.describe_index(PINECONE_INDEX).status.get("ready", False):
            logger.info("  Waiting for index to be ready...")
            time.sleep(2)
        logger.info("  Index ready.")
    else:
        logger.info("Using existing Pinecone index '%s'.", PINECONE_INDEX)

    _index = _pc.Index(PINECONE_INDEX)
    return _index


# ---------------------------------------------------------------------------
# OpenAI-compatible embeddings via OpenRouter
# ---------------------------------------------------------------------------
_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    import openai
    _openai_client = openai.OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
    )
    return _openai_client


def _embed_texts(texts: list[str]) -> list[list[float]]:
    """
    Embed a list of strings using OpenRouter text-embedding-3-small.
    Returns list of 1536-dim float vectors.
    Batches 100 texts per API call to respect limits.
    """
    client = _get_openai_client()
    all_embeddings = []
    batch_size = 100

    for i in range(0, len(texts), batch_size):
        batch = texts[i: i + batch_size]
        # Clean: replace newlines (OpenAI embedding best practice)
        batch = [t.replace("\n", " ").strip() or "N/A" for t in batch]
        try:
            response = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=batch,
            )
            all_embeddings.extend([item.embedding for item in response.data])
            logger.debug("  Embedded batch %d-%d", i, i + len(batch))
        except Exception as exc:
            logger.error("Embedding batch %d failed: %s", i, exc)
            # Insert zero vectors on failure so upsert can continue
            all_embeddings.extend([[0.0] * PINECONE_DIMENSION] * len(batch))

    return all_embeddings


def _upsert_batch(index, vectors: list[dict], namespace: str):
    """Upsert vectors in batches of 100 (Pinecone free tier safe)."""
    batch_size = 100
    total = 0
    for i in range(0, len(vectors), batch_size):
        batch = vectors[i: i + batch_size]
        index.upsert(vectors=batch, namespace=namespace)
        total += len(batch)
        logger.debug("  Upserted %d / %d to namespace '%s'", total, len(vectors), namespace)
    return total


# ---------------------------------------------------------------------------
# Article upsert
# ---------------------------------------------------------------------------

def _article_text(a: dict) -> str:
    """Convert article dict to embedding text."""
    tickers = a.get("tickers", [])
    if isinstance(tickers, str):
        try:
            tickers = json.loads(tickers)
        except Exception:
            tickers = []
    ticker_str = ", ".join(tickers) if tickers else "general"
    title   = a.get("clean_title") or a.get("title", "")
    summary = a.get("clean_summary") or a.get("summary", "")
    return (
        f"{title}. {summary}. "
        f"Sentiment: {a.get('sentiment', 'neutral')} "
        f"(score: {a.get('sentiment_score', 0):.3f}). "
        f"Tickers: {ticker_str}. Source: {a.get('source', 'unknown')}."
    )


def upsert_articles(articles: list) -> int:
    """Embed and upsert news articles into Pinecone namespace 'articles'."""
    if not articles:
        logger.warning("No articles to upsert.")
        return 0

    index = _get_index()
    texts = [_article_text(a) for a in articles]
    embeddings = _embed_texts(texts)

    vectors = []
    for a, emb, text in zip(articles, embeddings, texts):
        doc_id = a.get("id") or hashlib.sha256(a.get("url", text).encode()).hexdigest()[:32]
        tickers = a.get("tickers", [])
        if isinstance(tickers, str):
            try:
                tickers = json.loads(tickers)
            except Exception:
                tickers = []
        vectors.append({
            "id":     doc_id,
            "values": emb,
            "metadata": {
                "text":            text[:1000],
                "source":          str(a.get("source", "")),
                "title":           str(a.get("title", ""))[:200],
                "summary":         str(a.get("summary", ""))[:500],
                "url":             str(a.get("url", "")),
                "published":       str(a.get("published", "")),
                "tickers":         tickers,
                "sentiment":       str(a.get("sentiment", "neutral")),
                "sentiment_score": float(a.get("sentiment_score", 0.0)),
            },
        })

    count = _upsert_batch(index, vectors, namespace="articles")
    logger.info("Pinecone: upserted %d articles", count)
    return count


# ---------------------------------------------------------------------------
# Price bars upsert
# ---------------------------------------------------------------------------

def _price_text(b: dict) -> str:
    """Convert price bar dict to embedding text."""
    return (
        f"{b.get('ticker', 'UNKNOWN')} price on {b.get('date', 'N/A')}: "
        f"open=${b.get('open', 0):.2f}, high=${b.get('high', 0):.2f}, "
        f"low=${b.get('low', 0):.2f}, close=${b.get('close', 0):.2f}, "
        f"volume={b.get('volume', 0):,}, change={b.get('change_pct', 0):.2f}%, "
        f"volatility={b.get('volatility', 0) or 0:.4f}."
    )


def upsert_price_bars(bars: list) -> int:
    """Embed and upsert price bars into Pinecone namespace 'prices'."""
    if not bars:
        logger.warning("No price bars to upsert.")
        return 0

    index = _get_index()
    texts = [_price_text(b) for b in bars]
    embeddings = _embed_texts(texts)

    vectors = []
    for b, emb, text in zip(bars, embeddings, texts):
        ticker = str(b.get("ticker", "UNKNOWN"))
        date   = str(b.get("date", "N/A"))
        vec_id = hashlib.sha256(f"{ticker}_{date}".encode()).hexdigest()[:32]
        vectors.append({
            "id":     vec_id,
            "values": emb,
            "metadata": {
                "text":       text[:500],
                "ticker":     ticker,
                "date":       date,
                "open":       float(b.get("open", 0) or 0),
                "high":       float(b.get("high", 0) or 0),
                "low":        float(b.get("low", 0) or 0),
                "close":      float(b.get("close", 0) or 0),
                "volume":     int(b.get("volume", 0) or 0),
                "change_pct": float(b.get("change_pct", 0) or 0),
                "volatility": float(b.get("volatility", 0) or 0),
            },
        })

    count = _upsert_batch(index, vectors, namespace="prices")
    logger.info("Pinecone: upserted %d price bars", count)
    return count


# ---------------------------------------------------------------------------
# Sentiment aggregates upsert
# ---------------------------------------------------------------------------

def _sentiment_text(agg_type: str, row: dict) -> str:
    """Convert a sentiment aggregate row to embedding text."""
    key = row.get("key") or row.get("ticker") or row.get("source") or row.get("date") or "unknown"
    return (
        f"{agg_type} sentiment for {key}: "
        f"positive={row.get('positive', 0)}, negative={row.get('negative', 0)}, "
        f"neutral={row.get('neutral', 0)}, total={row.get('total', 0)}, "
        f"avg_score={row.get('avg_score', 0.0):.4f}."
    )


def upsert_sentiment_aggregates(aggregates: dict) -> int:
    """
    Embed and upsert all sentiment aggregates into Pinecone namespace 'sentiment'.
    aggregates: dict with keys 'by_source', 'by_asset', 'by_time', 'top_movers'
    """
    index = _get_index()
    all_vectors = []

    for agg_type, rows in aggregates.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            key = str(row.get("key") or row.get("ticker") or row.get("source") or row.get("date") or "unknown")
            vec_id = hashlib.sha256(f"{agg_type}_{key}".encode()).hexdigest()[:32]
            all_vectors.append((agg_type, row, vec_id))

    if not all_vectors:
        return 0

    texts = [_sentiment_text(agg_type, row) for agg_type, row, _ in all_vectors]
    embeddings = _embed_texts(texts)

    vectors = []
    for (agg_type, row, vec_id), emb, text in zip(all_vectors, embeddings, texts):
        key = str(row.get("key") or row.get("ticker") or row.get("source") or row.get("date") or "unknown")
        vectors.append({
            "id":     vec_id,
            "values": emb,
            "metadata": {
                "text":      text[:400],
                "agg_type":  agg_type,
                "key":       key,
                "positive":  int(row.get("positive", 0) or 0),
                "negative":  int(row.get("negative", 0) or 0),
                "neutral":   int(row.get("neutral", 0) or 0),
                "total":     int(row.get("total", 0) or 0),
                "avg_score": float(row.get("avg_score", 0.0) or 0.0),
            },
        })

    count = _upsert_batch(index, vectors, namespace="sentiment")
    logger.info("Pinecone: upserted %d sentiment aggregates", count)
    return count


# ---------------------------------------------------------------------------
# Social posts upsert
# ---------------------------------------------------------------------------

def _social_text(p: dict) -> str:
    """Convert social post dict to embedding text."""
    return (
        f"{p.get('platform', 'Social')} — {p.get('ticker', 'UNKNOWN')}: "
        f"{p.get('text', '')} "
        f"Sentiment: {p.get('sentiment', 'neutral')} "
        f"(score: {float(p.get('sentiment_score', 0)):.3f}). "
        f"Mentions: {p.get('mentions', 0)}, Upvotes: {p.get('upvotes', 0)}."
    )


def upsert_social_posts(posts: list) -> int:
    """Embed and upsert social media posts into Pinecone namespace 'social'."""
    if not posts:
        logger.warning("No social posts to upsert.")
        return 0

    import hashlib as _hashlib
    index = _get_index()
    texts = [_social_text(p) for p in posts]
    embeddings = _embed_texts(texts)

    vectors = []
    for p, emb, text in zip(posts, embeddings, texts):
        doc_id = p.get("id") or _hashlib.sha256(
            f"{p.get('platform', '')}-{p.get('ticker', '')}-{p.get('timestamp', '')}".encode()
        ).hexdigest()[:32]
        vectors.append({
            "id":     doc_id,
            "values": emb,
            "metadata": {
                "text":            text[:500],
                "platform":        str(p.get("platform", "")),
                "ticker":          str(p.get("ticker", "")),
                "sentiment":       str(p.get("sentiment", "neutral")),
                "sentiment_score": float(p.get("sentiment_score", 0.0)),
                "mentions":        int(p.get("mentions", 0)),
                "upvotes":         int(p.get("upvotes", 0)),
                "rank":            int(p.get("rank", 0)),
                "timestamp":       str(p.get("timestamp", "")),
            },
        })

    count = _upsert_batch(index, vectors, namespace="social")
    logger.info("Pinecone: upserted %d social posts", count)
    return count


# ---------------------------------------------------------------------------
# Master upsert (called by batch_pipeline)
# ---------------------------------------------------------------------------

def upsert_all(articles: list, price_bars: list, aggregates: dict,
               social_posts: list = None) -> dict:
    """
    Embed and upsert all data types to Pinecone.
    Returns dict with counts per namespace.
    """
    logger.info("Starting Pinecone upsert for all namespaces...")
    results = {}

    try:
        results["articles"] = upsert_articles(articles)
    except Exception as exc:
        logger.error("Articles upsert failed: %s", exc)
        results["articles"] = 0

    try:
        results["prices"] = upsert_price_bars(price_bars)
    except Exception as exc:
        logger.error("Price bars upsert failed: %s", exc)
        results["prices"] = 0

    try:
        results["sentiment"] = upsert_sentiment_aggregates(aggregates)
    except Exception as exc:
        logger.error("Sentiment upsert failed: %s", exc)
        results["sentiment"] = 0

    if social_posts:
        try:
            results["social"] = upsert_social_posts(social_posts)
        except Exception as exc:
            logger.error("Social posts upsert failed: %s", exc)
            results["social"] = 0

    total = sum(results.values())
    logger.info("Pinecone upsert complete: %s (total=%d vectors)", results, total)
    return results
