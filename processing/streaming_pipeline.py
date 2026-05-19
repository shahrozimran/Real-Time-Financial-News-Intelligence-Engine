"""
streaming_pipeline.py
---------------------
PySpark Structured Streaming pipeline — reads live data from Kafka topics,
applies cleaning + sentiment analysis in micro-batches, and stores results
in real-time to SQLite, JSON files, and Pinecone vector database.

Topics consumed:
  - news-feed    → clean text → FinBERT/VADER sentiment → Pinecone + SQLite
  - social-posts → NLP sentiment → Pinecone + SQLite
  - stock-prices → price analytics (MA, volatility) → Pinecone + SQLite

The pipeline uses foreachBatch so each micro-batch is processed with the
same cleaning/sentiment code as the batch pipeline — no code duplication.

Kafka JARs are downloaded automatically on first run via Maven/ivy2.

Run:
    python processing/streaming_pipeline.py                  # all topics
    python processing/streaming_pipeline.py --topic news-feed
    python processing/streaming_pipeline.py --topic social-posts
    python processing/streaming_pipeline.py --topic stock-prices
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    KAFKA_BROKER,
    TOPICS,
    STREAMING_CHECKPOINT_DIR,
    STREAMING_TRIGGER_SECONDS,
    PROCESSED_DATA_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("streaming_pipeline")

_running_queries = []


# ---------------------------------------------------------------------------
# Helpers: shared processing logic
# ---------------------------------------------------------------------------

def _clean_and_score_articles(raw_records: list[dict]) -> list[dict]:
    """Clean and sentiment-score a list of article dicts."""
    from processing.cleaning_pipeline import clean_articles_pandas
    from intelligence.sentiment_model import predict_batch

    cleaned = clean_articles_pandas(raw_records)
    if not cleaned:
        return []

    titles = [a.get("clean_title", a.get("title", "")) for a in cleaned]
    sentiments = predict_batch(titles)
    for article, (label, score) in zip(cleaned, sentiments):
        article["sentiment"] = label
        article["sentiment_score"] = score
    return cleaned


def _score_social_posts(raw_posts: list[dict]) -> list[dict]:
    """Apply FinBERT/VADER sentiment to social post text field."""
    from intelligence.sentiment_model import predict_batch

    if not raw_posts:
        return []

    texts = [p.get("text", "") for p in raw_posts]
    sentiments = predict_batch(texts)
    for post, (label, score) in zip(raw_posts, sentiments):
        post["sentiment"] = label
        post["sentiment_score"] = score
    return raw_posts


def _compute_and_write_aggregates():
    """Compute sentiment aggregates from all SQLite articles and write to aggregates.json."""
    import sqlite3
    from collections import defaultdict
    from storage.json_writer import write_aggregates
    from config.settings import SQLITE_DB_PATH as db_path

    if not os.path.exists(db_path):
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT source, sentiment, sentiment_score, published, tickers FROM articles"
    ).fetchall()
    conn.close()

    if not rows:
        return

    by_source = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0, "score_sum": 0.0})
    by_asset  = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0, "score_sum": 0.0})
    by_time   = defaultdict(lambda: {"positive": 0, "neutral": 0, "negative": 0, "total": 0})
    dist      = {"positive": 0, "neutral": 0, "negative": 0}

    for r in rows:
        s   = r["sentiment"] or "neutral"
        src = r["source"] or "unknown"
        score = r["sentiment_score"] or 0.0
        date  = (r["published"] or "")[:10]

        by_source[src][s]        += 1
        by_source[src]["total"]  += 1
        by_source[src]["score_sum"] += score

        if date:
            by_time[date][s]       += 1
            by_time[date]["total"] += 1

        dist[s] += 1

        try:
            tickers = json.loads(r["tickers"]) if isinstance(r["tickers"], str) else (r["tickers"] or [])
        except Exception:
            tickers = []
        for t in (tickers if isinstance(tickers, list) else []):
            by_asset[t][s]        += 1
            by_asset[t]["total"]  += 1
            by_asset[t]["score_sum"] += score

    def _fmt_src(k, v):
        total = v["total"] or 1
        return {"source": k, "key": k, "positive": v["positive"], "neutral": v["neutral"],
                "negative": v["negative"], "total": v["total"],
                "avg_score": round(v["score_sum"] / total, 4)}

    def _fmt_asset(k, v):
        total = v["total"] or 1
        return {"ticker": k, "key": k, "positive": v["positive"], "neutral": v["neutral"],
                "negative": v["negative"], "total": v["total"],
                "avg_score": round(v["score_sum"] / total, 4)}

    def _fmt_time(k, v):
        return {"date": k, "key": k, "positive": v["positive"], "neutral": v["neutral"],
                "negative": v["negative"], "total": v["total"]}

    write_aggregates({
        "by_source":    [_fmt_src(k, v)   for k, v in sorted(by_source.items(), key=lambda x: -x[1]["total"])],
        "by_asset":     [_fmt_asset(k, v) for k, v in sorted(by_asset.items(),  key=lambda x: -x[1]["total"])],
        "by_time":      [_fmt_time(k, v)  for k, v in sorted(by_time.items())[-7:]],
        "distribution": dist,
        "top_movers":   [],
    })


def _store_articles(articles: list[dict]):
    """Persist cleaned+scored articles to JSON + SQLite + Pinecone."""
    if not articles:
        return

    from storage.json_writer import write_articles
    from storage.sqlite_writer import init_tables, upsert_articles
    write_articles(articles)
    init_tables()
    upsert_articles(articles)
    try:
        _compute_and_write_aggregates()
    except Exception as _agg_exc:
        logger.warning("Aggregates update skipped: %s", _agg_exc)

    try:
        from storage.pinecone_writer import upsert_articles as pc_upsert
        pc_upsert(articles)
        logger.info("  Pinecone: upserted %d articles", len(articles))
    except Exception as exc:
        logger.warning("  Pinecone article upsert skipped: %s", exc)


def _store_social_posts(posts: list[dict]):
    """Persist scored social posts to SQLite + Pinecone."""
    if not posts:
        return

    from storage.sqlite_writer import init_tables, upsert_social_posts
    init_tables()
    upsert_social_posts(posts)

    try:
        from storage.pinecone_writer import upsert_social_posts as pc_upsert
        pc_upsert(posts)
        logger.info("  Pinecone: upserted %d social posts", len(posts))
    except Exception as exc:
        logger.warning("  Pinecone social upsert skipped: %s", exc)


def _store_price_bars(bars: list[dict], spark):
    """Compute price analytics and persist to JSON + SQLite + Pinecone."""
    if not bars:
        return

    import tempfile
    from processing.stock_analytics import compute_price_analytics, latest_price_snapshot
    from storage.json_writer import write_prices
    from storage.sqlite_writer import init_tables, upsert_price_bars, upsert_price_snapshot
    from storage.pinecone_writer import upsert_price_bars as pc_upsert_bars

    init_tables()
    tmp = os.path.join(tempfile.gettempdir(), "finintel_stream_prices.json")
    with open(tmp, "w", encoding="utf-8") as f:
        for b in bars:
            f.write(json.dumps(b, default=str) + "\n")

    prices_df = spark.read.json(tmp)
    analytics_df = compute_price_analytics(prices_df)
    snapshot = latest_price_snapshot(analytics_df)
    bars_list = [row.asDict() for row in analytics_df.collect()]

    write_prices(snapshot)
    upsert_price_bars(bars_list)
    upsert_price_snapshot(snapshot)

    try:
        pc_upsert_bars(bars_list)
        logger.info("  Pinecone: upserted %d price bars", len(bars_list))
    except Exception as exc:
        logger.warning("  Pinecone price upsert skipped: %s", exc)


# ---------------------------------------------------------------------------
# foreachBatch handlers
# ---------------------------------------------------------------------------

_NEWS_SCHEMA = None


def _get_news_schema():
    """Lazily build the Kafka news message schema."""
    global _NEWS_SCHEMA
    if _NEWS_SCHEMA is not None:
        return _NEWS_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType
    _NEWS_SCHEMA = StructType([
        StructField("id",          StringType(), True),
        StructField("source",      StringType(), True),
        StructField("title",       StringType(), True),
        StructField("summary",     StringType(), True),
        StructField("url",         StringType(), True),
        StructField("published",   StringType(), True),
        StructField("tickers",     ArrayType(StringType()), True),
        StructField("category",    StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])
    return _NEWS_SCHEMA


def _news_batch_spark_native(batch_df, epoch_id):
    """
    Spark-native processing path for news articles:
      parse JSON → clean via UDFs (cleaning_pipeline) → sentiment via UDF (sentiment_processor).
    More scalable for large micro-batches.
    """
    from pyspark.sql.functions import from_json, col
    from processing.cleaning_pipeline import clean_articles_spark
    from processing.sentiment_processor import apply_sentiment

    parsed = (
        batch_df
        .select(from_json(col("value"), _get_news_schema()).alias("d"))
        .select("d.*")
        .filter(col("title").isNotNull())
    )

    cleaned_df  = clean_articles_spark(parsed)
    scored_df   = apply_sentiment(cleaned_df, text_col="clean_title")

    articles = [row.asDict() for row in scored_df.collect()]
    for a in articles:
        if isinstance(a.get("tickers"), type(None)):
            a["tickers"] = []
    _store_articles(articles)
    logger.info("[news][spark] epoch=%d — stored %d articles", epoch_id, len(articles))


def _news_batch_python_fallback(batch_df, epoch_id, raw_records: list):
    """Python/pandas fallback for news article processing."""
    articles = _clean_and_score_articles(raw_records)
    if articles:
        _store_articles(articles)
        logger.info("[news][python] epoch=%d — stored %d articles", epoch_id, len(articles))


def _news_batch_handler(batch_df, epoch_id):
    """Process a micro-batch of news-feed messages."""
    if batch_df.isEmpty():
        logger.debug("[news] epoch=%d — empty batch, skipping", epoch_id)
        return

    records_json = [row["value"] for row in batch_df.select("value").collect()]
    raw_records = []
    for r in records_json:
        try:
            raw_records.append(json.loads(r))
        except (json.JSONDecodeError, TypeError):
            pass

    logger.info("[news] epoch=%d — received %d raw articles", epoch_id, len(raw_records))
    if not raw_records:
        return

    try:
        _news_batch_spark_native(batch_df, epoch_id)
    except Exception as exc:
        logger.warning("[news] epoch=%d — Spark UDF path failed (%s), using Python fallback", epoch_id, exc)
        _news_batch_python_fallback(batch_df, epoch_id, raw_records)


def _social_batch_handler(batch_df, epoch_id):
    """Process a micro-batch of social-posts messages."""
    if batch_df.isEmpty():
        logger.debug("[social] epoch=%d — empty batch, skipping", epoch_id)
        return

    records_json = [row["value"] for row in batch_df.select("value").collect()]
    raw_posts = []
    for r in records_json:
        try:
            raw_posts.append(json.loads(r))
        except (json.JSONDecodeError, TypeError):
            pass

    logger.info("[social] epoch=%d — received %d raw posts", epoch_id, len(raw_posts))

    posts = _score_social_posts(raw_posts)
    if posts:
        _store_social_posts(posts)
        logger.info("[social] epoch=%d — stored %d social posts", epoch_id, len(posts))


def _prices_batch_handler(spark):
    """Return a foreachBatch handler closure with access to spark session."""
    def handler(batch_df, epoch_id):
        if batch_df.isEmpty():
            logger.debug("[prices] epoch=%d — empty batch, skipping", epoch_id)
            return

        records_json = [row["value"] for row in batch_df.select("value").collect()]
        raw_bars = []
        for r in records_json:
            try:
                bar = json.loads(r)
                if "date" not in bar:
                    bar["date"] = bar.get("bar_time", "")[:10]
                raw_bars.append(bar)
            except (json.JSONDecodeError, TypeError):
                pass

        logger.info("[prices] epoch=%d — received %d raw price bars", epoch_id, len(raw_bars))

        if raw_bars:
            _store_price_bars(raw_bars, spark)
            logger.info("[prices] epoch=%d — stored %d price bars", epoch_id, len(raw_bars))

    return handler


# ---------------------------------------------------------------------------
# Stream builders
# ---------------------------------------------------------------------------

def _kafka_stream(spark, topic: str):
    """Create a Kafka readStream for a given topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )


def start_news_stream(spark) -> object:
    """Start Structured Streaming query for news-feed topic."""
    checkpoint = os.path.join(STREAMING_CHECKPOINT_DIR, "news-feed")
    os.makedirs(checkpoint, exist_ok=True)

    stream_df = _kafka_stream(spark, TOPICS["news"])

    query = (
        stream_df.writeStream
        .foreachBatch(_news_batch_handler)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )
    logger.info("News stream started — topic: %s, trigger: %ds", TOPICS["news"], STREAMING_TRIGGER_SECONDS)
    return query


def start_social_stream(spark) -> object:
    """Start Structured Streaming query for social-posts topic."""
    checkpoint = os.path.join(STREAMING_CHECKPOINT_DIR, "social-posts")
    os.makedirs(checkpoint, exist_ok=True)

    stream_df = _kafka_stream(spark, TOPICS["social"])

    query = (
        stream_df.writeStream
        .foreachBatch(_social_batch_handler)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )
    logger.info("Social stream started — topic: %s, trigger: %ds", TOPICS["social"], STREAMING_TRIGGER_SECONDS)
    return query


def start_prices_stream(spark) -> object:
    """Start Structured Streaming query for stock-prices topic."""
    checkpoint = os.path.join(STREAMING_CHECKPOINT_DIR, "stock-prices")
    os.makedirs(checkpoint, exist_ok=True)

    stream_df = _kafka_stream(spark, TOPICS["prices"])

    query = (
        stream_df.writeStream
        .foreachBatch(_prices_batch_handler(spark))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{STREAMING_TRIGGER_SECONDS} seconds")
        .start()
    )
    logger.info("Prices stream started — topic: %s, trigger: %ds", TOPICS["prices"], STREAMING_TRIGGER_SECONDS)
    return query


# ---------------------------------------------------------------------------
# Signal handler for graceful shutdown
# ---------------------------------------------------------------------------

def _shutdown(sig, frame):
    logger.info("Shutdown signal received — stopping all streaming queries...")
    for q in _running_queries:
        try:
            q.stop()
        except Exception:
            pass
    sys.exit(0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(topic: str = "all"):
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("=" * 60)
    logger.info("FinIntel Structured Streaming Pipeline")
    logger.info("  Kafka broker: %s", KAFKA_BROKER)
    logger.info("  Topic mode:   %s", topic)
    logger.info("  Trigger:      %ds per micro-batch", STREAMING_TRIGGER_SECONDS)
    logger.info("  Checkpoint:   %s", STREAMING_CHECKPOINT_DIR)
    logger.info("=" * 60)

    from processing.spark_session import get_streaming_spark_session, stop_spark

    logger.info("Starting streaming SparkSession (downloading Kafka JARs if needed)...")
    spark = get_streaming_spark_session()
    logger.info("SparkSession ready: %s", spark.version)

    os.makedirs(STREAMING_CHECKPOINT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    if topic in ("all", "news-feed"):
        _running_queries.append(start_news_stream(spark))

    if topic in ("all", "social-posts"):
        _running_queries.append(start_social_stream(spark))

    if topic in ("all", "stock-prices"):
        _running_queries.append(start_prices_stream(spark))

    if not _running_queries:
        logger.error("No streams started. Valid topics: all, news-feed, social-posts, stock-prices")
        return

    logger.info("%d streaming queries running. Press Ctrl+C to stop.", len(_running_queries))

    try:
        for q in _running_queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        _shutdown(None, None)
    finally:
        stop_spark(spark)


def main():
    parser = argparse.ArgumentParser(description="FinIntel Structured Streaming Pipeline")
    parser.add_argument(
        "--topic",
        choices=["all", "news-feed", "social-posts", "stock-prices"],
        default="all",
        help="Kafka topic to consume (default: all three topics)",
    )
    args = parser.parse_args()
    run(topic=args.topic)


if __name__ == "__main__":
    main()
