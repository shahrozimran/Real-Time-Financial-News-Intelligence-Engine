"""
batch_pipeline.py
-----------------
Main orchestrator for the FinIntel PySpark pipeline.

Runs: load → clean → sentiment → aggregate → store

Usage:
    python processing/batch_pipeline.py              # sample data (default)
    python processing/batch_pipeline.py --source kafka   # Kafka (Phase 2)
"""

import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    SAMPLE_NEWS_PATH, SAMPLE_PRICES_PATH,
    PROCESSED_DATA_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("batch_pipeline")


def ensure_sample_data():
    """Generate sample data if it doesn't exist."""
    if not os.path.exists(SAMPLE_NEWS_PATH) or not os.path.exists(SAMPLE_PRICES_PATH):
        logger.info("Sample data not found — generating...")
        from data.sample.generate_sample_data import main as generate
        generate()


def run_pipeline(source: str = "sample"):
    """Execute the full batch pipeline."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("FinIntel Batch Pipeline — source: %s", source)
    logger.info("=" * 60)

    # ── Step 1: Ensure sample data exists ──
    if source == "sample":
        ensure_sample_data()

    # ── Step 2: Start Spark ──
    logger.info("[1/7] Starting SparkSession...")
    from processing.spark_session import get_spark_session, stop_spark
    spark = get_spark_session()
    logger.info("  Spark ready: %s", spark.version)

    try:
        # ── Step 3: Load data ──
        logger.info("[2/7] Loading data...")
        if source == "sample":
            with open(SAMPLE_NEWS_PATH, "r", encoding="utf-8") as f:
                articles_raw = json.load(f)
            with open(SAMPLE_PRICES_PATH, "r", encoding="utf-8") as f:
                prices_raw = json.load(f)
        else:
            logger.error("Kafka source not yet implemented (Phase 2)")
            return

        article_count = len(articles_raw)
        logger.info("  Loaded %d articles", article_count)

        # Flatten price bars into a single list
        price_rows = []
        for ticker, bars in prices_raw.items():
            for bar in bars:
                bar["ticker"] = ticker
                price_rows.append(bar)
        logger.info("  Loaded %d price bars across %d tickers",
                     len(price_rows), len(prices_raw))

        # ── Step 4: Clean articles (pandas-based — avoids UDF issues) ──
        logger.info("[3/7] Cleaning articles...")
        from processing.cleaning_pipeline import clean_articles_pandas
        articles_clean = clean_articles_pandas(articles_raw)
        clean_count = len(articles_clean)
        logger.info("  %d articles after cleaning (removed %d)",
                     clean_count, article_count - clean_count)

        # ── Step 5: Sentiment analysis (native Python) ──
        logger.info("[4/7] Running sentiment analysis (FinBERT / VADER)...")
        from intelligence.sentiment_model import predict_batch
        titles = [a.get("clean_title", a.get("title", "")) for a in articles_clean]
        sentiments = predict_batch(titles)
        for article, (label, score) in zip(articles_clean, sentiments):
            article["sentiment"] = label
            article["sentiment_score"] = score
        logger.info("  Sentiment applied to %d articles", len(articles_clean))

        # ── Step 6: Spark SQL analytics ──
        logger.info("[5/7] Computing Spark SQL analytics...")
        from processing.spark_sql_analytics import (
            sentiment_by_asset, sentiment_by_source,
            sentiment_by_time, overall_distribution, top_movers,
        )
        from processing.stock_analytics import compute_price_analytics, latest_price_snapshot

        # Write to temp JSON files for Spark to read (avoids Python→JVM serialization)
        import tempfile
        tmp_articles = os.path.join(tempfile.gettempdir(), "finintel_articles.json")
        tmp_prices = os.path.join(tempfile.gettempdir(), "finintel_prices.json")

        with open(tmp_articles, "w", encoding="utf-8") as f:
            # Write one JSON object per line (JSON Lines) for Spark
            for a in articles_clean:
                f.write(json.dumps(a, default=str) + "\n")

        with open(tmp_prices, "w", encoding="utf-8") as f:
            for p in price_rows:
                f.write(json.dumps(p, default=str) + "\n")

        sentiment_df = spark.read.json(tmp_articles)
        sentiment_df.cache()
        prices_df = spark.read.json(tmp_prices)

        by_asset_df = sentiment_by_asset(sentiment_df)
        by_source_df = sentiment_by_source(sentiment_df)
        by_time_df = sentiment_by_time(sentiment_df)
        distribution = overall_distribution(sentiment_df)
        movers_df = top_movers(sentiment_df)

        price_analytics_df = compute_price_analytics(prices_df)
        price_snapshot = latest_price_snapshot(price_analytics_df)

        logger.info("  Sentiment by asset: %d tickers", by_asset_df.count())
        logger.info("  Sentiment by source: %d sources", by_source_df.count())
        logger.info("  Sentiment by time: %d days", by_time_df.count())
        logger.info("  Distribution: %s", distribution)
        logger.info("  Price snapshot: %d tickers", len(price_snapshot))

        # ── Step 7: Store results ──
        logger.info("[6/7] Storing results...")

        # Collect Spark aggregates to Python
        by_asset_list = [row.asDict() for row in by_asset_df.collect()]
        by_source_list = [row.asDict() for row in by_source_df.collect()]
        by_time_list = [row.asDict() for row in by_time_df.collect()]
        movers_list = [row.asDict() for row in movers_df.collect()]
        price_bars_list = [row.asDict() for row in price_analytics_df.collect()]

        # Ensure tickers are plain lists for JSON serialization
        for a in articles_clean:
            if hasattr(a.get("tickers"), "__iter__") and not isinstance(a["tickers"], (str, list)):
                a["tickers"] = list(a["tickers"])

        # JSON output
        from storage.json_writer import write_articles, write_prices, write_aggregates
        write_articles(articles_clean)
        write_prices(price_snapshot)
        write_aggregates({
            "by_source": by_source_list,
            "by_asset": [
                {**a, "key": a["ticker"]} for a in by_asset_list
            ],
            "by_time": [
                {**t, "key": str(t.get("date", "")), "date": str(t.get("date", ""))}
                for t in by_time_list
            ],
            "distribution": distribution,
            "top_movers": movers_list,
        })

        # SQLite output
        from storage.sqlite_writer import (
            init_tables, upsert_articles, upsert_sentiment_aggregates,
            upsert_price_bars, upsert_price_snapshot,
        )
        init_tables()
        upsert_articles(articles_clean)
        upsert_sentiment_aggregates("by_source", [
            {**a, "key": a["source"]} for a in by_source_list
        ])
        upsert_sentiment_aggregates("by_asset", [
            {**a, "key": a["ticker"]} for a in by_asset_list
        ])
        upsert_sentiment_aggregates("by_time", [
            {**t, "key": str(t.get("date", ""))} for t in by_time_list
        ])
        upsert_price_bars(price_bars_list)
        upsert_price_snapshot(price_snapshot)

        # ── Done ──
        elapsed = time.time() - start
        logger.info("[7/7] Pipeline complete!")
        logger.info("  Articles processed: %d", len(articles_clean))
        logger.info("  Price bars stored: %d", len(price_bars_list))
        logger.info("  Output: %s", PROCESSED_DATA_DIR)
        logger.info("  Time: %.1fs", elapsed)
        logger.info("=" * 60)

    finally:
        stop_spark(spark)


def main():
    parser = argparse.ArgumentParser(description="FinIntel Batch Pipeline")
    parser.add_argument(
        "--source", choices=["sample", "kafka"], default="sample",
        help="Data source: sample (default) or kafka"
    )
    args = parser.parse_args()
    run_pipeline(source=args.source)


if __name__ == "__main__":
    main()
