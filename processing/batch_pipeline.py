"""
batch_pipeline.py
-----------------
Main orchestrator for the FinIntel PySpark pipeline.

Runs: load → clean → sentiment → aggregate → social → store

Usage:
    python processing/batch_pipeline.py              # live APIs (default)
    python processing/batch_pipeline.py --source live    # live Finnhub/Alpha Vantage/ApeWisdom
    python processing/batch_pipeline.py --source sample  # local sample data (offline fallback)
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


def run_pipeline(source: str = "live"):
    """Execute the full batch pipeline."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("FinIntel Batch Pipeline — source: %s", source)
    logger.info("=" * 60)

    # ── Step 1: Ensure sample data exists (offline fallback only) ──
    if source == "sample":
        ensure_sample_data()

    # ── Step 2: Start Spark ──
    logger.info("[1/9] Starting SparkSession...")
    from processing.spark_session import get_spark_session, stop_spark
    spark = get_spark_session()
    logger.info("  Spark ready: %s", spark.version)

    try:
        # ── Step 3: Load news + price data ──
        logger.info("[2/9] Loading news and price data (source=%s)...", source)
        if source == "sample":
            with open(SAMPLE_NEWS_PATH, "r", encoding="utf-8") as f:
                articles_raw = json.load(f)
            with open(SAMPLE_PRICES_PATH, "r", encoding="utf-8") as f:
                prices_raw = json.load(f)

            price_rows = []
            for ticker, bars in prices_raw.items():
                for bar in bars:
                    bar["ticker"] = ticker
                    price_rows.append(bar)

        else:  # live
            logger.info("  Fetching LIVE data from Finnhub / Alpha Vantage...")
            from ingestion.live_data_fetcher import fetch_all_news, fetch_all_prices
            from datetime import datetime as dt, timezone as tz

            articles_raw = fetch_all_news(days_back=7)
            price_rows = fetch_all_prices()

            for bar in price_rows:
                if "date" not in bar:
                    bar["date"] = bar.get("bar_time", dt.now(tz.utc).isoformat())[:10]

        article_count = len(articles_raw)
        logger.info("  Loaded %d articles", article_count)
        logger.info("  Loaded %d price bars", len(price_rows))

        # ── Step 4: Load social media posts ──
        logger.info("[3/9] Fetching live social media posts (ApeWisdom / Reddit)...")
        social_posts_raw = []
        if source == "live":
            try:
                from ingestion.live_data_fetcher import fetch_all_social_sentiment
                social_posts_raw = fetch_all_social_sentiment()
                logger.info("  Loaded %d social posts", len(social_posts_raw))
            except Exception as exc:
                logger.warning("  Social fetch failed (non-fatal): %s", exc)
        else:
            logger.info("  Social posts skipped in sample mode (no live API available)")

        # ── Step 5: Clean articles ──
        logger.info("[4/9] Cleaning articles...")
        from processing.cleaning_pipeline import clean_articles_pandas
        articles_clean = clean_articles_pandas(articles_raw)
        clean_count = len(articles_clean)
        logger.info("  %d articles after cleaning (removed %d)",
                     clean_count, article_count - clean_count)

        # ── Step 6: Sentiment analysis on articles ──
        logger.info("[5/9] Running sentiment analysis on articles (FinBERT / VADER)...")
        from intelligence.sentiment_model import predict_batch
        titles = [a.get("clean_title", a.get("title", "")) for a in articles_clean]
        sentiments = predict_batch(titles)
        for article, (label, score) in zip(articles_clean, sentiments):
            article["sentiment"] = label
            article["sentiment_score"] = score
        logger.info("  Sentiment applied to %d articles", len(articles_clean))

        # ── Step 7: Sentiment analysis on social posts ──
        logger.info("[6/9] Running sentiment analysis on social posts (FinBERT / VADER)...")
        if social_posts_raw:
            social_texts = [p.get("text", "") for p in social_posts_raw]
            social_sentiments = predict_batch(social_texts)
            for post, (label, score) in zip(social_posts_raw, social_sentiments):
                post["sentiment"] = label
                post["sentiment_score"] = score
            logger.info("  Sentiment applied to %d social posts", len(social_posts_raw))
        else:
            logger.info("  No social posts to score")

        # ── Step 8: Spark SQL analytics ──
        logger.info("[7/9] Computing Spark SQL analytics...")
        from processing.spark_sql_analytics import (
            sentiment_by_asset, sentiment_by_source,
            sentiment_by_time, overall_distribution, top_movers,
        )
        from processing.stock_analytics import compute_price_analytics, latest_price_snapshot

        import tempfile
        tmp_articles = os.path.join(tempfile.gettempdir(), "finintel_articles.json")
        tmp_prices   = os.path.join(tempfile.gettempdir(), "finintel_prices.json")

        with open(tmp_articles, "w", encoding="utf-8") as f:
            for a in articles_clean:
                f.write(json.dumps(a, default=str) + "\n")

        with open(tmp_prices, "w", encoding="utf-8") as f:
            for p in price_rows:
                f.write(json.dumps(p, default=str) + "\n")

        sentiment_df = spark.read.json(tmp_articles)
        sentiment_df.cache()
        prices_df = spark.read.json(tmp_prices)

        by_asset_df   = sentiment_by_asset(sentiment_df)
        by_source_df  = sentiment_by_source(sentiment_df)
        by_time_df    = sentiment_by_time(sentiment_df)
        distribution  = overall_distribution(sentiment_df)
        movers_df     = top_movers(sentiment_df)

        price_analytics_df = compute_price_analytics(prices_df)
        price_snapshot     = latest_price_snapshot(price_analytics_df)

        logger.info("  Sentiment by asset:  %d tickers",  by_asset_df.count())
        logger.info("  Sentiment by source: %d sources",  by_source_df.count())
        logger.info("  Sentiment by time:   %d days",     by_time_df.count())
        logger.info("  Distribution: %s",                 distribution)
        logger.info("  Price snapshot: %d tickers",       len(price_snapshot))

        # ── Step 9: Collect Spark results ──
        by_asset_list    = [row.asDict() for row in by_asset_df.collect()]
        by_source_list   = [row.asDict() for row in by_source_df.collect()]
        by_time_list     = [row.asDict() for row in by_time_df.collect()]
        movers_list      = [row.asDict() for row in movers_df.collect()]
        price_bars_list  = [row.asDict() for row in price_analytics_df.collect()]

        for a in articles_clean:
            if hasattr(a.get("tickers"), "__iter__") and not isinstance(a["tickers"], (str, list)):
                a["tickers"] = list(a["tickers"])

        # ── Step 10: Persist to JSON + SQLite ──
        logger.info("[8/9] Storing results to JSON and SQLite...")

        from storage.json_writer import write_articles, write_prices, write_aggregates
        write_articles(articles_clean)
        write_prices(price_snapshot)
        write_aggregates({
            "by_source": by_source_list,
            "by_asset":  [{**a, "key": a["ticker"]} for a in by_asset_list],
            "by_time":   [
                {**t, "key": str(t.get("date", "")), "date": str(t.get("date", ""))}
                for t in by_time_list
            ],
            "distribution": distribution,
            "top_movers": movers_list,
        })

        from storage.sqlite_writer import (
            init_tables, upsert_articles, upsert_sentiment_aggregates,
            upsert_price_bars, upsert_price_snapshot, upsert_social_posts,
        )
        init_tables()
        upsert_articles(articles_clean)
        upsert_sentiment_aggregates("by_source", [{**a, "key": a["source"]}  for a in by_source_list])
        upsert_sentiment_aggregates("by_asset",  [{**a, "key": a["ticker"]}  for a in by_asset_list])
        upsert_sentiment_aggregates("by_time",   [{**t, "key": str(t.get("date", ""))} for t in by_time_list])
        upsert_price_bars(price_bars_list)
        upsert_price_snapshot(price_snapshot)

        if social_posts_raw:
            upsert_social_posts(social_posts_raw)
            logger.info("  Stored %d social posts to SQLite", len(social_posts_raw))

        # ── Step 11: Pinecone vector database ──
        logger.info("[9/9] Uploading to Pinecone vector database...")
        try:
            from storage.pinecone_writer import upsert_all
            pinecone_counts = upsert_all(
                articles=articles_clean,
                price_bars=price_bars_list,
                aggregates={
                    "by_source":  by_source_list,
                    "by_asset":   [{**a, "key": a["ticker"]} for a in by_asset_list],
                    "by_time":    [{**t, "key": str(t.get("date", ""))} for t in by_time_list],
                    "top_movers": movers_list,
                },
                social_posts=social_posts_raw if social_posts_raw else None,
            )
            logger.info("  Pinecone vectors stored: %s", pinecone_counts)
        except Exception as pc_exc:
            logger.warning("Pinecone upsert skipped (non-fatal): %s", pc_exc)

        # ── Done ──
        elapsed = time.time() - start
        logger.info("Pipeline complete!")
        logger.info("  Articles processed:  %d", len(articles_clean))
        logger.info("  Social posts stored: %d", len(social_posts_raw))
        logger.info("  Price bars stored:   %d", len(price_bars_list))
        logger.info("  Output: %s", PROCESSED_DATA_DIR)
        logger.info("  Time: %.1fs", elapsed)
        logger.info("=" * 60)

    finally:
        stop_spark(spark)


def main():
    parser = argparse.ArgumentParser(description="FinIntel Batch Pipeline")
    parser.add_argument(
        "--source", choices=["live", "sample"], default="live",
        help="Data source: live (default, Finnhub/Alpha Vantage/ApeWisdom APIs) or sample (offline fallback)"
    )
    args = parser.parse_args()
    run_pipeline(source=args.source)


if __name__ == "__main__":
    main()
