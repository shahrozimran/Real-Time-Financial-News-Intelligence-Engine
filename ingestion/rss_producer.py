"""
rss_producer.py
---------------
Fetches live financial news from Finnhub API (market news + company news)
and publishes new articles as JSON messages to the Kafka 'news-feed' topic.

APIs used:
  - Finnhub /news          → general market, forex, crypto, merger news
  - Finnhub /company-news  → per-ticker company-specific news

Run:
    python ingestion/rss_producer.py
"""

import json
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    KAFKA_BROKER,
    TOPICS,
    NEWS_POLL_INTERVAL_SECONDS,
)
from ingestion.live_data_fetcher import fetch_all_news

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def run():
    """Main loop: fetch Finnhub news and publish new articles to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["news"]
    seen_ids: set[str] = set()

    logger.info("Starting Finnhub news producer — polling every %ds", NEWS_POLL_INTERVAL_SECONDS)
    logger.info("Publishing to topic: %s", topic)

    while True:
        new_count = 0
        articles = fetch_all_news(days_back=3)

        for article in articles:
            aid = article["id"]
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            try:
                future = producer.send(topic, key=aid, value=article)
                future.get(timeout=10)
                new_count += 1
                logger.debug("Published: [%s] %s", article["source"], article["title"][:80])
            except KafkaError as exc:
                logger.error("Kafka send error: %s", exc)

        logger.info("Cycle complete — %d new articles published (seen total: %d)",
                     new_count, len(seen_ids))

        # Trim seen_ids to avoid unbounded memory growth (keep last 5 000)
        if len(seen_ids) > 5000:
            seen_ids = set(list(seen_ids)[-5000:])

        time.sleep(NEWS_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
