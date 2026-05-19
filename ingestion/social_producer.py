"""
social_producer.py
------------------
Fetches live social sentiment data from Tradestie API (Reddit + StockTwits)
and publishes posts as JSON messages to the Kafka 'social-posts' topic.

APIs used:
  - Tradestie /reddit     → Reddit stock sentiment (r/wallstreetbets, etc.)
  - Tradestie /stocktwits → StockTwits community sentiment scores

Run:
    python ingestion/social_producer.py
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

from config.settings import KAFKA_BROKER, TOPICS, SOCIAL_POLL_INTERVAL_SECONDS
from ingestion.live_data_fetcher import fetch_all_social_sentiment

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
    """Main loop: fetch Tradestie social sentiment and publish to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["social"]

    logger.info("Starting social sentiment producer (Tradestie API)")
    logger.info("  Polling every %ds", SOCIAL_POLL_INTERVAL_SECONDS)
    logger.info("  Publishing to topic: %s", topic)

    seen_ids: set[str] = set()
    total_published = 0

    while True:
        posts = fetch_all_social_sentiment()
        published = 0

        for post in posts:
            pid = post["id"]
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            try:
                future = producer.send(topic, key=post["ticker"], value=post)
                future.get(timeout=10)
                published += 1
                logger.debug("Published: [%s] %s — %s",
                             post["platform"], post["ticker"], post["text"][:60])
            except KafkaError as exc:
                logger.error("Kafka send error: %s", exc)

        total_published += published
        logger.info("Cycle complete — %d new posts published (total: %d, sources: Reddit + StockTwits)",
                    published, total_published)

        # Trim seen_ids to avoid unbounded memory growth
        if len(seen_ids) > 10000:
            seen_ids = set(list(seen_ids)[-5000:])

        time.sleep(SOCIAL_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
