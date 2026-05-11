"""
rss_producer.py
---------------
Polls configured RSS feeds on a fixed interval, deduplicates articles by URL,
and publishes new articles as JSON messages to the Kafka 'news-feed' topic.

Run:
    python ingestion/rss_producer.py
"""

import json
import logging
import time
import hashlib
from datetime import datetime, timezone

import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    KAFKA_BROKER,
    TOPICS,
    RSS_FEEDS,
    RSS_POLL_INTERVAL_SECONDS,
)

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


def article_id(url: str) -> str:
    """Return a short stable hash for a URL (used as deduplication key)."""
    return hashlib.md5(url.encode()).hexdigest()


def parse_published(entry) -> str:
    """Extract ISO-8601 timestamp from feed entry, fall back to now()."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    return datetime.now(timezone.utc).isoformat()


def fetch_feed(feed_cfg: dict) -> list[dict]:
    """Parse a single RSS feed and return a list of article dicts."""
    articles = []
    try:
        parsed = feedparser.parse(
            feed_cfg["url"],
            request_headers={"User-Agent": "FinanceIntelEngine/1.0"},
        )
        if parsed.bozo and parsed.bozo_exception:
            logger.warning("Feed parse warning [%s]: %s", feed_cfg["name"], parsed.bozo_exception)

        for entry in parsed.entries:
            url = getattr(entry, "link", "") or ""
            if not url:
                continue
            articles.append({
                "id":        article_id(url),
                "source":    feed_cfg["name"],
                "title":     getattr(entry, "title",   ""),
                "summary":   getattr(entry, "summary", ""),
                "url":       url,
                "published": parse_published(entry),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })
    except Exception as exc:
        logger.error("Failed to fetch feed [%s]: %s", feed_cfg["name"], exc)
    return articles


def run():
    """Main loop: poll all feeds and publish new articles to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["news"]
    seen_ids: set[str] = set()

    logger.info("Starting RSS producer — polling every %ds", RSS_POLL_INTERVAL_SECONDS)
    logger.info("Publishing to topic: %s", topic)

    while True:
        new_count = 0
        for feed_cfg in RSS_FEEDS:
            articles = fetch_feed(feed_cfg)
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

        logger.info("Cycle complete — %d new articles published (seen total: %d)", new_count, len(seen_ids))

        # Trim seen_ids to avoid unbounded memory growth (keep last 5 000)
        if len(seen_ids) > 5000:
            seen_ids = set(list(seen_ids)[-5000:])

        time.sleep(RSS_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
