"""
stock_producer.py
-----------------
Fetches live stock quotes from Finnhub (stocks) and Alpha Vantage
(crypto + index ETF proxies) and publishes them as JSON messages to
the Kafka 'stock-prices' topic.

APIs used:
  - Finnhub /quote        → real-time stock prices (AAPL, TSLA, …)
  - Alpha Vantage CURRENCY_EXCHANGE_RATE → crypto (BTC-USD, ETH-USD)
  - Alpha Vantage GLOBAL_QUOTE           → index proxies (SPY, QQQ)

Run:
    python ingestion/stock_producer.py
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
    ALL_TICKERS,
    STOCK_POLL_INTERVAL_SECONDS,
    CRYPTO_POLL_INTERVAL_SECONDS,
)
from ingestion.live_data_fetcher import (
    fetch_all_stock_quotes,
    fetch_all_crypto_quotes,
    fetch_all_index_quotes,
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
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def _publish_bars(producer: KafkaProducer, topic: str, bars: list[dict]) -> int:
    """Publish a list of price bars to Kafka. Returns count of published."""
    published = 0
    for bar in bars:
        try:
            future = producer.send(topic, key=bar["ticker"], value=bar)
            future.get(timeout=10)
            published += 1
            logger.debug(
                "Published: %s close=%.4f @ %s",
                bar["ticker"], bar["close"], bar["bar_time"],
            )
        except KafkaError as exc:
            logger.error("Kafka send error for %s: %s", bar["ticker"], exc)
    return published


def run():
    """Main loop: fetch live prices and publish to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["prices"]

    logger.info("Starting stock price producer (Finnhub + Alpha Vantage)")
    logger.info("  Stock polling every %ds, Crypto/Index every %ds",
                STOCK_POLL_INTERVAL_SECONDS, CRYPTO_POLL_INTERVAL_SECONDS)
    logger.info("  Tickers: %s", ALL_TICKERS)
    logger.info("  Publishing to topic: %s", topic)

    last_crypto_fetch = 0.0

    while True:
        published = 0

        # ── Stocks (Finnhub) — every cycle ──
        stock_bars = fetch_all_stock_quotes()
        published += _publish_bars(producer, topic, stock_bars)

        # ── Crypto + Indices (Alpha Vantage) — less frequent ──
        now = time.time()
        if now - last_crypto_fetch >= CRYPTO_POLL_INTERVAL_SECONDS:
            crypto_bars = fetch_all_crypto_quotes()
            published += _publish_bars(producer, topic, crypto_bars)

            index_bars = fetch_all_index_quotes()
            published += _publish_bars(producer, topic, index_bars)

            last_crypto_fetch = now
            logger.info("  Crypto + Index refresh done")

        logger.info("Cycle complete — %d tickers published", published)
        time.sleep(STOCK_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
