"""
stock_producer.py
-----------------
Fetches the latest price bar for each configured ticker using yfinance and
publishes it as a JSON message to the Kafka 'stock-prices' topic.

Run:
    python ingestion/stock_producer.py
"""

import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
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
    YFINANCE_PERIOD,
    YFINANCE_INTERVAL,
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


def fetch_latest_bar(ticker: str) -> dict | None:
    """
    Download recent data for a ticker and return the last available bar
    as a plain dict.  Returns None if no data is available.
    """
    try:
        df = yf.download(
            ticker,
            period=YFINANCE_PERIOD,
            interval=YFINANCE_INTERVAL,
            progress=False,
            auto_adjust=True,
        )
        if df.empty:
            logger.warning("No data returned for %s", ticker)
            return None

        # Flatten multi-level columns (yfinance >= 0.2.38 returns MultiIndex)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        row = df.iloc[-1]
        bar_time = str(df.index[-1])

        return {
            "ticker":      ticker,
            "open":        round(float(row["Open"]),   4),
            "high":        round(float(row["High"]),   4),
            "low":         round(float(row["Low"]),    4),
            "close":       round(float(row["Close"]),  4),
            "volume":      int(row["Volume"]),
            "bar_time":    bar_time,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        logger.error("Error fetching data for %s: %s", ticker, exc)
        return None


def run():
    """Main loop: fetch latest price bar for each ticker and publish to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["prices"]

    logger.info("Starting stock price producer — polling every %ds", STOCK_POLL_INTERVAL_SECONDS)
    logger.info("Tickers: %s", ALL_TICKERS)
    logger.info("Publishing to topic: %s", topic)

    while True:
        published = 0
        for ticker in ALL_TICKERS:
            bar = fetch_latest_bar(ticker)
            if bar is None:
                continue
            try:
                future = producer.send(topic, key=ticker, value=bar)
                future.get(timeout=10)
                published += 1
                logger.debug(
                    "Published: %s close=%.4f @ %s",
                    ticker, bar["close"], bar["bar_time"],
                )
            except KafkaError as exc:
                logger.error("Kafka send error for %s: %s", ticker, exc)

        logger.info("Cycle complete — %d/%d tickers published", published, len(ALL_TICKERS))
        time.sleep(STOCK_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
