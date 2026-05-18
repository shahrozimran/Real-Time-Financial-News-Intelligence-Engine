"""
social_producer.py
------------------
Generates simulated social media posts about financial assets and publishes
them to the Kafka 'social-posts' topic. In a production system this would
connect to Twitter/X API, Reddit, or StockTwits — here we simulate realistic
posts for demonstration purposes.

Run:
    python ingestion/social_producer.py
"""

import json
import logging
import time
import random
import hashlib
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_BROKER, TOPICS, ALL_TICKERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Simulated social post templates
BULLISH_TEMPLATES = [
    "{ticker} looking strong today! Breakout imminent 🚀",
    "Just loaded up on {ticker}. This dip is a gift 💰",
    "{ticker} earnings gonna crush it. Mark my words.",
    "Technical analysis shows {ticker} about to rip. Cup and handle forming.",
    "Institutional buying on {ticker} is off the charts 📈",
    "{ticker} is undervalued at these levels. Easy 2x from here.",
    "Big money flowing into {ticker} options. Something's brewing.",
    "Love the {ticker} setup here. Strong support at current levels.",
    "{ticker} just broke resistance. Next stop: all-time highs!",
    "Everyone sleeping on {ticker} right now. Not for long.",
    "Added more {ticker} to my portfolio today. Conviction play.",
    "{ticker} fundamentals are insane at this price. Buying more.",
]

BEARISH_TEMPLATES = [
    "{ticker} is overvalued. Shorting this garbage 🐻",
    "Sold all my {ticker}. This company is done.",
    "{ticker} chart looks terrible. Head and shoulders forming.",
    "Insiders dumping {ticker} shares. Red flag 🚩",
    "{ticker} revenue growth slowing. Time to exit.",
    "Bearish divergence on {ticker}. Expect a pullback.",
    "{ticker} competition is eating their lunch. Avoid.",
    "Stay away from {ticker}. Management can't execute.",
    "{ticker} about to get rekt. Overextended on every metric.",
    "Smart money exiting {ticker} positions quietly.",
    "{ticker} guidance was disappointing. Downtrend ahead.",
    "Too much debt on {ticker} balance sheet. Risky.",
]

NEUTRAL_TEMPLATES = [
    "{ticker} consolidating here. Waiting for direction.",
    "Anyone else watching {ticker}? Can't decide if buy or sell.",
    "{ticker} earnings tomorrow. Could go either way.",
    "Holding {ticker} for now but setting tight stop-loss.",
    "{ticker} trading sideways. Need a catalyst.",
    "What's everyone's take on {ticker}? Mixed signals.",
    "{ticker} range-bound. Waiting for breakout or breakdown.",
    "Watching {ticker} closely. Key level approaching.",
]

PLATFORMS = ["Twitter/X", "Reddit r/wallstreetbets", "Reddit r/stocks",
             "StockTwits", "Reddit r/investing", "Discord Trading"]

USERNAMES = [
    "TraderJoe99", "WallStBull", "DiamondHands420", "OptionsKing",
    "ValueInvestor", "TechTrader", "CryptoWhale", "SwingMaster",
    "DayTraderPro", "StockPickerX", "BearishBob", "MoonShot",
    "AlphaSeeker", "RetailTrader", "DegenPlays", "QuantMind",
    "MarketNerd", "FinTechGuru", "ChartWizard", "VolatilityTrader",
]


def build_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def generate_post() -> dict:
    """Generate a random simulated social media post."""
    ticker = random.choice(ALL_TICKERS)
    sentiment_type = random.choices(
        ["bullish", "bearish", "neutral"],
        weights=[0.45, 0.30, 0.25],
        k=1
    )[0]

    if sentiment_type == "bullish":
        template = random.choice(BULLISH_TEMPLATES)
    elif sentiment_type == "bearish":
        template = random.choice(BEARISH_TEMPLATES)
    else:
        template = random.choice(NEUTRAL_TEMPLATES)

    text = template.format(ticker=ticker)
    post_id = hashlib.md5(f"{text}{datetime.now().isoformat()}".encode()).hexdigest()

    return {
        "id": post_id,
        "platform": random.choice(PLATFORMS),
        "username": random.choice(USERNAMES),
        "ticker": ticker,
        "text": text,
        "sentiment_hint": sentiment_type,
        "likes": random.randint(1, 5000),
        "reposts": random.randint(0, 500),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def run():
    """Main loop: generate and publish social posts to Kafka."""
    logger.info("Connecting to Kafka broker at %s …", KAFKA_BROKER)
    producer = build_producer()
    topic = TOPICS["social"]

    logger.info("Starting social posts producer — generating every 5s")
    logger.info("Publishing to topic: %s", topic)

    total_published = 0

    while True:
        # Generate 3-8 posts per cycle (simulating burst of social activity)
        batch_size = random.randint(3, 8)
        published = 0

        for _ in range(batch_size):
            post = generate_post()
            try:
                future = producer.send(topic, key=post["ticker"], value=post)
                future.get(timeout=10)
                published += 1
                logger.debug("Published: [%s] %s — %s",
                             post["platform"], post["ticker"], post["text"][:60])
            except KafkaError as exc:
                logger.error("Kafka send error: %s", exc)

        total_published += published
        logger.info("Cycle complete — %d posts published (total: %d)",
                    published, total_published)

        time.sleep(5)


if __name__ == "__main__":
    run()
