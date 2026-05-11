"""
consumer_test.py
----------------
Manual verification consumer.  Subscribes to all three Kafka topics and
pretty-prints every incoming message.  Use this to confirm producers are
working correctly.  NOT part of the production pipeline.

Run:
    python ingestion/consumer_test.py

Optional: filter to a single topic
    python ingestion/consumer_test.py news-feed
    python ingestion/consumer_test.py stock-prices
    python ingestion/consumer_test.py social-posts
"""

import json
import sys
import logging
import os

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_BROKER, TOPICS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

COLORS = {
    TOPICS["news"]:   "\033[94m",   # blue
    TOPICS["social"]: "\033[93m",   # yellow
    TOPICS["prices"]: "\033[92m",   # green
}
RESET = "\033[0m"


def build_consumer(topics: list[str]) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="consumer-test-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=300_000,  # stop after 5 min of silence
    )


def run():
    # Allow optional single-topic filter via CLI argument
    if len(sys.argv) > 1:
        topics = [sys.argv[1]]
    else:
        topics = list(TOPICS.values())

    logger.info("Subscribing to topics: %s", topics)
    logger.info("Broker: %s", KAFKA_BROKER)
    logger.info("Waiting for messages … (Ctrl+C to stop)\n")

    consumer = build_consumer(topics)
    msg_count = 0

    try:
        for msg in consumer:
            msg_count += 1
            color = COLORS.get(msg.topic, "")
            header = f"{color}[{msg.topic}]{RESET} partition={msg.partition} offset={msg.offset}"

            if msg.key:
                header += f" key={msg.key}"

            print(header)
            print(json.dumps(msg.value, indent=2, default=str))
            print("-" * 60)

    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except KafkaError as exc:
        logger.error("Kafka error: %s", exc)
    finally:
        consumer.close()
        logger.info("Total messages received: %d", msg_count)


if __name__ == "__main__":
    run()
