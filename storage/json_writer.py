"""
json_writer.py
--------------
Write processed pipeline output to JSON files for quick dashboard reads.

Outputs:
  - data/processed/articles.json    — articles with sentiment
  - data/processed/prices.json      — latest price snapshot
  - data/processed/aggregates.json  — sentiment by source/asset/time
"""

import json
import os
import logging
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    PROCESSED_DATA_DIR,
    PROCESSED_ARTICLES_PATH,
    PROCESSED_PRICES_PATH,
    PROCESSED_AGGREGATES_PATH,
)

logger = logging.getLogger(__name__)


def _ensure_dir():
    """Ensure the processed data directory exists."""
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)


def write_articles(articles: list):
    """Write processed articles to JSON."""
    _ensure_dir()
    with open(PROCESSED_ARTICLES_PATH, "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Wrote %d articles → %s", len(articles), PROCESSED_ARTICLES_PATH)


def write_prices(snapshot: dict):
    """Write latest price snapshot to JSON."""
    _ensure_dir()
    with open(PROCESSED_PRICES_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Wrote %d tickers → %s", len(snapshot), PROCESSED_PRICES_PATH)


def write_aggregates(aggregates: dict):
    """
    Write sentiment aggregates to JSON.

    Args:
        aggregates: dict with keys 'by_source', 'by_asset', 'by_time', 'distribution'
    """
    _ensure_dir()
    with open(PROCESSED_AGGREGATES_PATH, "w", encoding="utf-8") as f:
        json.dump(aggregates, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Wrote aggregates → %s", PROCESSED_AGGREGATES_PATH)


def read_articles() -> list:
    """Read processed articles from JSON."""
    if not os.path.exists(PROCESSED_ARTICLES_PATH):
        return []
    try:
        with open(PROCESSED_ARTICLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


def read_prices() -> dict:
    """Read latest price snapshot from JSON."""
    if not os.path.exists(PROCESSED_PRICES_PATH):
        return {}
    try:
        with open(PROCESSED_PRICES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def read_aggregates() -> dict:
    """Read sentiment aggregates from JSON."""
    if not os.path.exists(PROCESSED_AGGREGATES_PATH):
        return {}
    try:
        with open(PROCESSED_AGGREGATES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}
