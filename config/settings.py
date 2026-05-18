import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Kafka Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPICS = {
    "news":   "news-feed",
    "social": "social-posts",
    "prices": "stock-prices",
}

# ---------------------------------------------------------------------------
# Financial Assets
# ---------------------------------------------------------------------------
ASSETS = {
    "stocks":  ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA"],
    "indices": ["^GSPC", "^IXIC"],       # S&P 500, NASDAQ Composite
    "crypto":  ["BTC-USD", "ETH-USD"],
}

# Flat list used by stock producer
ALL_TICKERS = ASSETS["stocks"] + ASSETS["indices"] + ASSETS["crypto"]

# ---------------------------------------------------------------------------
# RSS Feed Sources
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    {"name": "Reuters Business",  "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "Yahoo Finance",     "url": "https://finance.yahoo.com/news/rssindex"},
    {"name": "CNBC Top News",     "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "MarketWatch",       "url": "https://feeds.marketwatch.com/marketwatch/topstories/"},
    {"name": "Seeking Alpha",     "url": "https://seekingalpha.com/feed.xml"},
    {"name": "Investing.com",     "url": "https://www.investing.com/rss/news.rss"},
]

# ---------------------------------------------------------------------------
# Producer / Polling Settings
# ---------------------------------------------------------------------------
RSS_POLL_INTERVAL_SECONDS   = 60   # how often to re-fetch RSS feeds
STOCK_POLL_INTERVAL_SECONDS = 60   # how often to fetch latest price bar

# yfinance fetch window for stock producer
YFINANCE_PERIOD   = "1d"
YFINANCE_INTERVAL = "1m"

# ---------------------------------------------------------------------------
# Flask Settings
# ---------------------------------------------------------------------------
FLASK_HOST  = os.getenv("FLASK_HOST", "0.0.0.0")
FLASK_PORT  = int(os.getenv("FLASK_PORT", 5000))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR          = os.path.join(PROJECT_ROOT, "data")
SAMPLE_DATA_DIR   = os.path.join(DATA_DIR, "sample")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")

SAMPLE_NEWS_PATH   = os.path.join(SAMPLE_DATA_DIR, "news_articles.json")
SAMPLE_PRICES_PATH = os.path.join(SAMPLE_DATA_DIR, "stock_prices.json")

PROCESSED_ARTICLES_PATH   = os.path.join(PROCESSED_DATA_DIR, "articles.json")
PROCESSED_PRICES_PATH     = os.path.join(PROCESSED_DATA_DIR, "prices.json")
PROCESSED_AGGREGATES_PATH = os.path.join(PROCESSED_DATA_DIR, "aggregates.json")

SQLITE_DB_PATH = os.path.join(DATA_DIR, "analytics.db")

# ---------------------------------------------------------------------------
# PySpark Settings
# ---------------------------------------------------------------------------
SPARK_APP_NAME   = "FinIntel-Pipeline"
SPARK_MASTER     = os.getenv("SPARK_MASTER", "local[*]")
SPARK_LOG_LEVEL  = os.getenv("SPARK_LOG_LEVEL", "WARN")

# ---------------------------------------------------------------------------
# Sentiment Model Settings
# ---------------------------------------------------------------------------
FINBERT_MODEL_NAME = "ProsusAI/finbert"
SENTIMENT_MAX_LENGTH = 512
SENTIMENT_BATCH_SIZE = 16
