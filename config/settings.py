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
# API Keys (from .env)
# ---------------------------------------------------------------------------
FINNHUB_API_KEY       = os.getenv("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")

# ---------------------------------------------------------------------------
# Pinecone Vector Database
# ---------------------------------------------------------------------------
PINECONE_API_KEY    = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX      = os.getenv("PINECONE_INDEX", "finintel")
PINECONE_CLOUD      = os.getenv("PINECONE_CLOUD", "aws")
PINECONE_REGION     = os.getenv("PINECONE_REGION", "us-east-1")
PINECONE_DIMENSION  = 1536   # text-embedding-3-small

# ---------------------------------------------------------------------------
# OpenRouter (LLM + Embeddings)
# ---------------------------------------------------------------------------
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL           = os.getenv("LLM_MODEL", "openai/gpt-4o-mini")
EMBEDDING_MODEL     = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")

# ---------------------------------------------------------------------------
# API Base URLs
# ---------------------------------------------------------------------------
FINNHUB_BASE_URL       = "https://finnhub.io/api/v1"
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
APEWISDOM_BASE_URL     = "https://apewisdom.io/api/v1.0"  # Reddit sentiment (free, no key)

# ---------------------------------------------------------------------------
# Rate-Limit Settings
# ---------------------------------------------------------------------------
FINNHUB_MAX_CALLS_PER_MIN      = 60   # free tier
ALPHA_VANTAGE_MAX_CALLS_PER_DAY = 25   # free tier
ALPHA_VANTAGE_CALL_DELAY_SEC    = 12   # ~5 calls/min safety margin

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

# Separate lists for API routing
FINNHUB_TICKERS   = ASSETS["stocks"]                  # Finnhub handles normal stocks
ALPHA_CRYPTO      = ASSETS["crypto"]                   # Alpha Vantage for crypto
ALPHA_INDICES     = ASSETS["indices"]                   # Alpha Vantage for indices

# Crypto symbol mapping (project format → Alpha Vantage format)
CRYPTO_SYMBOL_MAP = {"BTC-USD": ("BTC", "USD"), "ETH-USD": ("ETH", "USD")}
# Index symbol mapping (project format → Alpha Vantage symbol)
INDEX_SYMBOL_MAP  = {"^GSPC": "SPY", "^IXIC": "QQQ"}  # ETF proxies

# ---------------------------------------------------------------------------
# Finnhub News Categories
# ---------------------------------------------------------------------------
FINNHUB_NEWS_CATEGORIES = ["general", "forex", "crypto", "merger"]

# ---------------------------------------------------------------------------
# Producer / Polling Settings
# ---------------------------------------------------------------------------
NEWS_POLL_INTERVAL_SECONDS   = 60   # how often to fetch Finnhub news
STOCK_POLL_INTERVAL_SECONDS  = 60   # how often to fetch stock quotes
CRYPTO_POLL_INTERVAL_SECONDS = 1800 # 30 min — saves Alpha Vantage quota
SOCIAL_POLL_INTERVAL_SECONDS = 60   # how often to fetch Tradestie data

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
SPARK_APP_NAME      = "FinIntel-Pipeline"
SPARK_MASTER        = os.getenv("SPARK_MASTER", "local[*]")
SPARK_LOG_LEVEL     = os.getenv("SPARK_LOG_LEVEL", "WARN")
SPARK_KAFKA_PACKAGES = os.getenv(
    "SPARK_KAFKA_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)
STREAMING_CHECKPOINT_DIR = os.getenv(
    "STREAMING_CHECKPOINT_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "checkpoints"),
)
STREAMING_TRIGGER_SECONDS = int(os.getenv("STREAMING_TRIGGER_SECONDS", "30"))

# ---------------------------------------------------------------------------
# Sentiment Model Settings
# ---------------------------------------------------------------------------
FINBERT_MODEL_NAME = "ProsusAI/finbert"
SENTIMENT_MAX_LENGTH = 512
SENTIMENT_BATCH_SIZE = 16
