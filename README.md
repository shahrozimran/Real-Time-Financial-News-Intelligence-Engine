# Real-Time Financial News Intelligence Engine

A big-data streaming platform that ingests live financial news and stock prices,
performs sentiment analysis, and provides investment insights through a Flask dashboard
and RAG-powered conversational AI.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | `python --version` |
| Java JDK | 17 | Required by Kafka & PySpark. Set `JAVA_HOME`. |
| Apache Kafka | 3.6.x | Download from https://kafka.apache.org/downloads |
| pip | latest | `python -m pip install --upgrade pip` |

---

## Quick Setup

### 1. Clone & create virtual environment
```powershell
git clone https://github.com/shahrozimran/Real-Time-Financial-News-Intelligence-Engine.git
cd "Big Data/Project"
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 2. Install Python dependencies
```powershell
pip install -r requirements.txt
```

### 2b. Download NLTK data (one-time)
```powershell
python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('punkt_tab')"
```

### 3. Configure environment
```powershell
copy .env .env.local   # optional; .env already has defaults
```
Edit `.env` if your Kafka broker runs on a different host/port.

---

## Running Kafka (Windows)

Open **three separate PowerShell terminals**:

**Terminal 1 — ZooKeeper**
```powershell
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
```

**Terminal 2 — Kafka Broker**
```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

**Terminal 3 — Create Topics** (run once)
```powershell
C:\kafka\bin\windows\kafka-topics.bat --create --topic news-feed    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic social-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic stock-prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify topics:
```powershell
C:\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

---

## Running the Producers

**RSS News Producer** (polls 6 RSS feeds every 60 s → `news-feed` topic)
```powershell
python ingestion/rss_producer.py
```

**Stock Price Producer** (fetches latest bar via yfinance every 60 s → `stock-prices` topic)
```powershell
python ingestion/stock_producer.py
```

---

## Verifying Kafka Messages

Subscribe to all topics and print incoming messages:
```powershell
python ingestion/consumer_test.py

# Or filter to a single topic:
python ingestion/consumer_test.py news-feed
python ingestion/consumer_test.py stock-prices
```

---

## Running the PySpark Pipeline

Process sample data through the full cleaning → sentiment → analytics pipeline:
```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
python processing/batch_pipeline.py
```

This generates:
- `data/processed/articles.json` — 55 articles with sentiment labels
- `data/processed/prices.json` — latest price snapshot for 10 tickers
- `data/processed/aggregates.json` — sentiment by source, asset, and time
- `data/analytics.db` — SQLite database with all aggregates

---

## Running the Flask Dashboard

```powershell
python webapp/app.py
```

Open http://localhost:5000 in your browser.

The dashboard automatically reads from pipeline output in `data/processed/`.
If the pipeline hasn't run yet, placeholder data is shown instead.

### Available API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Main dashboard |
| GET | `/api/news` | Latest news (`?limit=10&sentiment=positive`) |
| GET | `/api/prices` | Market prices (`?ticker=AAPL`) |
| GET | `/api/sentiment-trend` | 7-day sentiment data |
| GET | `/api/sentiment-by-source` | Sentiment breakdown by news source |
| GET | `/api/sentiment-by-asset` | Sentiment breakdown by ticker |
| GET | `/api/risk-alerts` | Risk alerts (`?level=high`) |
| POST | `/api/chat` | AI chat (`{"question": "..."}`) |
| GET | `/api/status` | Health check |

---

## Project Structure

```
Project/
├── config/
│   └── settings.py                # Central config: Kafka, Spark, assets, storage paths
├── ingestion/
│   ├── rss_producer.py            # RSS → Kafka producer
│   ├── stock_producer.py          # yfinance → Kafka producer
│   ├── social_producer.py         # Social media → Kafka producer
│   └── consumer_test.py           # Manual Kafka verification consumer
├── processing/
│   ├── spark_session.py         # SparkSession factory (batch + streaming modes)
│   ├── cleaning_pipeline.py     # HTML strip, URL remove, stopwords, dedup
│   ├── sentiment_processor.py   # FinBERT Spark UDF wrapper (used by streaming pipeline)
│   ├── spark_sql_analytics.py   # Aggregations by asset/source/time
│   ├── stock_analytics.py       # MA, volatility, volume anomaly
│   ├── batch_pipeline.py        # Batch orchestrator: load→clean→sentiment→social→store
│   └── streaming_pipeline.py    # PySpark Structured Streaming: Kafka→clean→sentiment→store
├── intelligence/
│   └── sentiment_model.py         # FinBERT + VADER sentiment analysis
├── storage/
│   ├── sqlite_writer.py           # SQLite persistence layer
│   └── json_writer.py             # JSON file I/O for dashboard
├── webapp/
│   ├── app.py                     # Flask application (reads processed data)
│   ├── templates/
│   │   └── index.html             # Dashboard with charts & analytics
│   └── static/
│       └── style.css              # Custom dark-theme styles
├── data/
│   ├── sample/
│   │   └── generate_sample_data.py  # Generates realistic test data
│   └── processed/                   # Pipeline output (gitignored)
├── requirements.txt
└── README.md
```

---

## Week-by-Week Progress

| Week | Status | Focus |
|------|--------|-------|
| Week 1 | ✅ Complete | Environment, Kafka, ingestion producers (news/stock/social), Flask skeleton |
| Week 2 | ✅ Complete | PySpark batch pipeline, FinBERT/VADER sentiment, Spark SQL analytics, dashboard wiring |
| Week 3 | ✅ Complete | Pinecone vector DB (3 namespaces), RAG retriever, LangGraph multi-agent AI framework |
| Week 4 | ✅ Complete | PySpark Structured Streaming, social sentiment pipeline, full integration, live data throughout |

---

## Target Financial Assets

- **Stocks:** AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA
- **Indices:** S&P 500 (^GSPC), NASDAQ (^IXIC)
- **Crypto:** BTC-USD, ETH-USD

## RSS Feed Sources

- Reuters Business
- Yahoo Finance News
- CNBC Top News
- MarketWatch
- Seeking Alpha
- Investing.com

---

## Week 2 Highlights

- **PySpark batch pipeline** — cleans 55 sample articles (HTML strip, URL removal, stopword removal, deduplication) and computes Spark SQL aggregations.
- **Sentiment analysis** — FinBERT model with automatic VADER fallback. Produces positive/negative/neutral labels with confidence scores.
- **Spark SQL analytics** — sentiment by asset (8 tickers), by source (6 feeds), by time (7-day trend), overall distribution, and top movers.
- **Stock analytics** — 5-bar and 20-bar moving averages, rolling volatility, daily change %, volume anomaly detection.
- **Dual storage** — results written to both JSON files (`data/processed/`) and SQLite (`data/analytics.db`).
- **Live dashboard** — Flask routes now read from pipeline output; new stacked bar charts for sentiment by source and by asset; dynamic risk alerts generated from sentiment data.

---

## Limitations (Week 2)

- FinBERT requires PyTorch; on Python 3.14 the CPU torch build may fall back to VADER automatically.
- Chat endpoint returns a placeholder — RAG integration in Week 3.
- Social media stream uses simulated data (live API access may require approval).
- Free stock APIs (yfinance) may return delayed data outside market hours.
- Kafka streaming integration is Phase 2 — current pipeline runs on sample data.
