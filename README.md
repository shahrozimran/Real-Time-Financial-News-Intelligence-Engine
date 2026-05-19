# Real-Time Financial News Intelligence Engine

A **big-data streaming platform** that ingests live financial news, stock prices, and Reddit social sentiment through Apache Kafka, processes them with PySpark (batch + structured streaming), applies FinBERT-powered NLP sentiment analysis, stores results in SQLite and a Pinecone vector database, and delivers investment insights through a Flask dashboard with a LangGraph multi-agent AI chat interface.

**Student:** Shahroz Imran | **Course:** Big Data (6th Semester) | **University:** UCP

---

## Project Status — All 4 Weeks Complete ✅

| Week | Status | Deliverables |
|------|--------|--------------|
| Week 1 | ✅ Complete | Kafka KRaft setup, 3 ingestion producers (news/stock/social), Flask dashboard skeleton, dark-theme UI |
| Week 2 | ✅ Complete | PySpark batch pipeline, FinBERT/VADER sentiment, Spark SQL analytics, SQLite storage, live dashboard wiring |
| Week 3 | ✅ Complete | Pinecone vector DB (4 namespaces), RAG retriever, LangGraph 5-node multi-agent AI framework |
| Week 4 | ✅ Complete | PySpark Structured Streaming, Docker Compose (7 services), persistent HuggingFace model cache, 20+ API endpoints |

---

## Architecture Overview

```
External APIs (Finnhub, Alpha Vantage, ApeWisdom/Reddit)
        │
        ▼
Layer 1: Ingestion  →  rss_producer.py / stock_producer.py / social_producer.py
        │
        ▼
Layer 2: Apache Kafka (KRaft)  →  news-feed / stock-prices / social-posts  (3 partitions each)
        │
        ▼
Layer 3: PySpark  →  streaming_pipeline.py (micro-batches) + batch_pipeline.py (full runs)
        │
        ▼
Layer 4: NLP  →  FinBERT (ProsusAI/finbert)  with  VADER fallback
        │
        ▼
Layer 5: Storage  →  JSON flat files  +  SQLite (5 tables)  +  Pinecone vector DB (4 namespaces)
        │
        ▼
Layer 6: Intelligence  →  RAG retriever (rag_retriever.py) + LangGraph multi-agent (agents.py)
        │
        ▼
Layer 7: Flask Dashboard  →  20+ REST endpoints + Jinja2 dark-theme UI + Chart.js charts
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | `python --version` |
| Java JDK | 17 | Required by PySpark. Set `JAVA_HOME`. |
| Apache Kafka | 3.7.x (KRaft) | https://kafka.apache.org/downloads — no ZooKeeper needed |
| Docker + Compose | latest | Optional — runs everything in containers |
| pip | latest | `python -m pip install --upgrade pip` |

---

## Quick Setup (Local)

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

### 3. Download NLTK data (one-time)
```powershell
python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('punkt_tab')"
```

### 4. Configure environment variables
Copy `.env.example` to `.env` and fill in your API keys:

```powershell
copy .env.example .env
```

| Variable | Description |
|----------|-------------|
| `FINNHUB_API_KEY` | Finnhub REST API key (news + stock quotes) |
| `ALPHAVANTAGE_API_KEY` | Alpha Vantage key (crypto + indices) |
| `PINECONE_API_KEY` | Pinecone vector DB key |
| `PINECONE_INDEX` | Pinecone index name (default: `finintel`) |
| `OPENROUTER_API_KEY` | OpenRouter key for GPT-4o-mini + embeddings |
| `KAFKA_BROKER` | Default: `localhost:9092` |

---

## Running Kafka (Windows — KRaft Mode)

Kafka 3.7.x runs without ZooKeeper. Open **two** PowerShell terminals:

**Terminal 1 — Format storage (one-time only)**
```powershell
C:\kafka\bin\windows\kafka-storage.bat format --standalone -t <UUID> -c C:\kafka\config\kraft\server.properties
```

**Terminal 1 — Start Kafka broker**
```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties
```

**Terminal 2 — Create topics (run once)**
```powershell
C:\kafka\bin\windows\kafka-topics.bat --create --topic news-feed    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic social-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic stock-prices  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify:
```powershell
C:\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

---

## Running the Producers

Each producer polls its data source every 60 seconds and publishes to the appropriate Kafka topic.

**News Producer** (Finnhub → `news-feed`)
```powershell
python ingestion/rss_producer.py
```

**Stock Price Producer** (Finnhub + Alpha Vantage → `stock-prices`)
```powershell
python ingestion/stock_producer.py
```

**Social Sentiment Producer** (ApeWisdom/Reddit → `social-posts`)
```powershell
python ingestion/social_producer.py
```

**Verify messages are flowing:**
```powershell
python ingestion/consumer_test.py           # all topics
python ingestion/consumer_test.py news-feed # single topic
```

---

## Running the Batch Pipeline

Fetches live data from APIs, runs the full 9-step pipeline, and writes all output files:

```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
python processing/batch_pipeline.py
```

**Pipeline steps:** fetch live data → ensure sample data → start Spark → clean articles → sentiment analysis (FinBERT/VADER) → social sentiment → Spark SQL aggregations → price analytics (MA5/MA20/volatility) → persist to JSON + SQLite + Pinecone

**Output files generated:**

| File | Contents |
|------|----------|
| `data/processed/articles.json` | All articles with sentiment labels + confidence scores |
| `data/processed/prices.json` | Latest price snapshot for all 10 tickers |
| `data/processed/aggregates.json` | Sentiment by source, asset, time, top movers |
| `data/analytics.db` | SQLite database — 5 tables, 6 indexes |

---

## Running the Streaming Pipeline

Continuously consumes all three Kafka topics using PySpark Structured Streaming:

```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
python processing/streaming_pipeline.py --topic all
```

Processes a micro-batch every 30 seconds (configurable via `STREAMING_TRIGGER_SECONDS`). Checkpoint files in `data/checkpoints/` ensure exactly-once processing across restarts.

---

## Running the Flask Dashboard

```powershell
python webapp/app.py
```

Open **http://localhost:5000** in your browser.

The dashboard uses a **3-level fallback** for every route:
1. Read from `data/processed/` JSON files (fastest)
2. Fall back to SQLite `analytics.db` query
3. Fall back to a live API call (always works, even before any pipeline run)

---

## Docker Compose (Full Stack)

Runs all 7 services with a single command:

```powershell
docker compose up -d
```

| Service | Container | Role |
|---------|-----------|------|
| `kafka` | `finintel-kafka` | Kafka KRaft broker (port 9092) |
| `kafka-init` | `finintel-kafka-init` | Creates 3 topics on first boot, then exits |
| `webapp` | `finintel-webapp` | Flask dashboard (port 5000 → 80) |
| `rss-producer` | `finintel-rss-producer` | News ingestion loop |
| `stock-producer` | `finintel-stock-producer` | Price ingestion loop |
| `social-producer` | `finintel-social-producer` | Reddit sentiment ingestion loop |
| `streaming-pipeline` | `finintel-streaming-pipeline` | PySpark Structured Streaming |

**Run batch pipeline manually inside Docker:**
```powershell
docker compose run --rm batch-pipeline
```

**Enable streaming pipeline profile:**
```powershell
docker compose --profile streaming up -d
```

> The `streaming-pipeline` service mounts a persistent `hf_cache` volume so FinBERT (~400 MB) downloads only once and survives container restarts.

---

## API Endpoints

All served by Flask at `http://localhost:5000`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Main dashboard HTML |
| GET | `/api/status` | Health check — version, pipeline status, tickers |
| GET | `/api/news` | Processed articles (`?limit=10&source=X&sentiment=positive`) |
| GET | `/api/prices` | Latest price snapshot (`?ticker=TSLA`) |
| GET | `/api/sentiment-trend` | 7-day daily sentiment percentages |
| GET | `/api/sentiment-by-source` | Sentiment counts per news source |
| GET | `/api/sentiment-by-asset` | Sentiment counts per ticker |
| GET | `/api/risk-alerts` | Risk alerts (`?level=high\|medium`) |
| GET | `/api/social-sentiment` | Live Reddit posts from ApeWisdom |
| GET | `/api/social` | Stored social posts from SQLite (`?ticker=X&sentiment=X`) |
| GET | `/api/live-prices` | Real-time prices direct from APIs (bypasses pipeline) |
| GET | `/api/live-news` | Real-time news direct from Finnhub (bypasses pipeline) |
| GET | `/api/search` | Stock symbol search (`?q=tesla`) |
| GET | `/api/stock/<SYMBOL>` | Quote + company profile + financials |
| GET | `/api/stock/<SYMBOL>/candles` | OHLCV candle data for charts |
| GET | `/api/stock/<SYMBOL>/news` | Company-specific news (last 7 days) |
| GET | `/api/stock/<SYMBOL>/peers` | Industry peer comparison |
| GET | `/api/stock/<SYMBOL>/social` | Reddit sentiment for a specific ticker |
| POST | `/api/chat` | RAG + multi-agent AI (`{"message": "Why is TSLA dropping?"}`) |
| GET | `/api/rag/search` | Direct Pinecone search (`?q=X&namespace=articles&top_k=5`) |

---

## AI Chat — Multi-Agent Pipeline

`POST /api/chat` runs a **LangGraph StateGraph** with 5 sequential nodes:

```
retrieve_context (Pinecone RAG)
        ↓
market_analyst_node   — market movements, price trends, news impact
        ↓
risk_manager_node     — red flags, volatility, negative signals
        ↓
portfolio_advisor_node — educational portfolio considerations
        ↓
summarizer_node       — 4-6 sentence unified synthesis
        ↓
Response: { market_analyst, risk_manager, portfolio_advisor, summary, sources }
```

- **LLM:** `openai/gpt-4o-mini` via OpenRouter (temperature 0.3)
- **Embeddings:** `text-embedding-3-small` (1536-dim) via OpenRouter
- **Non-financial queries** are routed directly to a single LLM call (skips RAG pipeline to save cost)

---

## Project Structure

```
Project/
├── config/
│   └── settings.py              # All env vars, API keys, Kafka topics, asset lists, paths
├── ingestion/
│   ├── live_data_fetcher.py     # All external API calls (Finnhub, Alpha Vantage, ApeWisdom)
│   ├── rss_producer.py          # News articles → news-feed Kafka topic
│   ├── stock_producer.py        # OHLCV quotes → stock-prices Kafka topic
│   ├── social_producer.py       # Reddit posts → social-posts Kafka topic
│   └── consumer_test.py         # Dev utility: print Kafka messages to console
├── processing/
│   ├── spark_session.py         # SparkSession factory (batch + streaming configs)
│   ├── cleaning_pipeline.py     # HTML strip, URL remove, stopwords, deduplication
│   ├── sentiment_processor.py   # PySpark UDF wrapper for FinBERT/VADER
│   ├── spark_sql_analytics.py   # SparkSQL aggregations: by_asset, by_source, by_time
│   ├── stock_analytics.py       # Price analytics: MA5, MA20, volatility, volume anomaly
│   ├── batch_pipeline.py        # 9-step batch orchestrator
│   └── streaming_pipeline.py    # PySpark Structured Streaming (foreachBatch, 30s trigger)
├── intelligence/
│   ├── sentiment_model.py       # FinBERT + VADER model loading, predict_batch()
│   ├── rag_retriever.py         # Pinecone semantic search (4 namespaces)
│   └── agents.py                # LangGraph multi-agent graph (5 nodes)
├── storage/
│   ├── json_writer.py           # Read/write data/processed/*.json flat files
│   ├── sqlite_writer.py         # SQLite: init tables, upsert, query helpers
│   └── pinecone_writer.py       # Embed + upsert all data to Pinecone
├── webapp/
│   ├── app.py                   # Flask app: 20+ REST routes + Jinja2 dashboard
│   ├── templates/index.html     # Dark-theme dashboard with Chart.js charts
│   └── static/style.css         # Custom CSS design system
├── data/
│   ├── sample/
│   │   └── generate_sample_data.py  # Offline fallback sample data generator
│   └── processed/               # Pipeline output (gitignored)
├── docker-compose.yml           # 7-service orchestration
├── Dockerfile                   # python:3.11-slim + OpenJDK + requirements
├── requirements.txt             # All Python dependencies (Weeks 1–4)
├── .env.example                 # Template for secrets
├── ARCHITECTURE.md              # Full detailed architecture report
├── KAFKA_STARTUP.md             # Kafka KRaft startup reference
└── WEEK1_REPORT.md              # Week 1 project report
```

---

## Technology Stack

### Core Big Data
| Component | Technology | Version |
|-----------|------------|---------|
| Message Broker | Apache Kafka (KRaft) | 3.7.1 |
| Stream Processing | PySpark Structured Streaming | 3.5.3 |
| Batch Processing | PySpark + SparkSQL | 3.5.3 |

### NLP & AI
| Component | Technology |
|-----------|------------|
| Primary Sentiment | ProsusAI/FinBERT (HuggingFace Transformers) |
| Fallback Sentiment | VADER (vaderSentiment) |
| LLM | GPT-4o-mini via OpenRouter |
| Embeddings | text-embedding-3-small (1536-dim) via OpenRouter |
| Agentic Framework | LangGraph (StateGraph) |
| LLM Client | LangChain + langchain-openai |

### Storage
| Tier | Technology | Use Case |
|------|------------|----------|
| Hot | JSON flat files (`data/processed/`) | Flask dashboard fast reads |
| Warm | SQLite (WAL mode, 5 tables) | Filtered queries by ticker/sentiment/source |
| Semantic | Pinecone Serverless (4 namespaces, cosine) | RAG retrieval, semantic search |

### Data Sources
| Source | API | Data |
|--------|-----|------|
| Finnhub | `finnhub.io/api/v1` | Stock quotes, market news, company news |
| Alpha Vantage | `alphavantage.co/query` | Crypto rates, index ETF proxies, OHLCV |
| ApeWisdom | `apewisdom.io/api/v1.0` | Reddit stock/crypto mentions & sentiment |

### Web & Infrastructure
| Component | Technology |
|-----------|------------|
| Web Server | Flask 3.0.3 |
| Frontend Charts | Chart.js (CDN) |
| Templating | Jinja2 |
| Containerization | Docker + Docker Compose |

---

## Target Financial Assets

- **Stocks:** AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA
- **Indices:** S&P 500 (`^GSPC` → SPY proxy), NASDAQ (`^IXIC` → QQQ proxy)
- **Crypto:** BTC-USD, ETH-USD

---

## Week-by-Week Highlights

### Week 1 — Ingestion & Dashboard Skeleton
- Kafka KRaft mode (no ZooKeeper) with 3 topics, 3 partitions each
- `rss_producer.py` — polls Finnhub for news every 60 s, MD5 deduplication
- `stock_producer.py` — Finnhub OHLCV + Alpha Vantage crypto/index quotes
- `social_producer.py` — ApeWisdom Reddit mentions with momentum scoring
- Flask dashboard skeleton with dark glassmorphism UI, scrolling ticker tape, Chart.js charts

### Week 2 — PySpark Processing & Sentiment
- `batch_pipeline.py` — 9-step orchestrator (fetch → clean → sentiment → analytics → store)
- `cleaning_pipeline.py` — HTML strip (BeautifulSoup), URL removal, stopwords (NLTK), dedup
- `sentiment_model.py` — FinBERT (`ProsusAI/finbert`, batch=16) with VADER fallback chain
- `spark_sql_analytics.py` — SparkSQL: sentiment by asset, source, time (7-day), top movers
- `stock_analytics.py` — PySpark Window: MA-5, MA-20, rolling volatility, volume anomaly flag
- Dual write: JSON flat files + SQLite (WAL mode, 5 tables, 6 indexes)
- Flask routes wired to pipeline output; dynamic risk alerts from sentiment data

### Week 3 — Vector DB & Multi-Agent AI
- `pinecone_writer.py` — embeds all data and upserts to 4 Pinecone namespaces (`articles`, `prices`, `sentiment`, `social`)
- `rag_retriever.py` — ticker-specific, risk-signal, and market-overview retrieval strategies
- `agents.py` — LangGraph `StateGraph` with 5 nodes: retrieve → market_analyst → risk_manager → portfolio_advisor → summarizer
- Financial vs non-financial query routing (90-keyword classifier) to save LLM cost
- `/api/chat` and `/api/rag/search` endpoints fully live

### Week 4 — Streaming Pipeline & Docker
- `streaming_pipeline.py` — PySpark Structured Streaming consuming all 3 Kafka topics simultaneously with `foreachBatch` handlers (30 s trigger)
- Spark → Pandas fallback path in streaming for UDF serialization robustness
- Kafka offset checkpointing (`data/checkpoints/`) for exactly-once processing
- Docker Compose with 7 services, named volumes, health checks, and dependency ordering
- Persistent `hf_cache` Docker volume — FinBERT (~400 MB) downloads once and survives restarts
- `HF_HOME` + `TRANSFORMERS_CACHE` env vars set for container model caching

---

## Known Limitations

- FinBERT requires PyTorch (~2 GB RAM). On low-memory machines the system automatically falls back to VADER.
- Alpha Vantage free tier: 25 calls/day. Crypto/index quotes are throttled to 1 call per 12 seconds.
- Finnhub free tier: 60 calls/min. Company news is fetched for 6 tickers per poll cycle.
- Free stock APIs return delayed data outside market hours.
- Pinecone free tier: upserts batched at 100 vectors/call. Initial embedding of a large dataset may be slow.
- PySpark on Windows requires `JAVA_HOME` set correctly and `winutils.exe` for some operations.
