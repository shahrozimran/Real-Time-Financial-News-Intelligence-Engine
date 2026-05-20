# Real-Time Financial News Intelligence Engine — Detailed Architecture Report

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Full Technology Stack](#3-full-technology-stack)
4. [Directory & File Structure](#4-directory--file-structure)
5. [Step-by-Step Data Flow (End-to-End Workflow)](#5-step-by-step-data-flow-end-to-end-workflow)
6. [Layer 1 — Data Ingestion](#6-layer-1--data-ingestion)
7. [Layer 2 — Message Streaming (Apache Kafka)](#7-layer-2--message-streaming-apache-kafka)
8. [Layer 3 — Processing Pipelines (PySpark)](#8-layer-3--processing-pipelines-pyspark)
9. [Layer 4 — NLP & Sentiment Analysis](#9-layer-4--nlp--sentiment-analysis)
10. [Layer 5 — Storage Layer](#10-layer-5--storage-layer)
11. [Layer 6 — Intelligence Layer (RAG + Multi-Agent AI)](#11-layer-6--intelligence-layer-rag--multi-agent-ai)
12. [Layer 7 — Web Application (Flask Dashboard)](#12-layer-7--web-application-flask-dashboard)
13. [Containerization & Deployment (Docker)](#13-containerization--deployment-docker)
14. [Configuration & Environment](#14-configuration--environment)
15. [Key Design Decisions](#15-key-design-decisions)
16. [Data Schema Reference](#16-data-schema-reference)
17. [API Endpoints Reference](#17-api-endpoints-reference)

---

## 1. Project Overview

**System Name:** Real-Time Financial News Intelligence Engine (FinIntel)

This system is a **Big Data pipeline** built for real-time collection, processing, analysis, and intelligent querying of financial market data. It ingests live financial news, stock prices, and Reddit social sentiment, processes them through Apache Kafka and PySpark, applies FinBERT-based NLP sentiment analysis, stores results in SQLite and a Pinecone vector database, and exposes a Flask web dashboard with a multi-agent AI chat interface powered by LangGraph and GPT-4o-mini via OpenRouter.

### What It Solves
- Aggregating financial signals from multiple live sources into one place
- Automatically understanding market mood (positive/negative/neutral) using transformer-based NLP
- Providing an AI assistant that can answer "Why is TSLA dropping?" using real retrieved context (RAG)
- Detecting risk signals (negative sentiment spikes, price volatility) in real time

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL DATA SOURCES                            │
│  Finnhub API        Alpha Vantage API        ApeWisdom API (Reddit)     │
│  (news + stocks)    (crypto + indices)       (social sentiment)         │
└────────┬────────────────────┬──────────────────────────┬────────────────┘
         │                    │                          │
         ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LAYER 1: DATA INGESTION                             │
│   rss_producer.py     stock_producer.py      social_producer.py         │
│   (news articles)     (OHLCV prices)         (Reddit mentions)          │
└────────┬────────────────────┬──────────────────────────┬────────────────┘
         │                    │                          │
         ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                LAYER 2: APACHE KAFKA (KRaft Mode)                       │
│   Topic: news-feed    Topic: stock-prices    Topic: social-posts        │
│   (3 partitions each, replication-factor=1)                             │
└────────┬────────────────────┬──────────────────────────┬────────────────┘
         │                    │                          │
         ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 LAYER 3: PYSPARK PROCESSING                             │
│     Streaming Pipeline (streaming_pipeline.py)  — micro-batches        │
│     Batch Pipeline    (batch_pipeline.py)       — full runs             │
│                                                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐ │
│  │ cleaning_pipeline│  │  stock_analytics  │  │  spark_sql_analytics  │ │
│  │ (HTML strip,     │  │  (MA5, MA20,      │  │  (sentiment by asset/ │ │
│  │  URL removal,    │  │  volatility,      │  │   source / time,      │ │
│  │  stopwords,      │  │  volume anomaly)  │  │   top movers)         │ │
│  │  deduplication)  │  └──────────────────┘  └───────────────────────┘ │
│  └──────────────────┘                                                   │
└────────┬────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 LAYER 4: NLP / SENTIMENT ANALYSIS                       │
│   Primary:   ProsusAI/FinBERT (Transformer — financial domain)         │
│   Fallback:  VADER (rule-based, no GPU needed)                          │
│   Output:    label ∈ {positive, negative, neutral} + confidence score   │
└────────┬────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      LAYER 5: STORAGE                                   │
│  ┌───────────────┐  ┌────────────────────────┐  ┌──────────────────┐  │
│  │  JSON Files   │  │  SQLite (analytics.db) │  │  Pinecone        │  │
│  │  (processed/) │  │  5 tables + indexes    │  │  Vector DB       │  │
│  │  articles.json│  │  articles              │  │  3 namespaces:   │  │
│  │  prices.json  │  │  price_bars            │  │  - articles      │  │
│  │  aggregates   │  │  price_analytics       │  │  - prices        │  │
│  │  .json        │  │  sentiment_aggregates  │  │  - sentiment     │  │
│  └───────────────┘  │  social_posts          │  │  - social        │  │
│                      └────────────────────────┘  │  (1536-dim vec)  │  │
│                                                   └──────────────────┘  │
└────────┬────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              LAYER 6: INTELLIGENCE (RAG + MULTI-AGENT AI)               │
│                                                                         │
│  User question → keyword router                                         │
│       ├── Financial query  → Pinecone semantic search (RAG)             │
│       │     → LangGraph StateGraph:                                     │
│       │       [retrieve] → [market_analyst] → [risk_manager]           │
│       │       → [portfolio_advisor] → [summarizer] → response          │
│       └── Non-financial   → Direct GPT-4o-mini (decline politely)      │
│                                                                         │
│  LLM: OpenRouter / GPT-4o-mini                                          │
│  Embedding: text-embedding-3-small (1536-dim) via OpenRouter            │
└────────┬────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   LAYER 7: WEB APPLICATION (Flask)                      │
│  Dashboard: live prices, news feed, sentiment charts, risk alerts       │
│  Chat: /api/chat → multi-agent AI responses                             │
│  REST APIs: 20+ endpoints for prices, news, social, RAG search         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Full Technology Stack

### Core Big Data
| Component | Technology | Version | Purpose |
|---|---|---|---|
| Message Broker | Apache Kafka (KRaft) | 3.7.1 | Real-time pub/sub streaming for all data types |
| Stream Processing | PySpark Structured Streaming | 3.5.1+ | Micro-batch processing of Kafka topics |
| Batch Processing | PySpark (SparkSQL) | 3.5.1+ | Full pipeline runs — clean → sentiment → aggregate → store |
| Kafka-Spark Bridge | spark-sql-kafka-0-10_2.12 | 3.5.1 | Kafka source/sink connector for PySpark |

### NLP & AI
| Component | Technology | Purpose |
|---|---|---|
| Primary Sentiment Model | ProsusAI/FinBERT (HuggingFace Transformers) | Finance-domain BERT for positive/negative/neutral classification |
| Fallback Sentiment | VADER (vaderSentiment) | Rule-based, no GPU, used when FinBERT fails |
| LLM | GPT-4o-mini via OpenRouter | Powers multi-agent intelligence nodes |
| Embeddings | text-embedding-3-small via OpenRouter | 1536-dim vectors for semantic search |
| Agentic Framework | LangGraph (StateGraph) | Orchestrates multi-agent pipeline |
| LLM Client | LangChain + langchain-openai | LLM wrapping and prompt management |

### Storage
| Component | Technology | Purpose |
|---|---|---|
| Structured DB | SQLite (WAL mode) | Persistent analytics — articles, prices, social, aggregates |
| Vector DB | Pinecone (Serverless, AWS us-east-1) | Semantic search over news, prices, sentiment (cosine similarity) |
| File Storage | JSON files (data/processed/) | Fast flat-file reads for the Flask dashboard |

### Data Sources (External APIs)
| Source | API | Data Type | Rate Limit |
|---|---|---|---|
| Finnhub | `finnhub.io/api/v1` | Stock quotes, market news, company news, company profiles, peers | 60 calls/min (free tier) |
| Alpha Vantage | `alphavantage.co/query` | Crypto exchange rates, index ETF proxies, OHLCV candles | 25 calls/day (free tier) |
| ApeWisdom | `apewisdom.io/api/v1.0` | Reddit stock + crypto mentions, upvotes, rank, momentum | Free, no key |

### Web Framework
| Component | Technology | Purpose |
|---|---|---|
| Web Server | Flask 3.0.3 | REST API + HTML dashboard |
| Frontend Charts | Chart.js (CDN) | Sentiment trend charts, price charts |
| HTML Templating | Jinja2 | Server-side rendered dashboard |

### Infrastructure
| Component | Technology | Purpose |
|---|---|---|
| Containerization | Docker + Docker Compose | Multi-service orchestration |
| Base Image | python:3.11-slim + OpenJDK (JRE headless) | Python 3.11 with Java for PySpark |
| Config/Secrets | python-dotenv (.env file) | API keys, Kafka broker, Flask settings |

### Python Libraries (key dependencies)
| Library | Version | Usage |
|---|---|---|
| `kafka-python-ng` | ≥2.2.2 | Kafka producer/consumer in Python |
| `pyspark` | ≥3.5.1 | Spark batch + streaming engine |
| `transformers` | ≥4.40.0 | FinBERT model loading |
| `torch` | ≥2.2.0 | PyTorch backend for FinBERT inference |
| `vaderSentiment` | ≥3.3.2 | VADER fallback sentiment |
| `nltk` | ≥3.8.1 | Stopword removal |
| `beautifulsoup4` + `lxml` | ≥4.12.3 / ≥5.2.2 | HTML stripping from article text |
| `pinecone` | ≥3.2.0 | Pinecone vector database client |
| `openai` | ≥1.30.0 | OpenRouter-compatible embedding + LLM calls |
| `langchain` + `langgraph` | ≥0.2.0 / ≥0.1.0 | Agent orchestration |
| `pandas` | ≥2.2.2 | Data manipulation in non-Spark paths |
| `requests` | ≥2.31.0 | HTTP calls to external APIs |
| `flask` | ≥3.0.3 | Web server |
| `python-dotenv` | ≥1.0.1 | .env secret management |

---

## 4. Directory & File Structure

```
Project/
│
├── config/
│   ├── __init__.py
│   └── settings.py              ← Single source of truth: all env vars, API keys,
│                                   Kafka topics, asset lists, rate limits, paths
│
├── ingestion/
│   ├── __init__.py
│   ├── live_data_fetcher.py     ← All external API calls (Finnhub, Alpha Vantage, ApeWisdom)
│   ├── rss_producer.py          ← Kafka producer: news articles → news-feed topic
│   ├── stock_producer.py        ← Kafka producer: OHLCV quotes → stock-prices topic
│   ├── social_producer.py       ← Kafka producer: Reddit posts → social-posts topic
│   └── consumer_test.py         ← Dev utility: print Kafka messages to console
│
├── processing/
│   ├── __init__.py
│   ├── batch_pipeline.py        ← Full batch orchestrator (9-step pipeline)
│   ├── streaming_pipeline.py    ← PySpark Structured Streaming (foreachBatch)
│   ├── cleaning_pipeline.py     ← Text cleaning: HTML strip, URL remove, stopwords
│   ├── sentiment_processor.py   ← PySpark UDF wrapper for FinBERT/VADER
│   ├── spark_sql_analytics.py   ← SparkSQL aggregations: by_asset, by_source, by_time
│   ├── stock_analytics.py       ← PySpark price analytics: MA5, MA20, volatility
│   └── spark_session.py         ← SparkSession factory (batch + streaming configs)
│
├── intelligence/
│   ├── __init__.py
│   ├── sentiment_model.py       ← FinBERT + VADER model loading, predict_batch()
│   ├── rag_retriever.py         ← Pinecone semantic search (articles/prices/sentiment ns)
│   └── agents.py                ← LangGraph multi-agent graph (5 nodes)
│
├── storage/
│   ├── __init__.py
│   ├── json_writer.py           ← Read/write data/processed/*.json flat files
│   ├── sqlite_writer.py         ← SQLite: init tables, upsert all data, query helpers
│   └── pinecone_writer.py       ← Embed + upsert all data to Pinecone (4 namespaces)
│
├── webapp/
│   ├── __init__.py
│   ├── app.py                   ← Flask app: 20+ REST routes + dashboard
│   ├── static/
│   │   └── style.css            ← Dashboard CSS
│   └── templates/
│       └── index.html           ← Jinja2 dashboard template
│
├── data/
│   ├── sample/
│   │   ├── .gitkeep
│   │   └── generate_sample_data.py  ← Offline fallback sample data generator
│   └── processed/               ← Pipeline output (gitignored): articles.json,
│                                   prices.json, aggregates.json, analytics.db
│
├── docker-compose.yml           ← 7-service orchestration (kafka, webapp, producers, pipelines)
├── Dockerfile                   ← python:3.11-slim + JRE + requirements
├── requirements.txt             ← All Python dependencies
├── .env.example                 ← Template for secrets
├── .env                         ← Actual secrets (gitignored)
└── config/settings.py           ← Centralised configuration
```

---

## 5. Step-by-Step Data Flow (End-to-End Workflow)

This section traces what happens to a single piece of data — for example, a news article about TSLA — from the moment it is published on the internet to when an AI agent uses it to answer your question.

---

### Step 1 — External APIs Polled (every 60 seconds)

**File:** `ingestion/live_data_fetcher.py`

Three separate Kafka producer processes each run in an infinite loop:

**News Producer (`rss_producer.py`)**
- Calls `fetch_all_news(days_back=3)` from `live_data_fetcher.py`
- Internally calls Finnhub `/news` endpoint for 4 categories: `general`, `forex`, `crypto`, `merger`
- Also calls Finnhub `/company-news` for each tracked ticker: `AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA`
- Each article is normalised into a dict: `{id, source, title, summary, url, published, tickers, category, ingested_at}`
- Article `id` is an MD5 hash of the URL for deduplication
- Maintains a `seen_ids` set in memory (trimmed to last 5,000) to avoid republishing

**Stock Price Producer (`stock_producer.py`)**
- Calls `fetch_all_prices()` → combines:
  - `fetch_finnhub_quote()` for each stock (`AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA`)
  - `fetch_alpha_crypto_quote()` for `BTC-USD, ETH-USD` via Alpha Vantage `CURRENCY_EXCHANGE_RATE`
  - `fetch_alpha_index_quote()` for `^GSPC → SPY`, `^IXIC → QQQ` via Alpha Vantage `GLOBAL_QUOTE`
- Alpha Vantage calls are throttled to 1 call per 12 seconds to respect free-tier rate limits
- Each price bar: `{ticker, open, high, low, close, prev_close, change_pct, volume, bar_time, ingested_at}`

**Social Producer (`social_producer.py`)**
- Calls `fetch_all_social_sentiment()` → combines:
  - `fetch_apewisdom_stocks()` → top Reddit-mentioned stocks (r/wallstreetbets, r/stocks, r/investing)
  - `fetch_apewisdom_crypto()` → top Reddit-mentioned crypto (r/CryptoCurrency, r/Bitcoin)
- Momentum formula: `(mentions_now - mentions_24h_ago) / mentions_24h_ago`
- Momentum > 0.2 → `bullish`, < -0.2 → `bearish`, otherwise `neutral`

---

### Step 2 — Messages Published to Kafka Topics

**File:** `ingestion/rss_producer.py`, `stock_producer.py`, `social_producer.py`

Each producer creates a `KafkaProducer` with:
- `value_serializer`: JSON → UTF-8 bytes
- `acks="all"`: confirms all in-sync replicas received the message
- `retries=3`: automatic retry on transient failures

| Kafka Topic | Key | Message | Partitions |
|---|---|---|---|
| `news-feed` | article ID (MD5 hash) | Full article JSON | 3 |
| `stock-prices` | ticker symbol | OHLCV price bar JSON | 3 |
| `social-posts` | post ID (MD5 hash) | Reddit sentiment JSON | 3 |

Kafka runs in **KRaft mode** (no ZooKeeper) with cluster ID `MkU3OEVBNTcwNTJENDM2Qk`. Topics are auto-created by the `kafka-init` one-shot container on first boot.

---

### Step 3 — PySpark Consumes Kafka Topics

**File:** `processing/streaming_pipeline.py`

The **Structured Streaming pipeline** subscribes to all three topics simultaneously using `spark.readStream.format("kafka")`. It uses `startingOffsets=latest` so it only processes new messages after startup.

For each topic, a separate streaming query is created with `writeStream.foreachBatch(handler)`. The trigger is set to fire every `STREAMING_TRIGGER_SECONDS` (default: 30 seconds).

```
Kafka topic → Spark readStream → micro-batch DataFrame (every 30s)
                                         ↓
                              foreachBatch handler
                                         ↓
                     parse JSON → clean → sentiment → store
```

Checkpoint directories (in `data/checkpoints/`) track Kafka offsets so the pipeline can resume after a restart without reprocessing old messages.

**Batch pipeline** (`batch_pipeline.py`) does the same 9-step job but as a single one-shot run — it fetches live data directly from the APIs rather than reading from Kafka, and produces complete output files.

---

### Step 4 — Text Cleaning

**File:** `processing/cleaning_pipeline.py`

Each news article and social post goes through a 5-stage cleaning pipeline:

| Stage | Function | Action |
|---|---|---|
| 1 | `_strip_html()` | Removes all HTML tags using BeautifulSoup (lxml parser) |
| 2 | `_remove_urls()` | Strips `http://`, `https://`, and `www.` URLs via regex |
| 3 | `_remove_special_chars()` | Removes non-alphanumeric chars except `. , ! ? ; : ' " -` |
| 4 | Lowercase | Converts everything to lowercase |
| 5 | `_remove_stopwords()` | Removes English NLTK stopwords from the cleaned summary |

Two implementations exist:
- **`clean_articles_pandas()`** — primary path, works on plain Python lists of dicts
- **`clean_articles_spark()`** — PySpark UDF-based alternative for large Spark DataFrames

Deduplication happens by `article["id"]`. Articles with `clean_title` ≤ 5 chars or `clean_summary` ≤ 10 chars are dropped.

**Output fields added:** `clean_title`, `clean_summary`, `clean_summary_no_stop`

---

### Step 5 — Sentiment Analysis

**File:** `intelligence/sentiment_model.py`

Every article title and social post text is passed to `predict_batch(texts)`:

```
texts list → FinBERT pipeline (batches of 16)
                     ↓
           If FinBERT fails → VADER fallback
                     ↓
       (label, score) per article
```

**FinBERT (primary):**
- Model: `ProsusAI/finbert` from HuggingFace Hub (~400 MB download on first run)
- Max token length: 512
- Batch size: 16
- Returns: `{label: "positive"/"negative"/"neutral", score: float}`
- Loaded lazily as a singleton (only once per process lifetime)

**VADER (fallback):**
- Used when FinBERT fails to load (no PyTorch, memory error, etc.)
- Compound score ≥ 0.05 → `positive`, ≤ -0.05 → `negative`, otherwise `neutral`

Each processed record gets two new fields: `sentiment` and `sentiment_score`.

---

### Step 6 — Spark SQL Analytics

**File:** `processing/spark_sql_analytics.py`, `processing/stock_analytics.py`

After sentiment scoring, the batch pipeline runs SparkSQL aggregations on the full article DataFrame:

**Sentiment Analytics (spark_sql_analytics.py)**
- **`sentiment_by_asset(df)`** — for each ticker mentioned in articles, count positive/negative/neutral and calculate average sentiment score. Uses `explode(col("tickers"))` to handle multi-ticker articles.
- **`sentiment_by_source(df)`** — group by `source` (e.g. "Reuters", "Bloomberg"), count all three labels
- **`sentiment_by_time(df)`** — daily trend for the past N days (percentages of each label per day)
- **`overall_distribution(df)`** — total counts across all articles
- **`top_movers(df)`** — top 10 articles with highest sentiment scores (excluding neutral)

**Price Analytics (stock_analytics.py)**
Using PySpark Window functions ordered by `date` per `ticker`:
- **MA-5** — 5-bar rolling average of `close`
- **MA-20** — 20-bar rolling average of `close`
- **Volatility** — 5-bar rolling standard deviation of `close`
- **Daily change %** — `(close - prev_close) / prev_close * 100`
- **Volume anomaly** — boolean flag: `volume > 2x the 5-bar average volume`

---

### Step 7 — Stored to All Three Backends

**Files:** `storage/json_writer.py`, `storage/sqlite_writer.py`, `storage/pinecone_writer.py`

After processing, results are simultaneously persisted to three storage backends:

**A) JSON Files** (`data/processed/`)
- `articles.json` — list of all cleaned + sentiment-scored articles
- `prices.json` — latest price snapshot `{ticker: {close, change_pct, volume, ma_5, ma_20, volatility}}`
- `aggregates.json` — `{by_source, by_asset, by_time, distribution, top_movers}`
- These are flat files read directly by Flask for fast response (no DB query overhead)

**B) SQLite** (`data/analytics.db`, WAL mode)

Five tables:

| Table | Primary Key | Key Columns |
|---|---|---|
| `articles` | `id` (MD5 hash) | source, title, clean_title, clean_summary, url, published, tickers (JSON), sentiment, sentiment_score |
| `price_bars` | `(ticker, date)` | OHLCV + ma_5, ma_20, volatility, daily_change_pct, volume_anomaly |
| `price_analytics` | `ticker` | Latest snapshot: close, change_pct, ma_5, ma_20, volatility, high, low |
| `sentiment_aggregates` | `(agg_type, agg_key)` | positive, negative, neutral, total, avg_score — for by_source, by_asset, by_time |
| `social_posts` | `id` | platform, ticker, text, sentiment, sentiment_score, mentions, upvotes, rank |

Six indexes added for fast dashboard queries (sentiment, source, published date, ticker, social ticker).

**C) Pinecone Vector Database** (`finintel` index, cosine similarity, 1536-dim)

Each article, price bar, sentiment aggregate, and social post is embedded using `text-embedding-3-small` (via OpenRouter), then upserted to Pinecone in four namespaces:

| Namespace | Content | Text format embedded |
|---|---|---|
| `articles` | News articles with sentiment | `"<title>. <summary>. Sentiment: <label> (score: X). Tickers: X. Source: X."` |
| `prices` | OHLCV price bars | `"<ticker> price on <date>: open=$X, high=$X, low=$X, close=$X, volume=X, change=X%..."` |
| `sentiment` | Aggregated sentiment stats | `"by_asset sentiment for AAPL: positive=5, negative=2, neutral=3..."` |
| `social` | Reddit sentiment posts | `"Reddit — TSLA: 250 mentions, 89 upvotes... Sentiment: bullish (score: 0.35)."` |

Upserts are batched in groups of 100 (Pinecone free-tier safe). Embedding batches are 100 texts per API call.

---

### Step 8 — User Asks a Question (AI Chat)

**Files:** `intelligence/agents.py`, `intelligence/rag_retriever.py`

When the user types a question in the web dashboard chat box, it hits `POST /api/chat`.

**Step 8a — Financial vs Non-financial Routing**

`_is_financial(question)` checks if any word in the question matches a 90-word set of financial keywords (`stock, market, tsla, nvda, bitcoin, risk, sentiment, earnings, ...`).

- **Financial** → Full RAG + LangGraph pipeline
- **Non-financial** → Direct GPT-4o-mini call that politely declines and redirects to finance topics

**Step 8b — Context Retrieval (RAG)**

`retrieve_context()` node runs first:
- Extracts ticker mentions from the question using regex against the configured asset list
- If tickers found → `retrieve_for_ticker()`: fetches from `articles` namespace + `prices` namespace filtered by ticker
- If risk keywords found → `retrieve_risk_signals()`: fetches negative-sentiment articles + risk sentiment aggregates
- Otherwise → `retrieve_market_overview()`: broad articles + overall sentiment documents
- Pinecone query: embed the query → cosine similarity search → top-K documents returned
- Deduplicates by first 80 chars of `page_content`

**Step 8c — LangGraph Multi-Agent Pipeline (Sequential StateGraph)**

```
retrieve_context
       ↓
market_analyst_node     ← "Explain market movements and trends from the context"
       ↓
risk_manager_node       ← "Identify risks, red flags, volatility in the context"
       ↓
portfolio_advisor_node  ← "Educational portfolio considerations (NOT financial advice)"
       ↓
summarizer_node         ← "Synthesize all three perspectives into one summary paragraph"
       ↓
      END
```

Each agent node:
1. Receives the full `AgentState` (question + retrieved context + previous agents' outputs)
2. Builds a system prompt + user message
3. Calls GPT-4o-mini via LangChain's `ChatOpenAI` (pointing to OpenRouter base URL)
4. Returns its analysis in the state for the next agent

The shared LLM instance is a lazy singleton (`_llm`) created once and reused across all nodes.

**Step 8d — Response Returned**

The Flask `/api/chat` endpoint returns:
```json
{
  "mode": "rag",
  "market_analyst": "...",
  "risk_manager": "...",
  "portfolio_advisor": "...",
  "summary": "...",
  "sources": [{"title": "...", "url": "...", "source": "...", "sentiment": "..."}]
}
```

---

### Step 9 — Dashboard Displays Results

**File:** `webapp/app.py`, `webapp/templates/index.html`

The Flask dashboard polls several REST endpoints to populate the UI:
- `/api/prices` → latest price snapshot from JSON/SQLite
- `/api/news` → filtered/paginated articles from JSON/SQLite
- `/api/sentiment-trend` → 7-day daily chart data
- `/api/sentiment-by-source` → bar chart by news source
- `/api/sentiment-by-asset` → bar chart by ticker
- `/api/risk-alerts` → generated risk signals (negative spikes + high volatility)
- `/api/social-sentiment` → Reddit posts from SQLite / live ApeWisdom
- `/api/chat` → AI chat responses (multi-agent)

Each API route has a **layered fallback**:
1. Read from processed JSON files (fastest)
2. Fall back to SQLite query
3. Fall back to live API call (if pipeline hasn't run yet)

---

## 6. Layer 1 — Data Ingestion

**Directory:** `ingestion/`

### `live_data_fetcher.py`
The unified HTTP client module. All external API calls go through here. Key design:
- Single `requests.Session` with `User-Agent: FinIntelEngine/2.0`
- `_safe_get()` wraps all HTTP calls with error handling, returns `None` on any failure
- `_throttle_alpha_vantage()` ensures ≥12 second gap between Alpha Vantage calls (prevents 429 errors)
- Alpha Vantage call timestamp tracked as module-level `_last_av_call` float

### Tracked Assets (from `config/settings.py`)
| Category | Assets |
|---|---|
| Stocks | AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA |
| Crypto | BTC-USD, ETH-USD |
| Indices | ^GSPC (→ SPY proxy), ^IXIC (→ QQQ proxy) |

### News Categories (Finnhub)
`general`, `forex`, `crypto`, `merger`

### Poll Intervals
| Data Type | Interval |
|---|---|
| News articles | 60 seconds |
| Stock quotes | 60 seconds |
| Social sentiment | 60 seconds |
| Crypto quotes | 1800 seconds (30 min — conserves Alpha Vantage quota) |

---

## 7. Layer 2 — Message Streaming (Apache Kafka)

**Config:** `docker-compose.yml`, `config/settings.py`

### Kafka Setup (KRaft Mode — No ZooKeeper)
Kafka 3.7.1 runs as a combined broker+controller node:
- `KAFKA_PROCESS_ROLES=broker,controller` — single node acts as both
- `KAFKA_NODE_ID=1` — unique broker ID
- `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093` — quorum voting
- Internal controller port: 9093 (not exposed externally)
- External broker port: 9092

### Topic Configuration
| Topic | Partitions | Replication Factor | Consumer Group |
|---|---|---|---|
| `news-feed` | 3 | 1 | `finintel-streaming-news-feed` |
| `stock-prices` | 3 | 1 | `finintel-streaming-stock-prices` |
| `social-posts` | 3 | 1 | `finintel-streaming-social-posts` |

### Producer Settings
- `acks="all"` — strongest durability guarantee
- `retries=3` — transient network failure tolerance
- `key_serializer` — article/post ID → UTF-8 bytes (enables partition routing by key)
- `value_serializer` — dict → JSON → UTF-8 bytes

### Health Check
Kafka container has a healthcheck: `kafka-topics.sh --list` must succeed before any producer or consumer starts (Docker `depends_on: condition: service_healthy`).

---

## 8. Layer 3 — Processing Pipelines (PySpark)

**Directory:** `processing/`

### Batch Pipeline (`batch_pipeline.py`) — 9 Steps

| Step | Action |
|---|---|
| 1 | Ensure sample data exists (offline mode only) |
| 2 | Start `SparkSession` (local[*] — uses all available CPU cores) |
| 3 | Load raw news + price data (live API or sample JSON) |
| 4 | Fetch live social media posts (ApeWisdom) |
| 5 | Clean articles (`clean_articles_pandas`) |
| 6 | Sentiment analysis on articles (`predict_batch`) |
| 7 | Sentiment analysis on social posts (`predict_batch`) |
| 8 | SparkSQL analytics (sentiment_by_asset, by_source, by_time, top_movers, price analytics) |
| 9 | Persist to JSON + SQLite + Pinecone |

### Streaming Pipeline (`streaming_pipeline.py`)

Uses `spark.readStream.format("kafka")` for all three topics in parallel:

```
Kafka topic → Spark Streaming → foreachBatch (every 30s)
```

**News batch handler** (`_news_batch_handler`):
1. Tries Spark-native path: `from_json()` → `clean_articles_spark()` UDFs → `apply_sentiment()` UDF
2. Falls back to Python path if any UDF error: `clean_articles_pandas()` + `predict_batch()`
3. Stores to JSON + SQLite + Pinecone

**Social batch handler** (`_social_batch_handler`):
- Scores social post `text` field with `predict_batch()`
- Stores to SQLite + Pinecone

**Prices batch handler** (`_prices_batch_handler`):
- Computes MA5, MA20, volatility, volume anomaly via `compute_price_analytics()`
- Stores price bars + latest snapshot to JSON + SQLite + Pinecone

### Checkpointing
Each Spark Streaming query writes Kafka offset progress to `data/checkpoints/<topic>/`. This ensures **exactly-once processing** — on restart, Spark resumes from the last committed offset.

---

## 9. Layer 4 — NLP & Sentiment Analysis

**File:** `intelligence/sentiment_model.py`

### FinBERT
- Full name: `ProsusAI/finbert`
- Architecture: BERT base + classification head fine-tuned on financial news
- Pre-trained specifically for financial domain — outperforms generic BERT on earnings calls, news headlines, analyst reports
- Loaded with `transformers.pipeline("sentiment-analysis")`
- Truncated to 512 tokens (Transformer limit)
- Batch inference: processes 16 texts at once for GPU/CPU efficiency

### VADER
- Rule-based lexicon and heuristics model
- Returns a `compound` score in [-1, 1]
- No model download required — pure Python, instant loading
- Used as fallback when FinBERT is unavailable (e.g., no PyTorch, insufficient RAM)

### Output Labels
- `positive` — bullish market signal
- `negative` — bearish market signal
- `neutral` — no directional signal

---

## 10. Layer 5 — Storage Layer

**Directory:** `storage/`

### Three-Tier Storage Strategy

| Tier | Technology | Speed | Use Case |
|---|---|---|---|
| Hot | JSON flat files | Fastest (disk read) | Flask dashboard reads on every API call |
| Warm | SQLite (WAL mode) | Fast (indexed queries) | Filtered queries (by ticker, sentiment, source) |
| Cold/Semantic | Pinecone Vector DB | Network (100-500ms) | AI agent RAG retrieval, semantic similarity search |

### SQLite Design Details
- **WAL (Write-Ahead Logging)** mode enabled: allows concurrent reads while writing
- `INSERT OR REPLACE` used for all upserts — idempotent on repeated pipeline runs
- `row_factory = sqlite3.Row` — returns dict-like rows for direct JSON serialisation

### Pinecone Design Details
- Index name: `finintel`
- Cloud: AWS, Region: `us-east-1` (Serverless spec)
- Dimension: 1536 (matches `text-embedding-3-small` output)
- Metric: cosine similarity
- Auto-creates index if it doesn't exist on first upsert
- Upsert batches: 100 vectors per API call (free-tier limit)
- Embedding batches: 100 texts per OpenRouter API call

---

## 11. Layer 6 — Intelligence Layer (RAG + Multi-Agent AI)

**Directory:** `intelligence/`

### Retrieval-Augmented Generation (RAG) — `rag_retriever.py`

RAG is the technique of enriching LLM prompts with real retrieved data before generating a response. Instead of the model relying on training memory, it gets fresh, specific context from Pinecone.

**Retrieval functions:**
| Function | Namespace queried | Use case |
|---|---|---|
| `retrieve_for_ticker(ticker)` | `articles` + `prices` | "Tell me about AAPL" |
| `retrieve_risk_signals()` | `articles` + `sentiment` | "What are the risk signals?" |
| `retrieve_market_overview()` | `articles` + `sentiment` | General market questions |
| `retrieve_recent_news()` | `articles` | Latest news context |
| `retrieve_price_context(ticker)` | `prices` | Price-specific questions |

Each retrieval: embed the query string → Pinecone cosine search → return top-K `Document` objects with `page_content` and `metadata`.

### LangGraph Multi-Agent System — `agents.py`

**`AgentState` (TypedDict):**
```python
{
  "question": str,
  "context": list[Document],      # from Pinecone
  "sources": list[dict],          # for frontend citation display
  "market_analysis": str,         # Node 2 output
  "risk_assessment": str,         # Node 3 output
  "portfolio_advice": str,        # Node 4 output
  "final_summary": str,           # Node 5 output
}
```

**Five Agent Nodes:**

| Node | Role | System Prompt Focus |
|---|---|---|
| `retrieve_context` | RAG retriever | No LLM call — fetches Pinecone documents |
| `market_analyst_node` | Investment banker analyst | Market movements, news impact, price trends |
| `risk_manager_node` | Risk management | Red flags, volatility, negative signals (3-5 bullets) |
| `portfolio_advisor_node` | Educational advisor | Portfolio considerations (NOT financial advice) |
| `summarizer_node` | Synthesizer | 4-6 sentence unified summary of all three perspectives |

**LLM Config:**
- Model: `openai/gpt-4o-mini` via OpenRouter
- Temperature: 0.3 (low randomness for factual financial analysis)
- Max tokens: 800 per agent call
- Shared singleton `_llm` across all nodes (one connection, reused)

**Graph compiled once and cached** as module-level `_graph` singleton — avoids rebuilding the StateGraph on every request.

---

## 12. Layer 7 — Web Application (Flask Dashboard)

**File:** `webapp/app.py`

### Architecture Pattern
Flask acts as both the REST API server and the HTML dashboard server. The same process serves the Jinja2-rendered frontend and all JSON API endpoints.

### Data Loading Pattern (3-level fallback)
```python
# Example for articles:
1. read_articles()          # from data/processed/articles.json (instant)
2. query_articles()         # from SQLite analytics.db
3. fetch_all_news()         # live Finnhub API call (slowest, always works)
```

This ensures the dashboard works at every stage:
- Before the pipeline has ever run → falls back to live API
- After pipeline runs → serves from fast flat files
- When files are stale → SQLite has the latest persisted data

### Risk Alert Generation (`_generate_risk_alerts()`)
Logic runs entirely from processed data:
1. Count negative articles per ticker
2. ≥3 negative articles → HIGH alert; ≥2 → MEDIUM alert
3. Volatility > 5.0 → MEDIUM volatility alert
4. Returns structured alert list with `{id, level, ticker, signal, detail, time}`

---

## 13. Containerization & Deployment (Docker)

**Files:** `Dockerfile`, `docker-compose.yml`

### Dockerfile
```
Base: python:3.11-slim
+ OpenJDK (JRE headless) — required for PySpark
+ pip install -r requirements.txt
+ COPY project source
+ mkdir data/processed data/sample
Default CMD: python webapp/app.py
```

### Docker Compose Services (7 total)

| Service | Container | Command | Restart Policy |
|---|---|---|---|
| `kafka` | `finintel-kafka` | Kafka KRaft broker | — |
| `kafka-init` | `finintel-kafka-init` | Create 3 Kafka topics (one-shot) | — |
| `webapp` | `finintel-webapp` | `python webapp/app.py` | `unless-stopped` |
| `rss-producer` | `finintel-rss-producer` | `python ingestion/rss_producer.py` | `unless-stopped` |
| `stock-producer` | `finintel-stock-producer` | `python ingestion/stock_producer.py` | `unless-stopped` |
| `social-producer` | `finintel-social-producer` | `python ingestion/social_producer.py` | `unless-stopped` |
| `streaming-pipeline` | `finintel-streaming-pipeline` | `python processing/streaming_pipeline.py --topic all` | `unless-stopped` |
| `batch-pipeline` | `finintel-batch-pipeline` | `python processing/batch_pipeline.py --source live` | — (run manually) |

**`streaming-pipeline` service — additional environment variables (added v2):**

| Variable | Value | Purpose |
|---|---|---|
| `HF_HOME` | `/hf_cache` | Redirects HuggingFace Hub cache to the persistent Docker volume |
| `TRANSFORMERS_CACHE` | `/hf_cache` | Redirects `transformers` model cache to the same persistent volume |

> These two variables ensure the FinBERT model (~400 MB) is downloaded only **once** and persisted across container restarts via the `hf_cache` named volume. Without them, every `docker compose up` would trigger a fresh model download.

### Docker Volumes
| Volume | Mounted At | Service(s) | Contents |
|---|---|---|---|
| `kafka_data` | `/var/lib/kafka/data` | `kafka` | Kafka log segments, topic partition data |
| `app_data` | `/app/data` | `webapp`, `streaming-pipeline`, `batch-pipeline` | Processed JSON, SQLite DB, Spark checkpoints |
| `hf_cache` | `/hf_cache` | `streaming-pipeline` | Cached HuggingFace models (FinBERT ~400 MB) — persisted across restarts |

### Docker Compose Profiles
- `streaming` profile → enables `streaming-pipeline` service
- `pipeline` profile → enables `batch-pipeline` service (run with `docker compose run`)
- Default `docker compose up` starts: kafka + kafka-init + webapp + 3 producers

### Startup Order (dependency chain)
```
kafka (healthcheck: topics list OK)
  └─ kafka-init (creates topics, exits)
       ├─ rss-producer
       ├─ stock-producer
       ├─ social-producer
       └─ streaming-pipeline
kafka (healthy)
  └─ webapp
```

---

## 14. Configuration & Environment

**File:** `config/settings.py`

All configuration is loaded from environment variables via `python-dotenv` (`.env` file).

### Required Environment Variables

| Variable | Used By | Description |
|---|---|---|
| `FINNHUB_API_KEY` | Ingestion | Finnhub REST API key |
| `ALPHAVANTAGE_API_KEY` | Ingestion | Alpha Vantage API key |
| `PINECONE_API_KEY` | Storage + Intelligence | Pinecone vector DB key |
| `PINECONE_INDEX` | Storage + Intelligence | Pinecone index name (default: `finintel`) |
| `PINECONE_CLOUD` | Storage | Cloud provider (default: `aws`) |
| `PINECONE_REGION` | Storage | Region (default: `us-east-1`) |
| `OPENROUTER_API_KEY` | Intelligence | OpenRouter key for GPT-4o-mini + embeddings |
| `OPENROUTER_BASE_URL` | Intelligence | `https://openrouter.ai/api/v1` |
| `LLM_MODEL` | Intelligence | Default: `openai/gpt-4o-mini` |
| `EMBEDDING_MODEL` | Intelligence | Default: `openai/text-embedding-3-small` |
| `KAFKA_BROKER` | All | Default: `localhost:9092` (Docker: `kafka:9092`) |
| `FLASK_HOST` | Webapp | Default: `0.0.0.0` |
| `FLASK_PORT` | Webapp | Default: `5000` |
| `FLASK_DEBUG` | Webapp | Default: `true` |
| `SPARK_MASTER` | Processing | Default: `local[*]` |
| `STREAMING_TRIGGER_SECONDS` | Streaming | Default: `30` |
| `HF_HOME` | Streaming Pipeline (Docker) | `/hf_cache` — persistent HuggingFace Hub cache directory |
| `TRANSFORMERS_CACHE` | Streaming Pipeline (Docker) | `/hf_cache` — persistent Transformers model cache directory |

---

## 15. Key Design Decisions

### 1. KRaft over ZooKeeper
Kafka 3.7.1 runs in KRaft (Kafka Raft) mode. This eliminates the ZooKeeper dependency, reducing the number of containers from 3 to 1 for the Kafka layer, and simplifies the deployment significantly.

### 2. FinBERT → VADER Fallback Chain
Loading a 400MB PyTorch model is resource-intensive. The system gracefully degrades to VADER if FinBERT cannot load (insufficient RAM, no PyTorch, container resource limits). This ensures sentiment analysis always produces a result.

### 3. Three-Tier Storage
JSON files act as a cache layer for the Flask dashboard. SQLite handles structured queries. Pinecone enables semantic (meaning-based) search that no SQL or JSON file could provide. Each tier serves a distinct purpose.

### 4. Pandas Path as Primary (not Spark UDFs)
The `clean_articles_pandas()` function is used as the primary cleaning path rather than PySpark UDFs because serializing Python closures (especially those importing `bs4` or `nltk`) across Spark worker JVM processes is fragile. The Spark UDF path (`clean_articles_spark`) is retained as an alternative for very large scale.

### 5. Lazy Singletons for Expensive Resources
FinBERT, VADER, Pinecone client, OpenAI client, and the LangGraph graph are all lazy singletons. They are created once on first use and reused. This avoids repeated model loading and connection overhead on each API request.

### 6. LangGraph Sequential over Parallel
The five agent nodes run sequentially: each agent reads the previous agent's analysis before responding. This creates a **collaborative deliberation** — the risk manager reacts to the market analyst's findings, and the portfolio advisor considers both. This produces more coherent and contextually aware responses than parallel independent calls.

### 7. Non-financial Query Filtering
The multi-agent pipeline is expensive (1 Pinecone query + 4 LLM calls). For greetings and clearly non-financial queries, the system skips RAG entirely and routes to a single direct LLM call that politely declines. This saves cost and latency.

---

## 16. Data Schema Reference

### News Article Schema
```json
{
  "id": "a3f4c8...",           // MD5 of URL
  "source": "Reuters",
  "title": "TSLA surges on delivery beat",
  "summary": "Tesla reported...",
  "url": "https://...",
  "published": "2025-05-19T10:00:00+00:00",
  "tickers": ["TSLA"],
  "category": "company",
  "image": "https://...",
  "ingested_at": "2025-05-19T10:01:00+00:00",
  "clean_title": "tsla surges delivery beat",        // added by cleaning
  "clean_summary": "tesla reported strong...",       // added by cleaning
  "clean_summary_no_stop": "tesla reported strong...", // stopwords removed
  "sentiment": "positive",                           // added by sentiment
  "sentiment_score": 0.9341                          // confidence
}
```

### Price Bar Schema
```json
{
  "ticker": "TSLA",
  "open": 175.20,
  "high": 178.50,
  "low": 174.10,
  "close": 177.80,
  "prev_close": 174.30,
  "change_pct": 2.01,
  "volume": 45000000,
  "bar_time": "2025-05-19T15:00:00+00:00",
  "ingested_at": "2025-05-19T15:00:05+00:00",
  "ma_5": 175.40,         // added by Spark
  "ma_20": 172.10,        // added by Spark
  "volatility": 1.23,     // added by Spark
  "daily_change_pct": 2.01, // added by Spark
  "volume_anomaly": false  // added by Spark
}
```

### Social Post Schema
```json
{
  "id": "reddit-TSLA-...",
  "platform": "Reddit (ApeWisdom)",
  "ticker": "TSLA",
  "text": "TSLA (Tesla) — 1200 mentions, 5400 upvotes on Reddit (rank #3)",
  "sentiment_hint": "bullish",
  "sentiment_score": 0.35,
  "mentions": 1200,
  "mentions_24h_ago": 900,
  "upvotes": 5400,
  "rank": 3,
  "likes": 5400,
  "reposts": 0,
  "timestamp": "2025-05-19T10:00:00+00:00",
  "ingested_at": "2025-05-19T10:00:00+00:00",
  "sentiment": "positive",        // added by FinBERT/VADER
  "sentiment_score": 0.7821       // overwritten by model
}
```

---

## 17. API Endpoints Reference

All endpoints served by Flask at `http://localhost:5000` (or port 80 in Docker).

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Main dashboard HTML page |
| GET | `/api/status` | Health check — version, pipeline status, topics, tickers |
| GET | `/api/news` | Processed articles (`?limit=10&source=X&sentiment=positive`) |
| GET | `/api/prices` | Latest price snapshot (`?ticker=TSLA`) |
| GET | `/api/sentiment-trend` | 7-day daily sentiment percentages |
| GET | `/api/sentiment-by-source` | Sentiment counts per news source |
| GET | `/api/sentiment-by-asset` | Sentiment counts per ticker |
| GET | `/api/risk-alerts` | Risk alerts (`?level=high\|medium`) |
| GET | `/api/social-sentiment` | Live Reddit posts (ApeWisdom) |
| GET | `/api/social` | Stored social posts from SQLite (`?ticker=X&sentiment=X&limit=50`) |
| GET | `/api/live-prices` | Real-time prices direct from APIs (bypass pipeline) |
| GET | `/api/live-news` | Real-time news direct from Finnhub (bypass pipeline) |
| GET | `/api/search` | Stock symbol search (`?q=tesla`) |
| GET | `/api/stock/<SYMBOL>` | Full stock detail: quote + profile + financials |
| GET | `/api/stock/<SYMBOL>/candles` | OHLCV candle data for charts |
| GET | `/api/stock/<SYMBOL>/news` | Company-specific news |
| GET | `/api/stock/<SYMBOL>/peers` | Industry peer comparison |
| GET | `/api/stock/<SYMBOL>/social` | Reddit sentiment for specific ticker |
| POST | `/api/chat` | AI chat — RAG + multi-agent response (`{"message": "..."}`) |
| GET | `/api/rag/search` | Direct Pinecone search (`?q=X&namespace=articles&top_k=5`) |

---

---

## 18. Changelog / Version History

This section records every infrastructure and configuration change made after the initial architecture was established.

| Version | File Changed | What Changed | Why |
|---|---|---|---|
| v1 | `docker-compose.yml` | Initial setup: `kafka`, `kafka-init`, `webapp`, `rss-producer`, `stock-producer`, `social-producer` services | Week 1 baseline |
| v1 | `docker-compose.yml` | `streaming-pipeline` service added (profile: `streaming`) | Week 2 — PySpark Structured Streaming |
| v1 | `docker-compose.yml` | `batch-pipeline` service added (profile: `pipeline`) | Week 2 — full batch run support |
| v1 | `docker-compose.yml` | Named volumes: `kafka_data`, `app_data` | Persist Kafka data and processed pipeline output |
| **v2** | **`docker-compose.yml`** | **`streaming-pipeline` — added `HF_HOME=/hf_cache` and `TRANSFORMERS_CACHE=/hf_cache` env vars** | **Prevent FinBERT (~400 MB) from re-downloading on every container restart** |
| **v2** | **`docker-compose.yml`** | **`streaming-pipeline` — added `hf_cache:/hf_cache` volume mount** | **Persistent model cache storage across container lifecycle** |
| **v2** | **`docker-compose.yml`** | **New named volume `hf_cache` declared in top-level `volumes:` block** | **Docker-managed persistent volume for HuggingFace model files** |

---

*Report generated for: Real-Time Financial News Intelligence Engine*
*Architecture covers Weeks 1–4: Data Ingestion → Streaming → NLP → Vector DB → Multi-Agent AI*
*Last updated: v2 — FinBERT persistent cache volume added to streaming-pipeline*
