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

| Tool | Version | Purpose | Install |
|------|---------|---------|--------|
| Python | 3.10 – 3.11 | Runtime for all services | [python.org](https://www.python.org/downloads/) |
| Java JDK | 17 (LTS) | PySpark requires a JVM on the host | [Microsoft Build of OpenJDK](https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-17) |
| Apache Kafka | 3.7.x or 4.2.x | Message broker (KRaft — no ZooKeeper) | [kafka.apache.org/downloads](https://kafka.apache.org/downloads) |
| Docker Desktop | 4.x + Compose v2 | Containerised full-stack deployment | [docs.docker.com](https://docs.docker.com/desktop/windows/install/) |
| pip | 24+ | Python package manager | `python -m pip install --upgrade pip` |

### Java — JAVA_HOME (Windows)

PySpark will fail silently if `JAVA_HOME` is not set. Set it permanently:

```powershell
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot", "Machine")
[System.Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$env:JAVA_HOME\bin", "Machine")
```

Verify: `java -version` should print `openjdk 17`.

### Python virtual environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

> On first activation, if scripts are blocked: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

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

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `FINNHUB_API_KEY` | — | **Yes** | Finnhub REST API — market news + OHLCV quotes. Free at [finnhub.io](https://finnhub.io) |
| `ALPHAVANTAGE_API_KEY` | — | **Yes** | Alpha Vantage — crypto rates + index ETF proxies. Free at [alphavantage.co](https://www.alphavantage.co) |
| `PINECONE_API_KEY` | — | **Yes** | Pinecone vector DB. Free serverless tier at [pinecone.io](https://www.pinecone.io) |
| `PINECONE_INDEX` | `finintel` | No | Name of the Pinecone index to create/use |
| `PINECONE_CLOUD` | `aws` | No | Cloud provider for serverless index (`aws` or `gcp`) |
| `PINECONE_REGION` | `us-east-1` | No | Region for Pinecone serverless |
| `OPENROUTER_API_KEY` | — | **Yes** | OpenRouter — routes to GPT-4o-mini + text-embedding-3-small. Get at [openrouter.ai](https://openrouter.ai) |
| `LLM_MODEL` | `openai/gpt-4o-mini` | No | LLM model string passed to OpenRouter |
| `EMBEDDING_MODEL` | `openai/text-embedding-3-small` | No | Embedding model (1536-dim) |
| `KAFKA_BROKER` | `localhost:9092` | No | Kafka broker address (use `kafka:9092` inside Docker) |
| `FLASK_HOST` | `0.0.0.0` | No | Bind address for Flask |
| `FLASK_PORT` | `5000` | No | Port Flask listens on |
| `FLASK_DEBUG` | `true` | No | Enable Flask debug mode (set `false` in production/Docker) |
| `SPARK_MASTER` | `local[*]` | No | PySpark master URL (`local[*]` uses all CPU cores) |
| `SPARK_LOG_LEVEL` | `WARN` | No | PySpark log verbosity (`ERROR`, `WARN`, `INFO`, `DEBUG`) |
| `STREAMING_TRIGGER_SECONDS` | `30` | No | How often the Structured Streaming micro-batch fires |
| `STREAMING_CHECKPOINT_DIR` | `data/checkpoints` | No | Directory for Kafka offset checkpoints (fault tolerance) |

---

## Apache Kafka — Detailed Configuration

### What is KRaft Mode?

KRaft (Kafka Raft Metadata mode) replaces ZooKeeper for cluster metadata management. A single node acts as **both broker and controller**. Introduced in Kafka 3.3 and made production-stable in 3.7.

### Step 1 — Download & Extract

Download the binary tarball from [kafka.apache.org/downloads](https://kafka.apache.org/downloads).
Extract to `C:\kafka` (or any path without spaces). This guide uses `$KAFKA` as a variable:

```powershell
$KAFKA = "C:\Users\SHAHROZ\kafka_2.13-4.2.0"
```

### Step 2 — server.properties (KRaft)

Key properties inside `$KAFKA\config\server.properties`:

| Property | Value | Explanation |
|----------|-------|-------------|
| `process.roles` | `broker,controller` | This node is both a broker and a metadata controller |
| `node.id` | `1` | Unique integer ID for this node |
| `controller.quorum.voters` | `1@localhost:9093` | Quorum membership: `nodeId@host:controllerPort` |
| `listeners` | `PLAINTEXT://:9092,CONTROLLER://:9093` | Broker listens on 9092; controller on 9093 |
| `advertised.listeners` | `PLAINTEXT://localhost:9092` | Address published to clients |
| `log.dirs` | `C:/kafka-logs` | Where partition data is stored |
| `num.partitions` | `3` | Default partition count for auto-created topics |
| `offsets.topic.replication.factor` | `1` | Single-node cluster — must be 1 |
| `auto.create.topics.enable` | `true` | Brokers auto-create topics on first produce |

### Step 3 — Format Storage (one-time per install)

Generates a unique `CLUSTER_ID` and stamps the log directory:

```powershell
$ID = & "$KAFKA\bin\windows\kafka-storage.bat" random-uuid
& "$KAFKA\bin\windows\kafka-storage.bat" format --standalone -t $ID -c "$KAFKA\config\server.properties"
```

> **Warning:** Do NOT re-run this unless you want to wipe all stored messages. Delete `C:\kafka-logs` first if you need a clean reset.

### Step 4 — Start the Broker

```powershell
& "$KAFKA\bin\windows\kafka-server-start.bat" "$KAFKA\config\server.properties"
```

Wait for: `[KafkaRaftServer nodeId=1] Kafka Server started` before proceeding.

### Step 5 — Create the 3 Topics

Each topic uses **3 partitions** (enables parallel producer/consumer lanes) and **replication-factor 1** (single-node):

```powershell
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic news-feed    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic social-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic stock-prices  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify all 3 exist:
```powershell
& "$KAFKA\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092
```

### Topic Design

| Topic | Producer | Consumer | Message Format | Partitions |
|-------|----------|----------|----------------|------------|
| `news-feed` | `rss_producer.py` | `streaming_pipeline.py`, `batch_pipeline.py` | JSON — headline, summary, source, ticker | 3 |
| `stock-prices` | `stock_producer.py` | `streaming_pipeline.py`, `batch_pipeline.py` | JSON — ticker, open, high, low, close, volume | 3 |
| `social-posts` | `social_producer.py` | `streaming_pipeline.py`, `batch_pipeline.py` | JSON — ticker, mentions, upvotes, sentiment | 3 |

### Kafka Ports

| Port | Listener | Purpose |
|------|----------|---------|
| `9092` | `PLAINTEXT` | Client connections (producers, consumers, PySpark) |
| `9093` | `CONTROLLER` | Internal Raft metadata consensus (not exposed to clients) |

### Kafka Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `controller.quorum.voters is not set` | Missing `--standalone` flag | Add `--standalone` to the `format` command |
| `NoSuchFileException: config\kraft\server.properties` | Wrong config path | Use `config\server.properties` (Kafka 4.x has no `kraft/` subfolder) |
| `No module named kafka.vendor.six.moves` | Wrong kafka-python package | `pip uninstall kafka-python -y && pip install kafka-python-ng` |
| `Topic already exists` | Re-running create | Safe to ignore — topic is already present |
| Producer connects but no messages appear | Poll interval | Wait 60 s — producers poll on a 60-second cycle |

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

## Docker — Detailed Configuration

### Dockerfile Breakdown

The single `Dockerfile` at the project root builds an image used by all Python services:

```dockerfile
FROM python:3.11-slim              # Lean Debian-based Python 3.11 image
RUN apt-get install default-jre-headless  # OpenJDK for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt   # All deps baked in
COPY . .                           # Copy entire project source
RUN mkdir -p data/processed data/sample
CMD ["python", "webapp/app.py"]    # Default — overridden per service
```

- `python:3.11-slim` is chosen over `alpine` because PySpark + PyTorch need glibc.
- `--no-cache-dir` keeps the image smaller by not storing pip's download cache.
- `default-jre-headless` installs OpenJDK without a display server (~180 MB).

### Docker Compose — Full Stack

Start all core services (minus streaming and batch profiles):

```powershell
docker compose up -d
```

Start with the streaming pipeline enabled:
```powershell
docker compose --profile streaming up -d
```

Run the batch pipeline once and exit:
```powershell
docker compose --profile pipeline run --rm batch-pipeline
```

Stop everything and remove containers:
```powershell
docker compose down
```

Stop everything **and** delete all stored data volumes:
```powershell
docker compose down -v
```

### Service Breakdown

#### `kafka` — `finintel-kafka`

```yaml
image: apache/kafka:3.7.1     # Official Apache Kafka image (KRaft)
ports: ["9092:9092"]          # Expose broker port to host
```

| Environment Variable | Value | Explanation |
|----------------------|-------|-------------|
| `KAFKA_NODE_ID` | `1` | Unique broker/controller ID |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Combined mode — one node does both |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:9093` | Raft voter list; uses service DNS name `kafka` |
| `KAFKA_LISTENERS` | `PLAINTEXT://:9092,CONTROLLER://:9093` | Internal bind addresses |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:9092` | What clients see — uses Docker service DNS |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT` | No TLS in dev |
| `CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` | Pre-generated stable UUID (avoids re-formatting) |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | Topics auto-created on first produce |
| `KAFKA_NUM_PARTITIONS` | `3` | Default partition count |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Required for single-node |
| `KAFKA_LOG_DIRS` | `/var/lib/kafka/data` | Mapped to named volume `kafka_data` |

**Health check:** polls `kafka-topics.sh --list` every 10 s, 10 retries, 30 s start period. Downstream services use `condition: service_healthy` to wait.

#### `kafka-init` — `finintel-kafka-init`

A one-shot container that runs after Kafka is healthy, creates the 3 topics with `--if-not-exists`, then exits with code 0. All producers use `condition: service_completed_successfully` to depend on this.

#### `webapp` — `finintel-webapp`

```yaml
build: .           # Uses project Dockerfile
ports: ["80:5000", "5000:5000"]  # Accessible on both port 80 and 5000
env_file: .env     # Loads all API keys
volumes: [app_data:/app/data]    # Persists pipeline output and SQLite DB
restart: unless-stopped
```

| Env Variable | Container Value |
|--------------|-----------------|
| `KAFKA_BROKER` | `kafka:9092` (Docker DNS, not localhost) |
| `FLASK_HOST` | `0.0.0.0` |
| `FLASK_PORT` | `5000` |
| `FLASK_DEBUG` | `false` |

#### `rss-producer`, `stock-producer`, `social-producer`

All three share the same pattern:
- `build: .` — same image as webapp
- `env_file: .env` — needs `FINNHUB_API_KEY` / `ALPHAVANTAGE_API_KEY`
- `KAFKA_BROKER=kafka:9092` — overridden to Docker-internal DNS
- `depends_on: kafka-init: condition: service_completed_successfully`
- `restart: unless-stopped` — auto-restarts on transient API failures

#### `streaming-pipeline` — `finintel-streaming-pipeline`

```yaml
profiles: [streaming]      # Not started by default — use --profile streaming
volumes:
  - app_data:/app/data     # Shares processed/ and analytics.db with webapp
  - hf_cache:/hf_cache     # Persists FinBERT model download (~400 MB)
```

| Env Variable | Value | Purpose |
|--------------|-------|--------|
| `SPARK_MASTER` | `local[*]` | Use all available CPU cores inside container |
| `HF_HOME` | `/hf_cache` | HuggingFace model cache directory |
| `TRANSFORMERS_CACHE` | `/hf_cache` | Legacy env var also checked by older transformers versions |

#### `batch-pipeline` — `finintel-batch-pipeline`

```yaml
profiles: [pipeline]      # Not started by default
command: ["python", "processing/batch_pipeline.py", "--source", "live"]
```

Run on-demand with: `docker compose --profile pipeline run --rm batch-pipeline`

### Named Volumes

| Volume | Mounted By | Contents |
|--------|------------|----------|
| `kafka_data` | `kafka` | Kafka partition log segments, offsets, metadata |
| `app_data` | `webapp`, `streaming-pipeline`, `batch-pipeline` | `data/processed/*.json`, `data/analytics.db`, `data/checkpoints/` |
| `hf_cache` | `streaming-pipeline` | HuggingFace model weights — FinBERT (~400 MB), tokenizer |

### Docker Networking

All services share the default Compose bridge network (`project_default`). Service names resolve as DNS hostnames, so `KAFKA_BROKER=kafka:9092` works inside any container without knowing the container IP.

### Common Docker Commands

```powershell
docker compose logs -f kafka              # Stream Kafka broker logs
docker compose logs -f streaming-pipeline # Stream PySpark logs
docker compose ps                         # Show running containers + health
docker exec -it finintel-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
docker compose restart webapp             # Restart only the Flask service
```

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

## Python Dependencies — Complete Reference

All packages are declared in `requirements.txt`. Below is a per-package explanation of what each dependency does in this project.

### Week 1 — Ingestion & Web

| Package | Min Version | Role in Project |
|---------|-------------|----------------|
| `kafka-python-ng` | 2.2.2 | Kafka producer/consumer client. **Must use `kafka-python-ng`**, not the unmaintained `kafka-python` (missing `six.moves` error). Used by all three producers and `consumer_test.py`. |
| `requests` | 2.31.0 | HTTP client for Finnhub, Alpha Vantage, ApeWisdom REST APIs inside `live_data_fetcher.py`. |
| `flask` | 3.0.3 | WSGI web framework powering `webapp/app.py`. Serves 20+ REST endpoints and Jinja2 HTML templates. |
| `pandas` | 2.2.2 | DataFrame manipulation for cleaning, aggregation, and JSON serialisation throughout the pipeline. |
| `python-dotenv` | 1.0.1 | Loads `.env` into `os.environ` via `load_dotenv()` in `config/settings.py`. |

### Week 2 — PySpark + NLP

| Package | Min Version | Role in Project |
|---------|-------------|----------------|
| `pyspark` | **3.5.3** (pinned) | Batch pipeline (`batch_pipeline.py`) and Structured Streaming (`streaming_pipeline.py`). Pinned to 3.5.3 for compatibility with the Kafka connector JAR. |
| `transformers` | 4.40.0 | HuggingFace Transformers — loads `ProsusAI/finbert` for financial sentiment classification in `sentiment_model.py`. |
| `torch` | 2.2.0 | PyTorch backend for FinBERT inference. The CPU-only wheel (`--extra-index-url https://download.pytorch.org/whl/cpu`) is used to keep Docker image size smaller (~800 MB vs ~2 GB for CUDA). |
| `vaderSentiment` | 3.3.2 | Rule-based sentiment fallback when FinBERT is unavailable or times out. Zero-dependency, very fast. |
| `nltk` | 3.8.1 | Stopword list and tokenisation for `cleaning_pipeline.py`. Requires one-time corpus download. |
| `beautifulsoup4` | 4.12.3 | HTML tag stripping from Finnhub article bodies in `cleaning_pipeline.py`. |
| `lxml` | 5.2.2 | Fast XML/HTML parser backend used by BeautifulSoup. |

### Week 3 — Vector DB + AI Agents

| Package | Min Version | Role in Project |
|---------|-------------|----------------|
| `pinecone` | 3.2.0 | Pinecone Python SDK v3. Used in `pinecone_writer.py` to upsert vectors and in `rag_retriever.py` to query 4 namespaces (`articles`, `prices`, `sentiment`, `social`). |
| `openai` | 1.30.0 | OpenAI-compatible client pointed at OpenRouter base URL. Used for `text-embedding-3-small` calls (1536-dim) and GPT-4o-mini chat completions. |
| `langchain` | 0.2.0 | LangChain core — prompt templates, message types, and chain utilities used inside `agents.py`. |
| `langchain-openai` | 0.1.0 | `ChatOpenAI` and `OpenAIEmbeddings` wrappers that connect LangChain to the OpenRouter endpoint. |
| `langchain-core` | 0.2.0 | Runnable protocol, `BaseMessage`, and schema types required by both `langchain` and `langgraph`. |
| `langgraph` | 0.1.0 | Stateful multi-agent graph framework. Builds the 5-node `StateGraph` in `agents.py` (retrieve → market_analyst → risk_manager → portfolio_advisor → summarizer). |

### PySpark Kafka Connector (auto-downloaded at runtime)

| JAR Artifact | Version | Purpose |
|--------------|---------|--------|
| `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` | 3.5.1 | Maven JAR auto-fetched by PySpark on first run. Enables `spark.readStream.format("kafka")` and `spark.read.format("kafka")`. Set via `SPARK_KAFKA_PACKAGES` in `config/settings.py`. |

> The JAR is cached in `~/.ivy2` (local) or in the Docker layer after first download.

### API Rate Limits Reference

| API | Free Tier Limit | Handling in Code |
|-----|----------------|------------------|
| Finnhub | 60 calls/min | `FINNHUB_MAX_CALLS_PER_MIN=60`; news + quotes share budget |
| Alpha Vantage | 25 calls/day | `ALPHA_VANTAGE_MAX_CALLS_PER_DAY=25`; `ALPHA_VANTAGE_CALL_DELAY_SEC=12` sleep between calls |
| ApeWisdom | Unlimited (public) | No key needed; `SOCIAL_POLL_INTERVAL_SECONDS=60` |
| Pinecone | 100 vectors/upsert batch | `pinecone_writer.py` batches in groups of 100 |
| OpenRouter | Pay-per-token | Non-financial queries skip RAG to save cost |

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

## Module Reference

Every source file and its responsibilities:

### `config/`

| File | Purpose |
|------|---------|
| `settings.py` | Single source of truth for all config. Loads `.env` via `python-dotenv`, exposes constants for Kafka topics, API keys, asset lists, rate limits, poll intervals, Spark settings, file paths, and model names. All other modules import from here — no module reads `.env` directly. |

### `ingestion/`

| File | Purpose |
|------|---------|
| `live_data_fetcher.py` | All outbound HTTP calls. Functions: `fetch_finnhub_news()`, `fetch_stock_quotes()`, `fetch_crypto_quote()`, `fetch_reddit_sentiment()`. Handles rate-limit delays and returns normalised Python dicts. |
| `rss_producer.py` | Connects to Kafka, loops forever polling `live_data_fetcher.fetch_finnhub_news()` every 60 s, MD5-deduplicates articles, serialises to JSON, and produces to `news-feed` topic. |
| `stock_producer.py` | Produces OHLCV price records to `stock-prices`. Routes stocks to Finnhub, crypto to Alpha Vantage, indices to Alpha Vantage ETF proxies (SPY/QQQ). Crypto polled every 30 min to preserve the 25-call/day quota. |
| `social_producer.py` | Fetches Reddit mention data from ApeWisdom, calculates a momentum score (`mentions * upvote_ratio`), and produces to `social-posts`. |
| `consumer_test.py` | Dev utility. Subscribes to all 3 topics (or a single topic passed as argv[1]) and prints messages to console. Not used in production. |

### `processing/`

| File | Purpose |
|------|---------|
| `spark_session.py` | Factory function `get_spark_session(mode)`. Mode `batch` returns a session optimised for batch with Kafka package. Mode `streaming` adds Kafka structured streaming configs. Sets `JAVA_HOME` from env and configures log level. |
| `cleaning_pipeline.py` | PySpark UDF + Pandas pipeline: strips HTML tags (BeautifulSoup), removes URLs/special chars, lowercases, removes NLTK stopwords, deduplicates by MD5 of cleaned text. |
| `sentiment_processor.py` | PySpark UDF wrapper. Calls `sentiment_model.predict_batch()` inside a UDF registered on a Spark DataFrame column. Falls back to VADER per-article on FinBERT failure. |
| `spark_sql_analytics.py` | Registers Spark temp views and runs SQL aggregations: sentiment counts by asset, by source, and by day (7-day window). Returns a dict of DataFrames. |
| `stock_analytics.py` | PySpark Window functions: 5-day MA, 20-day MA, rolling volatility (stddev of daily returns), and volume anomaly flag (volume > 2× 20-day avg). |
| `batch_pipeline.py` | 9-step orchestrator. Accepts `--source live\|sample`. Steps: fetch data → ensure sample fallback → start Spark → clean → FinBERT sentiment → social sentiment → SQL analytics → stock analytics → write JSON + SQLite + Pinecone. |
| `streaming_pipeline.py` | PySpark Structured Streaming. Reads all 3 Kafka topics simultaneously with `spark.readStream.format("kafka")`. Uses `foreachBatch` to apply cleaning + sentiment on each 30-second micro-batch. Checkpoints offsets in `data/checkpoints/`. |

### `intelligence/`

| File | Purpose |
|------|---------|
| `sentiment_model.py` | Loads `ProsusAI/finbert` from HuggingFace (cached to `HF_HOME`). Exposes `predict_batch(texts, batch_size=16)` returning `[(label, confidence)]`. VADER fallback is initialised alongside. |
| `rag_retriever.py` | Pinecone retriever. `search_articles(query, ticker)`, `search_risk_signals(query)`, `search_market_overview(query)` — each queries a specific namespace with cosine similarity. Returns top-k metadata dicts. |
| `agents.py` | LangGraph `StateGraph` with typed `AgentState`. Five nodes: `retrieve_context` (calls `rag_retriever`), `market_analyst_node`, `risk_manager_node`, `portfolio_advisor_node`, `summarizer_node`. Financial query classifier (90-keyword regex) routes non-financial questions directly to a single LLM call. |

### `storage/`

| File | Purpose |
|------|---------|
| `json_writer.py` | Read/write helpers for `data/processed/articles.json`, `prices.json`, `aggregates.json`. Thread-safe atomic writes via temp file + rename. |
| `sqlite_writer.py` | SQLite in WAL mode. `init_db()` creates 5 tables (`articles`, `prices`, `social_posts`, `sentiment_aggregates`, `price_analytics`) with 6 indexes. `upsert_articles()`, `upsert_prices()`, `query_articles()` etc. |
| `pinecone_writer.py` | Connects to Pinecone index (creates serverless index if absent). Embeds data with `text-embedding-3-small`, upserts in batches of 100 to 4 namespaces: `articles`, `prices`, `sentiment`, `social`. |

### `webapp/`

| File | Purpose |
|------|---------|
| `app.py` | Flask application. 20+ routes. Each data route uses a 3-level fallback: JSON file → SQLite → live API call. Mounts `/api/chat` to `agents.run_agent()` and `/api/rag/search` to `rag_retriever.search_articles()`. |
| `templates/index.html` | Single-page dark glassmorphism dashboard. Chart.js for line/bar charts. Jinja2 for server-side data injection. Ticker tape, real-time price cards, sentiment dials. |
| `static/style.css` | Custom CSS design system: CSS variables, glassmorphism cards, responsive grid, dark theme palette. |

### `data/`

| Path | Contents |
|------|---------|
| `data/sample/generate_sample_data.py` | Generates deterministic fake articles, prices, and social posts for offline development and testing when API keys are absent. |
| `data/processed/` | Pipeline output (git-ignored). Contains `articles.json`, `prices.json`, `aggregates.json` written by the batch and streaming pipelines. |
| `data/checkpoints/` | Kafka offset checkpoints written by PySpark Structured Streaming for exactly-once semantics. Git-ignored. |
| `data/analytics.db` | SQLite database. Git-ignored. |

---

## Full Startup Order (Local)

Open **5 terminals** in this order. Do not skip.

| # | Terminal | Command | Keep Open |
|---|----------|---------|----------|
| 1 | **Kafka Broker** | `& "$KAFKA\bin\windows\kafka-server-start.bat" "$KAFKA\config\server.properties"` | Must stay open |
| 2 | **Create Topics** | `kafka-topics.bat --create ...` (first run only) | Close after done |
| 3 | **Flask Dashboard** | `python webapp/app.py` | Must stay open |
| 4 | **Stock Producer** | `python ingestion/stock_producer.py` | Must stay open |
| 5 | **News Producer** | `python ingestion/rss_producer.py` | Must stay open |
| 6 | *(Optional)* **Consumer Test** | `python ingestion/consumer_test.py` | Close after verifying |

**Shutdown order (always reverse):**
1. `Ctrl+C` on news producer
2. `Ctrl+C` on stock producer
3. `Ctrl+C` on Flask
4. `Ctrl+C` on Kafka broker

---

## Known Limitations

- FinBERT requires PyTorch (~2 GB RAM). On low-memory machines the system automatically falls back to VADER.
- Alpha Vantage free tier: 25 calls/day. Crypto/index quotes are throttled to 1 call per 12 seconds.
- Finnhub free tier: 60 calls/min. Company news is fetched for 6 tickers per poll cycle.
- Free stock APIs return delayed data outside market hours.
- Pinecone free tier: upserts batched at 100 vectors/call. Initial embedding of a large dataset may be slow.
- PySpark on Windows requires `JAVA_HOME` set correctly and `winutils.exe` for some operations.
