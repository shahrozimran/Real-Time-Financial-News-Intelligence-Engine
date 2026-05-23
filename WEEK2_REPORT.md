# Real-Time Financial News Intelligence Engine
## Week 2 — Project Report

**Student:** SHAHROZ
**Course:** Big Data (6th Semester)
**University:** University of Central Punjab (UCP)
**Report Date:** May 22, 2026
**Week Status:** ✅ COMPLETE

---

## 1. Week 2 Objective

Week 2 focused entirely on building the **Processing Layer** of the FinIntel pipeline — consuming raw Kafka messages, cleaning and normalizing financial news text, running sentiment analysis, computing stock price analytics, persisting results to SQLite and JSON storage, and wiring all of it into the Flask dashboard so the UI displays real processed data instead of placeholders.

---

## 2. Components Built in Week 2

### 2.1 SparkSession Builder — `processing/spark_session.py`

Provides two SparkSession factory functions used across the pipeline:

| Function | Purpose |
|----------|---------|
| `get_spark_session()` | Batch analytics (no Kafka JARs) |
| `get_streaming_spark_session()` | Structured Streaming with Kafka connector |

Both sessions are configured with `spark.sql.shuffle.partitions=4`, `spark.driver.memory=2g`, UTC timezone, and reduced log verbosity. The streaming session includes `spark.jars.packages` pointing to the `spark-sql-kafka` connector which auto-downloads via Maven on first run.

---

### 2.2 Text Cleaning Pipeline — `processing/cleaning_pipeline.py`

A multi-step NLP preprocessing pipeline applied to all financial news articles before sentiment scoring.

**Pipeline steps (in order):**

| Step | Function | Action |
|------|----------|--------|
| 1 | `_strip_html` | Removes HTML tags via BeautifulSoup, falls back to regex |
| 2 | `_remove_urls` | Strips `http://`, `https://`, and `www.` URLs |
| 3 | `_remove_special_chars` | Removes non-alphanumeric characters; normalizes whitespace |
| 4 | `_remove_stopwords` | Removes English stopwords via NLTK corpus |
| 5 | Deduplication | Skips already-seen article IDs |
| 6 | Filter | Drops articles with title ≤ 5 chars or summary ≤ 10 chars |

**Two implementations provided:**
- `clean_articles_pandas(articles)` — pandas-based; primary path used in both batch and streaming pipelines
- `clean_articles_spark(df)` — PySpark UDF-based; used by the Spark-native streaming path for scalability

**Output fields added to each article:**
- `clean_title` — HTML-stripped, URL-removed, lowercased title
- `clean_summary` — same cleaning applied to summary
- `clean_summary_no_stop` — summary with stopwords removed

---

### 2.3 Spark SQL Analytics — `processing/spark_sql_analytics.py`

Five aggregation queries built on PySpark DataFrames for the dashboard's sentiment analytics section.

| Function | Output | Description |
|----------|--------|-------------|
| `sentiment_by_asset(df)` | DataFrame | Avg sentiment score + pos/neg/neutral counts per ticker |
| `sentiment_by_source(df)` | DataFrame | Counts per news source with avg score |
| `sentiment_by_time(df)` | DataFrame | 7-day daily trend with % breakdown per day |
| `overall_distribution(df)` | dict | Total pos/neg/neutral counts across all articles |
| `top_movers(df, n=10)` | DataFrame | Top N articles with strongest non-neutral sentiment |

`sentiment_by_time` uses `to_date(published)` grouping and converts raw counts into percentage scores per day for Chart.js compatibility on the dashboard.

---

### 2.4 Stock Price Analytics — `processing/stock_analytics.py`

PySpark window-function-based price metrics computed on top of OHLCV bar data.

| Metric | Window | Description |
|--------|--------|-------------|
| `ma_5` | 5-bar rolling | Short-term moving average of close price |
| `ma_20` | 20-bar rolling | Medium-term moving average of close price |
| `volatility` | 5-bar rolling stddev | Rolling standard deviation of close |
| `daily_change_pct` | Lag(1) per ticker | % change vs. previous bar's close |
| `volume_anomaly` | 5-bar avg | Boolean flag: `True` if volume > 2× average |

`latest_price_snapshot(df)` uses a `row_number()` window to extract the most recent bar per ticker and returns a dict mapping `ticker → {close, change_pct, volume, ma_5, ma_20, volatility, high, low}` consumed by the Flask `/api/prices` endpoint.

---

### 2.5 Batch Pipeline Orchestrator — `processing/batch_pipeline.py`

A 9-step fully orchestrated pipeline that runs on demand.

```
[1/9] Start SparkSession
[2/9] Load news + price data (live Finnhub/Alpha Vantage or offline sample)
[3/9] Fetch live social media posts (ApeWisdom / Reddit)
[4/9] Clean articles via cleaning_pipeline
[5/9] Sentiment analysis on articles (FinBERT → VADER fallback)
[6/9] Sentiment analysis on social posts
[7/9] Spark SQL analytics (by asset, source, time, distribution, top movers)
[8/9] Store to JSON files + SQLite database
[9/9] Upload vectors to Pinecone (non-fatal if API key missing)
```

**CLI usage:**
```powershell
python processing/batch_pipeline.py              # live Finnhub/Alpha Vantage APIs
python processing/batch_pipeline.py --source sample  # offline sample data
```

**Output summary logged on completion:**
- Articles processed
- Social posts stored
- Price bars stored
- Output directory path
- Total elapsed time

---

### 2.6 PySpark Structured Streaming Pipeline — `processing/streaming_pipeline.py`

Real-time counterpart to the batch pipeline. Reads from all three Kafka topics as continuous micro-batch streams and applies the same cleaning and scoring logic.

**Architecture:**

```
Kafka: news-feed  ──► _news_batch_handler()
                        ├── Spark UDF path (clean_articles_spark → apply_sentiment)
                        └── Python fallback (clean_articles_pandas → predict_batch)
                              └── _store_articles() → JSON + SQLite + Pinecone

Kafka: social-posts ─► _social_batch_handler()
                              └── _store_social_posts() → SQLite + Pinecone

Kafka: stock-prices ─► _prices_batch_handler()
                              └── compute_price_analytics() → JSON + SQLite + Pinecone
```

**Key design decisions:**
- `foreachBatch` pattern — each micro-batch runs through the same code as the batch pipeline (no duplication)
- **Dual processing path for news:** Spark UDF path tried first; Python/pandas fallback used automatically if UDF serialization fails
- Aggregates recomputed from SQLite on every batch write (`_compute_and_write_aggregates`)
- Checkpoint directories per topic prevent reprocessing after restarts
- Graceful shutdown on `SIGINT` / `SIGTERM`

**CLI usage:**
```powershell
python processing/streaming_pipeline.py                   # all 3 topics
python processing/streaming_pipeline.py --topic news-feed
python processing/streaming_pipeline.py --topic stock-prices
```

---

### 2.7 Live Data Fetcher — `ingestion/live_data_fetcher.py`

Replaces the RSS-only approach with multi-source live financial data fetching.

**Data sources integrated:**

| Source | API | Data |
|--------|-----|------|
| Finnhub | `finnhub.io` | Financial news with ticker categorization |
| Alpha Vantage | `alphavantage.co` | OHLCV price bars (1-min, 5-min, daily) |
| ApeWisdom | `apewisdom.io` | Reddit/social sentiment for tickers |

**Key functions:**
- `fetch_all_news(days_back=7)` — fetches and deduplicates articles across all tickers
- `fetch_all_prices()` — fetches OHLCV bars for all 10 configured tickers
- `fetch_all_prices_as_snapshot()` — returns latest price per ticker for fast dashboard loads
- `fetch_all_social_sentiment()` — fetches Reddit-sourced sentiment posts

---

### 2.8 Social Media Producer — `ingestion/social_producer.py`

New Kafka producer added in Week 2 that publishes social media sentiment data to the `social-posts` topic.

**Message schema:**
```json
{
  "id": "<hash>",
  "platform": "reddit",
  "ticker": "AAPL",
  "text": "Strong earnings beat expected...",
  "upvotes": 142,
  "published": "2026-05-20T10:00:00+00:00",
  "ingested_at": "2026-05-20T10:01:05+00:00"
}
```

---

### 2.9 Storage Layer

#### SQLite Writer — `storage/sqlite_writer.py`

Persistent relational storage with the following tables:

| Table | Contents |
|-------|----------|
| `articles` | Cleaned + scored news articles |
| `social_posts` | Social media posts with sentiment |
| `price_bars` | OHLCV bars with computed analytics (MA, volatility) |
| `price_snapshot` | Latest price per ticker for dashboard |
| `sentiment_aggregates` | By-source, by-asset, by-time aggregations |

All writes use `INSERT OR REPLACE` (upsert semantics) keyed on article/ticker IDs.

#### JSON Writer — `storage/json_writer.py`

Fast file-based cache for the Flask API layer:
- `write_articles()` / `read_articles()` — `processed/articles.json`
- `write_prices()` / `read_prices()` — `processed/prices.json`
- `write_aggregates()` / `read_aggregates()` — `processed/aggregates.json`

Flask reads from JSON first for low-latency API responses; SQLite used for analytics queries.

---

### 2.10 Flask Dashboard — Live Data Integration (`webapp/app.py`)

All API endpoints updated in Week 2 to read from processed pipeline output:

| Endpoint | Week 1 | Week 2 |
|----------|--------|--------|
| `/api/news` | Placeholder list | Reads `articles.json` → falls back to live Finnhub API |
| `/api/prices` | Placeholder data | Reads `prices.json` → falls back to live Alpha Vantage |
| `/api/sentiment-trend` | Static dummy data | Reads `aggregates.json` for real 7-day trend |
| `/api/risk-alerts` | Static alerts | Derived from negative-sentiment articles in SQLite |

**Data loading strategy (all endpoints):**
1. Try processed JSON file (fastest, pipeline output)
2. Fall back to live API fetch if JSON missing
3. Fall back to empty/placeholder if API unavailable

---

## 3. Data Flow — Week 2 Complete

```
RSS / yfinance / Finnhub / ApeWisdom
           │
           ▼
    Kafka Topics (Week 1 ✅)
     news-feed | social-posts | stock-prices
           │
           ▼
  streaming_pipeline.py (foreachBatch, micro-batch)
  OR
  batch_pipeline.py (on-demand full run)
           │
           ▼
  ┌─────────────────────────────────────────┐
  │  cleaning_pipeline.py                   │
  │  • HTML strip, URL remove, lowercase    │
  │  • Stopword removal, deduplication      │
  └─────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────┐
  │  sentiment_model.py (intelligence/)     │
  │  • FinBERT (ProsusAI/finbert)           │
  │  • VADER fallback                       │
  └─────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────┐
  │  spark_sql_analytics + stock_analytics  │
  │  • by_asset / by_source / by_time       │
  │  • MA-5, MA-20, volatility, anomalies   │
  └─────────────────────────────────────────┘
           │
           ▼
  SQLite DB  +  JSON files  +  Pinecone (Week 3)
           │
           ▼
  Flask API → Live Dashboard
```

---

## 4. Technology Stack Additions (Week 2)

| Technology | Version | Purpose |
|-----------|---------|---------|
| PySpark | 3.5.1 | Batch + Structured Streaming processing |
| spark-sql-kafka | 3.5.1-3 | Kafka → Spark connector |
| BeautifulSoup4 | 4.12.x | HTML tag stripping |
| NLTK | 3.8.x | Stopword corpus |
| vaderSentiment | 3.3.2 | VADER rule-based sentiment (fallback) |
| transformers | 4.x | FinBERT model loading (HuggingFace) |
| SQLite3 | stdlib | Relational storage for pipeline output |
| Finnhub SDK | 1.x | Live financial news + prices |
| Alpha Vantage | API | OHLCV historical/intraday price bars |
| ApeWisdom | REST API | Reddit social sentiment data |

---

## 5. Pipeline Test Results

| Test | Result |
|------|--------|
| SparkSession starts (batch mode) | ✅ Passed |
| SparkSession starts (streaming mode, Kafka JARs downloaded) | ✅ Passed |
| Text cleaning removes HTML correctly | ✅ Passed |
| Deduplication prevents duplicate articles | ✅ Passed |
| FinBERT sentiment inference (batch) | ✅ Passed |
| VADER fallback triggered when FinBERT unavailable | ✅ Passed |
| Spark SQL aggregations return correct counts | ✅ Passed |
| MA-5, MA-20 computed correctly on price data | ✅ Passed |
| SQLite upsert (no duplicates on re-run) | ✅ Passed |
| JSON files written to `processed/` directory | ✅ Passed |
| Flask `/api/news` returns real articles | ✅ Passed |
| Flask `/api/sentiment-trend` returns real 7-day data | ✅ Passed |
| Streaming pipeline processes news-feed topic | ✅ Passed |
| Graceful shutdown on Ctrl+C | ✅ Passed |

---

## 6. Known Limitations (Week 2)

| Limitation | Explanation | Resolved In |
|------------|-------------|-------------|
| RAG chat returns placeholder | Pinecone + LangGraph not yet wired | Week 3 |
| FinBERT slow on CPU | No GPU available; VADER fallback is fast | Accepted |
| Finnhub free tier rate limits | 60 calls/min; pipeline sleeps between requests | Accepted |
| Streaming triggers every 30s | Configurable via `STREAMING_TRIGGER_SECONDS` setting | Accepted |
| SQLite not distributed | Sufficient for project scale; Pinecone handles vector queries | Accepted |

---

## 7. Week 3 Preview

The following will be built in Week 3:

1. **FinBERT deep integration** — primary sentiment model, not just fallback
2. **Pinecone vector database** — embed all articles + price bars + aggregates into 3 namespaces
3. **RAG retrieval pipeline** — semantic search over stored financial context
4. **LangGraph multi-agent framework** — Market Analyst → Risk Manager → Portfolio Advisor → Summarizer
5. **OpenRouter GPT-4o-mini** — LLM backbone for all agents
6. **Flask `/api/chat` live** — chat endpoint fully wired to multi-agent pipeline

---

*Report generated: May 22, 2026 | FinIntel Engine v2.0 — Week 2*
