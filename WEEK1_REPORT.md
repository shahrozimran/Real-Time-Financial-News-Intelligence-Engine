# Real-Time Financial News Intelligence Engine
## Week 1 — Project Report

**Student:** SHAHROZ  
**Course:** Big Data (6th Semester)  
**University:** University of Central Punjab (UCP)  
**Report Date:** May 12, 2026  
**Week Status:** ✅ COMPLETE  

---

## 1. Project Overview

The **Real-Time Financial News Intelligence Engine** is a big-data streaming platform designed to ingest live financial news articles and stock price data, process them using distributed computing tools, perform AI-powered sentiment analysis, and deliver actionable investment insights through an interactive web dashboard and a conversational AI agent.

The system is built entirely on open-source technologies and follows a modular, week-by-week development roadmap spanning four weeks.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  ┌──────────────────┐        ┌──────────────────────────────┐   │
│  │   6 RSS Feeds    │        │   yfinance Stock API         │   │
│  │ • Reuters        │        │   10 Tickers (AAPL, TSLA...) │   │
│  │ • Yahoo Finance  │        └──────────────────────────────┘   │
│  │ • CNBC           │                                           │
│  │ • MarketWatch    │                                           │
│  │ • Seeking Alpha  │                                           │
│  │ • Investing.com  │                                           │
│  └──────────────────┘                                           │
└───────────────┬─────────────────────────┬───────────────────────┘
                │                         │
                ▼                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Week 1 ✅)                    │
│                                                                  │
│   rss_producer.py              stock_producer.py                 │
│   (polls every 60s)            (polls every 60s)                 │
│        │                               │                         │
│        ▼                               ▼                         │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │           Apache Kafka 4.2.0 (KRaft Mode)                │    │
│  │  ┌─────────────┐ ┌──────────────┐ ┌──────────────────┐  │    │
│  │  │  news-feed  │ │ social-posts │ │  stock-prices    │  │    │
│  │  │ (3 parts)   │ │  (3 parts)   │ │   (3 parts)      │  │    │
│  │  └─────────────┘ └──────────────┘ └──────────────────┘  │    │
│  └──────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────────────────────┐
│              PROCESSING LAYER (Week 2 — Pending)                 │
│                                                                  │
│   PySpark Structured Streaming                                   │
│   • Text cleaning & normalisation                                │
│   • VADER / TextBlob sentiment scoring                           │
│   • Spark SQL aggregations                                       │
│   • Write results to storage                                     │
└──────────────────────────────────────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────────────────────┐
│             INTELLIGENCE LAYER (Week 3 — Pending)                │
│                                                                  │
│   ChromaDB Vector Store + RAG Pipeline                           │
│   Multi-Agent Framework:                                         │
│   • Market Analyst Agent                                         │
│   • Risk Manager Agent                                           │
│   • Portfolio Advisor Agent                                      │
└──────────────────────────────────────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────────────────────┐
│               PRESENTATION LAYER (Week 1 ✅ Skeleton)             │
│                                                                  │
│   Flask 3.0.3 Web Application                                    │
│   • Modern dark-theme dashboard (Bootstrap 5 + Chart.js)        │
│   • Sidebar navigation (6 sections)                              │
│   • Live ticker tape, sentiment charts, risk alerts              │
│   • AI chat interface (RAG active in Week 3)                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Language | Python | 3.14 | Core development language |
| Message Broker | Apache Kafka | 4.2.0 (KRaft) | Real-time data streaming |
| Stream Processing | PySpark | 3.5.1 | Distributed data processing (Week 2) |
| Web Framework | Flask | 3.0.3 | Dashboard and REST API |
| Stock Data | yfinance | 0.2.38 | Live stock price ingestion |
| News Feeds | feedparser | 6.0.11 | RSS feed parsing |
| Kafka Client | kafka-python-ng | 2.2.3 | Python Kafka producer/consumer |
| Frontend | Bootstrap 5 + Chart.js | 5.3.3 / 4.4.3 | UI components and charts |
| Icons | Bootstrap Icons | 1.11.3 | Dashboard iconography |
| Data Processing | pandas | latest | Data manipulation |
| Vector DB | ChromaDB | — | Semantic search (Week 3) |
| Environment | python-dotenv | — | Configuration management |

---

## 4. Project Folder Structure

```
Project/
│
├── config/
│   └── settings.py              ← Central config: Kafka broker, topics,
│                                   10 tickers, 6 RSS feeds, Flask settings
│
├── ingestion/
│   ├── __init__.py
│   ├── rss_producer.py          ← Polls 6 RSS feeds → news-feed topic
│   ├── stock_producer.py        ← yfinance 10 tickers → stock-prices topic
│   └── consumer_test.py         ← Manual verification consumer
│
├── processing/
│   └── __init__.py              ← Week 2: PySpark cleaning & sentiment jobs
│
├── intelligence/
│   └── __init__.py              ← Week 3: RAG + multi-agent framework
│
├── storage/
│   └── __init__.py              ← Week 3: ChromaDB vector store
│
├── webapp/
│   ├── app.py                   ← Flask app with 6 API routes
│   ├── templates/
│   │   └── index.html           ← Full dashboard (650+ lines, 6 sections)
│   └── static/
│       └── style.css            ← Custom design system (490+ lines)
│
├── data/
│   └── sample/                  ← Offline test data
│
├── .env                         ← Environment variables (not committed)
├── .gitignore                   ← Git ignore rules
├── requirements.txt             ← 36 pinned Python dependencies
├── README.md                    ← Setup and run instructions
├── KAFKA_STARTUP.md             ← Kafka startup reference guide
└── WEEK1_REPORT.md              ← This file
```

---

## 5. Components Built in Week 1

### 5.1 Configuration Module — `config/settings.py`

Central configuration file that defines all project-wide constants:

- **Kafka broker:** `localhost:9092`
- **Topics:** `news-feed`, `social-posts`, `stock-prices`
- **Financial assets (10 tickers):**

| Category | Tickers |
|----------|---------|
| Stocks | AAPL, TSLA, MSFT, GOOGL, AMZN, NVDA |
| Indices | ^GSPC (S&P 500), ^IXIC (NASDAQ) |
| Crypto | BTC-USD, ETH-USD |

- **RSS Feed Sources (6):**

| Source | URL |
|--------|-----|
| Reuters Business | feeds.reuters.com/reuters/businessNews |
| Yahoo Finance | finance.yahoo.com/news/rss |
| CNBC Top News | feeds.content.dowjones.io/public/rss/mktews |
| MarketWatch | feeds.marketwatch.com/marketwatch/topstories |
| Seeking Alpha | seekingalpha.com/feed.xml |
| Investing.com | investing.com/rss/news.rss |

---

### 5.2 RSS News Producer — `ingestion/rss_producer.py`

Polls all 6 RSS feeds every 60 seconds and publishes to Kafka.

**Key features:**
- Deduplication using MD5 URL hash — no duplicate articles published
- Structured JSON messages with: `id`, `source`, `title`, `summary`, `url`, `published`, `ingested_at`
- Graceful error handling per feed — one failing feed does not stop others
- Configurable polling interval via `settings.py`

**Sample message published to `news-feed` topic:**
```json
{
  "id": "d3c7262e69b4b45b4c17d9b9a001cfaf",
  "source": "Seeking Alpha",
  "title": "Cineplex Inc. (CGX:CA) Q1 2026 Earnings Call Transcript",
  "summary": "",
  "url": "https://seekingalpha.com/article/4902615-...",
  "published": "2026-05-11T19:00:45+00:00",
  "ingested_at": "2026-05-11T19:03:10.682612+00:00"
}
```

---

### 5.3 Stock Price Producer — `ingestion/stock_producer.py`

Fetches latest OHLCV price bars for all 10 tickers via yfinance every 60 seconds.

**Key features:**
- Fetches latest 1-minute bar for each ticker
- Publishes structured JSON with: `ticker`, `open`, `high`, `low`, `close`, `volume`, `timestamp`, `ingested_at`
- Handles market-hours limitations (yfinance returns delayed data outside hours)
- Continues to next ticker if one fails

**Sample message published to `stock-prices` topic:**
```json
{
  "ticker": "AAPL",
  "open": 189.50,
  "high": 190.10,
  "low": 189.20,
  "close": 189.75,
  "volume": 52340000,
  "timestamp": "2026-05-11T19:00:00+00:00",
  "ingested_at": "2026-05-11T19:03:05+00:00"
}
```

---

### 5.4 Consumer Verification Script — `ingestion/consumer_test.py`

Manual verification tool for confirming Kafka message flow.

**Features:**
- Subscribes to all 3 topics or a specific topic by CLI argument
- Colour-coded output per topic for easy reading
- Prints message metadata: topic, partition, offset, key
- Used to confirm end-to-end pipeline during Week 1 testing

---

### 5.5 Flask Web Application — `webapp/app.py`

REST API backend with 6 endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Renders main dashboard |
| GET | `/api/news` | Returns news articles (filter by sentiment, source, limit) |
| GET | `/api/prices` | Returns market price snapshots (filter by ticker) |
| GET | `/api/sentiment-trend` | Returns 7-day sentiment trend data for Chart.js |
| POST | `/api/chat` | Receives question, returns AI agent response |
| GET | `/api/risk-alerts` | Returns active risk alert signals |
| GET | `/api/status` | Health check endpoint |

> **Note:** All endpoints currently return rich placeholder data.
> Live Kafka consumer reads will be wired in Week 2.

---

### 5.6 Dashboard UI — `webapp/templates/index.html` + `webapp/static/style.css`

A fully redesigned, modern financial intelligence dashboard.

**Design System:**
- Deep space dark theme with CSS variables (`--bg-base: #05101f`)
- Accent palette: Cyan (#00d4ff), Blue (#3b82f6), Green (#10b981), Red (#ef4444)
- Inter font (Google Fonts)
- Glassmorphism-style cards with glow effects

**Dashboard Sections (6):**

| Section | Contents |
|---------|----------|
| Dashboard | KPI cards, market prices table, sentiment donut, 7-day trend chart, news feed, AI chat |
| Market Prices | Full ticker table with volume bars and change pills |
| News Feed | Filterable news with sentiment tabs (All/Positive/Negative/Neutral) |
| Sentiment Analysis | Side-by-side trend chart + donut breakdown |
| Risk Alerts | Colour-coded alerts (High/Medium/Low) with detail and timestamp |
| AI Chat | Full-screen multi-agent chat with 3 agent role badges |

**Interactive Features:**
- Scrolling live ticker tape in topbar (all 10 tickers)
- Real-time clock updating every second
- Page loader animation on startup
- News filter tabs (client-side)
- Animated typing dots in chat while waiting
- Global refresh button with spin animation
- Sidebar navigation with active state
- Mobile responsive with hamburger toggle

---

## 6. Kafka Setup — KRaft Mode

Kafka 4.2.0 removed ZooKeeper entirely. The project uses **KRaft (Kafka Raft) mode**, which is the modern architecture where Kafka manages its own metadata without a separate ZooKeeper process.

**Benefits for this project:**
- Simpler setup — only 1 process instead of 2 (ZooKeeper + Kafka)
- Faster startup time
- Better suited for future containerisation (Week 4)

**One-time initialisation performed:**
```powershell
kafka-storage.bat format --standalone -t <UUID> -c config/server.properties
```

**Result:**
```
Formatting dynamic metadata voter directory /tmp/kraft-combined-logs
with metadata.version 4.2-IV1.
```

---

## 7. Integration Test Results

The following was verified live on **May 12, 2026 at 00:03 UTC+05:00**:

| Test | Result |
|------|--------|
| Kafka broker starts on port 9092 | ✅ Passed |
| All 3 topics created successfully | ✅ Passed |
| RSS producer connects to Kafka | ✅ Passed |
| Stock producer connects to Kafka | ✅ Passed |
| Messages visible in consumer_test.py | ✅ Passed |
| Real news articles flowing (Seeking Alpha) | ✅ Passed |
| Flask dashboard loads at localhost:5000 | ✅ Passed |
| All 7 API endpoints return HTTP 200 | ✅ Passed |

---

## 8. Known Limitations (Week 1)

| Limitation | Explanation | Resolved In |
|------------|-------------|-------------|
| Dashboard shows placeholder data | Kafka → Flask bridge not yet built | Week 2 |
| No sentiment scoring | PySpark pipeline not yet built | Week 2 |
| Chat returns placeholder answer | RAG pipeline not yet connected | Week 3 |
| RSS summary field sometimes empty | Some feeds don't include description tag | Week 2 |
| `cURL error 11001` on some feeds | DNS resolution failure for that feed URL | Ongoing |
| Stock data delayed outside market hours | yfinance free tier limitation | Accepted |

---

## 9. Dependencies Installed

```
kafka-python-ng==2.2.3    feedparser==6.0.11     yfinance==0.2.38
flask==3.0.3              pandas==2.2.2          requests==2.31.0
python-dotenv==1.0.1      pyspark==3.5.1         numpy==1.26.4
```

> Full list in `requirements.txt` (36 packages total)

---

## 10. Week-by-Week Roadmap

| Week | Focus | Status |
|------|-------|--------|
| **Week 1** | Environment setup, Kafka ingestion, Flask skeleton, UI/UX | ✅ **COMPLETE** |
| **Week 2** | PySpark Structured Streaming, text cleaning, sentiment analysis, live dashboard | 🔲 Next |
| **Week 3** | ChromaDB vector store, RAG pipeline, multi-agent AI (Market Analyst, Risk Manager, Portfolio Advisor) | 🔲 Pending |
| **Week 4** | Full integration, end-to-end testing, optimisation, final report | 🔲 Pending |

---

## 11. Week 2 Preview

The following will be built in Week 2:

1. **PySpark Structured Streaming job** — reads from `news-feed` Kafka topic in real time
2. **Text cleaning pipeline** — removes HTML tags, stopwords, special characters
3. **Sentiment scoring** — VADER (rule-based) + optional TextBlob for each article
4. **Spark SQL aggregations** — group by source, hour, ticker mentions
5. **Storage writer** — writes processed results to SQLite / JSON for Flask to read
6. **Live dashboard** — Flask APIs updated to read from processed data instead of placeholders
7. **Stock price processor** — reads `stock-prices` topic, computes moving averages and volatility

---

*Report generated: May 12, 2026 | FinIntel Engine v1.0 — Week 1*
