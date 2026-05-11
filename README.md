# Real-Time Financial News Intelligence Engine

A big-data streaming platform that ingests live financial news and stock prices,
performs sentiment analysis, and provides investment insights through a Flask dashboard
and RAG-powered conversational AI.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.10+ | `python --version` |
| Java JDK | 11 | Required by Kafka & PySpark. Set `JAVA_HOME`. |
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

## Running the Flask Dashboard

```powershell
python webapp/app.py
```

Open http://localhost:5000 in your browser.

### Available API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Main dashboard |
| GET | `/api/news` | Latest news (`?limit=10&sentiment=positive`) |
| GET | `/api/prices` | Market prices (`?ticker=AAPL`) |
| GET | `/api/sentiment-trend` | 7-day sentiment data |
| POST | `/api/chat` | AI chat (`{"question": "..."}`) |
| GET | `/api/status` | Health check |

---

## Project Structure

```
Project/
├── config/
│   └── settings.py          # Central config: Kafka, assets, RSS feeds
├── ingestion/
│   ├── rss_producer.py      # RSS → Kafka producer
│   ├── stock_producer.py    # yfinance → Kafka producer
│   └── consumer_test.py     # Manual Kafka verification consumer
├── processing/              # Week 2: PySpark cleaning & sentiment jobs
├── intelligence/            # Week 3: RAG + multi-agent framework
├── storage/                 # Week 3: Vector DB integration
├── webapp/
│   ├── app.py               # Flask application
│   ├── templates/
│   │   └── index.html       # Bootstrap 5 dashboard
│   └── static/
│       └── style.css        # Custom dark-theme styles
├── data/
│   └── sample/              # Sample JSON/CSV for offline testing
├── .env                     # Secrets & config (not committed)
├── requirements.txt
└── README.md
```

---

## Week-by-Week Progress

| Week | Status | Focus |
|------|--------|-------|
| Week 1 | ✅ Complete | Environment, Kafka, ingestion producers, Flask skeleton |
| Week 2 | 🔲 Pending | PySpark cleaning, sentiment analysis, Spark SQL |
| Week 3 | 🔲 Pending | Vector DB, RAG, multi-agent AI framework |
| Week 4 | 🔲 Pending | Full integration, testing, report |

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

## Limitations (Week 1)

- Dashboard shows **placeholder data** — live Kafka consumption wired in Week 2.
- Chat endpoint returns a placeholder — RAG integration in Week 3.
- Social media stream uses simulated data (live API access may require approval).
- Free stock APIs (yfinance) may return delayed data outside market hours.
