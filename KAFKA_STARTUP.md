# Kafka 4.2.0 — Startup Guide (KRaft Mode)

> **Your Kafka path:** `C:\Users\SHAHROZ\kafka_2.13-4.2.0`
> **Your Project path:** `C:\Users\SHAHROZ\Desktop\UCP\6th semester\Big Data\Project`
> **Kafka version:** 4.2.0 (KRaft mode — no ZooKeeper needed)

---

## ⚠️ IMPORTANT — One-Time Setup (Already Done)

You already ran these. **DO NOT run again** unless you delete Kafka's log folder.

```powershell
$KAFKA = "C:\Users\SHAHROZ\kafka_2.13-4.2.0"
$ID = & "$KAFKA\bin\windows\kafka-storage.bat" random-uuid
& "$KAFKA\bin\windows\kafka-storage.bat" format --standalone -t $ID -c "$KAFKA\config\server.properties"
```

---

## Every Time You Want to Run the Project

You need **5 terminals** open simultaneously.
Open them in order — **do not skip steps**.

---

### TERMINAL 1 — Start Kafka Broker

```powershell
$KAFKA = "C:\Users\SHAHROZ\kafka_2.13-4.2.0"
& "$KAFKA\bin\windows\kafka-server-start.bat" "$KAFKA\config\server.properties"
```

**Wait for this line before opening other terminals:**
```
[KafkaRaftServer nodeId=1] Kafka Server started
```

> ❌ Keep this terminal open — closing it shuts down Kafka.

---

### TERMINAL 2 — Create Topics (First Time Only Per Session)

Only needed if topics were deleted or it is the first run after re-formatting storage.

```powershell
$KAFKA = "C:\Users\SHAHROZ\kafka_2.13-4.2.0"
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic news-feed    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic social-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
& "$KAFKA\bin\windows\kafka-topics.bat" --create --topic stock-prices  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify all 3 topics exist:
```powershell
$KAFKA = "C:\Users\SHAHROZ\kafka_2.13-4.2.0"
& "$KAFKA\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092
```

Expected output:
```
news-feed
social-posts
stock-prices
```

> ✅ You can close this terminal after topics are created.

---

### TERMINAL 3 — Start Flask Dashboard

```powershell
cd "C:\Users\SHAHROZ\Desktop\UCP\6th semester\Big Data\Project"
.\venv\Scripts\Activate.ps1
python webapp/app.py
```

Open browser at: **http://localhost:5000**

> ❌ Keep this terminal open.

---

### TERMINAL 4 — Start Stock Price Producer

```powershell
cd "C:\Users\SHAHROZ\Desktop\UCP\6th semester\Big Data\Project"
.\venv\Scripts\Activate.ps1
python ingestion/stock_producer.py
```

Expected output:
```
Connected to Kafka broker at localhost:9092
Tickers: ['AAPL', 'TSLA', 'MSFT', ...]
Publishing to topic: stock-prices
```

> ❌ Keep this terminal open — publishes every 60 seconds.

---

### TERMINAL 5 — Start RSS News Producer

```powershell
cd "C:\Users\SHAHROZ\Desktop\UCP\6th semester\Big Data\Project"
.\venv\Scripts\Activate.ps1
python ingestion/rss_producer.py
```

Expected output:
```
Connected to Kafka broker at localhost:9092
Probing RSS feeds, polling every 60s
Publishing to topic: news-feed
```

> ❌ Keep this terminal open — polls RSS feeds every 60 seconds.

---

### TERMINAL 6 — Verify Messages (Optional)

Use this any time to confirm messages are flowing through Kafka.

```powershell
cd "C:\Users\SHAHROZ\Desktop\UCP\6th semester\Big Data\Project"
.\venv\Scripts\Activate.ps1
python ingestion/consumer_test.py
```

Filter to a single topic:
```powershell
python ingestion/consumer_test.py news-feed
python ingestion/consumer_test.py stock-prices
```

> ✅ You can close this after verifying.

---

## Quick Reference — All Terminals at a Glance

| # | Terminal | Command | Keep Open? |
|---|----------|---------|------------|
| 1 | Kafka Broker | `kafka-server-start.bat server.properties` | ❌ Must stay open |
| 2 | Create Topics | `kafka-topics.bat --create ...` | ✅ Close after done |
| 3 | Flask Dashboard | `python webapp/app.py` | ❌ Must stay open |
| 4 | Stock Producer | `python ingestion/stock_producer.py` | ❌ Must stay open |
| 5 | RSS Producer | `python ingestion/rss_producer.py` | ❌ Must stay open |
| 6 | Consumer Test | `python ingestion/consumer_test.py` | ✅ Close after verifying |

---

## Startup Order (Always Follow This)

```
1. Start Kafka broker          ← wait for "Kafka Server started"
2. Create topics (if needed)   ← only first time or after reset
3. Start Flask dashboard       ← http://localhost:5000
4. Start Stock producer        ← publishes to stock-prices
5. Start RSS producer          ← publishes to news-feed
6. (Optional) Run consumer     ← verify messages flowing
```

---

## Shutdown Order

```
1. Ctrl+C on RSS producer
2. Ctrl+C on Stock producer
3. Ctrl+C on Flask dashboard
4. Ctrl+C on Kafka broker
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `CommandNotFoundException` on kafka bat | Check path — use `C:\Users\SHAHROZ\kafka_2.13-4.2.0` |
| `NoSuchFileException: config\kraft\server.properties` | Use `config\server.properties` (no kraft subfolder) |
| `controller.quorum.voters is not set` | Add `--standalone` to the format command |
| `No module named kafka.vendor.six.moves` | Run `pip uninstall kafka-python -y && pip install kafka-python-ng` |
| `Topic already exists` error | That is fine — topic is already created, continue |
| Producer connects but no messages | Wait 60 seconds — producers poll on a 60s interval |
| RSS feed cURL error 11001 | DNS issue for that feed URL — other feeds still work, ignore it |

---

## Week Progress

| Week | Status | What Gets Added |
|------|--------|-----------------|
| Week 1 | ✅ Complete | Kafka ingestion + Flask skeleton |
| Week 2 | 🔲 Next | PySpark reads Kafka → sentiment analysis → live dashboard |
| Week 3 | 🔲 Pending | ChromaDB + RAG + multi-agent AI |
| Week 4 | 🔲 Pending | Full integration + report |
