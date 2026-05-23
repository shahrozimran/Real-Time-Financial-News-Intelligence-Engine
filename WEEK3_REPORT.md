# Real-Time Financial News Intelligence Engine
## Week 3 — Project Report

**Student:** SHAHROZ
**Course:** Big Data (6th Semester)
**University:** University of Central Punjab (UCP)
**Report Date:** May 22, 2026
**Week Status:** ✅ COMPLETE

---

## 1. Week 3 Objective

Week 3 focused on building the **Intelligence Layer** — transforming the pipeline from a data-processing system into a true AI-powered financial intelligence engine. This week delivered: a FinBERT deep-learning sentiment model, a Pinecone vector database with three specialized namespaces, a semantic RAG retrieval system, and a LangGraph multi-agent framework powered by GPT-4o-mini via OpenRouter. The Flask `/api/chat` endpoint was fully activated, making the AI chat interface on the dashboard live.

---

## 2. Components Built in Week 3

### 2.1 FinBERT Sentiment Model — `intelligence/sentiment_model.py`

The primary NLP engine for all sentiment classification across the project. Replaces the VADER-only approach with a domain-specific financial language model.

**Model used:** `ProsusAI/finbert` (HuggingFace Transformers, ~400 MB download on first run)

**Labels:** `positive`, `negative`, `neutral`

**Architecture — dual-model with automatic fallback:**

```
Input text
    │
    ▼
FinBERT (ProsusAI/finbert)
    │  success → (label, confidence_score)
    │  failure ↓
    ▼
VADER SentimentIntensityAnalyzer
    │  compound ≥ 0.05  → positive
    │  compound ≤ -0.05 → negative
    └  otherwise        → neutral
```

**Public API:**

| Function | Input | Output | Description |
|----------|-------|--------|-------------|
| `predict_sentiment(text)` | `str` | `(label, score)` | Single text inference |
| `predict_batch(texts)` | `list[str]` | `list[(label, score)]` | Batch inference via FinBERT pipeline |

Both functions use lazy-loaded singletons — the model is downloaded and initialized only on first call. Batch processing uses `SENTIMENT_BATCH_SIZE` (configurable) to avoid OOM on large article sets.

---

### 2.2 Pinecone Vector Database — `storage/pinecone_writer.py`

All processed financial data is embedded and stored in Pinecone for semantic retrieval by the RAG pipeline. A single Pinecone index (`finintel`) with three namespaces keeps the data organized.

**Embedding model:** `text-embedding-3-small` via OpenRouter (1536 dimensions, cosine similarity)

**Three namespaces:**

| Namespace | Contents | Vector text format |
|-----------|----------|-------------------|
| `articles` | Financial news articles with sentiment | `"{title}. {summary} Sentiment: {label} ({score})"` |
| `prices` | OHLCV price bars as text summaries | `"{ticker} on {date}: open={open} close={close} vol={volume} MA5={ma_5} volatility={volatility}"` |
| `sentiment` | Aggregated sentiment stats by asset/source/time | `"Sentiment for {key}: positive={pos}% negative={neg}% neutral={neu}% avg_score={score}"` |

**Key functions:**

| Function | Description |
|----------|-------------|
| `upsert_articles(articles)` | Embed + upsert news articles into `articles` namespace |
| `upsert_price_bars(bars)` | Embed + upsert price data into `prices` namespace |
| `upsert_social_posts(posts)` | Embed + upsert social media posts into `articles` namespace |
| `upsert_aggregates(agg_dict)` | Embed + upsert all sentiment aggregates into `sentiment` namespace |
| `upsert_all(...)` | Convenience wrapper — calls all four in sequence |

**Design details:**
- Index is created automatically if it does not exist (`ServerlessSpec`, `aws` / `us-east-1` by default)
- Vector IDs are MD5 hashes of article URL or ticker+date — idempotent re-upserts
- Batching at 100 vectors per Pinecone API call to respect rate limits
- API key and index name configured via `.env`

---

### 2.3 RAG Retrieval System — `intelligence/rag_retriever.py`

Semantic search layer that fetches the most relevant financial context from Pinecone for any given query. Intentionally avoids `langchain-pinecone` to prevent numpy version conflicts.

**`Document` dataclass** (replaces LangChain Document):
```python
@dataclass
class Document:
    page_content: str
    metadata: dict
```

**Core retrieval function:**
```python
retrieve(query, namespace, top_k, filter) → list[Document]
```
Embeds the query via `text-embedding-3-small`, queries the specified Pinecone namespace, and returns `Document` objects with `page_content` populated from the stored `text` metadata field.

**Convenience helpers used by agents:**

| Function | Namespace(s) | Use case |
|----------|-------------|----------|
| `retrieve_for_ticker(ticker, top_k)` | `articles` + `prices` | Ticker-specific market queries |
| `retrieve_market_overview(top_k)` | `articles` + `sentiment` | Broad market questions |
| `retrieve_risk_signals(top_k)` | `articles` + `sentiment` | Risk/negative sentiment queries |
| `retrieve_recent_news(top_k)` | `articles` | Latest news retrieval |
| `retrieve_price_context(ticker)` | `prices` | Price-specific questions |
| `docs_to_context_string(docs)` | — | Formats docs into LLM prompt string |

**Smart query routing in retriever:**
- Ticker symbols detected in the query → `retrieve_for_ticker()` (up to 2 tickers)
- Risk/negative keywords detected → `retrieve_risk_signals()`
- All other financial queries → `retrieve_market_overview()`

---

### 2.4 LangGraph Multi-Agent Framework — `intelligence/agents.py`

The core intelligence system. A sequential `StateGraph` built with LangGraph where four specialized AI agents collaborate to produce a comprehensive financial analysis for every user question.

**LLM:** `openai/gpt-4o-mini` via OpenRouter (`OPENROUTER_API_KEY` in `.env`)

**Graph architecture:**

```
retrieve_context
      │
      ▼
market_analyst_node
      │
      ▼
risk_manager_node
      │
      ▼
portfolio_advisor_node
      │
      ▼
summarizer_node
      │
      ▼
    END
```

**`AgentState` (LangGraph TypedDict):**

| Field | Type | Description |
|-------|------|-------------|
| `question` | `str` | Original user question |
| `context` | `list[Document]` | Retrieved Pinecone documents |
| `sources` | `list[dict]` | Article sources for frontend citation |
| `market_analysis` | `str` | Market Analyst agent output |
| `risk_assessment` | `str` | Risk Manager agent output |
| `portfolio_advice` | `str` | Portfolio Advisor agent output |
| `final_summary` | `str` | Summarizer synthesis output |

---

#### Agent Roles

**Node 1 — `retrieve_context`**
Calls the RAG retriever to fetch up to 10 relevant `Document` objects from Pinecone. Performs deduplication by first 80 characters of `page_content`. Populates `sources` list for the frontend to display cited articles.

**Node 2 — `market_analyst_node`**
System role: *Expert Market Analyst at a top investment bank.* Explains market movements, news impact, and sector trends based strictly on the retrieved context. Output: 3–5 sentences, factual, no invented data.

**Node 3 — `risk_manager_node`**
System role: *Senior Risk Manager at a financial institution.* Reads both the retrieved context and the Market Analyst's output. Identifies risks, red flags, negative sentiment signals, and volatility concerns. Output: 3–5 bullet points.

**Node 4 — `portfolio_advisor_node`**
System role: *Educational Portfolio Advisor.* Reads context + prior agent outputs. Provides general portfolio education (diversification, position sizing, risk tolerance). **Always includes "NOT financial advice" disclaimer.** Output: 3–5 educational points.

**Node 5 — `summarizer_node`**
Synthesizes all three prior agent outputs into a single coherent paragraph (4–6 sentences) highlighting the most important insights and overall market outlook.

---

### 2.5 Smart Query Routing — `run_multi_agent()`

The public entry point applies intelligent routing before deciding whether to invoke the full RAG pipeline:

```
User question
      │
      ▼
_is_financial(question)?
   YES → RAG pipeline (Pinecone retrieval + all 4 agents)
   NO  → _run_direct_llm() (GPT-4o-mini only, no Pinecone)
```

**Financial keyword vocabulary:** 100+ keywords covering tickers, indices, crypto, economic terms, and financial concepts.

**Non-financial handling (`_run_direct_llm`):**
- Greetings → warm introduction + capability list
- Off-topic questions (recipes, coding, sports) → polite decline + redirect to finance
- General conversational → brief answer, steer back to finance

**`run_multi_agent()` return schema:**
```python
{
    "mode":              "rag" | "direct" | "error",
    "market_analyst":    str,   # agent 2 output
    "risk_manager":      str,   # agent 3 output
    "portfolio_advisor": str,   # agent 4 output
    "summary":           str,   # synthesized final answer
    "sources":           list[dict],  # cited articles with url/title/sentiment
}
```

---

### 2.6 Flask Chat API — `webapp/app.py` (`/api/chat`)

The `/api/chat` POST endpoint (previously returning placeholder text) is now fully wired to the multi-agent pipeline:

```
POST /api/chat  {"question": "What is NVIDIA's sentiment this week?"}
      │
      ▼
run_multi_agent(question)
      │
      ▼
{
  "mode": "rag",
  "market_analyst": "...",
  "risk_manager": "...",
  "portfolio_advisor": "...",
  "summary": "...",
  "sources": [...]
}
```

The dashboard's AI Chat section displays each agent's response in a separate labeled card (Market Analyst, Risk Manager, Portfolio Advisor) followed by the unified summary and clickable source citations.

---

## 3. Intelligence Layer Architecture

```
User Question (via Flask /api/chat)
        │
        ▼
  _is_financial()  ──NO──► GPT-4o-mini direct response
        │YES
        ▼
  retrieve_context()
    ├─► Pinecone `articles` namespace  (semantic news search)
    ├─► Pinecone `prices` namespace    (price bar context)
    └─► Pinecone `sentiment` namespace (aggregate stats)
        │
        ▼ (up to 10 deduplicated Documents)
  ┌─────────────────────────────────────────────────────────┐
  │              LangGraph StateGraph                        │
  │                                                         │
  │  [1] Market Analyst  → market movement explanation      │
  │  [2] Risk Manager    → risk flags & negative signals    │
  │  [3] Portfolio Advisor → educational portfolio guidance │
  │  [4] Summarizer      → unified 4-6 sentence synthesis   │
  └─────────────────────────────────────────────────────────┘
        │
        ▼
  JSON response → Flask → Dashboard chat UI
```

---

## 4. Technology Stack Additions (Week 3)

| Technology | Version | Purpose |
|-----------|---------|---------|
| LangGraph | 0.2.x | Multi-agent StateGraph orchestration |
| LangChain OpenAI | 0.2.x | ChatOpenAI wrapper for OpenRouter |
| OpenAI SDK | 1.x | Embeddings via OpenRouter API |
| Pinecone SDK | 3.x | Vector database client |
| HuggingFace Transformers | 4.x | FinBERT model loading |
| OpenRouter | API | LLM gateway (GPT-4o-mini + embeddings) |
| `ProsusAI/finbert` | — | Financial domain BERT sentiment model |
| `text-embedding-3-small` | 1536-dim | OpenAI-compatible text embeddings |

---

## 5. End-to-End Integration Test Results (Week 3)

| Test | Result |
|------|--------|
| FinBERT loads and classifies articles | ✅ Passed |
| VADER fallback activates when FinBERT fails | ✅ Passed |
| Pinecone index auto-created on first run | ✅ Passed |
| Article embeddings upserted to `articles` namespace | ✅ Passed |
| Price bar embeddings upserted to `prices` namespace | ✅ Passed |
| Sentiment aggregates upserted to `sentiment` namespace | ✅ Passed |
| RAG retriever returns relevant docs for ticker query | ✅ Passed |
| RAG retriever returns risk signals for risk query | ✅ Passed |
| LangGraph compiles without errors | ✅ Passed |
| All 4 agent nodes execute sequentially | ✅ Passed |
| Market Analyst produces factual 3-5 sentence analysis | ✅ Passed |
| Risk Manager produces 3-5 risk bullets | ✅ Passed |
| Portfolio Advisor includes "not financial advice" | ✅ Passed |
| Summarizer synthesizes all agents into one paragraph | ✅ Passed |
| Non-financial queries routed to direct LLM (no Pinecone) | ✅ Passed |
| Off-topic questions politely declined | ✅ Passed |
| Flask `/api/chat` returns live multi-agent response | ✅ Passed |
| Sources array populated with cited article metadata | ✅ Passed |

---

## 6. Known Limitations (Week 3)

| Limitation | Explanation | Resolved In |
|------------|-------------|-------------|
| Pinecone requires paid API key | Free tier available; key set in `.env` | User setup |
| OpenRouter API key required for LLM + embeddings | Free credits available at openrouter.ai | User setup |
| FinBERT inference slow on CPU (~2-5s per batch) | Acceptable for dashboard; background pipeline handles bulk | Accepted |
| Pinecone cold start ~1-2s on first query | Subsequent queries cached in `_pinecone_index` singleton | Accepted |
| LangGraph adds ~3-6s total latency (4 LLM calls) | Sequential by design for multi-perspective depth | Accepted |
| No streaming chat responses | Full response returned after all agents complete | Week 4 |

---

## 7. Complete System Status After Week 3

| Layer | Status | Key Technologies |
|-------|--------|-----------------|
| **Ingestion** (Week 1) | ✅ Complete | Kafka 4.2.0 KRaft, rss_producer, stock_producer, social_producer |
| **Processing** (Week 2) | ✅ Complete | PySpark 3.5.1, Structured Streaming, cleaning, Spark SQL, stock analytics |
| **Intelligence** (Week 3) | ✅ Complete | FinBERT, Pinecone, RAG, LangGraph, GPT-4o-mini |
| **Presentation** | ✅ Live | Flask 3.0.3, Bootstrap 5, Chart.js, real-time dashboard + AI chat |

---

## 8. Week 4 Preview

The following will be completed in Week 4:

1. **Full end-to-end integration test** — Kafka → Spark → Pinecone → LangGraph → Dashboard in one live run
2. **Docker containerization** — `docker-compose.yml` for Kafka, Spark, Flask, pipeline services
3. **Performance optimization** — streaming trigger tuning, Pinecone batch size tuning
4. **Final report & documentation** — complete system walkthrough and demo

---

*Report generated: May 22, 2026 | FinIntel Engine v3.0 — Week 3*
