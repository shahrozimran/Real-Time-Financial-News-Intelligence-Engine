"""
rag_retriever.py
----------------
Pinecone retriever using raw Pinecone client + OpenAI-compatible embeddings.
No langchain-pinecone dependency (avoids numpy<2 conflict).

Returns simple Document-like namedtuples compatible with LangGraph AgentState.

Namespaces:
  - articles  : financial news with sentiment
  - prices    : OHLCV price bar summaries
  - sentiment : aggregated sentiment analytics
"""

import logging
import os
import sys
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    PINECONE_API_KEY, PINECONE_INDEX,
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL, EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Simple Document class (replaces LangChain Document)
# ---------------------------------------------------------------------------

@dataclass
class Document:
    page_content: str
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Lazy singletons
# ---------------------------------------------------------------------------
_openai_client = None
_pinecone_index = None


def _get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "your_openrouter_api_key_here":
        raise ValueError(
            "OPENROUTER_API_KEY is not configured. "
            "Open your .env file and set OPENROUTER_API_KEY to your OpenRouter API key "
            "(get one at https://openrouter.ai/)."
        )
    import openai
    _openai_client = openai.OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
    )
    return _openai_client


def _get_pinecone_index():
    global _pinecone_index
    if _pinecone_index is not None:
        return _pinecone_index
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    _pinecone_index = pc.Index(PINECONE_INDEX)
    logger.info("Pinecone index '%s' connected for retrieval.", PINECONE_INDEX)
    return _pinecone_index


def _embed_query(query: str) -> list[float]:
    """Embed a single query string via OpenRouter."""
    client = _get_openai_client()
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[query.replace("\n", " ").strip()],
    )
    return response.data[0].embedding


# ---------------------------------------------------------------------------
# Core retrieve function
# ---------------------------------------------------------------------------

def retrieve(
    query: str,
    namespace: str = "articles",
    top_k: int = 8,
    filter: dict = None,
) -> list[Document]:
    """
    Semantic search against a Pinecone namespace.

    Args:
        query:     Natural language query string.
        namespace: 'articles', 'prices', or 'sentiment'.
        top_k:     Number of results to return.
        filter:    Pinecone metadata filter dict.

    Returns:
        List of Document objects with page_content and metadata.
    """
    try:
        index = _get_pinecone_index()
        vector = _embed_query(query)
        kwargs = dict(vector=vector, top_k=top_k, namespace=namespace, include_metadata=True)
        if filter:
            kwargs["filter"] = filter
        result = index.query(**kwargs)
        docs = []
        for match in result.get("matches", []):
            meta = match.get("metadata", {})
            text = meta.get("text", "")
            docs.append(Document(page_content=text, metadata=meta))
        logger.debug("Retrieved %d docs from namespace '%s'", len(docs), namespace)
        return docs
    except Exception as exc:
        logger.error("Retrieval failed (namespace=%s): %s", namespace, exc)
        return []


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def retrieve_for_ticker(ticker: str, top_k: int = 8) -> list[Document]:
    """Retrieve articles and prices mentioning a specific ticker."""
    ticker = ticker.upper()
    article_docs = retrieve(
        query=f"{ticker} stock market news sentiment analysis",
        namespace="articles",
        top_k=top_k,
    )
    price_docs = retrieve(
        query=f"{ticker} price OHLCV close volume change",
        namespace="prices",
        top_k=5,
        filter={"ticker": ticker},
    )
    if not price_docs:
        price_docs = retrieve(
            query=f"{ticker} open high low close",
            namespace="prices",
            top_k=5,
        )
    return article_docs + price_docs


def retrieve_recent_news(top_k: int = 10) -> list[Document]:
    """Retrieve the most semantically relevant recent news articles."""
    return retrieve(
        query="latest financial news market sentiment stock crypto",
        namespace="articles",
        top_k=top_k,
    )


def retrieve_risk_signals(top_k: int = 8) -> list[Document]:
    """Retrieve negative-sentiment articles and high-risk signals."""
    negative_docs = retrieve(
        query="market risk negative sentiment crash volatility warning bearish",
        namespace="articles",
        top_k=top_k,
    )
    sentiment_docs = retrieve(
        query="high negative sentiment risk ticker score",
        namespace="sentiment",
        top_k=5,
    )
    return negative_docs + sentiment_docs


def retrieve_price_context(ticker: str) -> list[Document]:
    """Retrieve price bar context for a specific ticker."""
    return retrieve(
        query=f"{ticker} price OHLCV close volume volatility change",
        namespace="prices",
        top_k=10,
        filter={"ticker": ticker.upper()},
    )


def retrieve_market_overview(top_k: int = 10) -> list[Document]:
    """Retrieve broad market overview: news + sentiment aggregates."""
    news = retrieve(
        query="market overview trends sectors stocks crypto performance",
        namespace="articles",
        top_k=top_k,
    )
    sentiment = retrieve(
        query="overall market sentiment positive negative distribution",
        namespace="sentiment",
        top_k=5,
    )
    return news + sentiment


def docs_to_context_string(docs: list[Document]) -> str:
    """Format retrieved Documents into a context string for LLM prompts."""
    if not docs:
        return "No relevant context available."
    parts = []
    for i, doc in enumerate(docs, 1):
        meta = doc.metadata if hasattr(doc, "metadata") else {}
        source = meta.get("source") or meta.get("ticker") or meta.get("agg_type", "")
        parts.append(f"[{i}] {doc.page_content}  (source: {source})")
    return "\n\n".join(parts)
