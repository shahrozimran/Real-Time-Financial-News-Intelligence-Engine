"""
agents.py
---------
LangGraph Multi-Agent system for financial market intelligence.

Architecture: Sequential StateGraph
  retrieve_context
    → market_analyst_node
      → risk_manager_node
        → portfolio_advisor_node
          → summarizer_node → END

Each agent reads the previous agent's output, creating a collaborative
multi-perspective analysis before delivering a final combined summary.

LLM: OpenRouter GPT-4o-mini
RAG: Pinecone via rag_retriever.py
"""

import logging
import os
import re
import sys
from typing import TypedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL, LLM_MODEL,
    ALL_TICKERS, ASSETS,
)
from intelligence.rag_retriever import Document

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LangGraph State
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    question:         str
    context:          list          # list of LangChain Document objects
    sources:          list[dict]    # [{title, url, source, sentiment}]
    market_analysis:  str
    risk_assessment:  str
    portfolio_advice: str
    final_summary:    str


# ---------------------------------------------------------------------------
# Shared LLM (lazy singleton)
# ---------------------------------------------------------------------------
_llm = None


def _get_llm():
    global _llm
    if _llm is not None:
        return _llm
    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "your_openrouter_api_key_here":
        raise ValueError(
            "OPENROUTER_API_KEY is not configured. "
            "Open your .env file and set OPENROUTER_API_KEY to your OpenRouter API key "
            "(get one at https://openrouter.ai/)."
        )
    from langchain_openai import ChatOpenAI
    _llm = ChatOpenAI(
        model=LLM_MODEL,
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
        temperature=0.3,
        max_tokens=800,
    )
    return _llm


# ---------------------------------------------------------------------------
# Helper: extract ticker mentions from question
# ---------------------------------------------------------------------------

def _detect_tickers(question: str) -> list[str]:
    """Detect likely ticker symbols in the question using configured asset lists."""
    known = set()
    for ticker in ALL_TICKERS:
        known.add(ticker.upper())
        known.add(ticker.replace("-USD", "").replace("^", "").upper())
    known.update({"S&P", "NASDAQ", "SP500"})
    words = re.findall(r"\b[A-Z0-9^][A-Z0-9]{0,5}\b", question.upper())
    found = [w for w in words if w in known]
    return list(set(found))


# ---------------------------------------------------------------------------
# Node 1: retrieve_context
# ---------------------------------------------------------------------------

def retrieve_context(state: AgentState) -> AgentState:
    """Retrieve relevant documents from Pinecone for the question."""
    from intelligence.rag_retriever import (
        retrieve_for_ticker, retrieve_market_overview,
        retrieve_risk_signals, docs_to_context_string,
    )

    question = state["question"]
    tickers = _detect_tickers(question)
    q_lower = question.lower()

    all_docs = []

    if tickers:
        for ticker in tickers[:2]:  # max 2 tickers to control API calls
            all_docs += retrieve_for_ticker(ticker, top_k=6)
    elif any(w in q_lower for w in ["risk", "negative", "crash", "drop", "fall", "warn"]):
        all_docs = retrieve_risk_signals(top_k=10)
    else:
        all_docs = retrieve_market_overview(top_k=10)

    # Deduplicate by page_content
    seen = set()
    unique_docs = []
    for doc in all_docs:
        key = doc.page_content[:80]
        if key not in seen:
            seen.add(key)
            unique_docs.append(doc)

    # Build sources list for frontend
    sources = []
    for doc in unique_docs:
        m = doc.metadata if hasattr(doc, "metadata") else {}
        if m.get("title") or m.get("url"):
            sources.append({
                "title":     m.get("title", "")[:120],
                "url":       m.get("url", ""),
                "source":    m.get("source") or m.get("ticker", ""),
                "sentiment": m.get("sentiment", ""),
            })

    logger.info("Retrieved %d unique docs for question: %.60s...", len(unique_docs), question)
    return {**state, "context": unique_docs, "sources": sources}


# ---------------------------------------------------------------------------
# Node 2: Market Analyst
# ---------------------------------------------------------------------------

def market_analyst_node(state: AgentState) -> AgentState:
    """Analyze market conditions and explain price/news movements."""
    from intelligence.rag_retriever import docs_to_context_string
    context_str = docs_to_context_string(state["context"])
    messages = [
        {"role": "system",  "content": (
            "You are an expert Market Analyst at a top investment bank. "
            "Explain market movements, news impact, and trends in clear factual language. "
            "Base your analysis strictly on the provided context. Be concise (3-5 sentences). "
            "Do NOT make up prices or events not in the context.")},
        {"role": "user", "content": (
            f"User question: {state['question']}\n\n"
            f"Relevant financial context:\n{context_str}\n\n"
            "Provide your market analysis:")},
    ]
    llm = _get_llm()
    response = llm.invoke(messages)
    analysis = response.content if hasattr(response, "content") else str(response)
    logger.info("Market Analyst completed.")
    return {**state, "market_analysis": analysis}


# ---------------------------------------------------------------------------
# Node 3: Risk Manager
# ---------------------------------------------------------------------------

def risk_manager_node(state: AgentState) -> AgentState:
    """Identify risks, negative signals, and volatility concerns."""
    from intelligence.rag_retriever import docs_to_context_string
    context_str = docs_to_context_string(state["context"])
    messages = [
        {"role": "system", "content": (
            "You are a senior Risk Manager at a financial institution. "
            "Identify potential risks, red flags, negative sentiment signals, volatility concerns. "
            "Be specific and data-driven. Limit to 3-5 bullet points. "
            "Only flag genuine risks visible in the context.")},
        {"role": "user", "content": (
            f"User question: {state['question']}\n\n"
            f"Financial context:\n{context_str}\n\n"
            f"Market Analyst's view:\n{state['market_analysis']}\n\n"
            "Identify key risks:")},
    ]
    response = _get_llm().invoke(messages)
    assessment = response.content if hasattr(response, "content") else str(response)
    logger.info("Risk Manager completed.")
    return {**state, "risk_assessment": assessment}


# ---------------------------------------------------------------------------
# Node 4: Portfolio Advisor
# ---------------------------------------------------------------------------

def portfolio_advisor_node(state: AgentState) -> AgentState:
    """Provide educational portfolio considerations (NOT financial advice)."""
    from intelligence.rag_retriever import docs_to_context_string
    context_str = docs_to_context_string(state["context"])
    messages = [
        {"role": "system", "content": (
            "You are an educational Portfolio Advisor. "
            "Provide general portfolio considerations for educational purposes only. "
            "IMPORTANT: Always state this is NOT financial advice. "
            "Give 3-5 actionable educational points about diversification, position sizing, risk tolerance.")},
        {"role": "user", "content": (
            f"User question: {state['question']}\n\n"
            f"Financial context:\n{context_str}\n\n"
            f"Market Analysis:\n{state['market_analysis']}\n\n"
            f"Risk Assessment:\n{state['risk_assessment']}\n\n"
            "Provide educational portfolio considerations:")},
    ]
    response = _get_llm().invoke(messages)
    advice = response.content if hasattr(response, "content") else str(response)
    logger.info("Portfolio Advisor completed.")
    return {**state, "portfolio_advice": advice}


# ---------------------------------------------------------------------------
# Node 5: Summarizer
# ---------------------------------------------------------------------------

def summarizer_node(state: AgentState) -> AgentState:
    """Produce a concise combined summary of all three agent perspectives."""
    messages = [
        {"role": "system", "content": (
            "You are a financial intelligence summarizer. "
            "Synthesize the Market Analyst, Risk Manager, and Portfolio Advisor analyses "
            "into a single clear actionable summary paragraph (4-6 sentences). "
            "Highlight the most important insights and overall market outlook.")},
        {"role": "user", "content": (
            f"Question: {state['question']}\n\n"
            f"Market Analysis:\n{state['market_analysis']}\n\n"
            f"Risk Assessment:\n{state['risk_assessment']}\n\n"
            f"Portfolio Considerations:\n{state['portfolio_advice']}\n\n"
            "Write a concise unified summary:")},
    ]
    response = _get_llm().invoke(messages)
    summary = response.content if hasattr(response, "content") else str(response)
    logger.info("Summarizer completed.")
    return {**state, "final_summary": summary}


# ---------------------------------------------------------------------------
# Build and cache the LangGraph
# ---------------------------------------------------------------------------
_graph = None


def _build_graph():
    """Build and compile the LangGraph StateGraph (cached)."""
    global _graph
    if _graph is not None:
        return _graph

    from langgraph.graph import StateGraph, END

    builder = StateGraph(AgentState)

    builder.add_node("retrieve",          retrieve_context)
    builder.add_node("market_analyst",    market_analyst_node)
    builder.add_node("risk_manager",      risk_manager_node)
    builder.add_node("portfolio_advisor", portfolio_advisor_node)
    builder.add_node("summarizer",        summarizer_node)

    builder.set_entry_point("retrieve")
    builder.add_edge("retrieve",          "market_analyst")
    builder.add_edge("market_analyst",    "risk_manager")
    builder.add_edge("risk_manager",      "portfolio_advisor")
    builder.add_edge("portfolio_advisor", "summarizer")
    builder.add_edge("summarizer",        END)

    _graph = builder.compile()
    logger.info("LangGraph compiled successfully.")
    return _graph


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_FINANCIAL_KEYWORDS = {
    "stock","market","price","share","crypto","bitcoin","btc","eth","ethereum","nasdaq","s&p","sp500",
    "tesla","tsla","aapl","apple","msft","microsoft","nvda","nvidia","googl","google","amzn","amazon",
    "risk","sentiment","news","sector","invest","portfolio","trade","bull","bear","etf","index","fund",
    "earning","revenue","profit","loss","volatility","analyst","inflation","rate","fed","economy",
    "tech","finance","financial","dividend","bond","yield","commodity","oil","gold","silver",
    "forex","currency","exchange","dow","jones","russell","spy","qqq","vix","hedge","derivative",
    "option","futures","short","long","margin","ipo","merger","acquisition","quarterly","report",
}


def _is_financial(question: str) -> bool:
    """Return True if the question is about finance/markets."""
    words = set(re.sub(r"[^a-z0-9 ]", " ", question.lower()).split())
    return bool(words & _FINANCIAL_KEYWORDS)


def _run_direct_llm(question: str) -> dict:
    """
    Handle a non-financial query directly with GPT-4o-mini.
    No Pinecone retrieval. Returns immediately.
    """
    messages = [
        {"role": "system", "content": (
            "You are FinIntel, an AI Financial Intelligence Agent specialized exclusively in "
            "financial markets, stocks, crypto, economic news, sentiment analysis, risk assessment, "
            "and portfolio education.\n\n"
            "STRICT RULES:\n"
            "1. If the user sends a greeting (hi, hello, hey, good morning, etc.) — respond warmly, "
            "introduce yourself briefly, and list what you can help with (market analysis, stock sentiment, "
            "risk signals, portfolio guidance).\n"
            "2. If the user asks a general conversational question (how are you, what is your name, etc.) — "
            "answer briefly and naturally, then gently steer back to finance.\n"
            "3. If the user asks ANYTHING outside finance and markets — such as recipes, coding problems, "
            "science homework, sports scores, travel advice, jokes, creative writing, or any non-financial topic — "
            "do NOT answer the question. Instead, politely explain that you are a specialized financial AI and "
            "cannot help with that topic, then suggest a relevant financial question they could ask instead.\n\n"
            "Examples of what to DECLINE:\n"
            "- 'Give me a cake recipe' → Decline, suggest asking about commodity prices\n"
            "- 'Write a recursion function' → Decline, suggest asking about tech stock sentiment\n"
            "- 'Who won the football match?' → Decline, suggest asking about sports betting ETFs or market news\n"
            "- 'What is the capital of France?' → Decline, redirect to financial topics\n\n"
            "Keep responses concise and professional. Never be rude — always be polite and helpful within your scope."
        )},
        {"role": "user", "content": question},
    ]
    try:
        response = _get_llm().invoke(messages)
        answer = response.content if hasattr(response, "content") else str(response)
    except Exception as exc:
        answer = f"I couldn't process that request: {exc}"
    return {
        "mode":             "direct",
        "market_analyst":   "",
        "risk_manager":     "",
        "portfolio_advisor":"",
        "summary":          answer,
        "sources":          [],
    }


def run_multi_agent(question: str) -> dict:
    """
    Route the question:
      - Financial/market query  → full RAG + LangGraph pipeline (Pinecone retrieval)
      - General/non-financial   → direct GPT-4o-mini response (no RAG)

    Returns:
        {
            "mode":             "rag" | "direct",
            "market_analyst":   str,
            "risk_manager":     str,
            "portfolio_advisor": str,
            "summary":          str,
            "sources":          list[dict],
        }
    """
    if not _is_financial(question):
        logger.info("Non-financial query — routing to direct LLM: %.60s", question)
        return _run_direct_llm(question)

    logger.info("Financial query — routing to RAG pipeline: %.60s", question)
    try:
        graph = _build_graph()
        initial_state: AgentState = {
            "question":        question,
            "context":         [],
            "sources":         [],
            "market_analysis": "",
            "risk_assessment": "",
            "portfolio_advice": "",
            "final_summary":   "",
        }
        result = graph.invoke(initial_state)
        return {
            "mode":             "rag",
            "market_analyst":   result.get("market_analysis", ""),
            "risk_manager":     result.get("risk_assessment", ""),
            "portfolio_advisor": result.get("portfolio_advice", ""),
            "summary":          result.get("final_summary", ""),
            "sources":          result.get("sources", []),
        }
    except Exception as exc:
        logger.error("Multi-agent pipeline failed: %s", exc, exc_info=True)
        return {
            "mode":             "error",
            "market_analyst":   "",
            "risk_manager":     "",
            "portfolio_advisor": "",
            "summary":          f"Agent pipeline error: {exc}",
            "sources":          [],
            "error":            str(exc),
        }
