"""
sentiment_model.py
------------------
FinBERT sentiment analysis with VADER fallback.

Provides:
  - predict_sentiment(text) → (label, score)
  - predict_batch(texts)    → list of (label, score)

Labels: positive, negative, neutral
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import FINBERT_MODEL_NAME, SENTIMENT_MAX_LENGTH, SENTIMENT_BATCH_SIZE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy-loaded model singletons
# ---------------------------------------------------------------------------
_finbert_pipeline = None
_vader_analyzer = None


def _load_finbert():
    """Load FinBERT model and tokenizer (downloads ~400MB on first use)."""
    global _finbert_pipeline
    if _finbert_pipeline is not None:
        return _finbert_pipeline

    try:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
        logger.info("Loading FinBERT model: %s", FINBERT_MODEL_NAME)
        tokenizer = AutoTokenizer.from_pretrained(FINBERT_MODEL_NAME)
        model = AutoModelForSequenceClassification.from_pretrained(FINBERT_MODEL_NAME)
        _finbert_pipeline = pipeline(
            "sentiment-analysis",
            model=model,
            tokenizer=tokenizer,
            truncation=True,
            max_length=SENTIMENT_MAX_LENGTH,
        )
        logger.info("FinBERT loaded successfully")
        return _finbert_pipeline
    except Exception as e:
        logger.warning("Failed to load FinBERT: %s — falling back to VADER", e)
        return None


def _load_vader():
    """Load VADER sentiment analyzer."""
    global _vader_analyzer
    if _vader_analyzer is not None:
        return _vader_analyzer

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _vader_analyzer = SentimentIntensityAnalyzer()
        logger.info("VADER analyzer loaded")
        return _vader_analyzer
    except Exception as e:
        logger.error("Failed to load VADER: %s", e)
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def predict_sentiment(text: str) -> tuple:
    """
    Predict sentiment for a single text string.

    Returns:
        (label, score) where label ∈ {positive, negative, neutral}
        and score is a float between 0 and 1.
    """
    if not text or not text.strip():
        return ("neutral", 0.0)

    # Try FinBERT first
    pipe = _load_finbert()
    if pipe is not None:
        try:
            result = pipe(text[:SENTIMENT_MAX_LENGTH])[0]
            label = result["label"].lower()
            score = round(result["score"], 4)
            return (label, score)
        except Exception as e:
            logger.warning("FinBERT inference failed: %s — using VADER", e)

    # VADER fallback
    analyzer = _load_vader()
    if analyzer is not None:
        scores = analyzer.polarity_scores(text)
        compound = scores["compound"]
        if compound >= 0.05:
            return ("positive", round(compound, 4))
        elif compound <= -0.05:
            return ("negative", round(abs(compound), 4))
        else:
            return ("neutral", round(1.0 - abs(compound), 4))

    return ("neutral", 0.0)


def predict_batch(texts: list) -> list:
    """
    Predict sentiment for a list of text strings.

    Returns:
        List of (label, score) tuples.
    """
    if not texts:
        return []

    # Try FinBERT batch
    pipe = _load_finbert()
    if pipe is not None:
        try:
            truncated = [t[:SENTIMENT_MAX_LENGTH] if t else "" for t in texts]
            results = pipe(truncated, batch_size=SENTIMENT_BATCH_SIZE)
            return [(r["label"].lower(), round(r["score"], 4)) for r in results]
        except Exception as e:
            logger.warning("FinBERT batch failed: %s — using VADER", e)

    # VADER fallback (one at a time)
    return [predict_sentiment(t) for t in texts]
