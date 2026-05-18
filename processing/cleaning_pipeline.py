"""
cleaning_pipeline.py
--------------------
Text-cleaning pipeline for financial news articles.

Steps:
  1. Strip HTML tags (BeautifulSoup)
  2. Remove URLs, special chars, excess whitespace
  3. Lowercase normalisation
  4. NLTK stopword removal
  5. Deduplication by article id
  6. Filter empty/irrelevant articles

Provides both PySpark (UDF) and pandas-based implementations.
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# Text cleaning functions
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Remove HTML tags using BeautifulSoup."""
    if not text:
        return ""
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(text, "html.parser").get_text(separator=" ")
    except Exception:
        return re.sub(r"<[^>]+>", " ", text)


def _remove_urls(text: str) -> str:
    """Strip URLs from text."""
    if not text:
        return ""
    return re.sub(r"https?://\S+|www\.\S+", "", text)


def _remove_special_chars(text: str) -> str:
    """Remove special characters, keep alphanumeric, basic punctuation."""
    if not text:
        return ""
    text = re.sub(r"[^\w\s.,!?;:'\"-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _remove_stopwords(text: str) -> str:
    """Remove English stopwords using NLTK."""
    if not text:
        return ""
    try:
        from nltk.corpus import stopwords
        stops = set(stopwords.words("english"))
        tokens = text.split()
        return " ".join(w for w in tokens if w.lower() not in stops)
    except Exception:
        return text


def _clean_text(text: str) -> str:
    """Full cleaning pipeline for a single text string."""
    text = _strip_html(text)
    text = _remove_urls(text)
    text = _remove_special_chars(text)
    return text.strip().lower()


# ---------------------------------------------------------------------------
# Pandas-based pipeline (primary — avoids PySpark UDF serialization issues)
# ---------------------------------------------------------------------------

def clean_articles_pandas(articles: list) -> list:
    """
    Apply full cleaning pipeline to a list of article dicts.

    Expected keys: id, source, title, summary, url, published, tickers
    Adds: clean_title, clean_summary, clean_summary_no_stop
    """
    seen_ids = set()
    cleaned = []

    for article in articles:
        # Deduplication
        aid = article.get("id", "")
        if aid in seen_ids:
            continue
        seen_ids.add(aid)

        # Clean title and summary
        clean_title = _clean_text(article.get("title", ""))
        clean_summary = _clean_text(article.get("summary", ""))

        # Filter empty
        if len(clean_title) <= 5 or len(clean_summary) <= 10:
            continue

        clean_summary_no_stop = _remove_stopwords(clean_summary)

        article["clean_title"] = clean_title
        article["clean_summary"] = clean_summary
        article["clean_summary_no_stop"] = clean_summary_no_stop
        cleaned.append(article)

    return cleaned


# ---------------------------------------------------------------------------
# PySpark UDF-based pipeline (alternative)
# ---------------------------------------------------------------------------

def clean_articles_spark(df):
    """
    Apply full cleaning pipeline via PySpark UDFs.
    Use clean_articles_pandas() if UDF serialization fails on your Python version.
    """
    from pyspark.sql.functions import udf, col, trim, lower, length
    from pyspark.sql.types import StringType

    strip_html_udf = udf(_strip_html, StringType())
    remove_urls_udf = udf(_remove_urls, StringType())
    remove_special_udf = udf(_remove_special_chars, StringType())
    remove_stopwords_udf = udf(_remove_stopwords, StringType())

    cleaned = df.withColumn("clean_title", strip_html_udf(col("title")))
    cleaned = cleaned.withColumn("clean_title", remove_urls_udf(col("clean_title")))
    cleaned = cleaned.withColumn("clean_title", remove_special_udf(col("clean_title")))
    cleaned = cleaned.withColumn("clean_title", trim(col("clean_title")))

    cleaned = cleaned.withColumn("clean_summary", strip_html_udf(col("summary")))
    cleaned = cleaned.withColumn("clean_summary", remove_urls_udf(col("clean_summary")))
    cleaned = cleaned.withColumn("clean_summary", remove_special_udf(col("clean_summary")))
    cleaned = cleaned.withColumn("clean_summary", trim(col("clean_summary")))

    cleaned = cleaned.withColumn("clean_title", lower(col("clean_title")))
    cleaned = cleaned.withColumn("clean_summary", lower(col("clean_summary")))

    cleaned = cleaned.withColumn("clean_summary_no_stop", remove_stopwords_udf(col("clean_summary")))

    cleaned = cleaned.dropDuplicates(["id"])
    cleaned = cleaned.filter(
        (length(col("clean_title")) > 5) &
        (length(col("clean_summary")) > 10)
    )

    return cleaned
