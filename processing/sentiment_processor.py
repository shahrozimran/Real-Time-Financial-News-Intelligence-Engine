"""
sentiment_processor.py
----------------------
Apply FinBERT sentiment analysis to cleaned articles via Spark UDF.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType


# Sentiment result schema
SENTIMENT_SCHEMA = StructType([
    StructField("label", StringType(), False),
    StructField("score", FloatType(), False),
])


def _sentiment_udf_fn(text: str):
    """UDF wrapper around the sentiment model."""
    from intelligence.sentiment_model import predict_sentiment
    label, score = predict_sentiment(text or "")
    return (label, float(score))


sentiment_udf = udf(_sentiment_udf_fn, SENTIMENT_SCHEMA)


def apply_sentiment(df: DataFrame, text_col: str = "clean_title") -> DataFrame:
    """
    Add sentiment_label and sentiment_score columns to the DataFrame.

    Args:
        df: DataFrame with cleaned text columns
        text_col: Column to run sentiment on (default: clean_title)

    Returns:
        DataFrame with added sentiment_label, sentiment_score columns
    """
    result = df.withColumn("_sentiment", sentiment_udf(col(text_col)))
    result = result.withColumn("sentiment", col("_sentiment.label"))
    result = result.withColumn("sentiment_score", col("_sentiment.score"))
    result = result.drop("_sentiment")
    return result
