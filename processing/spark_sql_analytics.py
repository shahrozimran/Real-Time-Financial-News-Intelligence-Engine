"""
spark_sql_analytics.py
----------------------
Spark SQL aggregation queries for sentiment analytics.

Provides:
  - sentiment_by_asset: avg sentiment per ticker
  - sentiment_by_source: count pos/neg/neutral per source
  - sentiment_by_time: daily trend (7 days)
  - overall_distribution: total pos/neg/neutral counts
  - top_movers: strongest sentiment articles
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, explode, count, avg, round as spark_round,
    sum as spark_sum, when, to_date, desc, lit
)


def sentiment_by_asset(df: DataFrame) -> DataFrame:
    """
    Average sentiment score per ticker.
    Expects columns: tickers (array), sentiment, sentiment_score
    """
    exploded = df.withColumn("ticker", explode(col("tickers")))
    result = (
        exploded.groupBy("ticker")
        .agg(
            spark_round(avg("sentiment_score"), 4).alias("avg_score"),
            count("*").alias("article_count"),
            spark_sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
            spark_sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
            spark_sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral"),
        )
        .orderBy(desc("article_count"))
    )
    return result


def sentiment_by_source(df: DataFrame) -> DataFrame:
    """
    Count positive/negative/neutral per news source.
    Expects columns: source, sentiment
    """
    result = (
        df.groupBy("source")
        .agg(
            count("*").alias("total"),
            spark_sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
            spark_sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
            spark_sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral"),
            spark_round(avg("sentiment_score"), 4).alias("avg_score"),
        )
        .orderBy(desc("total"))
    )
    return result


def sentiment_by_time(df: DataFrame) -> DataFrame:
    """
    Daily sentiment distribution for 7-day trend chart.
    Expects columns: published, sentiment
    """
    with_date = df.withColumn("date", to_date(col("published")))
    result = (
        with_date.groupBy("date")
        .agg(
            count("*").alias("total"),
            spark_sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
            spark_sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
            spark_sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral"),
        )
        .orderBy("date")
    )
    # Convert counts to percentages
    result = result.withColumn(
        "positive", spark_round(col("positive") / col("total") * 100, 0).cast("int")
    ).withColumn(
        "negative", spark_round(col("negative") / col("total") * 100, 0).cast("int")
    ).withColumn(
        "neutral", spark_round(col("neutral") / col("total") * 100, 0).cast("int")
    )
    return result


def overall_distribution(df: DataFrame) -> dict:
    """
    Total sentiment distribution across all articles.
    Returns dict: {positive: N, negative: N, neutral: N, total: N}
    """
    row = df.agg(
        count("*").alias("total"),
        spark_sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
        spark_sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
        spark_sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral"),
    ).collect()[0]

    return {
        "total": row["total"],
        "positive": row["positive"],
        "negative": row["negative"],
        "neutral": row["neutral"],
    }


def top_movers(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Top N articles with strongest sentiment scores.
    """
    return (
        df.filter(col("sentiment") != "neutral")
        .orderBy(desc("sentiment_score"))
        .limit(n)
    )
