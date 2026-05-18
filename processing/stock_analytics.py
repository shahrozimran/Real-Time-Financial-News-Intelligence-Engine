"""
stock_analytics.py
------------------
Price metrics computation using PySpark.

Provides:
  - moving averages (5-bar, 20-bar)
  - price change percentage
  - volatility (rolling standard deviation)
  - volume anomaly flags
  - latest price snapshot for dashboard
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, lag, lit, when, round as spark_round,
    max as spark_max, min as spark_min
)
from pyspark.sql.window import Window


def compute_price_analytics(df: DataFrame) -> DataFrame:
    """
    Compute analytics on price bar data.

    Expected columns: ticker, date, open, high, low, close, volume, change_pct
    """
    # Window for moving averages ordered by date per ticker
    w5 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-4, 0)
    w20 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-19, 0)
    w_prev = Window.partitionBy("ticker").orderBy("date")

    result = df

    # 5-bar and 20-bar moving averages
    result = result.withColumn("ma_5", spark_round(avg("close").over(w5), 2))
    result = result.withColumn("ma_20", spark_round(avg("close").over(w20), 2))

    # Volatility: rolling 5-bar standard deviation of close
    result = result.withColumn("volatility", spark_round(stddev("close").over(w5), 4))

    # Previous close for daily change
    result = result.withColumn("prev_close", lag("close", 1).over(w_prev))
    result = result.withColumn(
        "daily_change_pct",
        spark_round(
            when(col("prev_close").isNotNull(),
                 (col("close") - col("prev_close")) / col("prev_close") * 100
            ).otherwise(col("change_pct")),
            2
        )
    )

    # Volume anomaly: flag if volume > 2x the 5-bar average
    result = result.withColumn("avg_volume_5", avg("volume").over(w5))
    result = result.withColumn(
        "volume_anomaly",
        when(
            (col("volume") > 0) & (col("avg_volume_5") > 0) &
            (col("volume") > col("avg_volume_5") * 2),
            lit(True)
        ).otherwise(lit(False))
    )

    # Clean up intermediate columns
    result = result.drop("prev_close", "avg_volume_5")

    return result


def latest_price_snapshot(df: DataFrame) -> dict:
    """
    Extract the latest price bar per ticker for the dashboard.

    Returns: dict mapping ticker → {close, change_pct, volume, ma_5, ma_20, volatility}
    """
    # Get the most recent date per ticker
    from pyspark.sql.functions import row_number
    w_latest = Window.partitionBy("ticker").orderBy(col("date").desc())
    latest = df.withColumn("_rn", row_number().over(w_latest)).filter(col("_rn") == 1).drop("_rn")

    rows = latest.collect()
    snapshot = {}
    for row in rows:
        snapshot[row["ticker"]] = {
            "close": row["close"],
            "change_pct": row["daily_change_pct"] if row["daily_change_pct"] else row["change_pct"],
            "volume": row["volume"],
            "ma_5": row["ma_5"],
            "ma_20": row["ma_20"],
            "volatility": row["volatility"],
            "high": row["high"],
            "low": row["low"],
        }
    return snapshot
