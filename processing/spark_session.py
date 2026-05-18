"""
spark_session.py
----------------
SparkSession builder for the FinIntel pipeline.
Provides a singleton-style session with sensible defaults.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, SPARK_MASTER, SPARK_LOG_LEVEL


def get_spark_session(app_name: str = None) -> SparkSession:
    """Create or return an existing SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name or SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


def stop_spark(spark: SparkSession):
    """Gracefully stop the SparkSession."""
    if spark:
        spark.stop()
