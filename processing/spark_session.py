"""
spark_session.py
----------------
SparkSession builder for the FinIntel pipeline.
Provides a singleton-style session with sensible defaults.

Two session modes:
  - get_spark_session()          : batch analytics (no Kafka JARs)
  - get_streaming_spark_session(): Structured Streaming from Kafka topics
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from config.settings import (
    SPARK_APP_NAME, SPARK_MASTER, SPARK_LOG_LEVEL,
    SPARK_KAFKA_PACKAGES,
)


def get_spark_session(app_name: str = None) -> SparkSession:
    """Create or return an existing SparkSession (batch mode)."""
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


def get_streaming_spark_session(app_name: str = None) -> SparkSession:
    """
    Create a SparkSession with Kafka connector JARs for Structured Streaming.
    Downloads spark-sql-kafka JAR automatically on first run via Maven.
    """
    spark = (
        SparkSession.builder
        .appName(app_name or f"{SPARK_APP_NAME}-Streaming")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


def stop_spark(spark: SparkSession):
    """Gracefully stop the SparkSession."""
    if spark:
        spark.stop()
