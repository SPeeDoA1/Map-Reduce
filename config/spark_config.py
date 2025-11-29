"""
Spark Configuration Settings for PowerGrid AI
"""
import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "PowerGridAI") -> SparkSession:
    """
    Create and configure a Spark session for local development.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_spark_context(spark: SparkSession):
    """Get the SparkContext from a SparkSession."""
    return spark.sparkContext
