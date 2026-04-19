"""
=============================================================
Script 9 — Spark Structured Streaming
=============================================================
Task 21 — Reads live order events from Kafka topic
'marketplace-orders' using Spark Structured Streaming
and displays rolling counts in real time.

Pre-requisites:
  • Kafka running on localhost:9092
  • Topic 'marketplace-orders' exists
  • Producer (4_kafka_producer.py) running
=============================================================
"""

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import *

# --------------- Configuration ---------------
KAFKA_BROKER = "localhost:9092"
TOPIC        = "marketplace-orders"

JDBC_JAR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "jars", "postgresql-42.7.10.jar"
)

# Kafka + Spark SQL streaming JAR
KAFKA_JAR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def get_spark() -> SparkSession:
    """Create SparkSession with Kafka support."""
    spark = (
        SparkSession.builder
        .appName("MarketplaceStructuredStreaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_JAR)
        .config("spark.jars", JDBC_JAR)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Spark Structured Streaming")
    print("=" * 60)
    print(f"\n📡  Connecting to Kafka topic '{TOPIC}' …")
    print("    (Press Ctrl+C to stop)\n")

    spark = get_spark()
    print("✅  Spark session created")

    # Define schema for incoming Kafka messages
    schema = StructType([
        StructField("order_id",    StringType(),  True),
        StructField("customer_id", IntegerType(), True),
        StructField("vendor_id",   IntegerType(), True),
        StructField("product_id",  IntegerType(), True),
        StructField("amount",      DoubleType(),  True),
        StructField("timestamp",   StringType(),  True),
        StructField("status",      StringType(),  True),
    ])

    # Read stream from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON messages
    orders_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    # Analysis 1 — Live order count per vendor
    vendor_counts = (
        orders_stream
        .groupBy("vendor_id", "status")
        .count()
        .orderBy("vendor_id")
    )

    # Write to console in real time
    query = (
        vendor_counts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("✅  Streaming started — showing live order counts every 5 seconds\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑  Streaming stopped.")
    finally:
        spark.stop()
        print("🔒  Spark session closed.")


if __name__ == "__main__":
    main()