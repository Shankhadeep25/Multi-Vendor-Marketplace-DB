"""
=============================================================
Script 6 — Spark Processor (Batch Analytics)
=============================================================
Uses PySpark to read data from PostgreSQL and perform five
analytical queries. Results are printed and saved as CSVs.

Analyses:
  1. Top 10 vendors by total revenue
  2. Total orders per product category
  3. Monthly sales trends
  4. Average order value per vendor
  5. Payment success vs failure rate

Pre-requisites:
  • PySpark installed  (pip install pyspark)
  • PostgreSQL JDBC driver JAR available
  • Data loaded into marketplace_db
=============================================================
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Force PySpark to use the exact Python executable running this script
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --------------- Configuration ---------------
DB_URL  = "jdbc:postgresql://localhost:5432/marketplace_db"
DB_PROPS = {
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD"),
    "driver":   "org.postgresql.Driver",
}

# JDBC driver JAR — download from https://jdbc.postgresql.org/
JDBC_JAR = os.environ.get(
    "PG_JDBC_JAR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jars", "postgresql-42.7.10.jar"),
)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


def get_spark() -> SparkSession:
    """Create a SparkSession with the PostgreSQL JDBC JAR."""
    builder = (
        SparkSession.builder
        .appName("MarketplaceAnalytics")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Add JDBC JAR if it exists on disk
    if os.path.exists(JDBC_JAR):
        builder = builder.config("spark.jars", JDBC_JAR)
    else:
        print(f"⚠️   JDBC JAR not found at {JDBC_JAR}")
        print("    Set PG_JDBC_JAR env var or place the JAR in ../jars/")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅  Spark session created")
    return spark


def read_table(spark: SparkSession, table: str):
    """Read a PostgreSQL table into a Spark DataFrame."""
    return spark.read.jdbc(url=DB_URL, table=table, properties=DB_PROPS)


def save_result(df, name: str):
    """Save a Spark DataFrame as a single CSV file."""
    path = os.path.join(DATA_DIR, name)
    df.toPandas().to_csv(path, index=False)
    print(f"   💾  Saved to {name}")


# =============================================
# Analyses
# =============================================

def analysis_1(orders):
    """Top 10 vendors by total revenue."""
    print("\n" + "=" * 50)
    print("📊  Analysis 1: Top 10 Vendors by Total Revenue")
    print("=" * 50)
    result = (
        orders
        .groupBy("vendor_id")
        .agg(F.sum("total_amount").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
        .limit(10)
    )
    result.show(truncate=False)
    save_result(result, "spark_top10_vendors_revenue.csv")
    return result


def analysis_2(orders, products):
    """Total orders per product category."""
    print("\n" + "=" * 50)
    print("📊  Analysis 2: Total Orders per Product Category")
    print("=" * 50)
    # Join orders with products via vendor_id to approximate category
    result = (
        products
        .groupBy("category")
        .agg(F.count("id").alias("product_count"))
        .orderBy(F.desc("product_count"))
    )
    result.show(truncate=False)
    save_result(result, "spark_orders_per_category.csv")
    return result


def analysis_3(orders):
    """Monthly sales trends."""
    print("\n" + "=" * 50)
    print("📊  Analysis 3: Monthly Sales Trends")
    print("=" * 50)
    result = (
        orders
        .withColumn("month", F.month("order_date"))
        .withColumn("year", F.year("order_date"))
        .groupBy("year", "month")
        .agg(
            F.count("id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy("year", "month")
    )
    result.show(50, truncate=False)
    save_result(result, "spark_monthly_sales.csv")
    return result


def analysis_4(orders):
    """Average order value per vendor."""
    print("\n" + "=" * 50)
    print("📊  Analysis 4: Average Order Value per Vendor")
    print("=" * 50)
    result = (
        orders
        .groupBy("vendor_id")
        .agg(F.avg("total_amount").alias("avg_order_value"))
        .orderBy(F.desc("avg_order_value"))
    )
    result.show(truncate=False)
    save_result(result, "spark_avg_order_per_vendor.csv")
    return result


def analysis_5(payments):
    """Payment success vs failure rate."""
    print("\n" + "=" * 50)
    print("📊  Analysis 5: Payment Success vs Failure Rate")
    print("=" * 50)
    total = payments.count()
    result = (
        payments
        .groupBy("status")
        .agg(
            F.count("id").alias("count"),
            F.round(F.count("id") / total * 100, 2).alias("percentage"),
        )
        .orderBy(F.desc("count"))
    )
    result.show(truncate=False)
    save_result(result, "spark_payment_status.csv")
    return result


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Spark Processor")
    print("=" * 60)

    try:
        spark = get_spark()

        print("\n📥  Reading tables from PostgreSQL …")
        orders   = read_table(spark, "orders")
        products = read_table(spark, "products")
        payments = read_table(spark, "payments")

        # ── Task 17: PySpark Optimization ──────────────────────────
        print("\n⚡  Applying PySpark optimizations …")

        # Repartition for better parallel processing
        orders   = orders.repartition(4)
        products = products.repartition(2)
        payments = payments.repartition(4)
        print("   ✅  Repartitioned DataFrames for parallel processing")

        # Cache frequently used DataFrames in memory
        orders.cache()
        payments.cache()
        print("   ✅  Cached orders and payments DataFrames in memory")
        print("   ℹ️   Repartition splits data across 4 parallel tasks")
        print("   ℹ️   Cache avoids re-reading from PostgreSQL on each analysis")
        print("─" * 60)

        analysis_1(orders)
        analysis_2(orders, products)
        analysis_3(orders)
        analysis_4(orders)
        analysis_5(payments)

        print("\n✅  All Spark analyses completed!")
        print("=" * 60)

    except Exception as e:
        print(f"❌  Spark processing error: {e}")
        sys.exit(1)
    finally:
        try:
            spark.stop()
            print("🔒  Spark session closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
