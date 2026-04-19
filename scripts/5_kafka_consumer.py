"""
=============================================================
Script 5 — Kafka Consumer (Real-Time Order Ingestion)
=============================================================
Subscribes to the 'marketplace-orders' Kafka topic, reads each
order event, and inserts it into the PostgreSQL orders table.

Pre-requisites:
  • Kafka broker at localhost:9092
  • PostgreSQL (marketplace_db) with schema applied
  • Producer (4_kafka_producer.py) running
=============================================================
"""

import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

import psycopg2
from kafka import KafkaConsumer

# --------------- Configuration ---------------
KAFKA_BROKER = "localhost:9092"
TOPIC        = "marketplace-orders"
GROUP_ID     = "marketplace-consumer-group"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "marketplace_db",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD"),
}


def get_db_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        print("✅  Connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        print(f"❌  Database connection failed: {e}")
        sys.exit(1)


def create_consumer() -> KafkaConsumer:
    """Create and return a KafkaConsumer instance."""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print(f"✅  Subscribed to Kafka topic '{TOPIC}'")
        return consumer
    except Exception as e:
        print(f"❌  Kafka consumer error: {e}")
        sys.exit(1)


def insert_order(conn, event: dict):
    """Insert a single order event into the orders table."""
    insert_sql = """
        INSERT INTO orders (customer_id, vendor_id, order_date, status, total_amount)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """
    try:
        cur = conn.cursor()
        cur.execute(insert_sql, (
            event["customer_id"],
            event["vendor_id"],
            event["timestamp"],
            event["status"],
            event["amount"],
        ))
        new_id = cur.fetchone()[0]
        cur.close()
        return new_id
    except psycopg2.Error as e:
        print(f"⚠️   DB insert error: {e}")
        return None


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Kafka Consumer")
    print("=" * 60)

    conn     = get_db_connection()
    consumer = create_consumer()
    count    = 0

    print("\n📥  Listening for events …  (Press Ctrl+C to stop)\n")

    try:
        for message in consumer:
            event = message.value
            count += 1

            db_id = insert_order(conn, event)

            if db_id:
                print(f"[{count:>5}]  📥  "
                      f"kafka_order={event['order_id']}  →  db_id={db_id}  "
                      f"customer={event['customer_id']}  "
                      f"vendor={event['vendor_id']}  "
                      f"amount=₹{event['amount']:,.2f}  "
                      f"status={event['status']}")
            else:
                print(f"[{count:>5}]  ⚠️   Event received but DB insert failed")

    except KeyboardInterrupt:
        print(f"\n\n🛑  Stopped after consuming {count} events.")
    except Exception as e:
        print(f"❌  Consumer error: {e}")
    finally:
        consumer.close()
        conn.close()
        print("🔒  Kafka consumer & DB connection closed.")


if __name__ == "__main__":
    main()
