"""
=============================================================
Script 4 — Kafka Producer (Real-Time Order Events)
=============================================================
Continuously generates fake live order events every 2 seconds
and publishes them to the Kafka topic 'marketplace-orders'.

Pre-requisites:
  • Apache Kafka broker running at localhost:9092
  • Topic 'marketplace-orders' created (or auto-create enabled)
=============================================================
"""

import sys
import uuid
import json
import time
import random
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

# --------------- Configuration ---------------
KAFKA_BROKER = "localhost:9092"
TOPIC        = "marketplace-orders"
INTERVAL_SEC = 2  # seconds between events

fake   = Faker("en_IN")
random.seed()

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        print(f"✅  Connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"❌  Kafka connection failed: {e}")
        sys.exit(1)


def generate_order_event() -> dict:
    """Generate a single fake order event."""
    return {
        "order_id":    random.randint(1_000_000, 9_999_999),
        "customer_id": random.randint(1, 500),
        "vendor_id":   random.randint(1, 50),
        "product_id":  random.randint(1, 200),
        "amount":      round(random.uniform(100, 50_000), 2),
        "timestamp":   datetime.now().isoformat(),
        "status":      random.choice(ORDER_STATUSES),
    }


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Kafka Producer")
    print("=" * 60)

    producer = create_producer()
    event_count = 0

    print(f"\n📡  Sending events to topic '{TOPIC}' every {INTERVAL_SEC}s …")
    print("    (Press Ctrl+C to stop)\n")

    try:
        while True:
            event = generate_order_event()
            producer.send(TOPIC, value=event)
            producer.flush()
            event_count += 1

            print(f"[{event_count:>5}]  📤  "
                  f"order_id={event['order_id']}  "
                  f"customer={event['customer_id']}  "
                  f"vendor={event['vendor_id']}  "
                  f"amount=₹{event['amount']:,.2f}  "
                  f"status={event['status']}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\n\n🛑  Stopped after sending {event_count} events.")
    except Exception as e:
        print(f"❌  Producer error: {e}")
    finally:
        producer.close()
        print("🔒  Kafka producer closed.")


if __name__ == "__main__":
    main()
