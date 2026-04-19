"""
=============================================================
Script 1 — Synthetic Data Generator
=============================================================
Uses the Faker library to generate realistic marketplace data
and writes them as CSV files into the ../data/ folder.

Generated datasets:
  • vendors.csv   — 50 vendors
  • products.csv  — 200 products
  • customers.csv — 500 customers
  • orders.csv    — 10 000 orders
  • payments.csv  — 10 000 payments
=============================================================
"""

import os
import sys
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# --------------- configuration ---------------
NUM_VENDORS   = 50
NUM_PRODUCTS  = 200
NUM_CUSTOMERS = 500
NUM_ORDERS    = 10_000
NUM_PAYMENTS  = 10_000

# Seed for reproducibility
SEED = 42
random.seed(SEED)
Faker.seed(SEED)

fake = Faker("en_IN")  # Indian locale for realistic names/cities

# Output directory (../data relative to this script)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Product catalogue reference
CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Kitchen",
    "Beauty", "Sports", "Toys", "Grocery", "Automotive", "Health"
]

ORDER_STATUSES   = ["pending", "processing", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS  = ["credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"]
PAYMENT_STATUSES = ["pending", "completed", "failed", "refunded"]


# =============================================
# Generator functions
# =============================================

def generate_vendors(n: int) -> pd.DataFrame:
    """Generate *n* vendor records."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id":          i,
            "name":        fake.company(),
            "email":       fake.unique.company_email(),
            "city":        fake.city(),
            "rating":      round(random.uniform(1.0, 5.0), 2),
            "joined_date": fake.date_between(start_date="-3y", end_date="today"),
        })
    return pd.DataFrame(rows)


def generate_products(n: int, vendor_ids: list) -> pd.DataFrame:
    """Generate *n* product records linked to existing vendor IDs."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id":        i,
            "name":      fake.catch_phrase(),
            "category":  random.choice(CATEGORIES),
            "price":     round(random.uniform(50, 50_000), 2),
            "vendor_id": random.choice(vendor_ids),
            "stock":     random.randint(0, 500),
        })
    return pd.DataFrame(rows)


def generate_customers(n: int) -> pd.DataFrame:
    """Generate *n* customer records."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id":          i,
            "name":        fake.name(),
            "email":       fake.unique.email(),
            "city":        fake.city(),
            "joined_date": fake.date_between(start_date="-3y", end_date="today"),
        })
    return pd.DataFrame(rows)


def generate_orders(n: int, customer_ids: list, vendor_ids: list) -> pd.DataFrame:
    """Generate *n* order records."""
    rows = []
    for i in range(1, n + 1):
        order_date = fake.date_time_between(start_date="-2y", end_date="now")
        rows.append({
            "id":           i,
            "customer_id":  random.choice(customer_ids),
            "vendor_id":    random.choice(vendor_ids),
            "order_date":   order_date,
            "status":       random.choice(ORDER_STATUSES),
            "total_amount": round(random.uniform(100, 100_000), 2),
        })
    return pd.DataFrame(rows)


def generate_payments(n: int, order_ids: list) -> pd.DataFrame:
    """Generate *n* payment records linked to existing order IDs."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id":           i,
            "order_id":     random.choice(order_ids),
            "method":       random.choice(PAYMENT_METHODS),
            "status":       random.choice(PAYMENT_STATUSES),
            "amount":       round(random.uniform(100, 100_000), 2),
            "payment_date": fake.date_time_between(start_date="-2y", end_date="now"),
        })
    return pd.DataFrame(rows)


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Data Generator")
    print("=" * 60)

    try:
        # --- Vendors ---
        vendors_df = generate_vendors(NUM_VENDORS)
        vendors_df.to_csv(os.path.join(DATA_DIR, "vendors.csv"), index=False)
        print(f"✅  vendors.csv       → {len(vendors_df):>6,} rows")

        # --- Products ---
        products_df = generate_products(NUM_PRODUCTS, vendors_df["id"].tolist())
        products_df.to_csv(os.path.join(DATA_DIR, "products.csv"), index=False)
        print(f"✅  products.csv      → {len(products_df):>6,} rows")

        # --- Customers ---
        customers_df = generate_customers(NUM_CUSTOMERS)
        customers_df.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)
        print(f"✅  customers.csv     → {len(customers_df):>6,} rows")

        # --- Orders ---
        orders_df = generate_orders(NUM_ORDERS, customers_df["id"].tolist(), vendors_df["id"].tolist())
        orders_df.to_csv(os.path.join(DATA_DIR, "orders.csv"), index=False)
        print(f"✅  orders.csv        → {len(orders_df):>6,} rows")

        # --- Payments ---
        payments_df = generate_payments(NUM_PAYMENTS, orders_df["id"].tolist())
        payments_df.to_csv(os.path.join(DATA_DIR, "payments.csv"), index=False)
        print(f"✅  payments.csv      → {len(payments_df):>6,} rows")

        print("\n" + "-" * 60)
        print(f"📂  All files saved to: {os.path.abspath(DATA_DIR)}")
        print("=" * 60)

    except Exception as e:
        print(f"❌  Error during data generation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
