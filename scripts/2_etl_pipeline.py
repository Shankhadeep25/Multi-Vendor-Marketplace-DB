"""
=============================================================
Script 2 — ETL Pipeline  (Extract · Transform · Load to CSV)
=============================================================
Reads raw CSV files from the data/ folder, cleans and enriches
them, then writes *clean_*.csv* versions back to the same dir.

Cleaning steps:
  • Remove duplicate rows
  • Fill / drop null values
  • Normalize prices to 2 decimal places
  • Remove orders with negative amounts
  • Add calculated columns: order_month, order_year
=============================================================
"""

import os
import sys

import pandas as pd
import numpy as np

# --------------- paths ---------------
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")


def _path(name: str) -> str:
    """Return full path for a file inside DATA_DIR."""
    return os.path.join(DATA_DIR, name)


# =============================================
# Individual cleaners
# =============================================

def clean_vendors(df: pd.DataFrame) -> pd.DataFrame:
    """Clean vendors data."""
    before = len(df)
    df = df.drop_duplicates()
    df["email"] = df["email"].str.lower().str.strip()
    df["city"]  = df["city"].fillna("Unknown")
    df["rating"] = df["rating"].clip(0, 5).round(2)
    print(f"   vendors  : {before} → {len(df)} rows  (dropped {before - len(df)} duplicates)")
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean products data."""
    before = len(df)
    df = df.drop_duplicates()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0).round(2)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["category"] = df["category"].fillna("Uncategorized")
    print(f"   products : {before} → {len(df)} rows  (dropped {before - len(df)} duplicates)")
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customers data."""
    before = len(df)
    df = df.drop_duplicates()
    df["email"] = df["email"].str.lower().str.strip()
    df["city"]  = df["city"].fillna("Unknown")
    print(f"   customers: {before} → {len(df)} rows  (dropped {before - len(df)} duplicates)")
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean orders data — drop negatives, add month/year columns."""
    before = len(df)
    df = df.drop_duplicates()

    # Coerce and drop negatives
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    neg_count = (df["total_amount"] < 0).sum()
    df = df[df["total_amount"] >= 0].copy()
    df["total_amount"] = df["total_amount"].round(2)

    # Ensure order_date is datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Derived columns
    df["order_month"] = df["order_date"].dt.month
    df["order_year"]  = df["order_date"].dt.year

    after = len(df)
    print(f"   orders   : {before} → {after} rows  "
          f"(dropped {before - after} rows — {neg_count} negative amounts)")
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Clean payments data."""
    before = len(df)
    df = df.drop_duplicates()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).round(2)
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
    print(f"   payments : {before} → {len(df)} rows  (dropped {before - len(df)} duplicates)")
    return df


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — ETL Pipeline")
    print("=" * 60)

    try:
        # ---- Extract ----
        print("\n📥  Reading raw CSV files …")
        vendors   = pd.read_csv(_path("vendors.csv"))
        products  = pd.read_csv(_path("products.csv"))
        customers = pd.read_csv(_path("customers.csv"))
        orders    = pd.read_csv(_path("orders.csv"))
        payments  = pd.read_csv(_path("payments.csv"))

        # ---- Transform ----
        print("\n🔄  Cleaning data …")
        vendors   = clean_vendors(vendors)
        products  = clean_products(products)
        customers = clean_customers(customers)
        orders    = clean_orders(orders)
        payments  = clean_payments(payments)

        # ---- Load (back to CSV) ----
        print("\n💾  Saving cleaned CSV files …")
        vendors.to_csv(_path("clean_vendors.csv"),   index=False)
        products.to_csv(_path("clean_products.csv"), index=False)
        customers.to_csv(_path("clean_customers.csv"), index=False)
        orders.to_csv(_path("clean_orders.csv"),     index=False)
        payments.to_csv(_path("clean_payments.csv"), index=False)

        print("\n✅  ETL pipeline completed successfully!")
        print(f"📂  Cleaned files saved to: {os.path.abspath(DATA_DIR)}")
        print("=" * 60)

    except FileNotFoundError as e:
        print(f"❌  File not found — run 1_data_generator.py first.\n   {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌  ETL pipeline error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
