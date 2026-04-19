"""
=============================================================
Script 7 — Data Quality Checks
=============================================================
Reads data from PostgreSQL and runs five validation checks:

  1. No negative order amounts
  2. No orders without a valid vendor
  3. No duplicate order IDs
  4. No payments without a matching order
  5. Outlier detection — flag orders > 3 standard deviations

Prints a clear PASS / FAIL report for each check.

Pre-requisites:
  • PostgreSQL (marketplace_db) with data loaded
=============================================================
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

import pandas as pd
import numpy as np
import psycopg2

# --------------- PostgreSQL config ---------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "marketplace_db",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD"),
}


def get_connection():
    """Create and return a psycopg2 connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅  Connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        print(f"❌  Database connection failed: {e}")
        sys.exit(1)


def read_table(conn, table: str) -> pd.DataFrame:
    """Read a full table into a Pandas DataFrame."""
    return pd.read_sql(f"SELECT * FROM {table}", conn)


# =============================================
# Quality checks
# =============================================

def check_negative_amounts(orders: pd.DataFrame) -> bool:
    """Check 1 — No negative order amounts."""
    negatives = orders[orders["total_amount"] < 0]
    count = len(negatives)
    if count == 0:
        print("   ✅  PASS  — No negative order amounts found")
        return True
    else:
        print(f"   ❌  FAIL  — {count} orders have negative amounts")
        return False


def check_orphan_vendor_orders(orders: pd.DataFrame, vendors: pd.DataFrame) -> bool:
    """Check 2 — No orders referencing a non-existent vendor."""
    valid_vendor_ids = set(vendors["id"])
    orphans = orders[~orders["vendor_id"].isin(valid_vendor_ids)]
    count = len(orphans)
    if count == 0:
        print("   ✅  PASS  — All orders have a valid vendor_id")
        return True
    else:
        print(f"   ❌  FAIL  — {count} orders reference invalid vendor_id")
        return False


def check_duplicate_order_ids(orders: pd.DataFrame) -> bool:
    """Check 3 — No duplicate order IDs."""
    dupes = orders[orders.duplicated(subset=["id"], keep=False)]
    count = len(dupes)
    if count == 0:
        print("   ✅  PASS  — No duplicate order IDs")
        return True
    else:
        print(f"   ❌  FAIL  — {count} duplicate order ID rows found")
        return False


def check_orphan_payments(payments: pd.DataFrame, orders: pd.DataFrame) -> bool:
    """Check 4 — No payments without a matching order."""
    valid_order_ids = set(orders["id"])
    orphans = payments[~payments["order_id"].isin(valid_order_ids)]
    count = len(orphans)
    if count == 0:
        print("   ✅  PASS  — All payments have a matching order_id")
        return True
    else:
        print(f"   ❌  FAIL  — {count} payments have no matching order")
        return False


def check_outliers(orders: pd.DataFrame) -> bool:
    """Check 5 — Flag orders above 3 standard deviations from mean."""
    mean   = orders["total_amount"].mean()
    std    = orders["total_amount"].std()
    upper  = mean + 3 * std

    outliers = orders[orders["total_amount"] > upper]
    count = len(outliers)

    if count == 0:
        print(f"   ✅  PASS  — No outliers (threshold: ₹{upper:,.2f})")
        return True
    else:
        print(f"   ⚠️   WARN  — {count} orders above 3σ "
              f"(threshold: ₹{upper:,.2f})")
        # Warning, not a hard fail
        return True


# =============================================
# Main execution
# =============================================

def main():
    print("=" * 60)
    print("  Multi-Vendor Marketplace — Data Quality Report")
    print("=" * 60)

    conn = get_connection()

    try:
        print("\n📥  Loading tables …")
        orders   = read_table(conn, "orders")
        vendors  = read_table(conn, "vendors")
        payments = read_table(conn, "payments")

        results = []
        print("\n🔍  Running quality checks …\n")

        print("  Check 1 — Negative order amounts")
        results.append(check_negative_amounts(orders))

        print("  Check 2 — Orders without valid vendor")
        results.append(check_orphan_vendor_orders(orders, vendors))

        print("  Check 3 — Duplicate order IDs")
        results.append(check_duplicate_order_ids(orders))

        print("  Check 4 — Payments without matching order")
        results.append(check_orphan_payments(payments, orders))

        print("  Check 5 — Outlier detection (3σ)")
        results.append(check_outliers(orders))

        # Summary
        passed = sum(results)
        total  = len(results)
        print("\n" + "=" * 60)
        print(f"  📋  Quality Report:  {passed}/{total} checks passed")
        if passed == total:
            print("  🎉  All quality checks passed!")
        else:
            print("  ⚠️   Some checks failed — review data before production use")
        print("=" * 60)

    except Exception as e:
        print(f"❌  Quality check error: {e}")
        sys.exit(1)
    finally:
        conn.close()
        print("🔒  Database connection closed.")


if __name__ == "__main__":
    main()
