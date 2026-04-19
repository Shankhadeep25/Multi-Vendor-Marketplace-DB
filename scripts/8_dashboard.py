"""
=============================================================
Script 8 — Streamlit Dashboard (Live Analytics)
=============================================================
A rich, interactive Streamlit dashboard that reads live data
from PostgreSQL and visualises key marketplace metrics using
Plotly charts.

Run with:
    streamlit run scripts/8_dashboard.py

Visualisations:
  1. KPI cards  — Total Vendors, Products, Customers, Orders
  2. Top 10 vendors by revenue          (bar chart)
  3. Order volume over time             (line chart)
  4. Payment success vs failure         (pie chart)
  5. Product category breakdown         (bar chart)
  6. Recent orders table                (last 20 orders)

Sidebar:
  • Date range filter
  • Vendor filter
=============================================================
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# --------------- Page Config ---------------
st.set_page_config(
    page_title="Marketplace Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------- PostgreSQL config ---------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "marketplace_db",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD"),
}


# =============================================
# Database helpers
# =============================================

#@st.cache_resource
def get_connection():
    """Return a cached psycopg2 connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        st.error(f"Database connection failed: {e}")
        sys.exit(1)


# def run_query(query: str, params=None) -> pd.DataFrame:
#     """Execute a query and return a DataFrame."""
#     conn = get_connection()
#     return pd.read_sql(query, conn, params=params)

def run_query(query: str, params=None) -> pd.DataFrame:
    """Execute a query and return a DataFrame."""
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# =============================================
# Sidebar filters
# =============================================

st.sidebar.title("🔍 Filters")

# Date range
default_start = datetime.now() - timedelta(days=730)
default_end   = datetime.now()

date_range = st.sidebar.date_input(
    "Order Date Range",
    value=(default_start.date(), default_end.date()),
    key="date_range",
)
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start.date(), default_end.date()

# Vendor filter
vendors_df = run_query("SELECT id, name FROM vendors ORDER BY name")
vendor_options = ["All Vendors"] + vendors_df["name"].tolist()
selected_vendor = st.sidebar.selectbox("Vendor", vendor_options, key="vendor_filter")

# Build WHERE clause fragments
date_filter   = "o.order_date BETWEEN %s AND %s"
vendor_filter = ""
params: list  = [start_date, end_date]

if selected_vendor != "All Vendors":
    vendor_id = vendors_df.loc[vendors_df["name"] == selected_vendor, "id"].iloc[0]
    vendor_filter = " AND o.vendor_id = %s"
    params.append(int(vendor_id))


# =============================================
# KPI Cards
# =============================================

st.title("🛒 Multi-Vendor Marketplace Dashboard")
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)

total_vendors   = run_query("SELECT COUNT(*) AS cnt FROM vendors").iloc[0]["cnt"]
total_products  = run_query("SELECT COUNT(*) AS cnt FROM products").iloc[0]["cnt"]
total_customers = run_query("SELECT COUNT(*) AS cnt FROM customers").iloc[0]["cnt"]
total_orders    = run_query(
    f"SELECT COUNT(*) AS cnt FROM orders o WHERE {date_filter}{vendor_filter}",
    params,
).iloc[0]["cnt"]

col1.metric("🏪 Vendors",   f"{total_vendors:,}")
col2.metric("📦 Products",  f"{total_products:,}")
col3.metric("👥 Customers", f"{total_customers:,}")
col4.metric("🧾 Orders",    f"{total_orders:,}")

st.markdown("---")

# =============================================
# Charts — Row 1
# =============================================
chart_col1, chart_col2 = st.columns(2)

# --- Top 10 vendors by revenue ---
with chart_col1:
    st.subheader("💰 Top 10 Vendors by Revenue")
    revenue_query = f"""
        SELECT v.name AS vendor, SUM(o.total_amount) AS revenue
        FROM orders o
        JOIN vendors v ON o.vendor_id = v.id
        WHERE {date_filter}{vendor_filter}
        GROUP BY v.name
        ORDER BY revenue DESC
        LIMIT 10
    """
    rev_df = run_query(revenue_query, params)
    if not rev_df.empty:
        fig = px.bar(
            rev_df, x="revenue", y="vendor", orientation="h",
            color="revenue", color_continuous_scale="Teal",
            labels={"revenue": "Revenue (₹)", "vendor": "Vendor"},
        )
        fig.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")

# --- Order volume over time ---
with chart_col2:
    st.subheader("📈 Order Volume Over Time")
    volume_query = f"""
        SELECT DATE(o.order_date) AS order_day, COUNT(*) AS orders
        FROM orders o
        WHERE {date_filter}{vendor_filter}
        GROUP BY order_day
        ORDER BY order_day
    """
    vol_df = run_query(volume_query, params)
    if not vol_df.empty:
        fig = px.line(
            vol_df, x="order_day", y="orders",
            labels={"order_day": "Date", "orders": "Number of Orders"},
            color_discrete_sequence=["#00b4d8"],
        )
        fig.update_traces(line=dict(width=2))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")

# =============================================
# Charts — Row 2
# =============================================
chart_col3, chart_col4 = st.columns(2)

# --- Payment success vs failure ---
with chart_col3:
    st.subheader("💳 Payment Status Breakdown")
    payment_query = """
        SELECT status, COUNT(*) AS count
        FROM payments
        GROUP BY status
        ORDER BY count DESC
    """
    pay_df = run_query(payment_query)
    if not pay_df.empty:
        fig = px.pie(
            pay_df, names="status", values="count",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4,
        )
        fig.update_traces(textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No payment data available.")

# --- Product category breakdown ---
with chart_col4:
    st.subheader("📦 Products by Category")
    cat_query = """
        SELECT category, COUNT(*) AS count
        FROM products
        GROUP BY category
        ORDER BY count DESC
    """
    cat_df = run_query(cat_query)
    if not cat_df.empty:
        fig = px.bar(
            cat_df, x="category", y="count",
            color="count", color_continuous_scale="Sunset",
            labels={"category": "Category", "count": "Products"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product data available.")

# =============================================
# Recent Orders Table
# =============================================
st.markdown("---")
st.subheader("🕒 Recent 20 Orders")

recent_query = f"""
    SELECT o.id AS order_id, c.name AS customer, v.name AS vendor,
           o.order_date, o.status, o.total_amount
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN vendors v   ON o.vendor_id   = v.id
    WHERE {date_filter}{vendor_filter}
    ORDER BY o.order_date DESC
    LIMIT 20
"""
recent_df = run_query(recent_query, params)

if not recent_df.empty:
    st.dataframe(recent_df, use_container_width=True, hide_index=True)
else:
    st.info("No recent orders for the selected filters.")

# =============================================
# Footer
# =============================================
st.markdown("---")
st.caption("Multi-Vendor Marketplace Dashboard • Powered by Streamlit & Plotly")
