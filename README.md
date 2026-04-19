# 🛒 Multi-Vendor Marketplace — End-to-End Data Engineering Pipeline

A full-stack data engineering project that simulates a multi-vendor e-commerce marketplace. The pipeline covers the entire data lifecycle — from synthetic data generation and ETL processing to real-time streaming with Kafka, batch analytics with PySpark, automated data quality checks, and a live Streamlit dashboard.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Database Schema](#-database-schema)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Pipeline Scripts](#-pipeline-scripts)
- [How to Run](#-how-to-run)
- [Dashboard](#-dashboard)
- [Key Features](#-key-features)

---

## 🏗 Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│   Faker      │────▶│  Raw CSVs    │────▶│  ETL Pipeline    │
│  (Data Gen)  │     │  (5 files)   │     │  (Clean + Enrich)│
└──────────────┘     └──────────────┘     └────────┬─────────┘
                                                   │
                                                   ▼
                                          ┌──────────────────┐
                                          │   PostgreSQL     │
                                          │  (marketplace_db)│
                                          └──┬──────────┬────┘
                                             │          │
                          ┌──────────────────┘          └───────────────────┐
                          ▼                                                 ▼
                 ┌──────────────────┐                             ┌──────────────────┐
                 │  PySpark Batch   │                             │  Data Quality    │
                 │  Analytics       │                             │  Checks          │
                 └──────────────────┘                             └──────────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  CSV Reports     │
                 └──────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│ Kafka        │────▶│ Kafka        │────▶│  PostgreSQL      │
│ Producer     │     │ Consumer     │     │  (live inserts)  │
│ (live orders)│     │              │     └──────────────────┘
└──────────────┘     └──────────────┘
        │
        └────────────────────────────────▶┌──────────────────┐
                                          │ Spark Structured │
                                          │ Streaming        │
                                          └──────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Streamlit Dashboard — Live KPIs, Charts & Tables           │
└──────────────────────────────────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Layer              | Technology                        |
| ------------------ | --------------------------------- |
| Language           | Python 3.10+                      |
| Data Generation    | Faker                             |
| Data Processing    | Pandas, NumPy                     |
| Database           | PostgreSQL 15+                    |
| Streaming          | Apache Kafka + kafka-python       |
| Batch Analytics    | Apache Spark (PySpark)            |
| Real-Time Analytics| Spark Structured Streaming        |
| Dashboard          | Streamlit + Plotly                 |
| Environment        | python-dotenv                     |
| OS Automation      | Bash (Linux setup script)         |

---

## 📁 Project Structure

```
multi-vendor-marketplace/
│
├── data/                          # Generated & processed data
│   ├── raw/                       # Raw CSV files (post linux-setup)
│   ├── clean/                     # Cleaned CSV files (post linux-setup)
│   ├── spark_output/              # Spark analysis results (post linux-setup)
│   ├── vendors.csv                # Raw vendor data
│   ├── products.csv               # Raw product data
│   ├── customers.csv              # Raw customer data
│   ├── orders.csv                 # Raw order data
│   ├── payments.csv               # Raw payment data
│   ├── clean_vendors.csv          # Cleaned vendor data
│   ├── clean_products.csv         # Cleaned product data
│   ├── clean_customers.csv        # Cleaned customer data
│   ├── clean_orders.csv           # Cleaned order data
│   ├── clean_payments.csv         # Cleaned payment data
│   ├── spark_top10_vendors_revenue.csv
│   ├── spark_orders_per_category.csv
│   ├── spark_monthly_sales.csv
│   ├── spark_avg_order_per_vendor.csv
│   └── spark_payment_status.csv
│
├── scripts/                       # Pipeline scripts (run in order)
│   ├── 1_data_generator.py        # Synthetic data generation
│   ├── 1b_linux_setup.sh          # Linux directory setup & file mgmt
│   ├── 2_etl_pipeline.py          # Extract, Transform, Load to CSV
│   ├── 3_db_loader.py             # Bulk load CSVs into PostgreSQL
│   ├── 4_kafka_producer.py        # Real-time order event publisher
│   ├── 5_kafka_consumer.py        # Kafka consumer → PostgreSQL ingestion
│   ├── 6_spark_processor.py       # PySpark batch analytics (5 analyses)
│   ├── 7_data_quality.py          # Automated data quality checks
│   ├── 8_dashboard.py             # Streamlit live analytics dashboard
│   └── 9_spark_streaming.py       # Spark Structured Streaming from Kafka
│
├── sql/
│   └── schema.sql                 # PostgreSQL DDL (tables + indexes)
│
├── jars/
│   └── postgresql-42.7.10.jar     # JDBC driver for PySpark ↔ PostgreSQL
│
├── logs/                          # Setup & pipeline logs
├── docs/                          # Documentation
├── .env                           # Environment variables (DB_PASSWORD)
├── .env.example                   # Example env file
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## 🗄 Database Schema

The PostgreSQL database `marketplace_db` consists of **7 tables**:

| Table                  | Description                                    |
| ---------------------- | ---------------------------------------------- |
| `vendors`              | 50 marketplace vendors with ratings            |
| `products`             | 200 products across 10 categories              |
| `customers`            | 500 customers with profile info                |
| `orders`               | 10,000 orders with status & amount             |
| `order_items`          | Bridge table linking orders ↔ products         |
| `payments`             | 10,000 payments with method & status           |
| `daily_sales_summary`  | Star-schema fact table for aggregated metrics  |

**Entity Relationships:**

```
vendors ──┬──< products
          └──< orders ──< order_items >── products
                  │
                  └──< payments
customers ──< orders
```

---

## ✅ Prerequisites

Ensure the following are installed and running:

1. **Python 3.10+** — [python.org](https://www.python.org/downloads/)
2. **PostgreSQL 15+** — [postgresql.org](https://www.postgresql.org/download/)
3. **Apache Kafka** — [kafka.apache.org](https://kafka.apache.org/downloads) (required for scripts 4, 5, 9)
4. **Apache Spark / PySpark** — installed via pip (required for scripts 6, 9)
5. **Java 8 or 11** — required by Spark and Kafka

---

## 🚀 Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/multi-vendor-marketplace.git
cd multi-vendor-marketplace
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and set your PostgreSQL password:

```
DB_PASSWORD=your_postgres_password
```

### 5. Set up the database

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE marketplace_db;"

# Apply the schema
psql -U postgres -d marketplace_db -f sql/schema.sql
```

### 6. Start Kafka (for streaming scripts)

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Create the topic
bin/kafka-topics.sh --create --topic marketplace-orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 7. Linux directory setup (optional)

```bash
chmod +x scripts/1b_linux_setup.sh
./scripts/1b_linux_setup.sh
```

This organizes generated CSVs into `data/raw/`, `data/clean/`, and `data/spark_output/` subdirectories.

---

## 📜 Pipeline Scripts

Run the scripts **in order**. Each builds on the output of the previous one.

### Script 1 — `1_data_generator.py` · Synthetic Data Generation

Generates 5 realistic CSV datasets using the Faker library (Indian locale):

| Dataset          | Rows    |
| ---------------- | ------- |
| `vendors.csv`    | 50      |
| `products.csv`   | 200     |
| `customers.csv`  | 500     |
| `orders.csv`     | 10,000  |
| `payments.csv`   | 10,000  |

```bash
python scripts/1_data_generator.py
```

---

### Script 1b — `1b_linux_setup.sh` · Linux File System Setup

Demonstrates Linux file management: creates directory structure, sets permissions (`chmod 755`), and organizes files into `raw/`, `clean/`, and `spark_output/` directories.

```bash
bash scripts/1b_linux_setup.sh
```

---

### Script 2 — `2_etl_pipeline.py` · ETL Pipeline

Reads raw CSVs, applies cleaning transformations, and outputs `clean_*.csv` files:

- Remove duplicates
- Fill / drop null values
- Normalize prices to 2 decimal places
- Remove negative order amounts
- Add derived columns (`order_month`, `order_year`)

```bash
python scripts/2_etl_pipeline.py
```

---

### Script 3 — `3_db_loader.py` · PostgreSQL Bulk Loader

Loads cleaned CSVs into PostgreSQL tables using batch inserts with `ON CONFLICT DO NOTHING` for idempotent reruns.

```bash
python scripts/3_db_loader.py
```

---

### Script 4 — `4_kafka_producer.py` · Real-Time Event Producer

Generates and publishes fake live order events to the Kafka topic `marketplace-orders` every 2 seconds.

```bash
python scripts/4_kafka_producer.py
```

> Press `Ctrl+C` to stop.

---

### Script 5 — `5_kafka_consumer.py` · Kafka Consumer → PostgreSQL

Subscribes to the Kafka topic, reads each order event in real time, and inserts it into the PostgreSQL `orders` table.

```bash
python scripts/5_kafka_consumer.py
```

> Run this in a **separate terminal** alongside the producer.

---

### Script 6 — `6_spark_processor.py` · PySpark Batch Analytics

Reads data from PostgreSQL via JDBC and performs 5 analytical queries:

| #  | Analysis                              | Output CSV                          |
| -- | ------------------------------------- | ----------------------------------- |
| 1  | Top 10 vendors by total revenue       | `spark_top10_vendors_revenue.csv`   |
| 2  | Total orders per product category     | `spark_orders_per_category.csv`     |
| 3  | Monthly sales trends                  | `spark_monthly_sales.csv`           |
| 4  | Average order value per vendor        | `spark_avg_order_per_vendor.csv`    |
| 5  | Payment success vs failure rate       | `spark_payment_status.csv`          |

**Optimizations applied:** DataFrame repartitioning (4 partitions) and in-memory caching.

```bash
python scripts/6_spark_processor.py
```

---

### Script 7 — `7_data_quality.py` · Data Quality Checks

Runs 5 automated validation checks against the database:

| #  | Check                                          | Type    |
| -- | ---------------------------------------------- | ------- |
| 1  | No negative order amounts                      | PASS/FAIL |
| 2  | No orders referencing invalid vendors           | PASS/FAIL |
| 3  | No duplicate order IDs                          | PASS/FAIL |
| 4  | No orphan payments (without matching order)     | PASS/FAIL |
| 5  | Outlier detection (orders > 3σ from mean)       | WARNING   |

```bash
python scripts/7_data_quality.py
```

---

### Script 8 — `8_dashboard.py` · Streamlit Dashboard

Interactive, real-time analytics dashboard with:

- **KPI cards** — Total Vendors, Products, Customers, Orders
- **Top 10 vendors by revenue** — Horizontal bar chart
- **Order volume over time** — Line chart
- **Payment status breakdown** — Donut chart
- **Products by category** — Bar chart
- **Recent 20 orders** — Data table
- **Sidebar filters** — Date range & vendor selector

```bash
python -m streamlit run scripts/8_dashboard.py
```

> Opens at `http://localhost:8501`

---

### Script 9 — `9_spark_streaming.py` · Spark Structured Streaming

Reads live order events from Kafka using Spark Structured Streaming and displays rolling order counts per vendor/status every 5 seconds.

```bash
python scripts/9_spark_streaming.py
```

> Requires Kafka broker and producer (Script 4) running simultaneously.

---

## ▶️ How to Run

Execute the full pipeline in order:

```bash
# Step 1: Generate synthetic data
python scripts/1_data_generator.py

# Step 2: Clean & transform data
python scripts/2_etl_pipeline.py

# Step 3: Load into PostgreSQL
python scripts/3_db_loader.py

# Step 4 & 5: Real-time streaming (two terminals)
python scripts/4_kafka_producer.py      # Terminal 1
python scripts/5_kafka_consumer.py      # Terminal 2

# Step 6: Spark batch analytics
python scripts/6_spark_processor.py

# Step 7: Data quality validation
python scripts/7_data_quality.py

# Step 8: Launch dashboard
python -m streamlit run scripts/8_dashboard.py

# Step 9: Spark Structured Streaming (with producer running)
python scripts/9_spark_streaming.py
```

---

## 📊 Dashboard

The Streamlit dashboard provides a real-time view of marketplace metrics, powered by Plotly charts and live PostgreSQL queries.

| Feature                    | Visualization      |
| -------------------------- | ------------------- |
| KPI Metrics                | Metric cards        |
| Top 10 Vendors by Revenue  | Horizontal bar      |
| Order Volume Over Time     | Line chart          |
| Payment Status Breakdown   | Donut (pie) chart   |
| Products by Category       | Bar chart           |
| Recent Orders              | Interactive table   |

**Filters:** Date range picker and vendor dropdown in the sidebar.

---

## 🌟 Key Features

- **End-to-End Pipeline** — Data generation → ETL → Database → Analytics → Dashboard
- **Real-Time Streaming** — Kafka producer/consumer for live order ingestion
- **Spark Structured Streaming** — Live aggregation from Kafka topics
- **Batch Analytics** — 5 PySpark analytical queries with CSV output
- **PySpark Optimizations** — Repartitioning & in-memory caching for performance
- **Data Quality** — 5 automated validation checks with PASS/FAIL reporting
- **Interactive Dashboard** — Streamlit + Plotly with live filters
- **Idempotent Loads** — `ON CONFLICT DO NOTHING` for safe re-runs
- **Indian Locale Data** — Realistic Indian names, cities, and currency (₹)
- **Linux File Management** — Bash script for directory setup, permissions & file organization
- **Modular Design** — Each script is self-contained and independently runnable

---

## 📄 License

This project is for educational and demonstration purposes.

---

> Built with ❤️ as a Data Engineering portfolio project.
