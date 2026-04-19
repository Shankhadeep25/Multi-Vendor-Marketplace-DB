-- ============================================================
-- Multi-Vendor Marketplace Database Schema
-- PostgreSQL DDL — run once to bootstrap marketplace_db
-- ============================================================

-- 1. Vendors
CREATE TABLE IF NOT EXISTS vendors (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200)  NOT NULL,
    email           VARCHAR(200)  UNIQUE NOT NULL,
    city            VARCHAR(100),
    rating          NUMERIC(3, 2) CHECK (rating >= 0 AND rating <= 5),
    joined_date     DATE          NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_vendors_city   ON vendors(city);
CREATE INDEX idx_vendors_rating ON vendors(rating);

-- 2. Products
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(300)  NOT NULL,
    category        VARCHAR(100)  NOT NULL,
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    vendor_id       INTEGER       NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
    stock           INTEGER       NOT NULL DEFAULT 0 CHECK (stock >= 0)
);

CREATE INDEX idx_products_category  ON products(category);
CREATE INDEX idx_products_vendor    ON products(vendor_id);

-- 3. Customers
CREATE TABLE IF NOT EXISTS customers (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200)  NOT NULL,
    email           VARCHAR(200)  UNIQUE NOT NULL,
    city            VARCHAR(100),
    joined_date     DATE          NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_customers_city ON customers(city);

-- 4. Orders
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER       NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    vendor_id       INTEGER       NOT NULL REFERENCES vendors(id)   ON DELETE CASCADE,
    order_date      TIMESTAMP     NOT NULL DEFAULT NOW(),
    status          VARCHAR(30)   NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','shipped','delivered','cancelled')),
    total_amount    NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    order_month     INTEGER,
    order_year      INTEGER
);

CREATE INDEX idx_orders_customer   ON orders(customer_id);
CREATE INDEX idx_orders_vendor     ON orders(vendor_id);
CREATE INDEX idx_orders_date       ON orders(order_date);
CREATE INDEX idx_orders_status     ON orders(status);

-- 5. Order Items (bridge between orders and products)
CREATE TABLE IF NOT EXISTS order_items (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id      INTEGER       NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity        INTEGER       NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price      NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    subtotal        NUMERIC(12, 2) NOT NULL CHECK (subtotal >= 0)
);

CREATE INDEX idx_order_items_order   ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- 6. Payments
CREATE TABLE IF NOT EXISTS payments (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    method          VARCHAR(50)   NOT NULL
                        CHECK (method IN ('credit_card','debit_card','upi','net_banking','wallet','cod')),
    status          VARCHAR(30)   NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','completed','failed','refunded')),
    amount          NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    payment_date    TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_order  ON payments(order_id);
CREATE INDEX idx_payments_status ON payments(status);
CREATE INDEX idx_payments_date   ON payments(payment_date);

-- 7. Daily Sales Summary — Star Schema fact table
CREATE TABLE IF NOT EXISTS daily_sales_summary (
    id              SERIAL PRIMARY KEY,
    summary_date    DATE          NOT NULL,
    vendor_id       INTEGER       REFERENCES vendors(id),
    total_orders    INTEGER       NOT NULL DEFAULT 0,
    total_revenue   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(10, 2),
    top_category    VARCHAR(100),
    UNIQUE(summary_date, vendor_id)
);

CREATE INDEX idx_dss_date   ON daily_sales_summary(summary_date);
CREATE INDEX idx_dss_vendor ON daily_sales_summary(vendor_id);

-- ============================================================
-- End of Schema
-- ============================================================
