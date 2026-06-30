-- create_gold_tables.sql
-- Serving layer schema (Azure SQL Database in production, Postgres locally).
-- These mirror the Delta gold tables and are what Power BI / analysts query directly.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_store_sales (
    transaction_date   DATE NOT NULL,
    store_id            VARCHAR(10) NOT NULL,
    transaction_count   INT,
    units_sold           INT,
    net_revenue          NUMERIC(14,2),
    avg_order_value      NUMERIC(10,2),
    unique_customers     INT,
    PRIMARY KEY (transaction_date, store_id)
);

CREATE TABLE IF NOT EXISTS gold.category_performance (
    transaction_date   DATE NOT NULL,
    category             VARCHAR(50) NOT NULL,
    net_revenue           NUMERIC(14,2),
    units_sold            INT,
    PRIMARY KEY (transaction_date, category)
);

CREATE TABLE IF NOT EXISTS gold.customer_summary (
    customer_id           VARCHAR(20) PRIMARY KEY,
    lifetime_orders        INT,
    lifetime_value           NUMERIC(14,2),
    first_purchase_date      DATE,
    last_purchase_date       DATE
);

CREATE TABLE IF NOT EXISTS gold.region_performance (
    transaction_date   DATE NOT NULL,
    region                VARCHAR(20) NOT NULL,
    net_revenue           NUMERIC(14,2),
    transaction_count      INT,
    PRIMARY KEY (transaction_date, region)
);

-- Example analytical query interviewers love to ask about:
-- "Top 5 categories by revenue in the last 30 days"
-- SELECT category, SUM(net_revenue) AS revenue
-- FROM gold.category_performance
-- WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
-- GROUP BY category
-- ORDER BY revenue DESC
-- LIMIT 5;
