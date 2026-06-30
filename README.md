# Azure Retail Lakehouse ETL Pipeline

End-to-end batch ETL pipeline implementing the **Medallion Architecture**
(Bronze → Silver → Gold) for retail transaction data, designed for Azure
(Data Factory + Databricks + ADLS Gen2 + Azure SQL) and fully runnable
locally via Docker + Spark + Delta Lake for development and testing.

## Why this exists

Retail transaction data arrives messy: duplicate POS records, missing
prices, orphaned store IDs, guest checkouts with no customer ID. This
pipeline takes that raw mess and produces clean, query-ready star-schema
tables that a BI team or analyst can trust — with every cleaning decision
auditable (quarantine table, not silent drops).

## Architecture

```
Raw Sources (POS files / REST API / on-prem SQL)
        │
        ▼  ADF Copy Activity
   ADLS Gen2 raw/
        │
        ▼  Databricks Notebook (bronze_ingest.py)
   BRONZE  — raw, schema-enforced, with ingestion metadata
        │
        ▼  Databricks Notebook (silver_transform.py)
   SILVER  — deduplicated, validated, conformed, quarantine table for rejects
        │
        ▼  Databricks Notebook (gold_aggregate.py)
   GOLD    — star-schema aggregates (daily sales, category, region, customer LTV)
        │
        ▼  ADF Lookup quality check + alert webhook
   Azure SQL Database (serving layer)  →  Power BI
```

Orchestration is defined as a real ADF pipeline JSON (`adf_pipelines/`),
showing dependency chaining, parameterization, and failure alerting —
not just the Spark logic in isolation.

## What the Silver layer actually does (the part interviewers probe hardest)

- **Deduplication** on `transaction_id`, keeping the latest ingestion via window function
- **Quarantine, not delete** — invalid rows (negative/zero quantity, null keys) are
  written to `data/silver/_quarantine` with a rejection reason, not silently dropped
- **Referential integrity** — transactions are inner-joined against the stores
  dimension to drop orphaned records
- **Missing value handling** — ~1% of rows have null `unit_price` by design
  (simulated real-world gap); backfilled from the product catalog instead of
  dropping the transaction
- **Derived metrics** — gross/net amount computed with discount logic

## Stack

| Layer | Local Dev | Production (Azure) |
|---|---|---|
| Compute | Spark (local mode, Docker) | Azure Databricks |
| Storage | Delta Lake on disk | ADLS Gen2 + Delta Lake |
| Orchestration | `run_pipeline.py` | Azure Data Factory |
| Serving | Postgres | Azure SQL Database |
| BI | — | Power BI |

## Running locally

```bash
docker compose -f docker/docker-compose.yml up -d
docker exec -it retail-spark bash

# Inside the container:
python src/ingestion/generate_sample_data.py --rows 500000
python src/run_pipeline.py
pytest tests/
```

This generates ~500K synthetic transaction rows (with deliberately injected
data quality issues — nulls, duplicates, invalid quantities) and runs them
through all three layers.

## Deploying to Azure

1. Create ADLS Gen2 storage account, upload `data/raw/` contents to a `raw/` container
2. Create an Azure Databricks workspace, import `src/transformations/*.py` as notebooks
3. Import `adf_pipelines/retail_lakehouse_pipeline.json` into Azure Data Factory
4. Create linked services for Databricks and Azure SQL, point the pipeline parameters at them
5. Run `sql/create_gold_tables.sql` against your Azure SQL Database
6. Trigger the ADF pipeline manually or on a schedule

## Gold layer schema (what the output looks like)

`gold.daily_store_sales` is one row per store per day with columns:
`transaction_date, store_id, transaction_count, units_sold, net_revenue, avg_order_value, unique_customers`

`gold.customer_summary` holds customer lifetime value (`lifetime_orders`,
`lifetime_value`, `first_purchase_date`, `last_purchase_date`) — useful for
churn/segmentation work downstream. Exact figures depend on the synthetic
data generated at run time; run `src/run_pipeline.py` to produce real numbers.

## Repo structure

```
azure-retail-lakehouse-etl/
├── src/
│   ├── ingestion/generate_sample_data.py   # synthetic data generator
│   ├── transformations/bronze_ingest.py
│   ├── transformations/silver_transform.py
│   ├── transformations/gold_aggregate.py
│   └── run_pipeline.py                     # local orchestrator
├── adf_pipelines/retail_lakehouse_pipeline.json
├── sql/create_gold_tables.sql
├── docker/                                 # local dev environment
├── tests/test_silver_transform.py
└── data/                                   # bronze/silver/gold output (gitignored)
```

## Notes

This is a portfolio/learning project built to practice production ETL
patterns (Medallion architecture, data quality quarantine, idempotent
writes, ADF orchestration design). Data is synthetic. The Spark/Delta/SQL
code and tests are designed to be run via Docker locally (see "Running
locally" above) before any Azure deployment; the ADF/Databricks deployment
files are production-pattern templates ready to deploy against a real
Azure subscription.
