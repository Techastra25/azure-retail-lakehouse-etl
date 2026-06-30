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
