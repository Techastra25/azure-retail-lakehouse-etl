"""
bronze_ingest.py
-----------------
Bronze layer: raw files -> Delta tables, as-is, with minimal transformation.
Adds ingestion metadata for lineage/audit (required for any real Medallion setup).

In production this is the script ADF triggers via a Databricks Notebook Activity
after a Copy Activity lands files into ADLS Gen2 raw/ zone.

Run:
    spark-submit src/transformations/bronze_ingest.py
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

RAW_PATH = "data/raw"
BRONZE_PATH = "data/bronze"


def get_spark():
    return (
        SparkSession.builder.appName("BronzeIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ingest_transactions(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", IntegerType(), True),
        StructField("transaction_ts", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("customer_id", StringType(), True),
    ])

    df = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(f"{RAW_PATH}/transactions/*/*.csv")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.lit(datetime.utcnow().strftime("%Y-%m-%d")))
    )

    row_count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{BRONZE_PATH}/transactions")
    )
    print(f"[bronze] transactions ingested: {row_count} rows -> {BRONZE_PATH}/transactions")
    return row_count


def ingest_products(spark):
    df = (
        spark.read.option("multiline", "true").json(f"{RAW_PATH}/products/products.json")
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{BRONZE_PATH}/products"
    )
    print(f"[bronze] products ingested: {row_count} rows -> {BRONZE_PATH}/products")
    return row_count


def ingest_stores(spark):
    df = (
        spark.read.option("header", True).csv(f"{RAW_PATH}/stores/stores.csv")
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{BRONZE_PATH}/stores"
    )
    print(f"[bronze] stores ingested: {row_count} rows -> {BRONZE_PATH}/stores")
    return row_count


if __name__ == "__main__":
    spark = get_spark()
    t = ingest_transactions(spark)
    p = ingest_products(spark)
    s = ingest_stores(spark)
    print(f"[bronze] complete. transactions={t}, products={p}, stores={s}")
    spark.stop()
