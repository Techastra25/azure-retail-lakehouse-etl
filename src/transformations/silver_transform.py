"""
silver_transform.py
--------------------
Silver layer: cleaned, validated, deduplicated, conformed data.
This is where real data-quality engineering happens:
  - Drops/quarantines invalid rows (negative qty, null keys)
  - Deduplicates on business key
  - Fills/derives missing values where business-safe
  - Enforces referential integrity against dimension tables
  - Writes rejected rows to a separate quarantine table for audit (never silently drop in prod)

Run:
    spark-submit src/transformations/silver_transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
QUARANTINE_PATH = "data/silver/_quarantine"


def get_spark():
    return (
        SparkSession.builder.appName("SilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def transform_transactions(spark):
    txn = spark.read.format("delta").load(f"{BRONZE_PATH}/transactions")
    products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
    stores = spark.read.format("delta").load(f"{BRONZE_PATH}/stores")

    raw_count = txn.count()

    # --- Deduplication on business key, keep latest ingestion ---
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_ts").desc())
    deduped = (
        txn.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # --- Quarantine invalid rows instead of silently dropping ---
    invalid_mask = (
        (F.col("quantity").isNull())
        | (F.col("quantity") <= 0)
        | (F.col("store_id").isNull())
        | (F.col("product_id").isNull())
    )

    quarantined = deduped.filter(invalid_mask).withColumn("rejection_reason", F.lit("invalid_quantity_or_null_key"))
    valid = deduped.filter(~invalid_mask)

    quarantined.write.format("delta").mode("append").option("mergeSchema", "true").save(QUARANTINE_PATH)

    # --- Fill missing unit_price from product catalog instead of dropping the row ---
    valid = valid.join(
        products.select(F.col("product_id"), F.col("unit_price").alias("catalog_price")),
        on="product_id",
        how="left",
    ).withColumn(
        "unit_price", F.coalesce(F.col("unit_price"), F.col("catalog_price"))
    ).drop("catalog_price")

    # --- Referential integrity: only keep transactions for known stores ---
    valid = valid.join(
        stores.select("store_id"), on="store_id", how="inner"
    )

    # --- Derived business columns ---
    valid = (
        valid.withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn(
            "net_amount",
            F.round(F.col("gross_amount") * (1 - F.col("discount_pct") / 100), 2),
        )
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit("GUEST")))
    )

    clean_count = valid.count()
    quarantine_count = quarantined.count()

    (
        valid.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("transaction_date")
        .save(f"{SILVER_PATH}/transactions")
    )

    print(
        f"[silver] transactions: raw={raw_count}, clean={clean_count}, "
        f"quarantined={quarantine_count}, dedup_removed={raw_count - clean_count - quarantine_count}"
    )
    return clean_count, quarantine_count


def transform_dimensions(spark):
    products = spark.read.format("delta").load(f"{BRONZE_PATH}/products").dropDuplicates(["product_id"])
    stores = spark.read.format("delta").load(f"{BRONZE_PATH}/stores").dropDuplicates(["store_id"])

    products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{SILVER_PATH}/products"
    )
    stores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{SILVER_PATH}/stores"
    )
    print(f"[silver] dimensions refreshed: products={products.count()}, stores={stores.count()}")


if __name__ == "__main__":
    spark = get_spark()
    transform_dimensions(spark)
    transform_transactions(spark)
    spark.stop()
