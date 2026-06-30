"""
gold_aggregate.py
------------------
Gold layer: business-level aggregates and a star schema, ready for
Power BI / SQL analytics consumption. This is what BI tools and
the Azure SQL Database serving layer actually query.

Run:
    spark-submit src/transformations/gold_aggregate.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"


def get_spark():
    return (
        SparkSession.builder.appName("GoldAggregate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_daily_store_sales(spark, txn):
    agg = (
        txn.groupBy("transaction_date", "store_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("net_amount").alias("net_revenue"),
            F.avg("net_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{GOLD_PATH}/daily_store_sales"
    )
    print(f"[gold] daily_store_sales: {agg.count()} rows")


def build_category_performance(spark, txn, products):
    joined = txn.join(products.select("product_id", "category"), on="product_id", how="left")
    agg = (
        joined.groupBy("transaction_date", "category")
        .agg(
            F.sum("net_amount").alias("net_revenue"),
            F.sum("quantity").alias("units_sold"),
        )
    )
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{GOLD_PATH}/category_performance"
    )
    print(f"[gold] category_performance: {agg.count()} rows")


def build_customer_summary(spark, txn):
    agg = (
        txn.filter(F.col("customer_id") != "GUEST")
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("lifetime_orders"),
            F.sum("net_amount").alias("lifetime_value"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.min("transaction_date").alias("first_purchase_date"),
        )
    )
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{GOLD_PATH}/customer_summary"
    )
    print(f"[gold] customer_summary: {agg.count()} rows")


def build_region_performance(spark, txn, stores):
    joined = txn.join(stores.select("store_id", "region"), on="store_id", how="left")
    agg = joined.groupBy("transaction_date", "region").agg(
        F.sum("net_amount").alias("net_revenue"),
        F.count("transaction_id").alias("transaction_count"),
    )
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{GOLD_PATH}/region_performance"
    )
    print(f"[gold] region_performance: {agg.count()} rows")


if __name__ == "__main__":
    spark = get_spark()
    txn = spark.read.format("delta").load(f"{SILVER_PATH}/transactions")
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
    stores = spark.read.format("delta").load(f"{SILVER_PATH}/stores")

    build_daily_store_sales(spark, txn)
    build_category_performance(spark, txn, products)
    build_customer_summary(spark, txn)
    build_region_performance(spark, txn, stores)

    print("[gold] all gold tables built successfully")
    spark.stop()
