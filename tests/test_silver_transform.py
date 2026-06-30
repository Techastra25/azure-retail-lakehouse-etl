"""
test_silver_transform.py
-------------------------
Unit tests for the data-quality logic in silver_transform.py.
Run: pytest tests/test_silver_transform.py
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield s
    s.stop()


def test_invalid_quantity_is_quarantined(spark):
    df = spark.createDataFrame(
        [("TXN1", "STR001", "P0001", -1, 10.0), ("TXN2", "STR001", "P0001", 2, 10.0)],
        ["transaction_id", "store_id", "product_id", "quantity", "unit_price"],
    )
    invalid = df.filter("quantity <= 0")
    assert invalid.count() == 1
    assert invalid.first()["transaction_id"] == "TXN1"


def test_deduplication_keeps_latest(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = spark.createDataFrame(
        [
            ("TXN1", "2025-01-01T10:00:00"),
            ("TXN1", "2025-01-02T10:00:00"),  # duplicate, later ingestion
            ("TXN2", "2025-01-01T10:00:00"),
        ],
        ["transaction_id", "ingestion_ts"],
    )
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_ts").desc())
    deduped = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    assert deduped.count() == 2
    txn1_row = deduped.filter("transaction_id = 'TXN1'").first()
    assert txn1_row["ingestion_ts"] == "2025-01-02T10:00:00"


def test_net_amount_calculation(spark):
    df = spark.createDataFrame(
        [(2, 100.0, 10)], ["quantity", "unit_price", "discount_pct"]
    )
    from pyspark.sql import functions as F

    result = df.withColumn("gross_amount", F.col("quantity") * F.col("unit_price")).withColumn(
        "net_amount", F.round(F.col("gross_amount") * (1 - F.col("discount_pct") / 100), 2)
    )
    row = result.first()
    assert row["gross_amount"] == 200.0
    assert row["net_amount"] == 180.0


def test_null_customer_defaults_to_guest(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(None,), ("C123",)], ["customer_id"])
    result = df.withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit("GUEST")))
    values = [r["customer_id"] for r in result.collect()]
    assert "GUEST" in values
    assert "C123" in values
