"""
generate_sample_data.py
------------------------
Simulates raw retail data feeds that would normally arrive via ADF
(REST API pulls, on-prem SQL extracts, POS file drops) and lands them
in /data/raw as the entry point to the Bronze layer.

Run:
    python src/ingestion/generate_sample_data.py --rows 500000
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")

STORES = [f"STR{str(i).zfill(3)}" for i in range(1, 26)]
PRODUCTS = [
    {"product_id": f"P{str(i).zfill(4)}", "category": cat, "unit_price": round(random.uniform(5, 500), 2)}
    for i, cat in enumerate(
        random.choices(
            ["Electronics", "Apparel", "Grocery", "Home", "Sports", "Beauty"],
            k=2000,
        ),
        start=1,
    )
]


def generate_transactions(rows: int):
    start = datetime(2025, 1, 1)
    out_path = os.path.join(RAW_DIR, "transactions")
    os.makedirs(out_path, exist_ok=True)

    # Partition by day to mimic real POS file drops (one file per day per store)
    rows_per_day = max(rows // 365, 50)
    day_idx = 0
    written = 0
    f = None
    writer = None
    current_day = None

    for i in range(rows):
        if i % rows_per_day == 0:
            if f:
                f.close()
            current_day = (start + timedelta(days=day_idx)).strftime("%Y-%m-%d")
            day_idx += 1
            day_path = os.path.join(out_path, f"date={current_day}")
            os.makedirs(day_path, exist_ok=True)
            f = open(os.path.join(day_path, "transactions.csv"), "w", newline="")
            writer = csv.writer(f)
            writer.writerow(
                ["transaction_id", "store_id", "product_id", "quantity",
                 "unit_price", "discount_pct", "transaction_ts", "payment_method", "customer_id"]
            )

        product = random.choice(PRODUCTS)
        # Inject deliberate data quality issues so Silver-layer cleaning has real work to do
        quantity = random.choice([1, 2, 3, 5, -1, 0]) if random.random() < 0.02 else random.randint(1, 5)
        unit_price = product["unit_price"] if random.random() > 0.01 else None  # missing price ~1%
        customer_id = f"C{random.randint(1, 50000)}" if random.random() > 0.05 else None  # guest checkout

        writer.writerow([
            f"TXN{i:08d}",
            random.choice(STORES),
            product["product_id"],
            quantity,
            unit_price,
            random.choice([0, 0, 0, 5, 10, 15, 20]),
            f"{current_day}T{random.randint(8,21):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            random.choice(["CARD", "CASH", "UPI", "WALLET"]),
            customer_id,
        ])
        written += 1

    if f:
        f.close()
    print(f"Generated {written} transaction rows across {day_idx} daily files -> {out_path}")


def generate_products():
    out_path = os.path.join(RAW_DIR, "products")
    os.makedirs(out_path, exist_ok=True)
    with open(os.path.join(out_path, "products.json"), "w") as f:
        json.dump(PRODUCTS, f, indent=2)
    print(f"Generated {len(PRODUCTS)} product records -> {out_path}")


def generate_stores():
    out_path = os.path.join(RAW_DIR, "stores")
    os.makedirs(out_path, exist_ok=True)
    regions = ["North", "South", "East", "West"]
    with open(os.path.join(out_path, "stores.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["store_id", "store_name", "region", "opened_date"])
        for s in STORES:
            writer.writerow([s, f"Retail Store {s}", random.choice(regions), "2022-01-01"])
    print(f"Generated {len(STORES)} store records -> {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500000, help="Number of transaction rows to simulate")
    args = parser.parse_args()

    generate_products()
    generate_stores()
    generate_transactions(args.rows)
