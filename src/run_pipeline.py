"""
run_pipeline.py
----------------
End-to-end orchestrator: Bronze -> Silver -> Gold.
In production, Azure Data Factory triggers each stage as a separate
Databricks Notebook Activity (see adf_pipelines/retail_lakehouse_pipeline.json).
This script lets you run and test the full chain locally before deploying.

Run:
    python src/run_pipeline.py
"""

import subprocess
import sys
import time


STAGES = [
    ("Bronze Ingestion", "src/transformations/bronze_ingest.py"),
    ("Silver Transformation", "src/transformations/silver_transform.py"),
    ("Gold Aggregation", "src/transformations/gold_aggregate.py"),
]


def run_stage(name, script):
    print(f"\n{'='*60}\nSTAGE: {name}\n{'='*60}")
    start = time.time()
    result = subprocess.run(["spark-submit", script])
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"[FAILED] {name} after {elapsed:.1f}s")
        sys.exit(result.returncode)
    print(f"[OK] {name} completed in {elapsed:.1f}s")


if __name__ == "__main__":
    pipeline_start = time.time()
    for name, script in STAGES:
        run_stage(name, script)
    total = time.time() - pipeline_start
    print(f"\nPipeline complete in {total:.1f}s. Gold tables ready in data/gold/")
