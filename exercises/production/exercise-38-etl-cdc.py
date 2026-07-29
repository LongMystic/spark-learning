"""
Exercise 38: Large-Scale ETL & CDC
Purpose: bronze -> silver with an incremental watermark, a data-quality gate,
and an idempotent upsert (MERGE on Iceberg, or dynamic-overwrite fallback).

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
For the MERGE path:  ENABLE_ICEBERG=1 spark-submit --packages ... exercise-38-etl-cdc.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("ETL & CDC")
bronze = read_table(spark, "transactions")   # treat as bronze (raw, append-only)

print("=" * 60)
print("1. Data-quality gate (fail fast BEFORE publishing)")
print("=" * 60)
issues = []
if bronze.where("customer_id IS NULL").count() > 0:
    issues.append("null customer_id")
if bronze.count() == 0:
    issues.append("empty batch")
dupes = bronze.groupBy("txn_id").count().where("count > 1").count()
if dupes:
    issues.append(f"{dupes} duplicate txn_id")
print("  DQ issues:", issues or "none ✅")
assert not issues, f"DQ failed: {issues}"

print("\n" + "=" * 60)
print("2. Incremental silver (watermark by txn_date) + idempotent write")
print("=" * 60)
base = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
silver = os.path.join(base, "_ex38_silver")

if os.path.exists(silver):
    last = spark.read.parquet(silver).selectExpr("max(txn_date) m").first().m
    new_rows = bronze.where(F.col("txn_date") > F.lit(last))
    print(f"  incremental: rows with txn_date > {last}: {new_rows.count()}")
else:
    new_rows = bronze
    print(f"  first load: {new_rows.count()} rows")

cleaned = (new_rows
    .where(F.col("status") != "cancelled")            # simple cleaning rule
    .select("txn_id", "customer_id", "category", "amount", "txn_date"))

# Idempotent publish (dynamic partition overwrite). Re-running is safe.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
cleaned.write.mode("overwrite").partitionBy("txn_date").parquet(silver)
print("  silver rows total:", spark.read.parquet(silver).count())
print("  (For true CDC upsert semantics, use Iceberg MERGE — see Day 34.)")

print("\nAnalysis Questions")
print("1. Why gate on data quality BEFORE writing silver?")
print("2. What makes this pipeline safe to re-run/backfill?")
print("3. When would you quarantine bad rows instead of failing the whole batch?")

spark.stop()
