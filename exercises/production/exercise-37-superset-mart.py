"""
Exercise 37: Superset-friendly BI mart
Purpose: Build a small pre-aggregated mart and compare "dashboard query" cost
against querying the raw fact table.

Run:  python exercises/production/exercise-37-superset-mart.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Superset Mart")
txns = read_table(spark, "transactions")

base = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
mart = os.path.join(base, "_ex37_daily_category_mart")

print("=" * 60)
print("1. Build the mart: day x category x status (partitioned by txn_date)")
print("=" * 60)
(txns.groupBy("txn_date", "category", "status")
     .agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
     .write.mode("overwrite").partitionBy("txn_date").parquet(mart))
mart_rows = spark.read.parquet(mart).count()
fact_rows = txns.count()
print(f"  fact rows: {fact_rows:,}  ->  mart rows: {mart_rows:,}  "
      f"({fact_rows // max(mart_rows,1)}x smaller)")


def timed(label, df):
    t = time.time()
    df.collect()
    print(f"  {label}: {time.time() - t:.2f}s")


print("\n" + "=" * 60)
print("2. Same 'dashboard' query: raw fact vs mart")
print("=" * 60)
dash_raw = txns.groupBy("category").agg(F.sum("amount").alias("total"))
dash_mart = spark.read.parquet(mart).groupBy("category").agg(F.sum("total").alias("total"))
timed("from RAW fact", dash_raw)
timed("from MART    ", dash_mart)
print("  (Difference grows dramatically at --scale medium/large or on the cluster.)")

print("\nAnalysis Questions")
print("1. Why should Superset read the mart, not the raw fact?")
print("2. How do partitioning + compaction keep the mart fast?")
print("3. How would you align Superset cache TTL with the ETL schedule?")

spark.stop()
