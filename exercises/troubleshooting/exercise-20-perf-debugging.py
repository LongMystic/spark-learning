"""
Exercise 20: Performance Debugging (Spark UI & SQL tab)
Purpose: Produce three classic bottlenecks and read them from plans/metrics.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
Then open the SQL tab (http://localhost:4040 while running, or the history
server afterwards) and inspect each query's per-operator metrics.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Performance Debugging")

print("=" * 60)
print("A) SKEW: group the skewed table by its hot key")
print("=" * 60)
skew = read_table(spark, "transactions_skewed")
skew.groupBy("customer_id").agg(F.count("*").alias("c")).orderBy(F.desc("c")).show(5)
print("  -> In the UI, the stage's MAX task time >> MEDIAN. That's skew.")

print("\n" + "=" * 60)
print("B) NO PRUNING vs PRUNING: read one date vs the whole table")
print("=" * 60)
txns = read_table(spark, "transactions")
one_day = txns.where(F.col("txn_date") == F.date_sub(F.current_date(), 1))
print("--- With partition filter (should show PartitionFilters / few files) ---")
one_day.explain()
print("  Compare 'number of files read' at the Scan node: pruned vs full scan.")

print("\n" + "=" * 60)
print("C) JOIN STRATEGY: force SMJ vs allow broadcast")
print("=" * 60)
products = read_table(spark, "products")   # tiny dimension
print("--- Broadcast disabled (SortMergeJoin, shuffles both sides) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
txns.join(products, "product_id").explain()
print("\n--- Broadcast enabled (BroadcastHashJoin, no shuffle of the fact table) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
txns.join(products, "product_id").explain()

print("\nAnalysis Questions")
print("1. In (A), what is the max/median task-duration ratio in the slow stage?")
print("2. In (B), how many files/bytes does each version read at the Scan node?")
print("3. In (C), which Exchange nodes disappear when broadcast is enabled?")

spark.stop()
