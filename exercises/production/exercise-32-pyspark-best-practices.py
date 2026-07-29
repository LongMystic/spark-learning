"""
Exercise 32: PySpark Best Practices
Purpose: Refactor an RDD/UDF anti-pattern into DataFrame idioms; extract a pure,
testable transform.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("PySpark Best Practices")
txns = read_table(spark, "transactions")

print("=" * 60)
print("1. Anti-pattern: rdd.map + python logic (bypasses Catalyst/Tungsten)")
print("=" * 60)
rdd_way = (txns.rdd
    .filter(lambda r: r["status"] == "active")
    .map(lambda r: (r["category"], r["amount"] * 0.9)))
print("  rdd sample:", rdd_way.take(2))
print("  -> no codegen, no pushdown, python round-trips per row. AVOID.")

print("\n" + "=" * 60)
print("2. Idiomatic DataFrame version (pushdown + codegen)")
print("=" * 60)


def active_net_by_category(df):
    """Pure transform: no I/O -> easy to unit test with a tiny in-memory DataFrame."""
    return (df.where(F.col("status") == "active")
              .withColumn("net", F.col("amount") * F.lit(0.9))
              .groupBy("category").agg(F.sum("net").alias("net_total")))


result = active_net_by_category(txns)
result.explain()
result.show(5)

print("\n" + "=" * 60)
print("3. Testability: the same transform on a tiny in-memory DataFrame")
print("=" * 60)
tiny = spark.createDataFrame(
    [(1, "active", "a", 100.0), (2, "cancelled", "a", 50.0), (3, "active", "b", 200.0)],
    "customer_id long, status string, category string, amount double",
)
active_net_by_category(tiny).show()
print("  -> pure transforms make unit tests trivial (no cluster, no files).")

print("\nAnalysis Questions")
print("1. Why does the RDD version defeat the optimizer?")
print("2. What makes active_net_by_category() easy to unit test?")
print("3. How would you ship a matching Python env to executors on-prem?")

spark.stop()
