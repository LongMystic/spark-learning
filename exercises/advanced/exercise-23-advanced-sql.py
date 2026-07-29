"""
Exercise 23: Advanced SQL & Window Functions
Purpose: Running totals, top-N per group, lag/lead, and one-pass rollup.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import Window
from pyspark.sql import functions as F

spark = get_spark("Advanced SQL")
txns = read_table(spark, "transactions")

print("=" * 60)
print("1. Reuse ONE window spec for several columns")
print("=" * 60)
w = Window.partitionBy("customer_id").orderBy("txn_ts")
enriched = (txns
    .withColumn("running_total", F.sum("amount").over(w))
    .withColumn("txn_seq", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("amount").over(w)))
enriched.select("customer_id", "txn_ts", "amount",
                "running_total", "txn_seq", "prev_amount").show(8, truncate=False)
print("  Check the plan: a SINGLE Window operator handles all three columns.")
enriched.explain()

print("\n" + "=" * 60)
print("2. Top-3 transactions per category")
print("=" * 60)
wc = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = txns.withColumn("rn", F.row_number().over(wc)).where("rn <= 3")
top3.select("category", "amount", "rn").orderBy("category", "rn").show(12)

print("\n" + "=" * 60)
print("3. Multi-level totals in ONE pass with rollup")
print("=" * 60)
(txns.rollup("category", "status")
     .agg(F.sum("amount").alias("total"))
     .orderBy("category", "status")
     .show(20))

print("Analysis Questions")
print("1. Confirm only ONE Window node exists for the 3 reused columns.")
print("2. Which column drives skew risk in section 1/2? (the partitionBy key)")
print("3. How many shuffles does the rollup use vs 3 separate groupBys?")

spark.stop()
