"""
Exercise 18: Shuffle Error Resolution (FetchFailed)
Purpose: Understand partition sizing, the real driver of shuffle instability.
You usually can't reproduce a true FetchFailed on a laptop, so this exercise
focuses on the controllable root cause: per-partition shuffle size.

Run:  python exercises/troubleshooting/exercise-18-fetchfailed.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import sum as spark_sum

spark = get_spark("FetchFailed / Shuffle Sizing")
txns = read_table(spark, "transactions")

print("=" * 60)
print("Effect of shuffle.partitions on per-partition size")
print("=" * 60)
# Disable AQE so our chosen partition count actually sticks (AQE would coalesce it).
spark.conf.set("spark.sql.adaptive.enabled", "false")

for parts in [1, 8, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    agg = txns.groupBy("customer_id").agg(spark_sum("amount").alias("total"))
    n_out = agg.rdd.getNumPartitions()
    print(f"  shuffle.partitions={parts:<4} -> output partitions={n_out}")
    print("     Fewer partitions => bigger each => more map-side memory pressure")
    print("     => the OOM/GC that gets an executor killed => reducers see FetchFailed.")

print("\n" + "=" * 60)
print("Anatomy of a FetchFailed trace (read, don't run)")
print("=" * 60)
print("""
  FetchFailedException: Failed to connect to worker-3:7337
  MetadataFetchFailedException: Missing an output location for shuffle 4

  Root-cause checklist:
    1. Which MAP-side executor is gone? (Executors tab, around the failure time)
    2. Why did it die? OOM / YARN kill / long GC / disk full on spark.local.dir.
    3. Dynamic allocation on WITHOUT external shuffle service? -> enable
       spark.shuffle.service.enabled=true
    4. Right-size partitions (~100-200MB each) and/or enable AQE to coalesce.
""")
print("Analysis Questions")
print("1. Why is FetchFailed a *symptom* rather than a root cause?")
print("2. Why must dynamic allocation be paired with the external shuffle service?")
print("3. What per-partition shuffle size are you aiming for?")

spark.stop()
