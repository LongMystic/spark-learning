"""
Exercise 16: OOM Debugging
Purpose: See why collect()/toPandas() risk DRIVER OOM, and how to inspect memory.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
On a laptop the small dataset won't actually OOM — the point is to reason about
WHERE the memory goes and to practice the safe patterns.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

spark = get_spark("OOM Debugging")
txns = read_table(spark, "transactions")

print("=" * 60)
print("1. DRIVER-side danger: pulling everything to the driver")
print("=" * 60)
n = txns.count()
print(f"  transactions rows: {n:,}")
print("  collect() would materialize ALL rows in the DRIVER JVM.")
print("  On a big table this is the #1 cause of driver OOM.")

# SAFE: bounded inspection
print("\n  Safe bounded peek (limit before toPandas):")
txns.select("customer_id", "amount", "status").limit(5).show()

print("\n" + "=" * 60)
print("2. EXECUTOR-side: keep work distributed, write instead of collect")
print("=" * 60)
out = os.path.join(
    os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data")),
    "_ex16_out",
)
agg = txns.groupBy("category").sum("amount")
agg.write.mode("overwrite").parquet(out)
print(f"  Wrote aggregation to {out} (no driver memory pressure).")

print("\n" + "=" * 60)
print("3. Where to look when it DOES OOM")
print("=" * 60)
print("""
  Driver OOM   -> driver-pod log; caused by collect/toPandas/large broadcast.
                  Fix: write out; lower autoBroadcastJoinThreshold; raise driver mem.
  Executor OOM -> executor-pod log (kubectl logs <pod>):
     'OutOfMemoryError: Java heap space'      -> heap; more partitions / fix skew.
     pod OOMKilled (exit 137, kubectl describe -> Reason: OOMKilled), heap not full
                                              -> OVERHEAD (off-heap); raise
                                                 spark.executor.memoryOverhead
                                                 (esp. PySpark / Pandas UDFs).
  UI: Executors tab -> Peak memory & GC time; Stages tab -> Spill (Memory/Disk).
""")

print("Analysis Questions")
print("1. Why does adding executors NOT help a driver OOM?")
print("2. If the pod is OOMKilled (exit 137) with the heap not full, which knob do you change?")
print("3. Why do PySpark jobs need more memoryOverhead than Scala jobs?")

spark.stop()
