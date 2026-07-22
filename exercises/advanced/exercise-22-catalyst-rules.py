"""
Exercise 22: Watch Catalyst Optimize
Purpose: See built-in optimizer rules fire (constant folding, predicate pushdown,
column pruning) before deciding you need a custom rule.

Run:  python exercises/advanced/exercise-22-catalyst-rules.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Catalyst Rules")
txns = read_table(spark, "transactions")

print("=" * 60)
print("1. Constant folding & simplification (amount + 0, 1=1)")
print("=" * 60)
txns.createOrReplaceTempView("txns_v")
spark.sql("SELECT amount + 0 AS a, 1 = 1 AS always_true FROM txns_v").explain(True)

print("=" * 60)
print("2. Predicate pushdown + column pruning to the Parquet scan")
print("=" * 60)
(txns.select("customer_id", "amount", "status")
     .where(F.col("status") == "active")
     .explain())
print("  -> In the physical plan, note PushedFilters and the reduced ReadSchema.")

print("\n" + "=" * 60)
print("3. Build-vs-config: what would you use instead of a custom rule?")
print("=" * 60)
print("""
  'Join order is bad'          -> CBO + ANALYZE (Day 28)
  'Reduce/skew partitions'     -> AQE (Day 25)
  'Repeated join on a key'     -> bucketing (Day 26)
  'Star join reads whole fact' -> DPP (Day 27)
  Custom Catalyst rule         -> only for genuine framework-level rewrites.
""")
print("Analysis Questions")
print("1. Which rules already fired in (1) and (2)?")
print("2. For each 'I wish Spark did X', which config lever is cheapest?")

spark.stop()
