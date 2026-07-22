"""
Exercise 19: Serialization & UDF Issues
Purpose: Trigger a serialization/closure problem, fix it, and compare a Python UDF
to the equivalent built-in expression (plan + optimizer visibility).

Run:  python exercises/troubleshooting/exercise-19-serialization.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Serialization & UDFs")
txns = read_table(spark, "transactions")

print("=" * 60)
print("1. Capturing a large object in a closure — broadcast instead")
print("=" * 60)
# A big lookup dict. If captured directly in a UDF it is shipped with EVERY task.
big_lookup = {i: f"seg_{i % 3}" for i in range(100_000)}

# BAD pattern (works but ships the dict per task):
@F.udf("string")
def bad(cid):
    return big_lookup.get(cid, "unknown")

# GOOD pattern: broadcast once, reference .value
bc = spark.sparkContext.broadcast(big_lookup)

@F.udf("string")
def good(cid):
    return bc.value.get(cid, "unknown")

print("  Broadcasting the lookup avoids re-shipping it with every task.")
txns.select("customer_id").limit(3).withColumn("seg", good("customer_id")).show()

print("\n" + "=" * 60)
print("2. Python UDF vs built-in expression (compare the plans)")
print("=" * 60)

@F.udf("double")
def net_udf(amount):
    return amount * 0.9

print("--- Python UDF plan (note BatchEvalPython, opaque to Catalyst) ---")
txns.withColumn("net", net_udf("amount")).explain()

print("\n--- Built-in expression plan (pushdown + codegen) ---")
txns.withColumn("net", F.col("amount") * F.lit(0.9)).explain()

print("\n" + "=" * 60)
print("3. Vectorized alternative when you truly need Python: Pandas UDF")
print("=" * 60)
try:
    import pandas as pd  # noqa: F401
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("double")
    def net_pandas(s):
        return s * 0.9

    txns.withColumn("net", net_pandas("amount")).limit(3).show()
    print("  ArrowEvalPython node: whole-batch, Arrow transport -> far faster than row UDF.")
except Exception as e:  # noqa: BLE001
    print(f"  (Pandas UDF skipped: {type(e).__name__}: install pyarrow+pandas)")

print("\nAnalysis Questions")
print("1. What does 'Task not serializable' actually mean about your closure?")
print("2. Why can't Catalyst push a filter through a Python UDF?")
print("3. Rank: built-in vs Python UDF vs Pandas UDF, and why.")

spark.stop()
