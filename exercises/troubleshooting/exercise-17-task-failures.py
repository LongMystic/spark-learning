"""
Exercise 17: Task Failure & Retry Analysis
Purpose: See a DETERMINISTIC task failure (same task, same error every attempt)
vs. reasoning about transient failures and speculation.

Run:  python exercises/troubleshooting/exercise-17-task-failures.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, udf

# Lower maxFailures so a deterministic failure surfaces fast (default is 4).
spark = get_spark("Task Failures", extra_conf={"spark.task.maxFailures": "2"})
txns = read_table(spark, "transactions")

print("=" * 60)
print("Deterministic failure: a UDF that throws on a specific value")
print("=" * 60)

@udf("int")
def explode_on_zero_mod(x):
    # Fails on ~1/500 of rows -> the SAME task(s) fail every retry -> deterministic.
    if x % 500 == 0:
        raise ValueError(f"boom on {x}")
    return int(x)

try:
    txns.withColumn("v", explode_on_zero_mod(col("txn_id"))).count()
except Exception as e:  # noqa: BLE001
    print(f"  Job failed as expected: {type(e).__name__}")
    print("  Note in the trace: 'Task N ... failed 2 times' with the SAME ValueError.")
    print("  -> Deterministic. Retries cannot help; the data/UDF is the cause.")

print("\n" + "=" * 60)
print("Reasoning")
print("=" * 60)
print("""
  Transient (retry helps):     executor lost, network blip, preemption, FetchFailed.
  Deterministic (retry futile): same task index + same exception on every attempt.

  Speculation (spark.speculation=true) relaunches SLOW tasks:
    - helps when a node is slow;
    - does NOT help skew (the duplicate is just as slow) and can duplicate
      non-idempotent writes.
""")
print("Analysis Questions")
print("1. How would you find WHICH rows caused the failure? (hint: filter by txn_id % 500)")
print("2. Would raising spark.task.maxFailures to 16 fix this? Why not?")
print("3. When is speculation the wrong tool?")

spark.stop()
