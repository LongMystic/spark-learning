"""
Exercise 15: Error Triage
Purpose: Trigger one error from each family and practice classifying from the trace.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
Each block is wrapped in try/except so the script runs end-to-end and prints the
exception TYPE + which family it belongs to. Read the full traceback for each.
"""

import os
import sys
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, udf

spark = get_spark("Error Triage")
txns = read_table(spark, "transactions")


def attempt(name, family, fn):
    print("\n" + "=" * 60)
    print(f"{name}  (expected family: {family})")
    print("=" * 60)
    try:
        fn()
        print("  -> no error (unexpected)")
    except Exception as e:  # noqa: BLE001
        print(f"  -> {type(e).__name__}: {str(e).splitlines()[0]}")
        # Uncomment to see the full trace and practice reading it:
        # traceback.print_exc()


# 1. DATA family — plan-time AnalysisException (column does not exist)
attempt("Reference a missing column", "data / plan-time",
        lambda: txns.select("does_not_exist").explain())

# 2. DATA family — bad cast / arithmetic at runtime
attempt("Divide by zero in a projection", "data / runtime",
        lambda: txns.selectExpr("amount / (quantity - quantity) AS boom").collect())

# 3. SERIALIZATION family — capture a non-picklable object in a UDF
class NotPicklable:
    def __init__(self):
        self.handle = lambda: None  # lambdas aren't picklable

_obj = NotPicklable()

@udf("int")
def bad_udf(x):
    return 1 if _obj else 0        # captures _obj -> pickling error

attempt("UDF captures a non-serializable object", "serialization",
        lambda: txns.withColumn("b", bad_udf(col("amount"))).collect())

# 4. RESOURCE/DATA — reading a path that doesn't exist
attempt("Read a non-existent path", "data / io",
        lambda: spark.read.parquet(os.path.join("data", "no_such_table")).count())

print("\n" + "=" * 60)
print("Analysis Questions")
print("=" * 60)
print("1. Which errors happened BEFORE any job ran (plan-time)?")
print("2. For each, what is the last 'Caused by:' line (uncomment traceback)?")
print("3. Which would a retry fix, and which are deterministic?")

spark.stop()
