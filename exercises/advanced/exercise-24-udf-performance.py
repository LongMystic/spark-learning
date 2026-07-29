"""
Exercise 24: UDF Performance — three ways to do one transform
Purpose: Compare built-in expression vs Python UDF vs Pandas UDF (plan + boundary).

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("UDF Performance")
txns = read_table(spark, "transactions")


def timed(name, df):
    start = time.time()
    df.count()
    print(f"  {name}: {time.time() - start:.2f}s")


print("=" * 60)
print("1. Built-in expression (codegen, optimizer-visible) — preferred")
print("=" * 60)
builtin = txns.withColumn("net", F.col("amount") * F.lit(0.9))
builtin.explain()
timed("built-in", builtin)

print("\n" + "=" * 60)
print("2. Python UDF (row-by-row, BatchEvalPython) — slow")
print("=" * 60)

@F.udf("double")
def net_udf(amount):
    return amount * 0.9

py = txns.withColumn("net", net_udf("amount"))
py.explain()
timed("python-udf", py)

print("\n" + "=" * 60)
print("3. Pandas UDF (vectorized, ArrowEvalPython)")
print("=" * 60)
try:
    import pandas as pd  # noqa: F401
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("double")
    def net_pandas(s):
        return s * 0.9

    pandas_df = txns.withColumn("net", net_pandas("amount"))
    pandas_df.explain()
    timed("pandas-udf", pandas_df)

    # Grouped aggregate Pandas UDF: geometric mean per category
    @pandas_udf("double")
    def gmean(v):
        import numpy as np
        return float(np.exp(np.log(v.clip(lower=1e-9)).mean()))

    txns.groupBy("category").agg(gmean("amount").alias("geo_mean_amount")).show()
except Exception as e:  # noqa: BLE001
    print(f"  (Pandas UDF skipped: {type(e).__name__}: install pyarrow+pandas+numpy)")

print("Analysis Questions")
print("1. Which plan shows BatchEvalPython vs ArrowEvalPython vs neither?")
print("2. At --scale medium/large, how do the three timings compare?")
print("3. Why is the built-in the only one Catalyst can optimize through?")

spark.stop()
