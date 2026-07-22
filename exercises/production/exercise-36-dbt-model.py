"""
Exercise 36: DBT-style models on Spark (staging + incremental fact)
Purpose: Express what dbt would compile — a staging model and an incremental
fact model with is_incremental() logic — as Spark SQL, and run it locally.

Run:  python exercises/production/exercise-36-dbt-model.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

spark = get_spark("DBT-style Models")
read_table(spark, "transactions").createOrReplaceTempView("raw_transactions")

print("=" * 60)
print("stg_transactions: light cleaning (what a dbt staging model does)")
print("=" * 60)
stg = spark.sql("""
    SELECT txn_id, customer_id, product_id, category, status,
           CAST(amount AS DOUBLE) AS amount, txn_date
    FROM raw_transactions
    WHERE status IS NOT NULL
""")
stg.createOrReplaceTempView("stg_transactions")
stg.show(3)

print("=" * 60)
print("fct_daily_sales: INCREMENTAL model (merge/insert_overwrite by txn_date)")
print("=" * 60)

base = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
target = os.path.join(base, "_ex36_fct_daily_sales")


def build(is_incremental):
    where = ""
    if is_incremental and os.path.exists(target):
        max_date = spark.read.parquet(target).selectExpr("max(txn_date) m").first().m
        where = f"WHERE txn_date > DATE('{max_date}')"
        print(f"  incremental run: processing txn_date > {max_date}")
    else:
        print("  full run: processing all dates")
    return spark.sql(f"""
        SELECT txn_date, category, SUM(amount) AS total, COUNT(*) AS n
        FROM stg_transactions {where}
        GROUP BY txn_date, category
    """)


# First (full) run
full = build(is_incremental=False)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
full.write.mode("overwrite").partitionBy("txn_date").parquet(target)
print("  rows after full run:", spark.read.parquet(target).count())

# Incremental run (no new dates in sample -> processes 0, stays consistent)
inc = build(is_incremental=True)
print("  incremental new rows:", inc.count())

print("\nAnalysis Questions")
print("1. Which dbt materialization does this mirror (view/table/incremental)?")
print("2. Why prefer merge/insert_overwrite over bare append for incrementals?")
print("3. How does dbt concurrency (threads) interact with the Thrift Server?")

spark.stop()
