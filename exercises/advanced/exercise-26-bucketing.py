"""
Exercise 26: Bucketing
Purpose: Bucket two tables on the same key and confirm the join needs no shuffle.
Requires catalog (saveAsTable) support; falls back with a note on a bare session.

Run:  python exercises/advanced/exercise-26-bucketing.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

spark = get_spark("Bucketing", extra_conf={"spark.sql.sources.bucketing.enabled": "true"})
txns = read_table(spark, "transactions")
customers = read_table(spark, "customers")

N_BUCKETS = 16

try:
    print("=" * 60)
    print(f"Writing two tables bucketed by customer_id into {N_BUCKETS} buckets")
    print("=" * 60)
    for name, df in [("txns_bkt", txns.select("customer_id", "amount")),
                     ("cust_bkt", customers.select("customer_id", "segment"))]:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        (df.write.mode("overwrite")
           .bucketBy(N_BUCKETS, "customer_id")
           .sortBy("customer_id")
           .saveAsTable(name))
        print(f"  wrote {name}")

    print("\n--- Bucketed join (expect NO Exchange on the join inputs) ---")
    a = spark.table("txns_bkt")
    b = spark.table("cust_bkt")
    a.join(b, "customer_id").explain()

    print("\n--- Mismatched bucket counts reintroduce a shuffle ---")
    spark.sql("DROP TABLE IF EXISTS cust_bkt8")
    (customers.select("customer_id", "segment").write.mode("overwrite")
        .bucketBy(8, "customer_id").sortBy("customer_id").saveAsTable("cust_bkt8"))
    a.join(spark.table("cust_bkt8"), "customer_id").explain()
    print("  Note the Exchange that appears due to the 16-vs-8 mismatch.")
except Exception as e:  # noqa: BLE001
    print(f"  (Bucketing demo skipped: {type(e).__name__}: {e})")
    print("  Enable Hive support / run on the cluster to try bucketing.")

print("\nAnalysis Questions")
print("1. Which join had no Exchange, and why?")
print("2. Why must the bucket COUNTS match?")
print("3. When is the one-time bucketing write cost worth it?")

spark.stop()
