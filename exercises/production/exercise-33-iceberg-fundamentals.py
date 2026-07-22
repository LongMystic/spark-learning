"""
Exercise 33: Iceberg Fundamentals
Purpose: Create an Iceberg table with hidden partitioning, append, and inspect
snapshots/files metadata.

Run WITH the Iceberg runtime jar and catalog enabled:
    ENABLE_ICEBERG=1 spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
      exercises/production/exercise-33-iceberg-fundamentals.py

Plain `python ...` without the jar will hit the graceful fallback message.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
os.environ.setdefault("ENABLE_ICEBERG", "1")
from common.spark_session import get_spark, read_table

spark = get_spark("Iceberg Fundamentals")
txns = read_table(spark, "transactions")

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.txn_ice")
    print("=" * 60)
    print("Create Iceberg table with HIDDEN partitioning by day(txn_ts)")
    print("=" * 60)
    spark.sql("""
        CREATE TABLE local.db.txn_ice (
            txn_id BIGINT, customer_id BIGINT, amount DOUBLE, txn_ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (days(txn_ts))
    """)

    (txns.select("txn_id", "customer_id", "amount", "txn_ts")
         .writeTo("local.db.txn_ice").append())
    print("  appended rows:", spark.table("local.db.txn_ice").count())

    print("\n--- Hidden partitioning: filter on txn_ts (no partition column!) ---")
    spark.table("local.db.txn_ice").where("txn_ts >= current_date() - interval 3 days").explain()

    print("\n--- Snapshots metadata ---")
    spark.sql("SELECT snapshot_id, committed_at, operation FROM local.db.txn_ice.snapshots").show(truncate=False)
    print("\n--- Data files metadata (count) ---")
    print("  files:", spark.sql("SELECT * FROM local.db.txn_ice.files").count())
except Exception as e:  # noqa: BLE001
    print(f"  (Iceberg not available: {type(e).__name__}: {e})")
    print("  Re-run with the iceberg-spark-runtime jar (see the header) and ENABLE_ICEBERG=1.")

print("\nAnalysis Questions")
print("1. What does hidden partitioning give you over a Hive 'dt=' column?")
print("2. How many snapshots exist after one append?")
print("3. What metadata lets Iceberg prune files without listing directories?")

spark.stop()
