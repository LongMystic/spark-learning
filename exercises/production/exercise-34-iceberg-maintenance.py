"""
Exercise 34: Iceberg Maintenance — compaction, MERGE upsert, time travel
Run WITH the Iceberg runtime jar:
    ENABLE_ICEBERG=1 spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
      exercises/production/exercise-34-iceberg-maintenance.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
os.environ.setdefault("ENABLE_ICEBERG", "1")
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Iceberg Maintenance")
txns = read_table(spark, "transactions")

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.txn_m")
    spark.sql("""CREATE TABLE local.db.txn_m (customer_id BIGINT, total DOUBLE) USING iceberg""")

    print("=" * 60)
    print("1. Create many small files (write in small batches)")
    print("=" * 60)
    for i in range(6):
        (txns.where(F.col("txn_id") % 6 == i)
             .groupBy("customer_id").agg(F.sum("amount").alias("total"))
             .writeTo("local.db.txn_m").append())
    files_before = spark.sql("SELECT * FROM local.db.txn_m.files").count()
    print("  files before compaction:", files_before)

    print("\n2. Compaction (rewrite_data_files)")
    spark.sql("CALL local.system.rewrite_data_files(table => 'db.txn_m', "
              "options => map('target-file-size-bytes','134217728'))").show(truncate=False)
    files_after = spark.sql("SELECT * FROM local.db.txn_m.files").count()
    print("  files after compaction:", files_after)

    print("\n3. MERGE upsert")
    changes = spark.createDataFrame([(0, 999.0), (10 ** 9, 5.0)], "customer_id long, total double")
    changes.createOrReplaceTempView("changes")
    spark.sql("""
        MERGE INTO local.db.txn_m t USING changes s ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET t.total = t.total + s.total
        WHEN NOT MATCHED THEN INSERT *""")
    print("  merged. rows:", spark.table("local.db.txn_m").count())

    print("\n4. Time travel + snapshots")
    spark.sql("SELECT snapshot_id, operation FROM local.db.txn_m.snapshots").show(truncate=False)
except Exception as e:  # noqa: BLE001
    print(f"  (Iceberg not available: {type(e).__name__}: {e})")
    print("  Re-run with the iceberg-spark-runtime jar and ENABLE_ICEBERG=1.")

print("\nAnalysis Questions")
print("1. How did file count change after rewrite_data_files?")
print("2. Which procedure actually RECLAIMS storage (frees old files)?")
print("3. Why is MERGE safer than read-modify-overwrite for CDC?")

spark.stop()
