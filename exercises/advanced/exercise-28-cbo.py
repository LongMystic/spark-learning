"""
Exercise 28: Cost-Based Optimization
Purpose: Register tables, ANALYZE for stats, and compare join plans with CBO on/off.
Requires catalog support; falls back with a note on a bare session.

Run:  python exercises/advanced/exercise-28-cbo.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

spark = get_spark("Cost-Based Optimization")

try:
    print("=" * 60)
    print("1. Register tables and collect statistics")
    print("=" * 60)
    for name in ["transactions", "customers", "products"]:
        spark.sql(f"DROP TABLE IF EXISTS {name}_cbo")
        read_table(spark, name).write.mode("overwrite").saveAsTable(f"{name}_cbo")
        spark.sql(f"ANALYZE TABLE {name}_cbo COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"  analyzed {name}_cbo")

    print("\n--- Table statistics ---")
    spark.sql("DESCRIBE EXTENDED transactions_cbo").where("col_name = 'Statistics'").show(truncate=False)

    print("\n" + "=" * 60)
    print("2. Multi-way join: CBO ON vs OFF (compare join order + estimates)")
    print("=" * 60)
    query = """
        SELECT c.segment, p.category, SUM(t.amount) AS total
        FROM transactions_cbo t
        JOIN customers_cbo c ON t.customer_id = c.customer_id
        JOIN products_cbo  p ON t.product_id  = p.product_id
        GROUP BY c.segment, p.category
    """
    for flag in ["true", "false"]:
        print(f"\n--- spark.sql.cbo.enabled = {flag} ---")
        spark.conf.set("spark.sql.cbo.enabled", flag)
        spark.conf.set("spark.sql.cbo.joinReorder.enabled", flag)
        spark.sql(query).explain("cost")
except Exception as e:  # noqa: BLE001
    print(f"  (CBO demo skipped: {type(e).__name__}: {e})")
    print("  Enable Hive support / run on the cluster to register + ANALYZE tables.")

print("\nAnalysis Questions")
print("1. Do the estimated row counts in explain('cost') look realistic?")
print("2. Did the join ORDER change between CBO on and off?")
print("3. Why does CBO do nothing without column statistics?")

spark.stop()
