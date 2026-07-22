"""
Exercise 27: Dynamic Partition Pruning
Purpose: See DPP prune fact-table partitions using a filtered dimension.

Our sample `transactions` fact is partitioned by txn_date. We build a small
"date dimension" filtered to a few dates, join on txn_date, and look for a
dynamic pruning filter on the fact scan.

Run:  python exercises/advanced/exercise-27-dpp.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = get_spark("Dynamic Partition Pruning")
txns = read_table(spark, "transactions")   # partitioned by txn_date

# Build a tiny date dimension and flag "recent" dates.
dates = (txns.select("txn_date").distinct()
         .withColumn("is_recent", F.col("txn_date") >= F.date_sub(F.current_date(), 7)))
recent = dates.where("is_recent").select("txn_date")

print("=" * 60)
print("1. DPP ON (default): join filtered date-dim to partitioned fact")
print("=" * 60)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
q = txns.join(broadcast(recent), "txn_date").agg(F.sum("amount"))
q.explain()
print("  Look for 'dynamicpruningexpression' / 'PartitionFilters' on the fact Scan.")
q.count()

print("\n" + "=" * 60)
print("2. DPP OFF: same query reads all fact partitions")
print("=" * 60)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
q2 = txns.join(broadcast(recent), "txn_date").agg(F.sum("amount"))
q2.explain()
q2.count()

print("\nAnalysis Questions")
print("1. In (1), what appears in the fact Scan's PartitionFilters?")
print("2. Compare 'number of partitions read' at the Scan node: (1) vs (2).")
print("3. Why does broadcasting the dimension help DPP fire?")

spark.stop()
