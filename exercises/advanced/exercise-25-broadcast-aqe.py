"""
Exercise 25: Broadcast Strategies & AQE
Purpose: Toggle broadcast and AQE; watch the join node and post-shuffle partitions change.

Run:  python exercises/advanced/exercise-25-broadcast-aqe.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = get_spark("Broadcast & AQE")
txns = read_table(spark, "transactions")
products = read_table(spark, "products")   # tiny dimension

print("=" * 60)
print("1. Broadcast OFF -> SortMergeJoin (both sides shuffle)")
print("=" * 60)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
txns.join(products, "product_id").explain()

print("\n" + "=" * 60)
print("2. Broadcast ON (auto) -> BroadcastHashJoin (fact not shuffled)")
print("=" * 60)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
txns.join(products, "product_id").explain()

print("\n" + "=" * 60)
print("3. Explicit broadcast() hint overrides the estimate")
print("=" * 60)
txns.join(broadcast(products), "product_id").explain()

print("\n" + "=" * 60)
print("4. AQE on a skewed groupBy — coalesced post-shuffle partitions")
print("=" * 60)
skew = read_table(spark, "transactions_skewed")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")   # AQE will coalesce these
res = skew.groupBy("customer_id").agg(F.sum("amount").alias("t"))
res.count()
print("  Look in the SQL tab for 'AdaptiveSparkPlan' and 'AQEShuffleRead (coalesced)'.")
res.explain()

print("Analysis Questions")
print("1. Which Exchange nodes disappear when broadcast is enabled?")
print("2. What partition count did AQE coalesce 200 down to?")
print("3. When would broadcasting the dimension be dangerous?")

spark.stop()
