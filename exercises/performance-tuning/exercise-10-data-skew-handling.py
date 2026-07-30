"""
Exercise 10: Data Skew Handling
Purpose: Learn to detect and handle data skew

Instructions:
1. Detect skew on the real transactions_skewed table
2. Apply skew mitigation techniques
3. Compare performance with/without skew handling
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table  # noqa: F401,E402

from pyspark.sql.functions import col, sum as spark_sum, count, concat, lit, rand, split, explode, array  # noqa: E402

spark = get_spark("Data Skew Handling Exercise")

# Exercise 1: Detect Skew
print("=" * 50)
print("Exercise 1: Detect Skew on transactions_skewed")
print("=" * 50)

txns = read_table(spark, "transactions_skewed")
customers = read_table(spark, "customers")

print("Loaded 'transactions_skewed' -> ~80% of rows land on 5 'hot' customer_ids")
print("(see environment/generate_data.py: build_transactions_skewed)")


def detect_skew(df, key_col, threshold=3.0):
    """Detect skew by comparing per-key row counts."""
    counts = [r["cnt"] for r in df.groupBy(key_col).agg(count("*").alias("cnt")).collect()]
    if not counts:
        return False, 0, 0, 0
    min_c, max_c = min(counts), max(counts)
    ratio = max_c / min_c if min_c > 0 else float("inf")
    return ratio > threshold, ratio, min_c, max_c


is_skewed, ratio, min_c, max_c = detect_skew(txns, "customer_id")
print("\nSkew Detection Results (by customer_id):")
print(f"  Is Skewed: {is_skewed}")
print(f"  Skew Ratio: {ratio:.2f}")
print(f"  Min Key Count: {min_c}")
print(f"  Max Key Count: {max_c}")

print("\nTop 10 customer_ids by row count:")
key_counts = txns.groupBy("customer_id").agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
key_counts.show(10)

# Exercise 2: Observe Skew Impact
print("\n" + "=" * 50)
print("Exercise 2: Observe Skew Impact")
print("=" * 50)

print("Running groupBy on the skewed table -- check Spark UI for uneven task times")

start = time.time()
result1 = txns.groupBy("customer_id").agg(spark_sum("amount").alias("total"))
result1.count()
time_skewed = time.time() - start

print(f"  Execution time: {time_skewed:.2f}s")
print("""
Expected observations in the Spark UI (Stages tab):
- A handful of tasks (the hot customer_ids) take much longer than the rest
- Uneven Shuffle Read Size across tasks in the same stage
- Straggler tasks that hold up the whole stage
""")

# Exercise 3: Adaptive Query Execution (Spark 3.0+)
print("\n" + "=" * 50)
print("Exercise 3: AQE Skew Handling")
print("=" * 50)

# Enable AQE skew join
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

print("AQE Skew Handling Enabled:")
print(f"  AQE Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  Skew Join Enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

print("\nRunning join(transactions_skewed, customers) with AQE skew handling...")
start = time.time()
result2 = txns.join(customers, "customer_id")
result2.count()
time_aqe = time.time() - start

print(f"  Execution time: {time_aqe:.2f}s")
print("""
Check Spark UI (SQL tab):
- Look for "coalesced"/skew markers on the shuffle exchange node
- Compare partition counts before/after AQE splits the hot customer_id partitions
""")

# Exercise 4: Salting Technique
print("\n" + "=" * 50)
print("Exercise 4: Salting Technique")
print("=" * 50)

salt_buckets = 10

print("Adding salt to the skewed side (transactions_skewed)...")
txns_salted = txns.alias("t").withColumn(
    "salted_key",
    concat(col("customer_id").cast("string"), lit("_"), (rand() * salt_buckets).cast("int"))
)

print("Replicating and salting the other side (customers)...")
salt_array = array([lit(f"_{i}") for i in range(salt_buckets)])
customers_salted = customers.alias("c").withColumn("salt", explode(salt_array)) \
    .withColumn("salted_key", concat(col("customer_id").cast("string"), col("salt")))

print(f"  Salt buckets: {salt_buckets}")
print("  This distributes each hot customer_id across multiple partitions")

print("\nRunning join with salted keys...")
start = time.time()
result3 = txns_salted.join(customers_salted, "salted_key").select("t.customer_id", "t.amount")
result3_final = result3.groupBy("customer_id").agg(spark_sum("amount").alias("total"))
result3_final.count()
time_salted = time.time() - start

print(f"  Execution time: {time_salted:.2f}s")

# Exercise 5: Two-Phase Aggregation
print("\n" + "=" * 50)
print("Exercise 5: Two-Phase Aggregation")
print("=" * 50)

# Phase 1: Partial aggregation with salt
print("Phase 1: Partial aggregation with salt...")
txns_salted2 = txns.withColumn(
    "salted_key",
    concat(col("customer_id").cast("string"), lit("_"), (rand() * 10).cast("int"))
)

partial = txns_salted2.groupBy("salted_key").agg(
    spark_sum("amount").alias("partial_sum"),
    count("*").alias("partial_count")
)

# Phase 2: Final aggregation
print("Phase 2: Final aggregation...")
final = partial.withColumn("customer_id", split(col("salted_key"), "_")[0].cast("long")) \
    .groupBy("customer_id").agg(
        spark_sum("partial_sum").alias("total_sum"),
        spark_sum("partial_count").alias("total_count")
    )
final.count()

print("Two-phase aggregation complete")
print("  This reduces skew impact in aggregations")

# Exercise 6: Split Skewed Keys
print("\n" + "=" * 50)
print("Exercise 6: Split Skewed Keys")
print("=" * 50)

# Identify the hot customer_ids from the skew detection above
top_customers = [
    r["customer_id"]
    for r in key_counts.limit(5).collect()
]
print(f"Identified hot customer_ids: {top_customers}")

df_normal = txns.filter(~col("customer_id").isin(top_customers))
df_hot = txns.filter(col("customer_id").isin(top_customers))

print("Split data:")
print(f"  Normal customers: {df_normal.count()} rows")
print(f"  Hot customers: {df_hot.count()} rows")

# Process separately
result_normal = df_normal.groupBy("customer_id").agg(spark_sum("amount").alias("total"))

# For hot customers, use salting
df_hot_salted = df_hot.withColumn(
    "salted_key",
    concat(col("customer_id").cast("string"), lit("_"), (rand() * 10).cast("int"))
)
result_hot = df_hot_salted.groupBy("salted_key").agg(spark_sum("amount").alias("total")) \
    .withColumn("customer_id", split(col("salted_key"), "_")[0].cast("long")) \
    .groupBy("customer_id").agg(spark_sum("total").alias("total"))

# Union results
result_split = result_normal.union(result_hot)
result_split.count()

print("Split processing complete")
print("  Normal customers processed normally")
print("  Hot customers processed with salting")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How does skew affect task execution times?")
print("2. What is the performance impact of skew?")
print("3. How does AQE detect and handle skew?")
print("4. What are the trade-offs of salting?")
print("5. When should you use two-phase aggregation?")
print("6. Compare performance: with vs without skew handling")

spark.stop()
