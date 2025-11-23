"""
Exercise 10: Data Skew Handling
Purpose: Learn to detect and handle data skew

Instructions:
1. Create and detect skewed data
2. Apply skew mitigation techniques
3. Compare performance with/without skew handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, when, concat, lit, rand, split, explode, array
import time

spark = SparkSession.builder \
    .appName("Data Skew Handling Exercise") \
    .getOrCreate()

# Exercise 1: Create Skewed Data
print("=" * 50)
print("Exercise 1: Create and Detect Skew")
print("=" * 50)

# Create data with intentional skew
df = spark.range(0, 1000000).withColumn(
    "key",
    when(col("id") < 100000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
).withColumn("amount", (col("id") % 1000) + 1)

print("Created DataFrame with skewed key distribution")
print("  - 10% of data has key='skewed'")
print("  - 90% of data has key='normal_X'")

# Detect skew by checking partition sizes
def detect_skew(df, threshold=3.0):
    """Detect if DataFrame has skewed partitions"""
    partition_sizes = df.rdd.mapPartitions(
        lambda it: [sum(1 for _ in it)]
    ).collect()
    
    if not partition_sizes:
        return False, 0, 0, 0
    
    min_size = min(partition_sizes)
    max_size = max(partition_sizes)
    avg_size = sum(partition_sizes) / len(partition_sizes)
    ratio = max_size / min_size if min_size > 0 else float('inf')
    
    is_skewed = ratio > threshold
    return is_skewed, ratio, min_size, max_size

is_skewed, ratio, min_size, max_size = detect_skew(df)
print(f"\nSkew Detection Results:")
print(f"  Is Skewed: {is_skewed}")
print(f"  Skew Ratio: {ratio:.2f}")
print(f"  Min Partition Size: {min_size}")
print(f"  Max Partition Size: {max_size}")

# Check key distribution
print("\nKey Distribution (Top 10):")
key_counts = df.groupBy("key").agg(count("*").alias("count")).orderBy(col("count").desc())
key_counts.show(10)

# Exercise 2: Observe Skew Impact
print("\n" + "=" * 50)
print("Exercise 2: Observe Skew Impact")
print("=" * 50)

print("Running groupBy on skewed data...")
print("  Check Spark UI for uneven task execution times")

start = time.time()
result1 = df.groupBy("key").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time_skewed = time.time() - start

print(f"  Execution time: {time_skewed:.2f}s")
print("""
Expected observations:
- Some tasks take much longer than others
- Uneven task execution times in Spark UI
- Straggler tasks
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

# Create join scenario
df1 = df
df2 = spark.range(0, 100000).withColumn(
    "key",
    when(col("id") < 10000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
).withColumn("value", col("id") * 2)

print("\nRunning join with AQE skew handling...")
start = time.time()
result2 = df1.join(df2, "key")
# result2.count()  # Uncomment to execute
time_aqe = time.time() - start

print(f"  Execution time: {time_aqe:.2f}s")
print("""
Check Spark UI:
- Look for skew join indicators
- Check if partitions were split
- Compare task execution times
""")

# Exercise 4: Salting Technique
print("\n" + "=" * 50)
print("Exercise 4: Salting Technique")
print("=" * 50)

salt_buckets = 10

# Add salt to skewed side
print("Adding salt to DataFrame...")
df1_salted = df1.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * salt_buckets).cast("int"))
)

# Replicate and salt the other side
print("Replicating and salting other side...")
salt_array = array([lit(f"_{i}") for i in range(salt_buckets)])
df2_salted = df2.withColumn("salt", explode(salt_array)) \
    .withColumn("salted_key", concat(col("key"), col("salt")))

print(f"  Salt buckets: {salt_buckets}")
print("  This distributes skewed keys across multiple partitions")

# Join on salted key
print("\nRunning join with salted keys...")
start = time.time()
result3 = df1_salted.join(df2_salted, "salted_key")

# Remove salt and aggregate
result3_final = result3.withColumn("key", split(col("salted_key"), "_")[0]) \
    .drop("salted_key", "salt") \
    .groupBy("key").agg(spark_sum("amount").alias("total"), spark_sum("value").alias("total_value"))

# result3_final.count()  # Uncomment to execute
time_salted = time.time() - start

print(f"  Execution time: {time_salted:.2f}s")

# Exercise 5: Two-Phase Aggregation
print("\n" + "=" * 50)
print("Exercise 5: Two-Phase Aggregation")
print("=" * 50)

# Phase 1: Partial aggregation with salt
print("Phase 1: Partial aggregation with salt...")
df_salted = df.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)

partial = df_salted.groupBy("salted_key").agg(
    spark_sum("amount").alias("partial_sum"),
    count("*").alias("partial_count")
)

# Phase 2: Final aggregation
print("Phase 2: Final aggregation...")
final = partial.withColumn("key", split(col("salted_key"), "_")[0]) \
    .groupBy("key").agg(
        spark_sum("partial_sum").alias("total_sum"),
        spark_sum("partial_count").alias("total_count")
    )

print("Two-phase aggregation complete")
print("  This reduces skew impact in aggregations")

# Exercise 6: Split Skewed Keys
print("\n" + "=" * 50)
print("Exercise 6: Split Skewed Keys")
print("=" * 50)

# Identify skewed keys
skewed_keys = ["skewed"]  # Known skewed values

# Split data
df_normal = df.filter(~col("key").isin(skewed_keys))
df_skewed = df.filter(col("key").isin(skewed_keys))

print(f"Split data:")
print(f"  Normal keys: {df_normal.count()} rows")
print(f"  Skewed keys: {df_skewed.count()} rows")

# Process separately
result_normal = df_normal.groupBy("key").agg(spark_sum("amount").alias("total"))

# For skewed keys, use salting
df_skewed_salted = df_skewed.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)
result_skewed = df_skewed_salted.groupBy("salted_key").agg(spark_sum("amount").alias("total")) \
    .withColumn("key", split(col("salted_key"), "_")[0]) \
    .groupBy("key").agg(spark_sum("total").alias("total"))

# Union results
result_split = result_normal.union(result_skewed)

print("Split processing complete")
print("  Normal keys processed normally")
print("  Skewed keys processed with salting")

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

