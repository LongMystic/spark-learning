"""
Exercise 12: Join Optimization
Purpose: Learn to optimize join operations

Instructions:
1. Compare different join strategies
2. Optimize join with filtering and column pruning
3. Test broadcast joins
4. Handle join skew
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast
import time

spark = SparkSession.builder \
    .appName("Join Optimization Exercise") \
    .getOrCreate()

# Exercise 1: Compare Join Strategies
print("=" * 50)
print("Exercise 1: Compare Join Strategies")
print("=" * 50)

# Create test data
df_large = spark.range(0, 10000000).withColumn("key", col("id") % 1000)
df_small = spark.range(0, 1000).withColumn("key", col("id"))

# Join without broadcast
print("--- Sort Merge Join (Default) ---")
start = time.time()
result1 = df_large.join(df_small, "key")
result1.explain()
# result1.count()  # Uncomment to execute
time_smj = time.time() - start
print(f"  Execution time: {time_smj:.2f}s")
print("  Check explain plan - should show SortMergeJoin")

# Join with broadcast
print("\n--- Broadcast Hash Join ---")
start = time.time()
result2 = df_large.join(broadcast(df_small), "key")
result2.explain()
# result2.count()  # Uncomment to execute
time_bhj = time.time() - start
print(f"  Execution time: {time_bhj:.2f}s")
print("  Check explain plan - should show BroadcastHashJoin")

print(f"\nImprovement: {((time_smj - time_bhj) / time_smj * 100):.1f}%")
print("  Broadcast join avoids shuffle for small table")

# Exercise 2: Optimize Join with Filtering
print("\n" + "=" * 50)
print("Exercise 2: Filter Before Join")
print("=" * 50)

# TODO: Replace with your actual table paths
df1 = spark.read.parquet("table1_path/")
df2 = spark.read.parquet("table2_path/")

# Bad: Join then filter
print("--- Join Then Filter ---")
start = time.time()
result_bad = df1.join(df2, "id").filter(col("status") == "active")
# result_bad.count()  # Uncomment to execute
time_bad = time.time() - start
print(f"  Execution time: {time_bad:.2f}s")
print("  Shuffles all data, then filters")

# Good: Filter then join
print("\n--- Filter Then Join ---")
start = time.time()
result_good = df1.filter(col("status") == "active").join(
    df2.filter(col("status") == "active"), "id"
)
# result_good.count()  # Uncomment to execute
time_good = time.time() - start
print(f"  Execution time: {time_good:.2f}s")
print("  Filters first, shuffles less data")

print(f"\nImprovement: {((time_bad - time_good) / time_bad * 100):.1f}%")

# Exercise 3: Column Pruning in Joins
print("\n" + "=" * 50)
print("Exercise 3: Column Pruning")
print("=" * 50)

# Bad: Join all columns
print("--- Join All Columns ---")
start = time.time()
result_bad = df1.join(df2, "id")
# result_bad.count()  # Uncomment to execute
time_bad = time.time() - start
print(f"  Execution time: {time_bad:.2f}s")

# Good: Select needed columns first
print("\n--- Select Needed Columns First ---")
start = time.time()
result_good = df1.select("id", "col1", "col2").join(
    df2.select("id", "col3"), "id"
)
# result_good.count()  # Uncomment to execute
time_good = time.time() - start
print(f"  Execution time: {time_good:.2f}s")
print("  Only shuffles needed columns")

print(f"\nImprovement: {((time_bad - time_good) / time_bad * 100):.1f}%")

# Exercise 4: Broadcast Join Threshold
print("\n" + "=" * 50)
print("Exercise 4: Broadcast Join Threshold")
print("=" * 50)

# Default threshold (10MB)
print("--- Default Threshold (10MB) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
result = df_large.join(df_small, "key")
result.explain()
print("  Check if small table is broadcasted")

# Increased threshold (50MB)
print("\n--- Increased Threshold (50MB) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")
result = df_large.join(df_small, "key")
result.explain()
print("  Larger tables may be broadcasted")

# Exercise 5: Join Order Optimization
print("\n" + "=" * 50)
print("Exercise 5: Join Order")
print("=" * 50)

# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

print("Cost-Based Optimization Enabled:")
print(f"  CBO Enabled: {spark.conf.get('spark.sql.cbo.enabled')}")
print(f"  Join Reorder: {spark.conf.get('spark.sql.cbo.joinReorder.enabled')}")

# TODO: Collect statistics first
# spark.sql("ANALYZE TABLE table1 COMPUTE STATISTICS FOR ALL COLUMNS")
# spark.sql("ANALYZE TABLE table2 COMPUTE STATISTICS FOR ALL COLUMNS")

df3 = spark.read.parquet("table3_path/")

# Multiple joins - Catalyst will optimize order
result = df1.join(df2, "id").join(df3, "id")
result.explain()
print("  Catalyst optimizer chooses join order based on statistics")

# Exercise 6: Bucket Join
print("\n" + "=" * 50)
print("Exercise 6: Bucket Join")
print("=" * 50)

# Create bucketed tables
print("Creating bucketed tables...")
df1.write.bucketBy(10, "id").sortBy("id").saveAsTable("bucketed_table1")
df2.write.bucketBy(10, "id").sortBy("id").saveAsTable("bucketed_table2")

# Read bucketed tables
df1_bucketed = spark.table("bucketed_table1")
df2_bucketed = spark.table("bucketed_table2")

# Join bucketed tables
print("Joining bucketed tables...")
result_bucketed = df1_bucketed.join(df2_bucketed, "id")
result_bucketed.explain()
print("  Check explain plan - should show no shuffle")
print("  Bucket join avoids shuffle when both tables are bucketed")

# Exercise 7: Join Skew Handling
print("\n" + "=" * 50)
print("Exercise 7: Join Skew Handling")
print("=" * 50)

# Enable AQE skew join
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

print("AQE Skew Join Enabled:")
print(f"  Skew Join: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# Create skewed join scenario
df_skewed1 = spark.range(0, 1000000).withColumn(
    "key",
    when(col("id") < 100000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
)

df_skewed2 = spark.range(0, 100000).withColumn(
    "key",
    when(col("id") < 10000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
)

print("\nRunning join with skew...")
start = time.time()
result_skewed = df_skewed1.join(df_skewed2, "key")
# result_skewed.count()  # Uncomment to execute
time_skewed = time.time() - start

print(f"  Execution time: {time_skewed:.2f}s")
print("""
Check Spark UI:
- Look for skew join indicators
- Check if partitions were split
- Compare task execution times
""")

# Exercise 8: Multiple Join Optimization
print("\n" + "=" * 50)
print("Exercise 8: Multiple Joins")
print("=" * 50)

# Chain of joins
print("Optimizing chain of joins...")
result_chain = df1.filter(col("status") == "active") \
    .join(broadcast(df2), "id") \
    .join(broadcast(df3), "id")

result_chain.explain()
print("""
Optimization strategies:
- Filter early
- Broadcast small tables
- Let Catalyst optimize order
""")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. When should you use broadcast joins?")
print("2. How does filtering before join improve performance?")
print("3. What is the impact of column pruning on joins?")
print("4. How does CBO optimize join order?")
print("5. What are the benefits of bucket joins?")
print("6. How does AQE handle join skew?")
print("7. Check Spark UI - compare join strategies")

spark.stop()

