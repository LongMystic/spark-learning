"""
Exercise 7: Caching and Persistence
Purpose: Learn when and how to cache data effectively

Instructions:
1. Compare caching vs no caching
2. Test different storage levels
3. Monitor cache effectiveness
4. Optimize caching strategy
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark import StorageLevel
import time

spark = SparkSession.builder \
    .appName("Caching and Persistence Exercise") \
    .getOrCreate()

# Exercise 1: Compare With and Without Cache
print("=" * 50)
print("Exercise 1: Compare With and Without Cache")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

# Without cache
print("--- Without Cache ---")
start = time.time()
result1 = df.filter(col("status") == "active").groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time1 = time.time() - start

start = time.time()
result2 = df.filter(col("status") == "active").select("category", "amount").show(10)
time2 = time.time() - start

total_without_cache = time1 + time2
print(f"  Query 1 time: {time1:.2f}s")
print(f"  Query 2 time: {time2:.2f}s")
print(f"  Total time: {total_without_cache:.2f}s")

# With cache
print("\n--- With Cache ---")
df_filtered = df.filter(col("status") == "active")
df_filtered.cache()

start = time.time()
result1_cached = df_filtered.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1_cached.count()  # Uncomment to execute (computes and caches)
time1_cached = time.time() - start

start = time.time()
result2_cached = df_filtered.select("category", "amount").show(10)
time2_cached = time.time() - start

total_with_cache = time1_cached + time2_cached
print(f"  Query 1 time: {time1_cached:.2f}s (computes and caches)")
print(f"  Query 2 time: {time2_cached:.2f}s (uses cache)")
print(f"  Total time: {total_with_cache:.2f}s")

improvement = ((total_without_cache - total_with_cache) / total_without_cache * 100)
print(f"\nImprovement: {improvement:.1f}%")

# Exercise 2: Compare Storage Levels
print("\n" + "=" * 50)
print("Exercise 2: Compare Storage Levels")
print("=" * 50)

storage_levels = [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
    ("DISK_ONLY", StorageLevel.DISK_ONLY)
]

for name, level in storage_levels:
    print(f"\n--- {name} ---")
    df_test = spark.read.parquet("your_table_path/")
    df_test.persist(level)
    
    start = time.time()
    df_test.count()  # Trigger persistence
    time_persist = time.time() - start
    
    start = time.time()
    df_test.groupBy("category").agg(spark_sum("amount").alias("total")).count()
    time_query = time.time() - start
    
    print(f"  Persist time: {time_persist:.2f}s")
    print(f"  Query time: {time_query:.2f}s")
    
    df_test.unpersist()

# Exercise 3: Monitor Cache in Spark UI
print("\n" + "=" * 50)
print("Exercise 3: Monitor Cache")
print("=" * 50)

df.cache()
df.count()  # Trigger caching

print("""
Check Spark UI - Storage tab:
- Cached RDDs/DataFrames
- Memory used for cached data
- Disk used (if spilled)
- Size of cached data
""")

print("\nCheck Spark UI - Executors tab:")
print("  - Memory usage per executor")
print("  - Cache hit/miss rates")

# Exercise 4: Cache Eviction
print("\n" + "=" * 50)
print("Exercise 4: Cache Eviction")
print("=" * 50)

# Cache multiple DataFrames
df1 = spark.read.parquet("table1_path/")
df2 = spark.read.parquet("table2_path/")
df3 = spark.read.parquet("table3_path/")

df1.cache()
df2.cache()
df3.cache()

df1.count()
df2.count()
df3.count()

print("""
Cached 3 DataFrames
- If memory is full, older caches may be evicted
- Check Spark UI to see which DataFrames are cached
- LRU (Least Recently Used) policy
""")

# Exercise 5: When NOT to Cache
print("\n" + "=" * 50)
print("Exercise 5: When NOT to Cache")
print("=" * 50)

print("Don't cache if:")
print("  1. Data used only once")
print("  2. Data is very large (larger than memory)")
print("  3. Data changes frequently")
print("  4. Simple operations (cache overhead > benefit)")

# Example: Simple operation (don't cache)
print("\nExample: Simple filter (don't cache)")
df_simple = spark.read.parquet("your_table_path/")
# df_simple.filter(...).cache()  # Bad - filter is fast, cache overhead not worth it

# Example: Expensive operation (do cache)
print("\nExample: Expensive join (do cache)")
df_expensive = spark.read.parquet("table1_path/").join(
    spark.read.parquet("table2_path/"), "id"
)
df_expensive.cache()  # Good - expensive operation, used multiple times
df_expensive.count()

# Exercise 6: Unpersist
print("\n" + "=" * 50)
print("Exercise 6: Unpersist")
print("=" * 50)

df_cached = spark.read.parquet("your_table_path/")
df_cached.cache()
df_cached.count()

print("Cached DataFrame")
print("  Check Spark UI - should show cached data")

df_cached.unpersist()

print("\nUnpersisted DataFrame")
print("  Check Spark UI - cached data should be removed")
print("  Memory freed for other operations")

# Exercise 7: Cache Strategy
print("\n" + "=" * 50)
print("Exercise 7: Cache Strategy")
print("=" * 50)

print("Good candidates for caching:")
print("  1. Iterative algorithms (ML, graph processing)")
print("  2. Multiple actions on same data")
print("  3. Small dimension tables")
print("  4. Intermediate results in complex pipelines")

# Example: Iterative algorithm
print("\nExample: Iterative computation")
df_base = spark.read.parquet("your_table_path/")
df_base.cache()

for i in range(5):
    result = df_base.filter(col("iteration") == i).groupBy("key").agg(spark_sum("amount"))
    # result.count()  # Uncomment to execute
    print(f"  Iteration {i}: Uses cached base DataFrame")

df_base.unpersist()

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. When is caching beneficial?")
print("2. What storage level is best for your use case?")
print("3. How do you monitor cache effectiveness?")
print("4. When should you unpersist?")
print("5. What are the trade-offs of different storage levels?")
print("6. Check Spark UI - how is cache being used?")

spark.stop()

