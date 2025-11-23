"""
Exercise 9: Resource Allocation and YARN Integration
Purpose: Understand dynamic allocation and resource management

Instructions:
1. Configure static vs dynamic allocation
2. Monitor resource allocation in YARN
3. Compare resource utilization patterns
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import time

# Exercise 1: Static Allocation
print("=" * 50)
print("Exercise 1: Static Allocation")
print("=" * 50)

spark_static = SparkSession.builder \
    .appName("Static Allocation Exercise") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.executor.instances", "10") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

print("Static Allocation Configuration:")
print(f"  Dynamic Allocation: {spark_static.conf.get('spark.dynamicAllocation.enabled')}")
print(f"  Executor Instances: {spark_static.conf.get('spark.executor.instances')}")

# TODO: Replace with your actual table path
df = spark_static.read.parquet("your_table_path/")

print("\nRunning query with static allocation...")
start = time.time()
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time_static = time.time() - start
print(f"  Execution time: {time_static:.2f}s")

# Exercise 2: Dynamic Allocation
print("\n" + "=" * 50)
print("Exercise 2: Dynamic Allocation")
print("=" * 50)

spark_dynamic = SparkSession.builder \
    .appName("Dynamic Allocation Exercise") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "30") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity") \
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

print("Dynamic Allocation Configuration:")
print(f"  Dynamic Allocation: {spark_dynamic.conf.get('spark.dynamicAllocation.enabled')}")
print(f"  Min Executors: {spark_dynamic.conf.get('spark.dynamicAllocation.minExecutors')}")
print(f"  Max Executors: {spark_dynamic.conf.get('spark.dynamicAllocation.maxExecutors')}")
print(f"  Initial Executors: {spark_dynamic.conf.get('spark.dynamicAllocation.initialExecutors')}")

df2 = spark_dynamic.read.parquet("your_table_path/")

print("\nRunning query with dynamic allocation...")
print("  Monitor Spark UI to see executor scaling...")
start = time.time()
result2 = df2.groupBy("category").agg(spark_sum("amount").alias("total"))
# result2.count()  # Uncomment to execute
time_dynamic = time.time() - start
print(f"  Execution time: {time_dynamic:.2f}s")

# Exercise 3: Monitor Resource Allocation
print("\n" + "=" * 50)
print("Exercise 3: Monitor Resource Allocation")
print("=" * 50)

print("""
Instructions for monitoring:
1. Open Spark UI: http://driver:4040
2. Navigate to Executors tab
3. Observe:
   - Number of executors over time
   - When executors are added/removed
   - Resource utilization per executor
4. Open YARN ResourceManager UI
5. Check:
   - Container allocations
   - Resource usage per application
   - Queue utilization
""")

# Exercise 4: Different Allocation Strategies
print("\n" + "=" * 50)
print("Exercise 4: Allocation Strategies")
print("=" * 50)

# Conservative Strategy
print("\n--- Conservative Strategy ---")
spark_conservative = SparkSession.builder \
    .appName("Conservative Allocation") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .getOrCreate()

print("  Min: 5, Max: 20, Initial: 5")
print("  Use case: Shared cluster, multiple users")

# Aggressive Strategy
print("\n--- Aggressive Strategy ---")
spark_aggressive = SparkSession.builder \
    .appName("Aggressive Allocation") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "20") \
    .config("spark.dynamicAllocation.maxExecutors", "100") \
    .config("spark.dynamicAllocation.initialExecutors", "50") \
    .getOrCreate()

print("  Min: 20, Max: 100, Initial: 50")
print("  Use case: Dedicated resources, performance critical")

# Exercise 5: Cached Executor Behavior
print("\n" + "=" * 50)
print("Exercise 5: Cached Executor Behavior")
print("=" * 50)

spark_cached = SparkSession.builder \
    .appName("Cached Executor Test") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "30") \
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .getOrCreate()

df3 = spark_cached.read.parquet("your_table_path/")

# Cache data
print("Caching DataFrame...")
df3.cache()
df3.count()  # Trigger caching

print("""
Executors with cached data should remain alive.
Check Spark UI:
- Executors tab: See which executors have cached data
- Storage tab: See cached RDDs/DataFrames
- Note: Executors with cached data won't be removed
""")

# Exercise 6: Queue Configuration
print("\n" + "=" * 50)
print("Exercise 6: YARN Queue Configuration")
print("=" * 50)

# Submit to specific queue
spark_queue = SparkSession.builder \
    .appName("Queue Test") \
    .config("spark.yarn.queue", "production") \
    .getOrCreate()

print(f"  Queue: {spark_queue.conf.get('spark.yarn.queue')}")
print("""
Note: Queue configuration may require:
1. YARN queue setup in capacity-scheduler.xml
2. Appropriate permissions
3. Queue capacity limits
""")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How does dynamic allocation scale executors?")
print("2. What triggers executor addition/removal?")
print("3. How do cached executors behave differently?")
print("4. Compare resource utilization: static vs dynamic")
print("5. What are the trade-offs of each allocation strategy?")
print("6. How does queue configuration affect resource allocation?")

# Cleanup
spark_static.stop()
spark_dynamic.stop()
spark_conservative.stop()
spark_aggressive.stop()
spark_cached.stop()
spark_queue.stop()

