"""
Exercise 9: Resource Allocation and YARN Integration
Purpose: Understand dynamic allocation and resource management

Instructions:
1. Configure static vs dynamic allocation
2. Monitor resource allocation in YARN
3. Compare resource utilization patterns
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, sum as spark_sum

# NOTE: allocation settings (executor.instances, dynamicAllocation.*, yarn.queue)
# only take effect at submit time on a YARN cluster. Locally we just SET and PRINT
# them so you can see exactly what you'd pass to spark-submit. `show()` reads a
# conf back with a friendly default when it isn't applicable locally.
spark = get_spark("Resource Allocation Exercise")


def show(key):
    return spark.conf.get(key, "(cluster-only)")


# Exercise 1: Static Allocation
print("=" * 50)
print("Exercise 1: Static Allocation")
print("=" * 50)

spark.conf.set("spark.executor.instances", "10")
print("Static Allocation Configuration (submit-time on YARN):")
print("  spark.dynamicAllocation.enabled = false")
print(f"  spark.executor.instances = {show('spark.executor.instances')}")

df = read_table(spark, "transactions")

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

dynamic_conf = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "5",
    "spark.dynamicAllocation.maxExecutors": "30",
    "spark.dynamicAllocation.initialExecutors": "10",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "infinity",
    "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
}
for k, v in dynamic_conf.items():
    spark.conf.set(k, v)

print("Dynamic Allocation Configuration (submit-time on YARN):")
print(f"  Dynamic Allocation: {show('spark.dynamicAllocation.enabled')}")
print(f"  Min Executors: {show('spark.dynamicAllocation.minExecutors')}")
print(f"  Max Executors: {show('spark.dynamicAllocation.maxExecutors')}")
print(f"  Initial Executors: {show('spark.dynamicAllocation.initialExecutors')}")

df2 = read_table(spark, "transactions")

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
print("  Min: 5, Max: 20, Initial: 5")
print("  Use case: Shared cluster, multiple users")

# Aggressive Strategy
print("\n--- Aggressive Strategy ---")
print("  Min: 20, Max: 100, Initial: 50")
print("  Use case: Dedicated resources, performance critical")

# Exercise 5: Cached Executor Behavior
print("\n" + "=" * 50)
print("Exercise 5: Cached Executor Behavior")
print("=" * 50)

df3 = read_table(spark, "transactions")

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

# Submit to specific queue (YARN): spark-submit --queue production ...
spark.conf.set("spark.yarn.queue", "production")
print(f"  Queue: {show('spark.yarn.queue')}")
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
spark.stop()

