"""
Exercise 9: Resource Allocation and Kubernetes Integration
Purpose: Understand dynamic allocation and resource management

Instructions:
1. Configure static vs dynamic allocation
2. Monitor resource allocation on Kubernetes (pods, requests, limits)
3. Compare resource utilization patterns
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, sum as spark_sum

# NOTE: allocation settings (executor.instances, dynamicAllocation.*,
# spark.kubernetes.namespace) only take effect at submit time on a Kubernetes
# cluster (spark-submit --master k8s:// or a SparkApplication CRD). Locally we
# just SET and PRINT them so you can see exactly what you'd pass at submit time.
# `show()` reads a conf back with a friendly default when it isn't applicable
# locally.
spark = get_spark("Resource Allocation Exercise")


def show(key):
    return spark.conf.get(key, "(cluster-only)")


# Exercise 1: Static Allocation
print("=" * 50)
print("Exercise 1: Static Allocation")
print("=" * 50)

spark.conf.set("spark.executor.instances", "10")
print("Static Allocation Configuration (submit-time on Kubernetes):")
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
    # REQUIRED on K8S: there is NO external shuffle service, so shuffle tracking
    # keeps executors holding shuffle blocks alive instead of losing that data.
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.timeout": "30m",
    "spark.dynamicAllocation.minExecutors": "5",
    "spark.dynamicAllocation.maxExecutors": "30",
    "spark.dynamicAllocation.initialExecutors": "10",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "infinity",
    "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
}
for k, v in dynamic_conf.items():
    spark.conf.set(k, v)

print("Dynamic Allocation Configuration (submit-time on Kubernetes):")
print(f"  Dynamic Allocation: {show('spark.dynamicAllocation.enabled')}")
print(f"  Shuffle Tracking (required on K8S): {show('spark.dynamicAllocation.shuffleTracking.enabled')}")
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
1. Open the Spark UI (port-forward the driver pod):
     kubectl -n spark-jobs port-forward <driver-pod> 4040
   Navigate to the Executors tab and observe:
   - Number of executors over time
   - When executor pods are added/removed
   - Resource utilization per executor
2. Watch the pods directly with kubectl:
     kubectl -n spark-jobs get pods -w      # driver + executor pods appear/disappear
     kubectl top pods -n spark-jobs         # live CPU/mem per pod
     kubectl -n spark-jobs describe pod <executor-pod>   # requests/limits, events
3. Check:
   - Executor pod requests/limits (the allocation unit)
   - Namespace ResourceQuota utilization
   - kubectl top nodes (node allocatable vs used)
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

# Exercise 6: Namespace + ResourceQuota Configuration
print("\n" + "=" * 50)
print("Exercise 6: Namespace + ResourceQuota Configuration")
print("=" * 50)

# Submit into a namespace (K8S): spark-submit --conf spark.kubernetes.namespace=production ...
# Namespaces + ResourceQuota replace YARN queues for multi-tenancy.
spark.conf.set("spark.kubernetes.namespace", "production")
print(f"  Namespace: {show('spark.kubernetes.namespace')}")
print("""
Note: Namespace + quota configuration may require:
1. A Namespace + ResourceQuota (+ LimitRange) manifest applied by the platform team
   (requests.cpu/memory = guaranteed share; limits.cpu/memory = burst ceiling)
2. A ServiceAccount + RBAC so the driver can create executor pods
3. Per-namespace pod/CPU/memory caps enforced by the ResourceQuota
""")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How does dynamic allocation scale executors?")
print("2. What triggers executor addition/removal?")
print("3. How do cached executors behave differently?")
print("4. Compare resource utilization: static vs dynamic")
print("5. What are the trade-offs of each allocation strategy?")
print("6. How does namespace + ResourceQuota configuration affect resource allocation?")

# Cleanup
spark.stop()

