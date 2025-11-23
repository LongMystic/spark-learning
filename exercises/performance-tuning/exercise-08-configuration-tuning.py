"""
Exercise 8: Configuration Tuning
Purpose: Learn to calculate and optimize Spark configurations

Instructions:
1. Calculate optimal configuration for your cluster
2. Compare different configuration strategies
3. Measure performance impact of configuration changes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
import time

# Exercise 1: Calculate Optimal Configuration
print("=" * 50)
print("Exercise 1: Calculate Optimal Configuration")
print("=" * 50)

# TODO: Update with your cluster specifications
cluster_specs = {
    'nodes': 20,
    'ram_per_node_gb': 64,
    'cores_per_node': 16,
    'os_reserved_gb': 16
}

def calculate_optimal_config(specs):
    """Calculate optimal Spark configuration"""
    nodes = specs['nodes']
    ram_per_node = specs['ram_per_node_gb']
    cores_per_node = specs['cores_per_node']
    os_reserved = specs['os_reserved_gb']
    
    # Calculate available resources
    available_ram = (ram_per_node - os_reserved) * nodes
    executors_per_node = 3  # Best practice: 2-3 executors per node
    total_executors = nodes * executors_per_node
    
    # Calculate per-executor resources
    executor_memory = int((available_ram / total_executors) * 0.9)  # 90% for executor
    executor_cores = (cores_per_node - 1) // executors_per_node  # Leave 1 core for OS
    executor_overhead = max(int(executor_memory * 0.1), 2)  # 10% or min 2GB
    
    # Calculate parallelism
    total_cores = total_executors * executor_cores
    optimal_parallelism = total_cores * 2  # 2-3x cores
    
    config = {
        'executor_memory': f"{executor_memory}g",
        'executor_cores': executor_cores,
        'executor_instances': total_executors,
        'executor_memory_overhead': f"{executor_overhead}g",
        'default_parallelism': optimal_parallelism,
        'shuffle_partitions': optimal_parallelism
    }
    
    return config

optimal_config = calculate_optimal_config(cluster_specs)
print("\nOptimal Configuration:")
for key, value in optimal_config.items():
    print(f"  {key}: {value}")

# Exercise 2: Apply Configuration
print("\n" + "=" * 50)
print("Exercise 2: Apply Configuration")
print("=" * 50)

spark = SparkSession.builder \
    .appName("Configuration Tuning Exercise") \
    .config("spark.executor.memory", optimal_config['executor_memory']) \
    .config("spark.executor.cores", str(optimal_config['executor_cores'])) \
    .config("spark.executor.instances", str(optimal_config['executor_instances'])) \
    .config("spark.executor.memoryOverhead", optimal_config['executor_memory_overhead']) \
    .config("spark.default.parallelism", str(optimal_config['default_parallelism'])) \
    .config("spark.sql.shuffle.partitions", str(optimal_config['shuffle_partitions'])) \
    .getOrCreate()

print("\nApplied Configuration:")
print(f"  Executor Memory: {spark.conf.get('spark.executor.memory')}")
print(f"  Executor Cores: {spark.conf.get('spark.executor.cores')}")
print(f"  Executor Instances: {spark.conf.get('spark.executor.instances')}")
print(f"  Default Parallelism: {spark.conf.get('spark.default.parallelism')}")
print(f"  Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Exercise 3: Compare Different Configurations
print("\n" + "=" * 50)
print("Exercise 3: Compare Configurations")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

# Configuration 1: Conservative (small executors)
print("\n--- Configuration 1: Conservative ---")
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.sql.shuffle.partitions", "200")

start = time.time()
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time1 = time.time() - start
print(f"  Execution time: {time1:.2f}s")

# Configuration 2: Aggressive (large executors)
print("\n--- Configuration 2: Aggressive ---")
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.cores", "8")
spark.conf.set("spark.sql.shuffle.partitions", "400")

start = time.time()
result2 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result2.count()  # Uncomment to execute
time2 = time.time() - start
print(f"  Execution time: {time2:.2f}s")

# Configuration 3: Optimal (calculated)
print("\n--- Configuration 3: Optimal ---")
spark.conf.set("spark.executor.memory", optimal_config['executor_memory'])
spark.conf.set("spark.executor.cores", str(optimal_config['executor_cores']))
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_config['shuffle_partitions']))

start = time.time()
result3 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result3.count()  # Uncomment to execute
time3 = time.time() - start
print(f"  Execution time: {time3:.2f}s")

# Exercise 4: Workload-Specific Configuration
print("\n" + "=" * 50)
print("Exercise 4: Workload-Specific Configuration")
print("=" * 50)

# ETL Workload (cache-heavy)
print("\n--- ETL Workload Configuration ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")
print("  Memory Fraction: 0.8")
print("  Storage Fraction: 0.6 (more for caching)")

# Analytical Workload (compute-heavy)
print("\n--- Analytical Workload Configuration ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
print("  Memory Fraction: 0.8")
print("  Storage Fraction: 0.3 (more for execution)")

# Exercise 5: Adaptive Query Execution
print("\n" + "=" * 50)
print("Exercise 5: Enable AQE")
print("=" * 50)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

print("  AQE Enabled: true")
print("  Coalesce Partitions: true")
print("  Skew Join Handling: true")

result4 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
print("\nExecution Plan with AQE:")
result4.explain(extended=True)

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. What is the optimal executor size for your cluster?")
print("2. How does executor size affect performance?")
print("3. What is the impact of parallelism on job performance?")
print("4. How do memory fractions affect different workloads?")
print("5. What benefits does AQE provide?")
print("6. Check Spark UI - how do configurations affect resource usage?")

spark.stop()

