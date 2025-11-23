"""
Exercise 13: Memory Optimization
Purpose: Learn to optimize memory usage and configuration

Instructions:
1. Compare different memory configurations
2. Optimize garbage collection
3. Monitor memory usage
4. Handle memory-related issues
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark import StorageLevel
import time

spark = SparkSession.builder \
    .appName("Memory Optimization Exercise") \
    .getOrCreate()

# Exercise 1: Compare Memory Configurations
print("=" * 50)
print("Exercise 1: Compare Memory Configurations")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

# Configuration 1: Default
print("--- Configuration 1: Default ---")
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")

start = time.time()
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time1 = time.time() - start
print(f"  Execution time: {time1:.2f}s")
print("  Check Spark UI for memory usage and spills")

# Configuration 2: More for execution
print("\n--- Configuration 2: More for Execution ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

start = time.time()
result2 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result2.count()  # Uncomment to execute
time2 = time.time() - start
print(f"  Execution time: {time2:.2f}s")
print("  More memory for shuffles, joins, aggregations")

# Configuration 3: More for storage
print("\n--- Configuration 3: More for Storage ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")

start = time.time()
result3 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result3.count()  # Uncomment to execute
time3 = time.time() - start
print(f"  Execution time: {time3:.2f}s")
print("  More memory for caching")

print("\nResults Summary:")
print(f"  Default: {time1:.2f}s")
print(f"  Execution-heavy: {time2:.2f}s")
print(f"  Storage-heavy: {time3:.2f}s")

# Exercise 2: Garbage Collection Tuning
print("\n" + "=" * 50)
print("Exercise 2: Garbage Collection")
print("=" * 50)

# Enable GC logging
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-XX:ConcGCThreads=20 "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCDateStamps "
    "-Xloggc:/tmp/gc.log")

print("G1GC Configuration:")
print("  MaxGCPauseMillis: 200ms")
print("  G1HeapRegionSize: 16m")
print("  InitiatingHeapOccupancyPercent: 35%")

print("\nRunning memory-intensive query...")
start = time.time()
result_gc = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result_gc.count()  # Uncomment to execute
time_gc = time.time() - start

print(f"  Execution time: {time_gc:.2f}s")
print("""
Check GC log (/tmp/gc.log):
- Count of GC events
- Average GC pause time
- Total GC time
- GC frequency
""")

# Exercise 3: Monitor Memory Usage
print("\n" + "=" * 50)
print("Exercise 3: Monitor Memory Usage")
print("=" * 50)

# Cache a DataFrame
print("Caching DataFrame...")
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
print("  - Memory Used vs Memory Total")
print("  - Peak Memory Usage")
print("  - GC Time")
print("  - GC Count")

# Exercise 4: Storage Levels
print("\n" + "=" * 50)
print("Exercise 4: Compare Storage Levels")
print("=" * 50)

# Test different storage levels
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

# Exercise 5: Memory Spill Detection
print("\n" + "=" * 50)
print("Exercise 5: Memory Spill Detection")
print("=" * 50)

print("Running query that may cause spills...")
start = time.time()
result_spill = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result_spill.count()  # Uncomment to execute
time_spill = time.time() - start

print(f"  Execution time: {time_spill:.2f}s")
print("""
Check Spark UI - Stages tab:
- Look for "Spill (Memory)" metrics
- Look for "Spill (Disk)" metrics
- High spills indicate memory pressure
""")

# If spills detected, increase memory
if True:  # Replace with actual spill detection
    print("\nIf spills detected, try:")
    print("  - Increase executor memory")
    print("  - Increase memory fraction")
    print("  - Reduce storage fraction")

# Exercise 6: Reduce Memory Usage in Code
print("\n" + "=" * 50)
print("Exercise 6: Code Optimization")
print("=" * 50)

# Bad: Using inefficient data types
print("--- Inefficient Data Types ---")
df_bad = spark.range(0, 1000000).withColumn("id_str", col("id").cast("string"))
print("  Using String for IDs (inefficient)")

# Good: Use appropriate types
print("\n--- Efficient Data Types ---")
df_good = spark.range(0, 1000000).withColumn("id_int", col("id").cast("int"))
print("  Using Integer for IDs (efficient)")

# Column pruning
print("\n--- Column Pruning ---")
df_all = spark.read.parquet("your_table_path/")
df_pruned = df_all.select("col1", "col2")  # Only needed columns
print("  Only select needed columns")

# Exercise 7: Memory Overhead Tuning
print("\n" + "=" * 50)
print("Exercise 7: Memory Overhead")
print("=" * 50)

# Default overhead
print("--- Default Overhead (10%) ---")
spark.conf.set("spark.executor.memory", "14g")
# Default: 10% of executor memory, min 384MB
print("  Overhead: ~1.4GB (10% of 14GB)")

# Increased overhead
print("\n--- Increased Overhead (20%) ---")
spark.conf.set("spark.executor.memoryOverhead", "3g")
print("  Overhead: 3GB")
print("  Use for: Native memory, Python processes, JNI libraries")

# Exercise 8: Driver Memory
print("\n" + "=" * 50)
print("Exercise 8: Driver Memory")
print("=" * 50)

# For large result collections
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")

print("Driver Memory Configuration:")
print(f"  Driver Memory: {spark.conf.get('spark.driver.memory')}")
print(f"  Max Result Size: {spark.conf.get('spark.driver.maxResultSize')}")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. What is the optimal memory fraction for your workload?")
print("2. How does GC configuration affect performance?")
print("3. What storage level is best for your use case?")
print("4. How can you reduce memory usage in code?")
print("5. When should you increase memory overhead?")
print("6. Check Spark UI - what are the memory usage patterns?")
print("7. How do spills affect performance?")

spark.stop()

