"""
Exercise 3: Memory Management
Purpose: Understand Spark memory architecture and optimization

Instructions:
1. Monitor memory usage in Spark UI
2. Compare different memory configurations
3. Analyze garbage collection
4. Optimize memory settings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark import StorageLevel
import time

spark = SparkSession.builder \
    .appName("Memory Management Exercise") \
    .getOrCreate()

# Exercise 1: Monitor Memory Usage
print("=" * 50)
print("Exercise 1: Monitor Memory Usage")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

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

# Exercise 2: Compare Memory Configurations
print("\n" + "=" * 50)
print("Exercise 2: Compare Memory Configurations")
print("=" * 50)

# Configuration 1: Default
print("--- Configuration 1: Default ---")
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")

start = time.time()
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time1 = time.time() - start
print(f"  Execution time: {time1:.2f}s")
print("  Check for spills in Spark UI")

# Configuration 2: More for execution
print("\n--- Configuration 2: More for Execution ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

start = time.time()
result2 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result2.count()  # Uncomment to execute
time2 = time.time() - start
print(f"  Execution time: {time2:.2f}s")

# Configuration 3: More for storage
print("\n--- Configuration 3: More for Storage ---")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")

start = time.time()
result3 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result3.count()  # Uncomment to execute
time3 = time.time() - start
print(f"  Execution time: {time3:.2f}s")

# Exercise 3: Garbage Collection
print("\n" + "=" * 50)
print("Exercise 3: Garbage Collection")
print("=" * 50)

# Enable GC logging
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCDateStamps "
    "-Xloggc:/tmp/gc.log")

print("G1GC Configuration:")
print("  MaxGCPauseMillis: 200ms")
print("  G1HeapRegionSize: 16m")
print("  GC Log: /tmp/gc.log")

print("\nRunning memory-intensive query...")
start = time.time()
result_gc = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result_gc.count()  # Uncomment to execute
time_gc = time.time() - start

print(f"  Execution time: {time_gc:.2f}s")
print("""
Analyze GC log:
- Count of GC events
- Average GC pause time
- Total GC time
- GC frequency
""")

# Exercise 4: Storage Levels
print("\n" + "=" * 50)
print("Exercise 4: Compare Storage Levels")
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

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. What is the memory usage pattern in Spark UI?")
print("2. How do memory fractions affect performance?")
print("3. What is the impact of GC configuration?")
print("4. Which storage level is best for your use case?")
print("5. How do spills affect performance?")

spark.stop()

