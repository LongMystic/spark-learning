"""
Exercise 11: Shuffle Optimization
Purpose: Learn to optimize shuffle operations

Instructions:
1. Measure shuffle size and performance
2. Optimize shuffle partitions
3. Compare compression codecs
4. Reduce shuffle data size
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
import time

spark = SparkSession.builder \
    .appName("Shuffle Optimization Exercise") \
    .getOrCreate()

# Exercise 1: Measure Shuffle Size
print("=" * 50)
print("Exercise 1: Measure Shuffle Size")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

print("Running query with shuffle (groupBy)...")
print("  Check Spark UI - Stages tab for shuffle metrics")

start = time.time()
result1 = df.groupBy("category").agg(
    spark_sum("amount").alias("total"),
    count("*").alias("count")
)
# result1.count()  # Uncomment to execute
time1 = time.time() - start

print(f"  Execution time: {time1:.2f}s")
print("""
In Spark UI, check:
- Shuffle Write Size
- Shuffle Read Size
- Shuffle Write Time
- Shuffle Read Time
- Compare with input data size
""")

# Exercise 2: Optimize Shuffle Partitions
print("\n" + "=" * 50)
print("Exercise 2: Optimize Shuffle Partitions")
print("=" * 50)

partition_counts = [50, 200, 400, 800]
results = []

for partitions in partition_counts:
    print(f"\n--- Testing with {partitions} partitions ---")
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    
    start = time.time()
    result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
    # result.count()  # Uncomment to execute
    elapsed = time.time() - start
    
    results.append((partitions, elapsed))
    print(f"  Execution time: {elapsed:.2f}s")
    print(f"  Check Spark UI for task count and distribution")

print("\nResults Summary:")
for partitions, elapsed in results:
    print(f"  {partitions} partitions: {elapsed:.2f}s")

optimal_partitions = min(results, key=lambda x: x[1])[0]
print(f"\nOptimal partition count: {optimal_partitions}")

# Exercise 3: Column Pruning
print("\n" + "=" * 50)
print("Exercise 3: Column Pruning")
print("=" * 50)

# Bad: Shuffle all columns
print("--- Without Column Pruning ---")
start = time.time()
result_bad = df.join(df, "id")  # Shuffles all columns
# result_bad.count()  # Uncomment to execute
time_bad = time.time() - start
print(f"  Execution time: {time_bad:.2f}s")

# Good: Only shuffle needed columns
print("\n--- With Column Pruning ---")
start = time.time()
result_good = df.select("id", "category", "amount").join(
    df.select("id", "status"), "id"
)  # Only shuffles selected columns
# result_good.count()  # Uncomment to execute
time_good = time.time() - start
print(f"  Execution time: {time_good:.2f}s")

print(f"\nImprovement: {((time_bad - time_good) / time_bad * 100):.1f}%")
print("  Column pruning reduces shuffle data size significantly")

# Exercise 4: Predicate Pushdown
print("\n" + "=" * 50)
print("Exercise 4: Predicate Pushdown")
print("=" * 50)

# Bad: Filter after join
print("--- Filter After Join ---")
start = time.time()
result_bad = df.join(df, "id").filter(col("status") == "active")
# result_bad.count()  # Uncomment to execute
time_bad = time.time() - start
print(f"  Execution time: {time_bad:.2f}s")
print("  Shuffles all data, then filters")

# Good: Filter before join
print("\n--- Filter Before Join ---")
start = time.time()
result_good = df.filter(col("status") == "active").join(
    df.filter(col("status") == "active"), "id"
)
# result_good.count()  # Uncomment to execute
time_good = time.time() - start
print(f"  Execution time: {time_good:.2f}s")
print("  Filters first, shuffles less data")

print(f"\nImprovement: {((time_bad - time_good) / time_bad * 100):.1f}%")

# Exercise 5: Compression Codecs
print("\n" + "=" * 50)
print("Exercise 5: Compression Codecs")
print("=" * 50)

codecs = ["lz4", "snappy", "zstd"]
compression_results = []

for codec in codecs:
    print(f"\n--- Testing {codec} compression ---")
    spark.conf.set("spark.shuffle.compress", "true")
    spark.conf.set("spark.io.compression.codec", codec)
    
    start = time.time()
    result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
    # result.count()  # Uncomment to execute
    elapsed = time.time() - start
    
    compression_results.append((codec, elapsed))
    print(f"  Execution time: {elapsed:.2f}s")
    print("  Check Spark UI for shuffle size")

print("\nCompression Results:")
for codec, elapsed in compression_results:
    print(f"  {codec}: {elapsed:.2f}s")

# Exercise 6: Adaptive Shuffle (Spark 3.0+)
print("\n" + "=" * 50)
print("Exercise 6: Adaptive Shuffle")
print("=" * 50)

# Enable adaptive shuffle
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")

print("Adaptive Shuffle Enabled:")
print(f"  AQE Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  Coalesce Partitions: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")

print("\nRunning query with adaptive shuffle...")
start = time.time()
result6 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result6.count()  # Uncomment to execute
time_adaptive = time.time() - start

print(f"  Execution time: {time_adaptive:.2f}s")
print("""
Check Spark UI:
- Partitions may be coalesced after shuffle
- Fewer partitions than configured
- Better resource utilization
""")

# Exercise 7: Shuffle File Management
print("\n" + "=" * 50)
print("Exercise 7: Multiple Local Directories")
print("=" * 50)

# Configure multiple local directories
spark.conf.set("spark.local.dir", "/data1/spark,/data2/spark,/data3/spark")

print("Multiple Local Directories Configured:")
print(f"  Local Dirs: {spark.conf.get('spark.local.dir')}")
print("""
Benefits:
- Parallel I/O across disks
- Better throughput
- Reduces disk bottleneck

Note: Update paths to match your cluster
""")

# Exercise 8: Shuffle Buffer Tuning
print("\n" + "=" * 50)
print("Exercise 8: Shuffle Buffer Tuning")
print("=" * 50)

# Default buffers
print("--- Default Buffers ---")
spark.conf.set("spark.shuffle.file.buffer", "32k")
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")

start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_default = time.time() - start
print(f"  Execution time: {time_default:.2f}s")

# Increased buffers
print("\n--- Increased Buffers ---")
spark.conf.set("spark.shuffle.file.buffer", "64k")
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_increased = time.time() - start
print(f"  Execution time: {time_increased:.2f}s")

print(f"\nDifference: {time_increased - time_default:.2f}s")
print("  Larger buffers = less I/O, more memory")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. What is the optimal number of shuffle partitions?")
print("2. How does column pruning affect shuffle size?")
print("3. What is the impact of compression on shuffle?")
print("4. How does adaptive shuffle optimize partitions?")
print("5. What are the trade-offs of larger buffers?")
print("6. Check Spark UI - how do optimizations affect shuffle metrics?")

spark.stop()

