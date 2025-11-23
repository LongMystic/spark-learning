"""
Exercise 4: Shuffle Mechanics
Purpose: Understand shuffle operations and optimization

Instructions:
1. Analyze shuffle in Spark UI
2. Measure shuffle size and performance
3. Optimize shuffle partitions
4. Compare shuffle configurations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import time

spark = SparkSession.builder \
    .appName("Shuffle Mechanics Exercise") \
    .getOrCreate()

# Exercise 1: Identify Shuffle Operations
print("=" * 50)
print("Exercise 1: Identify Shuffle Operations")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

# Operations that cause shuffle
print("Operations that cause shuffle:")

# GroupBy (shuffle)
print("\n1. GroupBy (causes shuffle):")
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
result1.explain()
print("  Check explain plan - should show Exchange (shuffle)")

# Join (shuffle)
print("\n2. Join (causes shuffle):")
df2 = spark.read.parquet("table2_path/")
result2 = df.join(df2, "id")
result2.explain()
print("  Check explain plan - should show Exchange (shuffle)")

# Filter (no shuffle)
print("\n3. Filter (no shuffle):")
result3 = df.filter(col("status") == "active")
result3.explain()
print("  Check explain plan - no Exchange (narrow transformation)")

# Exercise 2: Measure Shuffle Size
print("\n" + "=" * 50)
print("Exercise 2: Measure Shuffle Size")
print("=" * 50)

print("Running query with shuffle...")
start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time1 = time.time() - start

print(f"  Execution time: {time1:.2f}s")
print("""
Check Spark UI - Stages tab:
- Shuffle Write Size
- Shuffle Read Size
- Shuffle Write Time
- Shuffle Read Time
- Compare with input data size
""")

# Exercise 3: Optimize Shuffle Partitions
print("\n" + "=" * 50)
print("Exercise 3: Optimize Shuffle Partitions")
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
    print(f"  Check Spark UI for task count")

print("\nResults Summary:")
for partitions, elapsed in results:
    print(f"  {partitions} partitions: {elapsed:.2f}s")

# Exercise 4: Shuffle Compression
print("\n" + "=" * 50)
print("Exercise 4: Shuffle Compression")
print("=" * 50)

# Without compression
print("--- Without Compression ---")
spark.conf.set("spark.shuffle.compress", "false")
start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_no_comp = time.time() - start
print(f"  Execution time: {time_no_comp:.2f}s")

# With compression
print("\n--- With Compression (lz4) ---")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")
start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_comp = time.time() - start
print(f"  Execution time: {time_comp:.2f}s")
print("  Check Spark UI for shuffle size difference")

# Exercise 5: Shuffle File Management
print("\n" + "=" * 50)
print("Exercise 5: Multiple Local Directories")
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

# Exercise 6: Adaptive Shuffle (Spark 3.0+)
print("\n" + "=" * 50)
print("Exercise 6: Adaptive Shuffle")
print("=" * 50)

# Enable adaptive shuffle
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

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

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. Which operations cause shuffle?")
print("2. What is the shuffle size compared to input size?")
print("3. What is the optimal number of shuffle partitions?")
print("4. How does compression affect shuffle?")
print("5. How does adaptive shuffle optimize partitions?")
print("6. Check Spark UI - what are the shuffle metrics?")

spark.stop()

