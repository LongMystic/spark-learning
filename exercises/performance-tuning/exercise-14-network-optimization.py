"""
Exercise 14: Network Optimization
Purpose: Learn to optimize network I/O and shuffle operations

Instructions:
1. Measure network usage during shuffle
2. Optimize shuffle data size
3. Compare compression codecs
4. Tune network settings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast
import time

spark = SparkSession.builder \
    .appName("Network Optimization Exercise") \
    .getOrCreate()

# Exercise 1: Measure Network Usage
print("=" * 50)
print("Exercise 1: Measure Network Usage")
print("=" * 50)

# TODO: Replace with your actual table path
df = spark.read.parquet("your_table_path/")

print("Running query with shuffle (groupBy)...")
print("  Check Spark UI for network metrics")

start = time.time()
result1 = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result1.count()  # Uncomment to execute
time1 = time.time() - start

print(f"  Execution time: {time1:.2f}s")
print("""
In Spark UI - Stages tab, check:
- Shuffle Read Size
- Remote Bytes Read
- Fetch Wait Time
- Network I/O metrics
""")

# Exercise 2: Reduce Shuffle Data Size
print("\n" + "=" * 50)
print("Exercise 2: Reduce Shuffle Data")
print("=" * 50)

# Bad: Shuffle all columns
print("--- Shuffle All Columns ---")
start = time.time()
result_bad = df.join(df, "id")  # Shuffles all columns
# result_bad.count()  # Uncomment to execute
time_bad = time.time() - start
print(f"  Execution time: {time_bad:.2f}s")

# Good: Only shuffle needed columns
print("\n--- Shuffle Only Needed Columns ---")
start = time.time()
result_good = df.select("id", "category", "amount").join(
    df.select("id", "status"), "id"
)
# result_good.count()  # Uncomment to execute
time_good = time.time() - start
print(f"  Execution time: {time_good:.2f}s")
print("  Less data = less network transfer")

print(f"\nImprovement: {((time_bad - time_good) / time_bad * 100):.1f}%")

# Exercise 3: Compression Codecs
print("\n" + "=" * 50)
print("Exercise 3: Compression Codecs")
print("=" * 50)

codecs = ["lz4", "snappy", "zstd"]
compression_results = []

for codec in codecs:
    print(f"\n--- Testing {codec} ---")
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

print("\nRecommendation: Use lz4 for shuffle (fast compression)")

# Exercise 4: Shuffle Fetch Optimization
print("\n" + "=" * 50)
print("Exercise 4: Shuffle Fetch Settings")
print("=" * 50)

# Default settings
print("--- Default Settings ---")
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")
spark.conf.set("spark.reducer.maxReqsInFlight", "1")

start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_default = time.time() - start
print(f"  Execution time: {time_default:.2f}s")

# Optimized settings
print("\n--- Optimized Settings ---")
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
spark.conf.set("spark.reducer.maxReqsInFlight", "1")

start = time.time()
result = df.groupBy("category").agg(spark_sum("amount").alias("total"))
# result.count()  # Uncomment to execute
time_optimized = time.time() - start
print(f"  Execution time: {time_optimized:.2f}s")
print("  More data in flight = better network utilization")

# Exercise 5: Broadcast to Avoid Network Transfer
print("\n" + "=" * 50)
print("Exercise 5: Broadcast Join")
print("=" * 50)

# Create small table
df_small = spark.range(0, 1000).withColumn("key", col("id"))

# Without broadcast (shuffle)
print("--- Without Broadcast (Shuffle) ---")
start = time.time()
result_no_broadcast = df.join(df_small, "key")
# result_no_broadcast.count()  # Uncomment to execute
time_no_broadcast = time.time() - start
print(f"  Execution time: {time_no_broadcast:.2f}s")
print("  Small table shuffled over network")

# With broadcast (no shuffle for small table)
print("\n--- With Broadcast (No Shuffle) ---")
start = time.time()
result_broadcast = df.join(broadcast(df_small), "key")
# result_broadcast.count()  # Uncomment to execute
time_broadcast = time.time() - start
print(f"  Execution time: {time_broadcast:.2f}s")
print("  Small table broadcasted once, no network shuffle")

print(f"\nImprovement: {((time_no_broadcast - time_broadcast) / time_no_broadcast * 100):.1f}%")

# Exercise 6: Network Timeout Configuration
print("\n" + "=" * 50)
print("Exercise 6: Network Timeout")
print("=" * 50)

# Default timeout
print("--- Default Timeout (120s) ---")
spark.conf.set("spark.network.timeout", "120s")
print(f"  Network Timeout: {spark.conf.get('spark.network.timeout')}")

# Increased timeout (for slow networks)
print("\n--- Increased Timeout (300s) ---")
spark.conf.set("spark.network.timeout", "300s")
spark.conf.set("spark.rpc.askTimeout", "300s")
print(f"  Network Timeout: {spark.conf.get('spark.network.timeout')}")
print("  Use for: Slow networks, large data transfers")

# Exercise 7: Connection Pooling
print("\n" + "=" * 50)
print("Exercise 7: Connection Settings")
print("=" * 50)

# Default connections
print("--- Default Connections ---")
spark.conf.set("spark.shuffle.io.numConnectionsPerPeer", "1")
print(f"  Connections per peer: {spark.conf.get('spark.shuffle.io.numConnectionsPerPeer')}")

print("\nRecommendation:")
print("  - One connection per peer typically sufficient")
print("  - More connections = more overhead")
print("  - Adjust based on network capacity")

# Exercise 8: Monitor Network Health
print("\n" + "=" * 50)
print("Exercise 8: Network Monitoring")
print("=" * 50)

print("""
Key Metrics to Monitor:
1. Network I/O during shuffle
2. Bytes sent/received
3. Network utilization
4. Fetch wait time
5. Remote vs local bytes read

Tools:
- Spark UI (Stages, Tasks tabs)
- Network monitoring tools
- Cluster monitoring (Ganglia, Prometheus)
""")

# Exercise 9: Reduce Network Usage
print("\n" + "=" * 50)
print("Exercise 9: Reduce Network Usage")
print("=" * 50)

print("Strategies to reduce network usage:")
print("  1. Minimize shuffle data size (column pruning)")
print("  2. Filter before joins/groupBy")
print("  3. Use broadcast joins for small tables")
print("  4. Enable compression")
print("  5. Optimize partition count")
print("  6. Improve data locality")

# Example: Combined optimizations
print("\n--- Combined Optimizations ---")
df_optimized = df.select("id", "category", "amount").filter(col("status") == "active")
df_small_optimized = df_small.select("id", "key")

start = time.time()
result_optimized = df_optimized.join(broadcast(df_small_optimized), "key")
# result_optimized.count()  # Uncomment to execute
time_optimized = time.time() - start

print(f"  Execution time: {time_optimized:.2f}s")
print("  Applied: column pruning, filtering, broadcast")

# Exercise 10: Network Topology
print("\n" + "=" * 50)
print("Exercise 10: Network Topology")
print("=" * 50)

print("""
For On-Premise Clusters:
1. Understand network layout
2. Enable rack awareness (if available)
3. Optimize for network topology
4. Monitor cross-rack traffic

YARN Rack Awareness:
- Prefers same rack
- Reduces cross-rack traffic
- Better network utilization
""")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How much network I/O occurs during shuffle?")
print("2. What is the impact of compression on network transfer?")
print("3. How do broadcast joins reduce network usage?")
print("4. What are optimal shuffle fetch settings?")
print("5. How does network timeout affect performance?")
print("6. Check Spark UI - what are network usage patterns?")
print("7. How can you reduce network usage in your queries?")

spark.stop()

