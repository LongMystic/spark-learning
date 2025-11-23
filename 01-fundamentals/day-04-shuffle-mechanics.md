# Day 4: Shuffle Mechanics and Optimization

## 🎯 Learning Objectives
- Understand what shuffle is and when it occurs
- Learn the shuffle process in detail
- Master shuffle optimization techniques
- Diagnose shuffle-related performance issues
- Optimize shuffle for on-premise clusters

## 📚 Core Concepts

### 1. What is Shuffle?

**Definition:**
Shuffle is the process of redistributing data across partitions. It occurs when data needs to be grouped by a key that's different from the current partition key.

**When Shuffle Happens:**
- `groupBy()` / `groupByKey()`
- `join()` (unless broadcast join)
- `repartition()` / `coalesce()` with different partition count
- `distinct()` on non-partitioned columns
- `orderBy()` / `sortBy()`
- `reduceByKey()` / `aggregateByKey()`

**Example:**
```python
# No shuffle: filter and map
df.filter(df.age > 25).select("name")  # Narrow transformation

# Shuffle: groupBy
df.groupBy("category").agg(sum("amount"))  # Wide transformation

# Shuffle: join
df1.join(df2, "id")  # Wide transformation (unless broadcast)
```

### 2. Shuffle Process Overview

**Two-Phase Process:**

```
Phase 1: Map Phase (Shuffle Write)
├── Each task writes shuffle files
├── Files organized by partition ID
└── Stored in local disk (spark.local.dir)

Phase 2: Reduce Phase (Shuffle Read)
├── Tasks read shuffle files
├── From multiple map tasks
└── Network I/O intensive
```

### 3. Shuffle Write Process

**What Happens:**
1. **Map Task** processes its partition
2. **Partitioning**: Data grouped by target partition
3. **Sorting**: Data sorted within each partition (optional)
4. **Serialization**: Data serialized
5. **Writing**: Written to local disk as shuffle files

**Shuffle Files Structure:**
```
spark.local.dir/
├── shuffle_0/
│   ├── map_0.data
│   ├── map_0.index
│   ├── map_1.data
│   └── map_1.index
└── shuffle_1/
    └── ...
```

**File Components:**
- **.data files**: Actual data
- **.index files**: Index for fast lookup

### 4. Shuffle Read Process

**What Happens:**
1. **Reduce Task** needs data for its partition
2. **Fetch**: Reads shuffle files from multiple map tasks
3. **Network I/O**: Data transferred over network
4. **Deserialization**: Data deserialized
5. **Merge**: Data from multiple sources merged
6. **Processing**: Task processes the merged data

**Network Transfer:**
- Data moves from executor to executor
- Can be same node (fast) or different node (slower)
- Network bandwidth becomes bottleneck

## 🔍 Deep Dive: Shuffle Internals

### Shuffle Write Details

**Sort Shuffle (Default):**
```python
# Spark uses Sort Shuffle by default
# Process:
# 1. Accumulate records in memory
# 2. Sort by partition ID (and key if needed)
# 3. Write sorted data to disk
# 4. Create index file for fast lookup
```

**Unsafe Shuffle (Deprecated):**
- Used in older Spark versions
- Faster but less stable
- Not recommended

**Bypass Merge Sort Shuffle:**
- Used when `spark.shuffle.sort.bypassMergeThreshold` partitions
- Skips sorting for small number of partitions
- Faster for small datasets

### Shuffle Read Details

**Fetch Strategies:**
1. **Local Fetch**: Same executor (fastest)
2. **Same Rack**: Different executor, same rack
3. **Different Rack**: Network transfer required

**Merge Process:**
- Data from multiple map tasks merged
- Can use external merge if data doesn't fit in memory
- Spills to disk if needed

### Shuffle File Management

**Storage Location:**
```python
# Default: /tmp (spark.local.dir)
# Can be configured:
spark.conf.set("spark.local.dir", "/data1/spark,/data2/spark")

# Best practice: Use multiple disks
# Improves I/O parallelism
```

**Cleanup:**
- Shuffle files cleaned up after stage completes
- Can be persisted for fault tolerance
- Old shuffle files cleaned up periodically

## 💡 Shuffle Configuration

### Key Parameters

**Shuffle Partitions:**
```python
# Default: 200
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Rule of thumb: 2-3x number of cores
# Too many: Small tasks, overhead
# Too few: Large tasks, memory pressure
```

**Shuffle Memory:**
```python
# Fraction of execution memory for shuffle
# Default: 0.2 (20% of execution memory)
spark.conf.set("spark.shuffle.memoryFraction", "0.3")

# Buffer size for shuffle write
spark.conf.set("spark.shuffle.file.buffer", "64k")  # Default: 32k
```

**Shuffle Compression:**
```python
# Compress shuffle data (reduces network I/O)
spark.conf.set("spark.shuffle.compress", "true")  # Default: true
spark.conf.set("spark.shuffle.spill.compress", "true")  # Default: true

# Compression codec
spark.conf.set("spark.io.compression.codec", "lz4")  # Fast
# Options: lz4, snappy, zstd, lzf
```

**Shuffle Spill:**
```python
# Enable spilling to disk
spark.conf.set("spark.shuffle.spill", "true")  # Default: true

# Spill threshold
spark.conf.set("spark.shuffle.spill.initialMemoryThreshold", "5m")
```

## 🎯 Shuffle Optimization Strategies

### 1. Reduce Shuffle Data Size

**Column Pruning:**
```python
# Bad: Shuffles all columns
df1.join(df2, "id")

# Good: Only shuffle needed columns
df1.select("id", "col1").join(
    df2.select("id", "col2"), "id")
```

**Predicate Pushdown:**
```python
# Filter before shuffle
df1.filter(df1.status == "active").join(df2, "id")
# Instead of filtering after join
```

**Compression:**
```python
# Use efficient compression
spark.conf.set("spark.io.compression.codec", "lz4")
# lz4: Fast compression, good for shuffle
# zstd: Better compression, slightly slower
```

### 2. Optimize Shuffle Partitions

**Right Number of Partitions:**
```python
# Too many partitions (e.g., 1000)
# - Many small tasks
# - High overhead
# - Network overhead

# Too few partitions (e.g., 10)
# - Large tasks
# - Memory pressure
# - Skew issues

# Sweet spot: 2-3x number of cores
cores = spark.sparkContext.defaultParallelism
optimal_partitions = cores * 2
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
```

**Adaptive Shuffle (Spark 3.0+):**
```python
# Automatically adjusts partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
```

### 3. Optimize Shuffle I/O

**Multiple Local Directories:**
```python
# Use multiple disks for shuffle files
spark.conf.set("spark.local.dir", 
    "/data1/spark,/data2/spark,/data3/spark")

# Benefits:
# - Parallel I/O
# - Better throughput
# - Reduces disk bottleneck
```

**Network Optimization:**
```python
# For on-premise clusters
# - Ensure high-bandwidth network
# - Use dedicated network for shuffle (if available)
# - Monitor network utilization
```

### 4. Handle Data Skew

**Skew Detection:**
```python
# Check partition sizes in Spark UI
# Look for:
# - Uneven task execution times
# - Some tasks much larger than others
# - Skew metrics in stage details
```

**Skew Handling:**
```python
# Method 1: Salting (add random prefix)
from pyspark.sql.functions import col, concat, lit, rand

# Add salt to skewed key
df1_with_salt = df1.withColumn("salted_key", 
    concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# Join on salted key
result = df1_with_salt.join(df2_with_salt, "salted_key")

# Method 2: Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

## 🔍 Deep Dive: Shuffle Performance Analysis

### Metrics to Monitor

**Shuffle Write Metrics:**
- **Shuffle Write Time**: Time to write shuffle files
- **Shuffle Write Size**: Total data written
- **Shuffle Write Records**: Number of records written
- **Shuffle Write Spill**: Data spilled to disk

**Shuffle Read Metrics:**
- **Shuffle Read Time**: Time to read shuffle files
- **Shuffle Read Size**: Total data read
- **Shuffle Read Records**: Number of records read
- **Remote Bytes Read**: Data read from network
- **Local Bytes Read**: Data read locally

**Network Metrics:**
- **Network I/O**: Bytes transferred
- **Fetch Wait Time**: Time waiting for data
- **Remote Blocks Fetched**: Number of remote fetches

### Analyzing Shuffle in Spark UI

**Stages Tab:**
1. Look for stages with high shuffle read/write
2. Check shuffle size (should be reasonable)
3. Compare shuffle time vs compute time

**Tasks Tab:**
1. Check task execution times
2. Look for straggler tasks (indicates skew)
3. Check shuffle read/write per task

**SQL Tab:**
1. View query plan
2. Identify shuffle operations
3. Check if optimizations applied

## 🎯 Practical Exercises

### Exercise 1: Measure Shuffle Size

```python
# 1. Create a query with shuffle
df = spark.read.parquet("large_table/")
result = df.groupBy("category").agg(sum("amount"))

# 2. Run and check Spark UI
#    - Stages tab: Note shuffle write/read size
#    - Compare with input data size
#    - Calculate shuffle overhead

# 3. Try to reduce shuffle size
#    - Filter before groupBy
#    - Select only needed columns
#    - Compare shuffle sizes
```

### Exercise 2: Optimize Shuffle Partitions

```python
# 1. Run query with default partitions (200)
spark.conf.set("spark.sql.shuffle.partitions", "200")
df.groupBy("key").count().collect()

# 2. Measure: execution time, task count

# 3. Try different partition counts
for partitions in [50, 200, 400, 800]:
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    # Run and measure
    # Find optimal for your data size
```

### Exercise 3: Detect and Handle Skew

```python
# 1. Create skewed data
df = spark.range(0, 1000000).withColumn("key", 
    when(col("id") < 100, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string"))))

# 2. GroupBy and observe skew
result = df.groupBy("key").count()

# 3. Check Spark UI for uneven task times

# 4. Apply skew handling
#    - Use AQE skew join
#    - Or implement salting
#    - Compare performance
```

## 💡 Best Practices for On-Premise

### 1. Configure Shuffle Storage

```python
# Use multiple local directories on different disks
spark.conf.set("spark.local.dir", 
    "/data1/spark,/data2/spark,/data3/spark")

# Ensure sufficient disk space
# Rule: 2-3x your data size for shuffle
```

### 2. Tune for Your Cluster

**Small Cluster (< 10 nodes):**
```python
spark.sql.shuffle.partitions = cores * 2
spark.shuffle.file.buffer = "64k"
```

**Large Cluster (> 50 nodes):**
```python
spark.sql.shuffle.partitions = cores * 3
spark.shuffle.file.buffer = "128k"
# Consider dedicated shuffle network
```

### 3. Monitor Shuffle Health

**Weekly Review:**
- Average shuffle size per job
- Shuffle time as % of total time
- Network utilization during shuffle
- Disk I/O during shuffle

**Alerts:**
- Shuffle size > 10x input size (investigate)
- Shuffle time > 50% of total time (optimize)
- Frequent spills (increase memory)

## 🚨 Common Issues & Solutions

### Issue 1: Excessive Shuffle Data

**Symptom**: Shuffle size much larger than input size

**Root Causes:**
- Shuffling unnecessary columns
- No predicate pushdown
- Inefficient join order

**Solution:**
```python
# 1. Select only needed columns
df.select("col1", "col2").join(...)

# 2. Filter early
df.filter(...).join(...)

# 3. Use broadcast joins for small tables
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

### Issue 2: Too Many Small Shuffle Files

**Symptom**: Many small tasks, high overhead

**Root Cause**: Too many shuffle partitions

**Solution:**
```python
# Reduce partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Or use adaptive coalescing
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Issue 3: Shuffle Skew

**Symptom**: Some tasks take much longer than others

**Root Cause**: Uneven data distribution

**Solution:**
```python
# Enable AQE skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Or implement salting
# (See skew handling section above)
```

### Issue 4: Slow Shuffle Read

**Symptom**: Long fetch wait times

**Root Causes:**
- Network bottleneck
- Disk I/O bottleneck
- Too many remote fetches

**Solution:**
```python
# 1. Increase shuffle memory
spark.shuffle.memoryFraction = 0.3

# 2. Use multiple local directories
spark.local.dir = "/data1,/data2,/data3"

# 3. Optimize network (cluster-level)
#    - Ensure high bandwidth
#    - Consider dedicated network
```

### Issue 5: Shuffle Spills

**Symptom**: High disk I/O, spills visible in metrics

**Root Cause**: Insufficient memory for shuffle

**Solution:**
```python
# Increase execution memory
spark.memory.fraction = 0.8
spark.memory.storageFraction = 0.2

# Or increase executor memory
spark.executor.memory = "16g"
```

## 📝 Key Takeaways

1. **Shuffle is expensive** - avoid when possible
2. **Shuffle occurs** on wide transformations (groupBy, join, etc.)
3. **Shuffle has two phases**: write (map) and read (reduce)
4. **Network I/O** is often the bottleneck
5. **Right number of partitions** is critical (2-3x cores)
6. **Data skew** causes straggler tasks
7. **Adaptive Query Execution** can optimize shuffle automatically
8. **Monitor shuffle metrics** regularly

## 🔗 Next Steps

- **Day 5**: Partitioning Strategies
- Practice: Analyze shuffle in your common queries
- Experiment: Try different shuffle partition counts
- Optimize: Reduce shuffle size in your jobs

## 📚 Additional Resources

- [Spark Shuffle Tuning](https://spark.apache.org/docs/latest/tuning.html#shuffle-behavior)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Shuffle Internals](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)

---

**Progress**: Day 4/30+ ✅

