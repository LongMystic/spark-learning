# Day 11: Shuffle Optimization

## 🎯 Learning Objectives
- Master shuffle optimization techniques
- Learn to reduce shuffle data size
- Optimize shuffle partitions and I/O
- Handle shuffle-related performance issues
- Tune shuffle for on-premise clusters

## 📚 Core Concepts

### 1. Shuffle Cost Analysis

**Shuffle Components:**
- **Network I/O**: Data transfer between nodes
- **Disk I/O**: Writing and reading shuffle files
- **CPU**: Serialization/deserialization
- **Memory**: Buffering during shuffle

**Cost Factors:**
- **Data Size**: Larger data = more expensive
- **Number of Partitions**: More partitions = more overhead
- **Network Bandwidth**: Bottleneck for large shuffles
- **Disk Speed**: Affects shuffle file I/O

**Optimization Goal:**
Minimize shuffle data size, optimize partition count, reduce I/O

### 2. Reducing Shuffle Data Size

**Strategies:**
1. **Column Pruning**: Only shuffle needed columns
2. **Predicate Pushdown**: Filter before shuffle
3. **Compression**: Compress shuffle data
4. **Data Type Optimization**: Use efficient data types

**Example:**
```python
# Bad: Shuffles all columns
df1.join(df2, "id")

# Good: Only shuffle needed columns
df1.select("id", "col1").join(
    df2.select("id", "col2"), "id")
```

### 3. Optimizing Shuffle Partitions

**Key Parameter:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default: 200
```

**Choosing Right Number:**
- **Too Many**: Small tasks, high overhead
- **Too Few**: Large tasks, memory pressure
- **Optimal**: 2-3x number of cores

**Calculation:**
```python
total_cores = executor_cores * executor_instances
optimal_partitions = total_cores * 2
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
```

## 🔍 Deep Dive: Shuffle Optimization Techniques

### 1. Column Pruning

**Principle**: Only shuffle columns that are needed

**Implementation:**
```python
# Before: Shuffles all columns
df1.join(df2, "id").select("id", "col1", "col2")

# After: Only shuffle needed columns
df1.select("id", "col1").join(
    df2.select("id", "col2"), "id")
```

**Benefits:**
- Reduces shuffle data size significantly
- Faster network transfer
- Less disk I/O
- Lower memory usage

### 2. Predicate Pushdown

**Principle**: Filter data before shuffle

**Implementation:**
```python
# Bad: Filter after join (shuffles all data)
df1.join(df2, "id").filter(df1.status == "active")

# Good: Filter before join (shuffles less data)
df1.filter(df1.status == "active").join(df2, "id")
```

**Catalyst Optimizer:**
- Automatically pushes predicates when possible
- But explicit filtering is safer
- Check query plan to verify

### 3. Compression

**Enable Compression:**
```python
# Shuffle compression
spark.conf.set("spark.shuffle.compress", "true")  # Default: true
spark.conf.set("spark.shuffle.spill.compress", "true")  # Default: true

# Compression codec
spark.conf.set("spark.io.compression.codec", "lz4")  # Fast
# Options: lz4, snappy, zstd, lzf
```

**Codec Comparison:**
- **lz4**: Fast compression, good for shuffle
- **snappy**: Balanced, good default
- **zstd**: Better compression, slightly slower
- **lzf**: Fast but less compression

**Recommendation:**
```python
# For shuffle: Use lz4 (fast)
spark.conf.set("spark.io.compression.codec", "lz4")

# For storage: Use zstd (better compression)
spark.conf.set("spark.io.compression.codec", "zstd")
```

### 4. Adaptive Shuffle (Spark 3.0+)

**Automatic Partition Coalescing:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
```

**Benefits:**
- Automatically reduces partitions after shuffle
- Combines small partitions
- Optimizes based on actual data size

### 5. Shuffle File Management

**Multiple Local Directories:**
```python
# Use multiple disks for shuffle files
spark.conf.set("spark.local.dir", 
    "/data1/spark,/data2/spark,/data3/spark")
```

**Benefits:**
- Parallel I/O across disks
- Better throughput
- Reduces disk bottleneck

**Best Practice:**
- Use fast SSDs if available
- Distribute across multiple disks
- Ensure sufficient disk space (2-3x data size)

### 6. Shuffle Buffer Tuning

**Buffer Sizes:**
```python
# Shuffle write buffer
spark.conf.set("spark.shuffle.file.buffer", "64k")  # Default: 32k

# Shuffle read buffer
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  # Default: 48m
spark.conf.set("spark.reducer.maxReqsInFlight", "1")  # Default: Int.Max
```

**Tuning Guidelines:**
- Larger buffers = less I/O, more memory
- Balance between memory and I/O
- Increase if network is fast
- Decrease if memory is constrained

## 💡 Advanced Shuffle Optimization

### 1. Minimize Shuffle Operations

**Reduce Shuffles:**
```python
# Bad: Multiple shuffles
df.groupBy("key1").agg(sum("amount"))
df.groupBy("key2").agg(sum("amount"))

# Good: Single shuffle with multiple aggregations
df.groupBy("key1", "key2").agg(
    sum("amount").alias("total"),
    count("*").alias("count")
)
```

**Combine Operations:**
```python
# Chain operations to reduce shuffles
df.filter(...).groupBy(...).agg(...).orderBy(...)
# Instead of separate operations
```

### 2. Optimize Join Order

**Principle**: Join smaller tables first

**Example:**
```python
# Let Catalyst optimize (usually best)
df1.join(df2, "id").join(df3, "id")

# Or use hints
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id").join(df3, "id")
```

**Cost-Based Optimization:**
```python
# Enable CBO for better join order
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Collect statistics
spark.sql("ANALYZE TABLE table1 COMPUTE STATISTICS FOR ALL COLUMNS")
```

### 3. Shuffle Memory Optimization

**Increase Shuffle Memory:**
```python
# More memory for shuffle operations
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")  # Less for cache

# Or increase executor memory
spark.conf.set("spark.executor.memory", "16g")
```

**Reduce Spills:**
```python
# Increase spill threshold
spark.conf.set("spark.shuffle.spill.initialMemoryThreshold", "10m")

# Or increase execution memory
spark.conf.set("spark.memory.fraction", "0.8")
```

### 4. Network Optimization

**For On-Premise Clusters:**
- Ensure high-bandwidth network (10Gbps+)
- Use dedicated network for shuffle if possible
- Monitor network utilization
- Optimize network topology

**Configuration:**
```python
# Reduce network overhead
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
spark.conf.set("spark.reducer.maxReqsInFlight", "1")
```

## 🎯 Practical Exercises

### Exercise 1: Measure Shuffle Size

```python
# 1. Create query with shuffle
df = spark.read.parquet("large_table/")
result = df.groupBy("category").agg(sum("amount"))

# 2. Run and check Spark UI
#    - Stages tab: Note shuffle write/read size
#    - Compare with input data size
#    - Calculate shuffle overhead

# 3. Optimize shuffle size
#    - Select only needed columns
#    - Filter before groupBy
#    - Compare shuffle sizes
```

### Exercise 2: Optimize Shuffle Partitions

```python
# 1. Run query with default partitions (200)
spark.conf.set("spark.sql.shuffle.partitions", "200")
df.groupBy("key").count().collect()

# 2. Measure: execution time, task count, shuffle size

# 3. Try different partition counts
for partitions in [50, 200, 400, 800]:
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    # Run and measure
    # Find optimal for your data size
```

### Exercise 3: Compare Compression Codecs

```python
# 1. Test with lz4
spark.conf.set("spark.io.compression.codec", "lz4")
df.groupBy("key").count().collect()
# Measure: execution time, shuffle size

# 2. Test with zstd
spark.conf.set("spark.io.compression.codec", "zstd")
df.groupBy("key").count().collect()
# Measure: execution time, shuffle size

# 3. Compare:
#    - Compression ratio
#    - Execution time
#    - CPU usage
```

## 💡 Best Practices for On-Premise

### 1. Shuffle Configuration Template

```python
# Optimal shuffle configuration
spark.conf.set("spark.sql.shuffle.partitions", "400")  # 2-3x cores
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")
spark.conf.set("spark.shuffle.file.buffer", "64k")
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
spark.conf.set("spark.local.dir", "/data1/spark,/data2/spark,/data3/spark")
```

### 2. Monitor Shuffle Health

**Key Metrics:**
- Shuffle write/read size
- Shuffle time as % of total time
- Number of shuffle files
- Network I/O during shuffle
- Disk I/O during shuffle

**Regular Review:**
- Weekly shuffle metrics analysis
- Identify jobs with excessive shuffle
- Track improvements after optimization

### 3. Reduce Shuffle in Queries

**Checklist:**
- [ ] Only select needed columns
- [ ] Filter before joins/groupBy
- [ ] Use broadcast joins for small tables
- [ ] Combine multiple aggregations
- [ ] Optimize join order

### 4. Adaptive Shuffle Settings

**For Spark 3.0+:**
```python
# Enable adaptive shuffle
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## 🚨 Common Issues & Solutions

### Issue 1: Excessive Shuffle Data

**Symptom**: Shuffle size much larger than input

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

# 3. Use broadcast joins
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

### Issue 3: Slow Shuffle Read

**Symptom**: Long fetch wait times

**Root Causes:**
- Network bottleneck
- Disk I/O bottleneck
- Too many remote fetches

**Solution:**
```python
# 1. Increase shuffle memory
spark.conf.set("spark.memory.fraction", "0.8")

# 2. Use multiple local directories
spark.conf.set("spark.local.dir", "/data1,/data2,/data3")

# 3. Optimize network (cluster-level)
```

### Issue 4: Shuffle Spills

**Symptom**: High disk I/O, spills visible in metrics

**Root Cause**: Insufficient memory for shuffle

**Solution:**
```python
# Increase execution memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

# Or increase executor memory
spark.conf.set("spark.executor.memory", "16g")
```

### Issue 5: Network Bottleneck

**Symptom**: Shuffle limited by network bandwidth

**Root Cause**: Insufficient network capacity

**Solution:**
```python
# Optimize network usage
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

# Or reduce shuffle data size
# (column pruning, filtering, compression)
```

## 📝 Key Takeaways

1. **Shuffle is expensive** - minimize when possible
2. **Column pruning** significantly reduces shuffle size
3. **Filter before shuffle** to reduce data
4. **Compression** reduces network I/O
5. **Right partition count** is critical (2-3x cores)
6. **Multiple local directories** improve I/O
7. **Adaptive shuffle** optimizes automatically (Spark 3.0+)
8. **Monitor shuffle metrics** regularly

## 🔗 Next Steps

- **Day 12**: Join Optimization
- Practice: Analyze shuffle in your queries
- Experiment: Try different shuffle configurations
- Optimize: Reduce shuffle size in your jobs

## 📚 Additional Resources

- [Shuffle Tuning Guide](https://spark.apache.org/docs/latest/tuning.html#shuffle-behavior)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Network Optimization](https://spark.apache.org/docs/latest/tuning.html#network)

---

**Progress**: Day 11/30+ ✅

