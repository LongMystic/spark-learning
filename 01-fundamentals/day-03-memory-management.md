# Day 3: Memory Management and Garbage Collection

## 🎯 Learning Objectives
- Understand Spark's memory architecture
- Learn how memory is allocated and managed
- Master garbage collection tuning for Spark
- Optimize memory usage for on-premise clusters
- Diagnose and fix memory-related issues

## 📚 Core Concepts

### 1. Spark Memory Architecture

**Memory Hierarchy:**
```
Total Executor Memory
├── Reserved Memory (300MB - fixed)
├── User Memory (spark.memory.fraction)
│   └── For UDFs, aggregations, user code
└── Spark Memory (spark.memory.fraction)
    ├── Storage Memory (spark.memory.storageFraction)
    │   └── For caching and persistence
    └── Execution Memory (1 - storageFraction)
        └── For shuffles, joins, aggregations
```

### 2. Memory Configuration Parameters

**Key Settings:**
- `spark.executor.memory`: Total memory per executor (e.g., "8g")
- `spark.memory.fraction`: Fraction for Spark operations (default: 0.6)
- `spark.memory.storageFraction`: Fraction for caching (default: 0.5)
- `spark.executor.memoryOverhead`: Off-heap memory (default: executor.memory * 0.1, min 384MB)

**Calculation Example:**
```python
# If spark.executor.memory = "8g"
# Reserved: 300MB
# Available: 8GB - 300MB = 7.7GB
# Spark Memory: 7.7GB * 0.6 = 4.62GB
#   - Storage: 4.62GB * 0.5 = 2.31GB
#   - Execution: 4.62GB * 0.5 = 2.31GB
# User Memory: 7.7GB * 0.4 = 3.08GB
```

### 3. Memory Regions Explained

**Reserved Memory:**
- Fixed 300MB per executor
- Used by Spark internals
- Cannot be configured

**User Memory:**
- For user-defined functions (UDFs)
- For aggregations in user code
- Not managed by Spark
- Can cause OOM if not careful

**Storage Memory:**
- For cached RDDs/DataFrames
- For broadcast variables
- Can be evicted if execution needs space
- Managed by Spark's unified memory manager

**Execution Memory:**
- For shuffle operations
- For joins and aggregations
- Can borrow from storage memory
- Cannot be evicted once allocated

## 🔍 Deep Dive: Unified Memory Management

### How Memory Sharing Works

Spark uses a **unified memory manager** that allows:
- **Execution memory** can borrow from **storage memory**
- **Storage memory** can borrow from **execution memory**
- But storage memory can be evicted, execution cannot

**Example Scenario:**
```python
# Cache a DataFrame (uses storage memory)
df.cache()
df.count()  # Triggers caching

# Run a large join (needs execution memory)
df.join(large_df, "id").count()
# Execution memory can borrow from storage
# If needed, cached data is evicted
```

### Memory Eviction Policy

**Storage Memory Eviction:**
- LRU (Least Recently Used) policy
- Evicted when execution needs memory
- Automatically re-cached on next access (if still in plan)

**Execution Memory Eviction:**
- Never evicted during task execution
- Released after task completion
- Can cause OOM if task needs more than available

## 💡 Garbage Collection in Spark

### Why GC Matters

**Impact on Performance:**
- Long GC pauses slow down tasks
- Frequent GC indicates memory pressure
- GC overhead can be 10-50% of execution time

### GC Types

**Young Generation (Minor GC):**
- Collects short-lived objects
- Fast (milliseconds)
- Frequent during Spark execution

**Old Generation (Major GC / Full GC):**
- Collects long-lived objects
- Slow (seconds to minutes)
- Can cause task timeouts
- Indicates memory pressure

### GC Algorithms

**G1GC (Recommended for Spark):**
```python
spark.conf.set("spark.executor.extraJavaOptions", 
    "-XX:+UseG1GC "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCTimeStamps "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-XX:ConcGCThreads=20")
```

**Parallel GC (Default):**
- Good for small heaps (< 4GB)
- Not recommended for large Spark executors

**CMS (Concurrent Mark Sweep):**
- Deprecated in Java 9+
- Not recommended

## 🎯 Memory Optimization Strategies

### 1. Right-Size Executor Memory

**Best Practices:**
- **Too Small**: Frequent spills, OOM errors
- **Too Large**: Long GC pauses, wasted resources
- **Sweet Spot**: 8-16GB per executor typically

**Calculation:**
```python
# For a node with 64GB RAM
# Leave 16GB for OS and other processes
# Available: 48GB
# If using 3 executors per node: 48GB / 3 = 16GB per executor
spark.executor.memory = "14g"  # Leave some headroom
spark.executor.memoryOverhead = "2g"
```

### 2. Tune Memory Fractions

**For Cache-Heavy Workloads:**
```python
spark.conf.set("spark.memory.fraction", "0.8")  # More for Spark
spark.conf.set("spark.memory.storageFraction", "0.6")  # More for caching
```

**For Compute-Heavy Workloads:**
```python
spark.conf.set("spark.memory.fraction", "0.6")  # Default
spark.conf.set("spark.memory.storageFraction", "0.3")  # Less for caching
```

### 3. Optimize Data Structures

**Use Columnar Formats:**
```python
# Parquet is columnar - more memory efficient
df.write.parquet("output/")  # Better than JSON/CSV

# When reading, use column pruning
df.select("col1", "col2")  # Only reads needed columns
```

**Avoid Large Objects in UDFs:**
```python
# Bad: Large object in UDF
def bad_udf(x):
    large_dict = {...}  # Created for each row!
    return large_dict.get(x)

# Good: Broadcast large objects
large_dict_bc = spark.sparkContext.broadcast({...})
def good_udf(x):
    return large_dict_bc.value.get(x)
```

## 🔍 Deep Dive: Memory Troubleshooting

### Diagnosing Memory Issues

**1. Check Executor Memory Usage:**
```python
# In Spark UI: Executors tab
# Look for:
# - Memory Used vs Memory Total
# - Peak Memory Usage
# - GC Time
```

**2. Monitor GC Logs:**
```python
# Enable GC logging
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCDateStamps "
    "-Xloggc:/path/to/gc.log")
```

**3. Analyze GC Patterns:**
- **Frequent Minor GC**: Normal, but watch frequency
- **Long Major GC**: Memory pressure, increase memory or reduce data
- **Full GC**: Critical issue, likely OOM soon

### Common Memory Issues

**Issue 1: Out of Memory (OOM) Errors**

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
ExecutorLostFailure: Executor exited due to OOM
```

**Solutions:**
```python
# 1. Increase executor memory
spark.executor.memory = "16g"

# 2. Increase memory overhead
spark.executor.memoryOverhead = "4g"

# 3. Reduce data per partition
df.repartition(200)  # More partitions = less data per partition

# 4. Enable spill to disk
spark.conf.set("spark.sql.shuffle.spill.enabled", "true")
```

**Issue 2: Frequent Spills to Disk**

**Symptoms:**
- High disk I/O in Spark UI
- Slow query performance
- "Spilled" metrics in task details

**Solutions:**
```python
# Increase execution memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")  # Less for cache

# Or increase executor memory
spark.executor.memory = "16g"
```

**Issue 3: Long GC Pauses**

**Symptoms:**
- Tasks taking much longer than expected
- GC time > 10% of total time
- Task timeouts

**Solutions:**
```python
# Use G1GC
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m")

# Or reduce executor memory (smaller heap = faster GC)
spark.executor.memory = "8g"  # Instead of 16g
# But use more executors to compensate
spark.executor.instances = 10  # Instead of 5
```

## 🎯 Practical Exercises

### Exercise 1: Monitor Memory Usage

```python
# 1. Create a large DataFrame
df = spark.range(0, 100000000).repartition(100)

# 2. Cache it
df.cache()
df.count()  # Trigger caching

# 3. Check Spark UI:
#    - Storage tab: See cached data size
#    - Executors tab: See memory usage
#    - Note the memory breakdown
```

### Exercise 2: Compare Memory Configurations

```python
# Configuration 1: Default
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")

# Configuration 2: More for execution
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

# Run same query with both configs
# Compare: execution time, spills, GC time
```

### Exercise 3: Analyze GC Logs

```python
# Enable GC logging
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCDateStamps "
    "-Xloggc:/tmp/gc.log")

# Run a memory-intensive query
df = spark.read.parquet("large_table/")
result = df.groupBy("category").agg(sum("amount"))

# Analyze GC log:
# - Count of GC events
# - Average GC pause time
# - Total GC time
```

## 📊 Monitoring Memory in Spark UI

### Key Metrics to Watch

**1. Executors Tab:**
- **Memory Used**: Current memory consumption
- **Peak Memory**: Maximum memory used
- **GC Time**: Time spent in garbage collection
- **GC Count**: Number of GC events

**2. Storage Tab:**
- **Cached Data Size**: Total cached data
- **Memory Used**: Memory for cached data
- **Disk Used**: Spilled cached data

**3. Stages Tab:**
- **Shuffle Read/Write**: Memory used for shuffles
- **Spill (Memory)**: Data spilled to disk
- **Spill (Disk)**: Disk space used

### Memory Health Indicators

**Healthy:**
- Memory usage: 60-80% of available
- GC time: < 5% of total time
- No spills or minimal spills
- No OOM errors

**Warning Signs:**
- Memory usage: > 90%
- GC time: > 10% of total time
- Frequent spills
- Increasing task times

**Critical:**
- Memory usage: > 95%
- GC time: > 20% of total time
- OOM errors
- Task failures

## 💡 Best Practices for On-Premise

### 1. Right-Size for Your Cluster

```python
# Calculate based on cluster resources
# Example: 10 nodes, 64GB RAM each
# Leave 16GB per node for OS: 48GB available
# Use 3 executors per node: 16GB per executor
# Leave headroom: 14GB executor + 2GB overhead

spark.executor.memory = "14g"
spark.executor.memoryOverhead = "2g"
spark.executor.instances = 30  # 10 nodes * 3 executors
spark.executor.cores = 5  # Leave 1 core for OS
```

### 2. Tune for Workload Type

**ETL Workloads (Read-Heavy):**
```python
# More memory for caching
spark.memory.fraction = 0.8
spark.memory.storageFraction = 0.6
```

**Analytical Workloads (Compute-Heavy):**
```python
# More memory for execution
spark.memory.fraction = 0.8
spark.memory.storageFraction = 0.3
```

**Mixed Workloads:**
```python
# Balanced (default)
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5
```

### 3. Monitor and Adjust

- **Weekly Review**: Check memory metrics
- **After Major Changes**: Monitor for 24-48 hours
- **Before/After Optimization**: Compare metrics
- **Document Findings**: Track what works for your cluster

## 🚨 Common Issues & Solutions

### Issue 1: Executor OOM During Shuffle

**Symptom**: `ExecutorLostFailure` during shuffle operations

**Root Cause**: Execution memory insufficient for shuffle

**Solution:**
```python
# Increase execution memory
spark.memory.fraction = 0.8
spark.memory.storageFraction = 0.2

# Or increase executor memory
spark.executor.memory = "16g"

# Or reduce shuffle partitions (less data per task)
spark.sql.shuffle.partitions = 200  # Instead of 400
```

### Issue 2: Cache Eviction Too Frequent

**Symptom**: Cached data keeps getting evicted

**Root Cause**: Storage memory too small relative to execution needs

**Solution:**
```python
# Increase storage fraction
spark.memory.storageFraction = 0.6

# Or increase total memory
spark.memory.fraction = 0.8

# Or reduce execution memory needs
# - Use broadcast joins for small tables
# - Optimize queries to reduce shuffle
```

### Issue 3: Long GC Pauses Causing Timeouts

**Symptom**: Tasks timing out, long GC visible in logs

**Root Cause**: Heap too large for GC algorithm

**Solution:**
```python
# Use G1GC with tuned parameters
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m "
    "-XX:InitiatingHeapOccupancyPercent=35")

# Or reduce executor memory (faster GC)
spark.executor.memory = "8g"
# Compensate with more executors
spark.executor.instances = 20  # Instead of 10
```

## 📝 Key Takeaways

1. **Memory is divided** into reserved, user, storage, and execution regions
2. **Unified memory manager** allows borrowing between storage and execution
3. **GC tuning is critical** for performance, especially with large heaps
4. **Right-sizing executors** is more important than maximizing memory
5. **Monitor memory metrics** regularly to catch issues early
6. **Spills indicate** insufficient memory for operations
7. **G1GC is recommended** for Spark workloads with large heaps

## 🔗 Next Steps

- **Day 4**: Shuffle Mechanics and Optimization
- Practice: Monitor memory usage for your common queries
- Experiment: Try different memory configurations and measure impact
- Analyze: Review GC logs from your production jobs

## 📚 Additional Resources

- [Spark Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- [G1GC Tuning Guide](https://docs.oracle.com/javase/9/gctuning/g1-garbage-collector.htm)
- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)

---

**Progress**: Day 3/30+ ✅

