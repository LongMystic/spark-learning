# Day 13: Memory Optimization

## 🎯 Learning Objectives
- Master memory optimization techniques
- Learn to right-size memory configurations
- Optimize memory fractions and storage levels
- Handle memory-related performance issues
- Tune memory for different workloads

## 📚 Core Concepts

### 1. Memory Architecture Review

**Executor Memory Structure:**
```
Total Executor Memory
├── Reserved Memory (300MB - fixed)
├── User Memory (1 - spark.memory.fraction)
│   └── For UDFs, aggregations, user code
└── Spark Memory (spark.memory.fraction)
    ├── Storage Memory (spark.memory.storageFraction)
    │   └── For caching and persistence
    └── Execution Memory (1 - storageFraction)
        └── For shuffles, joins, aggregations
```

**Key Parameters:**
- `spark.executor.memory`: Total executor memory
- `spark.memory.fraction`: Fraction for Spark operations (default: 0.6)
- `spark.memory.storageFraction`: Fraction for caching (default: 0.5)
- `spark.executor.memoryOverhead`: Off-heap memory

### 2. Memory Optimization Goals

**Objectives:**
- Minimize spills to disk
- Reduce garbage collection time
- Maximize cache effectiveness
- Avoid OutOfMemory errors
- Balance between storage and execution memory

**Trade-offs:**
- More execution memory = less storage memory
- More storage memory = less execution memory
- Larger executor = slower GC but more memory
- Smaller executor = faster GC but less memory

## 🔍 Deep Dive: Memory Optimization Techniques

### 1. Right-Size Executor Memory

**Calculation:**
```python
# Example: Node with 64GB RAM
# Leave 16GB for OS: 48GB available
# Use 3 executors per node: 16GB per executor
# Leave headroom: 14GB executor + 2GB overhead

spark.conf.set("spark.executor.memory", "14g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
```

**Best Practices:**
- **8-16GB per executor** (sweet spot)
- **Too Small**: Frequent spills, OOM errors
- **Too Large**: Long GC pauses, wasted resources
- **Balance**: Between memory and GC performance

### 2. Tune Memory Fractions

**For Cache-Heavy Workloads:**
```python
# More memory for caching
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")
```

**For Compute-Heavy Workloads:**
```python
# More memory for execution
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
```

**For Balanced Workloads:**
```python
# Default (balanced)
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### 3. Optimize Garbage Collection

**G1GC Configuration:**
```python
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-XX:ConcGCThreads=20 "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCTimeStamps")
```

**Tuning Guidelines:**
- **MaxGCPauseMillis**: Target GC pause time (200ms typical)
- **G1HeapRegionSize**: Region size (16m for 8-16GB heaps)
- **InitiatingHeapOccupancyPercent**: When to start GC (35% typical)

**For Large Heaps (> 16GB):**
```python
# Consider reducing executor size instead
spark.conf.set("spark.executor.memory", "8g")  # Instead of 16g
# Use more executors to compensate
spark.conf.set("spark.executor.instances", "40")  # Instead of 20
```

### 4. Reduce Memory Usage in Code

**Use Efficient Data Types:**
```python
# Bad: Using String for IDs
df.withColumn("id", col("id").cast("string"))

# Good: Use appropriate numeric types
df.withColumn("id", col("id").cast("int"))  # Or long, if needed
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

**Column Pruning:**
```python
# Only select needed columns
df.select("col1", "col2")  # Instead of df.select("*")
```

### 5. Optimize Caching Strategy

**When to Cache:**
- Data used multiple times
- Expensive computations
- Small dimension tables

**Storage Level Selection:**
```python
from pyspark import StorageLevel

# For fast access (if fits in memory)
df.persist(StorageLevel.MEMORY_ONLY)

# For reliability (default)
df.persist(StorageLevel.MEMORY_AND_DISK)

# For memory efficiency
df.persist(StorageLevel.MEMORY_ONLY_SER)

# For large data
df.persist(StorageLevel.DISK_ONLY)
```

**Unpersist When Done:**
```python
df.cache()
# ... use cached data ...
df.unpersist()  # Free memory
```

## 💡 Advanced Memory Optimization

### 1. Memory Spill Optimization

**Reduce Spills:**
```python
# Increase execution memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

# Or increase executor memory
spark.conf.set("spark.executor.memory", "16g")
```

**Monitor Spills:**
- Check Spark UI for spill metrics
- Look for "Spill (Memory)" and "Spill (Disk)"
- High spills indicate memory pressure

### 2. Memory Overhead Tuning

**Configuration:**
```python
# Default: 10% of executor memory, min 384MB
# For large executors, may need more
spark.conf.set("spark.executor.memoryOverhead", "4g")  # For 16GB executor
```

**When to Increase:**
- Native memory usage (off-heap)
- Python processes (PySpark)
- JNI libraries
- Network buffers

### 3. Driver Memory Optimization

**For Large Result Collections:**
```python
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")
```

**For Broadcast Joins:**
```python
# Driver needs memory for broadcast
spark.conf.set("spark.driver.memory", "8g")
```

### 4. Memory Monitoring

**Key Metrics:**
- Executor memory usage
- Storage memory usage
- GC time and frequency
- Spill metrics
- Cache hit/miss rates

**Tools:**
- Spark UI (Executors, Storage tabs)
- GC logs
- Cluster monitoring tools

## 🎯 Practical Exercises

### Exercise 1: Compare Memory Configurations

```python
# Configuration 1: Default
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
# Run query and measure: execution time, spills, GC time

# Configuration 2: More for execution
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")
# Run same query and compare

# Configuration 3: More for storage
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")
# Run same query and compare
```

### Exercise 2: Optimize GC

```python
# 1. Enable GC logging
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:+PrintGCDetails "
    "-XX:+PrintGCDateStamps "
    "-Xloggc:/tmp/gc.log")

# 2. Run memory-intensive query
df = spark.read.parquet("large_table/")
result = df.groupBy("category").agg(sum("amount"))

# 3. Analyze GC log:
#    - Count of GC events
#    - Average GC pause time
#    - Total GC time
#    - GC frequency

# 4. Tune GC parameters and re-test
```

### Exercise 3: Monitor Memory Usage

```python
# 1. Run query with caching
df = spark.read.parquet("table/")
df.cache()
df.count()

# 2. Check Spark UI - Storage tab:
#    - Memory used for cached data
#    - Disk used (if spilled)
#    - Cached RDDs/DataFrames

# 3. Check Spark UI - Executors tab:
#    - Memory used vs total
#    - Peak memory usage
#    - GC time

# 4. Analyze memory patterns
```

## 💡 Best Practices for On-Premise

### 1. Memory Configuration Template

**For ETL Workloads:**
```python
spark.conf.set("spark.executor.memory", "14g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")  # More for caching
```

**For Analytical Workloads:**
```python
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")  # More for execution
```

**For Streaming:**
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "1g")
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### 2. Regular Memory Health Checks

**Weekly Review:**
- Average memory usage per executor
- GC time as % of total time
- Spill frequency
- Cache effectiveness
- OOM error frequency

**Optimization:**
- Adjust memory fractions based on workload
- Right-size executors
- Optimize GC settings
- Improve code to reduce memory usage

### 3. Memory Troubleshooting Workflow

**Step 1: Identify Issue**
- OOM errors?
- Frequent spills?
- Long GC pauses?
- High memory usage?

**Step 2: Analyze**
- Check Spark UI metrics
- Review GC logs
- Analyze memory usage patterns

**Step 3: Optimize**
- Adjust memory configuration
- Optimize code
- Right-size executors
- Tune GC

**Step 4: Validate**
- Run representative workload
- Measure improvements
- Monitor for 24-48 hours

## 🚨 Common Issues & Solutions

### Issue 1: Out of Memory (OOM) Errors

**Symptom**: `ExecutorLostFailure: Executor exited due to OOM`

**Root Causes:**
- Insufficient executor memory
- Too much data per partition
- Memory leak in code

**Solution:**
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "16g")

# Increase memory overhead
spark.conf.set("spark.executor.memoryOverhead", "4g")

# Reduce data per partition
df.repartition(200)  # More partitions

# Enable spill to disk
spark.conf.set("spark.sql.shuffle.spill.enabled", "true")
```

### Issue 2: Frequent Spills to Disk

**Symptom**: High disk I/O, slow performance

**Root Cause**: Insufficient execution memory

**Solution:**
```python
# Increase execution memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")

# Or increase executor memory
spark.conf.set("spark.executor.memory", "16g")
```

### Issue 3: Long GC Pauses

**Symptom**: Tasks timing out, high GC time

**Root Cause**: Heap too large for GC algorithm

**Solution:**
```python
# Use G1GC with tuned parameters
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:MaxGCPauseMillis=200 "
    "-XX:G1HeapRegionSize=16m")

# Or reduce executor size
spark.conf.set("spark.executor.memory", "8g")
# Compensate with more executors
spark.conf.set("spark.executor.instances", "40")
```

### Issue 4: Cache Eviction Too Frequent

**Symptom**: Cached data keeps getting evicted

**Root Cause**: Insufficient storage memory

**Solution:**
```python
# Increase storage memory
spark.conf.set("spark.memory.storageFraction", "0.6")

# Or increase total memory
spark.conf.set("spark.memory.fraction", "0.8")
```

### Issue 5: High Memory Usage but No Spills

**Symptom**: Memory usage high but no performance issues

**Analysis:**
- May be normal (caching working)
- Check if memory is being used effectively
- Monitor cache hit rates

**Action:**
- If cache hit rate is high: Good!
- If cache hit rate is low: Consider unpersisting
- Monitor for memory leaks

## 📝 Key Takeaways

1. **Right-size executors** - 8-16GB typically optimal
2. **Tune memory fractions** - balance storage vs execution
3. **Optimize GC** - use G1GC with tuned parameters
4. **Reduce memory in code** - efficient data types, column pruning
5. **Monitor memory metrics** - regular health checks
6. **Handle spills** - increase memory or reduce data
7. **Cache strategically** - only when beneficial
8. **Iterate and optimize** - memory tuning is ongoing

## 🔗 Next Steps

- **Day 14**: Network Optimization
- Practice: Analyze memory usage in your jobs
- Experiment: Try different memory configurations
- Optimize: Reduce memory usage in your code

## 📚 Additional Resources

- [Memory Management Guide](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- [G1GC Tuning](https://docs.oracle.com/javase/9/gctuning/g1-garbage-collector.htm)
- [Memory Configuration](https://spark.apache.org/docs/latest/configuration.html#memory-management)

---

**Progress**: Day 13/30+ ✅

