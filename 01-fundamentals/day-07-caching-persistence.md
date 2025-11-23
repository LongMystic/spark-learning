# Day 7: Caching and Persistence

## 🎯 Learning Objectives
- Understand when and how to cache data in Spark
- Learn different storage levels and their trade-offs
- Master persistence strategies for iterative algorithms
- Optimize caching for on-premise clusters
- Diagnose and fix caching-related issues

## 📚 Core Concepts

### 1. What is Caching?

**Definition:**
Caching stores RDD/DataFrame partitions in memory (or disk) to avoid recomputation. When you cache data, Spark stores it after the first action, making subsequent actions faster.

**Why Cache?**
- **Iterative Algorithms**: Reuse data multiple times
- **Interactive Queries**: Fast response for repeated queries
- **Cost Savings**: Avoid recomputing expensive operations

**Example:**
```python
# Without cache: Computed twice
df = spark.read.parquet("large_table/").filter(df.status == "active")
df.count()  # Computes filter
df.groupBy("category").count()  # Computes filter again

# With cache: Computed once
df = spark.read.parquet("large_table/").filter(df.status == "active")
df.cache()
df.count()  # Computes and caches
df.groupBy("category").count()  # Uses cache
```

### 2. Storage Levels

**Memory Only:**
```python
from pyspark import StorageLevel

# MEMORY_ONLY: Store in memory, deserialized
df.persist(StorageLevel.MEMORY_ONLY)
# Fastest, but may be evicted if memory full
```

**Memory and Disk:**
```python
# MEMORY_AND_DISK: Memory first, spill to disk
df.persist(StorageLevel.MEMORY_AND_DISK)
# Slower than memory-only, but more reliable
```

**Memory Serialized:**
```python
# MEMORY_ONLY_SER: Serialized in memory
df.persist(StorageLevel.MEMORY_ONLY_SER)
# More memory efficient, but deserialization overhead
```

**Disk Only:**
```python
# DISK_ONLY: Store only on disk
df.persist(StorageLevel.DISK_ONLY)
# Slowest, but doesn't use memory
```

**With Replication:**
```python
# MEMORY_ONLY_2: Replicated on 2 nodes
df.persist(StorageLevel.MEMORY_ONLY_2)
# Fault tolerant, but uses 2x memory
```

**Default Storage Level:**
```python
# cache() uses MEMORY_AND_DISK
df.cache()  # Equivalent to persist(StorageLevel.MEMORY_AND_DISK)
```

### 3. Caching vs Persistence

**Caching:**
```python
# Shortcut for MEMORY_AND_DISK
df.cache()
```

**Persistence:**
```python
# Explicit storage level
df.persist(StorageLevel.MEMORY_ONLY)
# or
df.persist()  # Same as cache()
```

**Unpersist:**
```python
# Remove from cache
df.unpersist()

# Unpersist and block until done
df.unpersist(blocking=True)
```

## 🔍 Deep Dive: How Caching Works

### Caching Process

**First Action:**
1. Compute RDD/DataFrame
2. Store partitions in memory/disk
3. Return result

**Subsequent Actions:**
1. Check if partition is cached
2. If cached: Read from cache
3. If not cached: Recompute

**Eviction:**
- LRU (Least Recently Used) policy
- Evicted when execution needs memory
- Automatically re-cached on next access (if still in plan)

### Memory Management

**Storage Memory:**
- Fraction of Spark memory for caching
- Configurable: `spark.memory.storageFraction` (default: 0.5)
- Can be borrowed by execution memory
- Can be evicted if execution needs space

**Example:**
```python
# If executor memory = 10GB
# Spark memory = 10GB * 0.6 = 6GB
# Storage memory = 6GB * 0.5 = 3GB
# This is available for caching
```

## 💡 When to Cache

### Good Candidates for Caching

**1. Iterative Algorithms:**
```python
# Machine learning iterations
for i in range(100):
    result = df.join(other_df, "id").filter(...)
    # Cache df if used in all iterations
df.cache()
```

**2. Multiple Actions on Same Data:**
```python
df = expensive_computation()
df.cache()

# Multiple actions
df.count()
df.groupBy("key").count()
df.filter(...).write.parquet("output/")
```

**3. Small Frequently-Accessed Tables:**
```python
# Dimension tables used in many joins
dim_df = spark.table("dimension_table")
dim_df.cache()

# Use in multiple joins
fact_df.join(dim_df, "dim_id")
fact_df2.join(dim_df, "dim_id")
```

**4. Interactive Queries:**
```python
# In notebooks or interactive sessions
df = spark.sql("SELECT * FROM large_table WHERE ...")
df.cache()

# Fast repeated queries
df.filter(...).show()
df.groupBy(...).show()
```

### When NOT to Cache

**1. One-Time Use:**
```python
# Bad: Cache if only used once
df.cache()
df.write.parquet("output/")  # Only one action
# Cache overhead without benefit
```

**2. Very Large Data:**
```python
# Bad: Cache data larger than memory
huge_df.cache()  # Will spill to disk, slow
# Better: Optimize query or partition data
```

**3. Frequently Changing Data:**
```python
# Bad: Cache data that changes often
df.cache()
# Next query may use stale data
# Better: Don't cache, or unpersist after use
```

**4. Simple Operations:**
```python
# Bad: Cache simple operations
df.filter(...).cache()  # Filter is fast, cache overhead not worth it
# Better: Only cache expensive operations
```

## 🎯 Caching Strategies

### 1. Cache Intermediate Results

**Example:**
```python
# Expensive computation
df = spark.read.parquet("table1/") \
    .join(spark.read.parquet("table2/"), "id") \
    .filter(complex_condition) \
    .select("col1", "col2", "col3")

# Cache intermediate result
df.cache()

# Multiple downstream operations
df.groupBy("col1").count()
df.filter(df.col2 > 100).count()
df.write.parquet("output/")
```

### 2. Cache Before Expensive Operations

**Example:**
```python
# Cache before multiple joins
base_df = spark.read.parquet("base/").filter(...)
base_df.cache()

# Multiple joins use cached data
result1 = base_df.join(df1, "id")
result2 = base_df.join(df2, "id")
result3 = base_df.join(df3, "id")
```

### 3. Choose Right Storage Level

**For Fast Access:**
```python
# MEMORY_ONLY: Fastest if fits in memory
df.persist(StorageLevel.MEMORY_ONLY)
```

**For Reliability:**
```python
# MEMORY_AND_DISK: Spills if memory full
df.persist(StorageLevel.MEMORY_AND_DISK)  # Default
```

**For Memory Efficiency:**
```python
# MEMORY_ONLY_SER: More data in same memory
df.persist(StorageLevel.MEMORY_ONLY_SER)
```

**For Large Data:**
```python
# DISK_ONLY: Doesn't use memory
df.persist(StorageLevel.DISK_ONLY)
```

### 4. Unpersist When Done

**Best Practice:**
```python
df.cache()
# Use cached data
result = df.groupBy("key").count()
# Unpersist when no longer needed
df.unpersist()
```

## 🔍 Deep Dive: Caching Performance

### Monitoring Cache Effectiveness

**Spark UI - Storage Tab:**
- **Cached RDDs/DataFrames**: List of cached data
- **Memory Used**: Memory consumed by cache
- **Disk Used**: Disk space used (if spilled)
- **Size**: Total size of cached data

**Metrics to Watch:**
- **Hit Rate**: How often cache is used
- **Eviction Rate**: How often data is evicted
- **Memory Usage**: Percentage of storage memory used

### Cache Hit vs Miss

**Cache Hit:**
- Data found in cache
- Fast access
- No recomputation

**Cache Miss:**
- Data not in cache (evicted or never cached)
- Recomputes from source
- Slower access

**Optimization:**
```python
# Check if cache is effective
# In Spark UI, look for:
# - High memory usage for cached data
# - Low eviction rate
# - Fast subsequent actions
```

## 🎯 Practical Exercises

### Exercise 1: Compare With and Without Cache

```python
# 1. Create expensive computation
df = spark.read.parquet("large_table/") \
    .join(spark.read.parquet("other_table/"), "id") \
    .filter(complex_condition)

# 2. Without cache
import time
start = time.time()
df.count()  # Action 1
df.groupBy("key").count()  # Action 2
df.filter(...).count()  # Action 3
time_without_cache = time.time() - start

# 3. With cache
df.cache()
start = time.time()
df.count()  # Action 1 (computes and caches)
df.groupBy("key").count()  # Action 2 (uses cache)
df.filter(...).count()  # Action 3 (uses cache)
time_with_cache = time.time() - start

# 4. Compare times
print(f"Without cache: {time_without_cache}")
print(f"With cache: {time_with_cache}")
print(f"Improvement: {(time_without_cache - time_with_cache) / time_without_cache * 100}%")
```

### Exercise 2: Test Different Storage Levels

```python
from pyspark import StorageLevel

df = spark.read.parquet("table/").filter(...)

# Test MEMORY_ONLY
df.persist(StorageLevel.MEMORY_ONLY)
df.count()  # Measure time
df.unpersist()

# Test MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # Measure time
df.unpersist()

# Test MEMORY_ONLY_SER
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.count()  # Measure time
df.unpersist()

# Compare performance and memory usage
```

### Exercise 3: Monitor Cache in Spark UI

```python
# 1. Cache a DataFrame
df = spark.read.parquet("table/")
df.cache()
df.count()

# 2. Check Spark UI - Storage tab
#    - Note memory used
#    - Check if data is cached

# 3. Run another action
df.groupBy("key").count()

# 4. Check if cache was used
#    - Look at stage details
#    - Cached stages should be faster
```

## 💡 Best Practices for On-Premise

### 1. Cache Strategically

**Do Cache:**
- Data used multiple times
- Expensive computations
- Small dimension tables
- Intermediate results in iterative algorithms

**Don't Cache:**
- Data used only once
- Very large data (larger than memory)
- Simple operations
- Frequently changing data

### 2. Monitor Cache Usage

**Regular Checks:**
- Storage tab in Spark UI
- Memory usage for cached data
- Cache hit/miss rates
- Eviction frequency

**Optimization:**
- If frequent evictions: Increase storage memory
- If low usage: Remove unnecessary caches
- If memory pressure: Use DISK_ONLY or unpersist

### 3. Tune Storage Memory

**For Cache-Heavy Workloads:**
```python
# Increase storage memory fraction
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")
```

**For Compute-Heavy Workloads:**
```python
# Decrease storage memory
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.3")
```

### 4. Clean Up Caches

**Best Practice:**
```python
# Unpersist when done
df.cache()
# ... use cached data ...
df.unpersist()

# Or use context manager pattern
try:
    df.cache()
    # ... use cached data ...
finally:
    df.unpersist()
```

## 🚨 Common Issues & Solutions

### Issue 1: Cache Not Effective

**Symptom**: Cached data keeps getting evicted

**Root Causes:**
- Insufficient storage memory
- Execution memory borrowing from storage
- Too much data cached

**Solution:**
```python
# Increase storage memory
spark.conf.set("spark.memory.storageFraction", "0.6")

# Or increase total memory
spark.conf.set("spark.memory.fraction", "0.8")

# Or cache less data
# Only cache what's truly needed
```

### Issue 2: OOM from Caching

**Symptom**: OutOfMemoryError when caching

**Root Cause**: Trying to cache too much data

**Solution:**
```python
# Use DISK_ONLY for large data
df.persist(StorageLevel.DISK_ONLY)

# Or cache in smaller chunks
df.repartition(100).cache()  # More partitions = smaller cache per partition

# Or don't cache, optimize query instead
```

### Issue 3: Stale Cached Data

**Symptom**: Queries return old data

**Root Cause**: Data changed but cache not updated

**Solution:**
```python
# Unpersist before refreshing
df.unpersist()

# Reload data
df = spark.read.parquet("table/")
df.cache()

# Or use cache with TTL (if available)
# Or don't cache frequently changing data
```

### Issue 4: Cache Overhead

**Symptom**: Caching slower than recomputing

**Root Causes:**
- Cache overhead > recomputation cost
- Data used only once
- Simple operations cached

**Solution:**
```python
# Don't cache simple operations
# Only cache expensive computations

# Measure before caching
# If cache overhead > benefit, don't cache
```

### Issue 5: Memory Pressure from Cache

**Symptom**: High memory usage, slow performance

**Root Cause**: Too much data cached

**Solution:**
```python
# Unpersist unused caches
df1.unpersist()
df2.unpersist()

# Use DISK_ONLY for less critical data
df3.persist(StorageLevel.DISK_ONLY)

# Reduce storage memory fraction
spark.conf.set("spark.memory.storageFraction", "0.3")
```

## 📝 Key Takeaways

1. **Cache is for reuse** - only cache if data used multiple times
2. **MEMORY_AND_DISK is default** - good balance of speed and reliability
3. **Monitor cache effectiveness** - check Spark UI regularly
4. **Unpersist when done** - free memory for other operations
5. **Choose storage level wisely** - balance speed vs memory
6. **Cache expensive operations** - not simple filters/maps
7. **Storage memory is limited** - cache strategically
8. **Eviction is automatic** - LRU policy manages memory

## 🔗 Next Steps

- **Day 8**: Configuration Tuning (Performance Tuning Phase)
- Practice: Identify caching opportunities in your queries
- Experiment: Try different storage levels
- Optimize: Monitor and improve cache usage

## 📚 Additional Resources

- [Spark Caching Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Storage Levels](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html)
- [Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)

---

**Progress**: Day 7/30+ ✅

