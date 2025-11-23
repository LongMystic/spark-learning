# Day 10: Data Skew Handling

## 🎯 Learning Objectives
- Understand what data skew is and its impact
- Learn techniques to detect data skew
- Master skew mitigation strategies
- Implement salting and other skew-handling techniques
- Optimize skewed joins and aggregations

## 📚 Core Concepts

### 1. What is Data Skew?

**Definition:**
Data skew occurs when data is unevenly distributed across partitions. Some partitions have significantly more data than others, causing straggler tasks.

**Impact:**
- **Straggler Tasks**: Some tasks take much longer
- **Resource Waste**: Most executors idle while few work
- **Slow Performance**: Job time = slowest task time
- **OOM Errors**: Large partitions may cause memory issues

**Example:**
```python
# Skewed data: 90% of rows have key='A'
# Partition with key='A': 9M rows
# Other partitions: 100K rows each
# Result: One task takes 90x longer
```

### 2. When Skew Occurs

**Common Scenarios:**
- **GroupBy Operations**: Uneven key distribution
- **Join Operations**: One side has many matches
- **Partitioning**: Poor partition key selection
- **Filtering**: Most data matches one condition

**Example:**
```python
# Skewed groupBy
df.groupBy("country").count()
# If 80% of data is from one country

# Skewed join
df1.join(df2, "user_id")
# If one user has millions of records
```

### 3. Detecting Data Skew

**Signs in Spark UI:**
- Uneven task execution times
- Some tasks much larger than others
- Long-running straggler tasks
- High shuffle read for some tasks

**Metrics to Check:**
```python
# In Spark UI - Stages tab:
# - Task execution time distribution
# - Shuffle read size per task
# - Records read per task
# - Data size per task
```

**Code Detection:**
```python
# Check partition sizes
partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
print(f"Min: {min(partition_sizes)}, Max: {max(partition_sizes)}")
print(f"Skew ratio: {max(partition_sizes) / min(partition_sizes):.2f}")

# If ratio > 3-5, likely skew
```

## 🔍 Deep Dive: Skew Detection Techniques

### 1. Spark UI Analysis

**Stages Tab:**
- Look for tasks with much longer execution time
- Check shuffle read size distribution
- Identify tasks with high input size

**Tasks Tab:**
- Sort by duration (descending)
- Check if top tasks are much slower
- Look at shuffle read metrics

**SQL Tab:**
- View query plan
- Check for skew indicators
- Look at stage metrics

### 2. Programmatic Detection

**Partition Size Analysis:**
```python
def detect_skew(df, threshold=3.0):
    """Detect if DataFrame has skewed partitions"""
    partition_sizes = df.rdd.mapPartitions(
        lambda it: [sum(1 for _ in it)]
    ).collect()
    
    if not partition_sizes:
        return False, 0
    
    min_size = min(partition_sizes)
    max_size = max(partition_sizes)
    ratio = max_size / min_size if min_size > 0 else float('inf')
    
    is_skewed = ratio > threshold
    return is_skewed, ratio

# Usage
is_skewed, ratio = detect_skew(df)
if is_skewed:
    print(f"Data is skewed! Ratio: {ratio:.2f}")
```

**Key Distribution Analysis:**
```python
# Check key distribution
key_counts = df.groupBy("key").count().orderBy(col("count").desc())
key_counts.show(20)  # Top 20 keys

# Calculate skew metrics
total = key_counts.agg(sum("count")).collect()[0][0]
top_key = key_counts.first()
skew_percentage = (top_key["count"] / total) * 100

if skew_percentage > 50:
    print(f"Top key has {skew_percentage:.1f}% of data - highly skewed!")
```

## 💡 Skew Mitigation Strategies

### 1. Adaptive Query Execution (Spark 3.0+)

**Automatic Skew Handling:**
```python
# Enable AQE skew join
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Automatically handles skew in joins
df1.join(df2, "key")
# Spark detects and splits skewed partitions
```

**How It Works:**
1. Detects skewed partitions during execution
2. Splits skewed partitions into smaller chunks
3. Processes chunks separately
4. Merges results

### 2. Salting Technique

**What is Salting?**
Adding a random prefix/suffix to skewed keys to distribute data more evenly.

**Implementation:**
```python
from pyspark.sql.functions import col, concat, lit, rand, split

# Step 1: Add salt to skewed side
salt_buckets = 10  # Number of salt values
df1_salted = df1.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * salt_buckets).cast("int"))
)

# Step 2: Replicate and salt the other side
# Create exploded version with all salt values
from pyspark.sql.functions import explode, array, lit as spark_lit

salt_array = array([spark_lit(f"_{i}") for i in range(salt_buckets)])
df2_salted = df2.withColumn("salt", explode(salt_array)) \
    .withColumn("salted_key", concat(col("key"), col("salt")))

# Step 3: Join on salted key
result = df1_salted.join(df2_salted, "salted_key")

# Step 4: Remove salt and aggregate
result = result.withColumn("key", split(col("salted_key"), "_")[0]) \
    .drop("salted_key", "salt")
```

**Salting for GroupBy:**
```python
# Add salt before groupBy
df_salted = df.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)

# GroupBy with salt
grouped = df_salted.groupBy("salted_key").agg(sum("amount"))

# Remove salt and re-aggregate
result = grouped.withColumn("key", split(col("salted_key"), "_")[0]) \
    .groupBy("key").agg(sum("amount"))
```

### 3. Two-Phase Aggregation

**Strategy:**
1. First phase: Partial aggregation with salt
2. Second phase: Final aggregation without salt

**Implementation:**
```python
# Phase 1: Partial aggregation with salt
df_salted = df.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)

partial = df_salted.groupBy("salted_key").agg(
    sum("amount").alias("partial_sum"),
    count("*").alias("partial_count")
)

# Phase 2: Final aggregation
result = partial.withColumn("key", split(col("salted_key"), "_")[0]) \
    .groupBy("key").agg(
        sum("partial_sum").alias("total_sum"),
        sum("partial_count").alias("total_count")
    )
```

### 4. Split Skewed Keys

**Strategy:**
Handle known skewed keys separately from normal keys.

**Implementation:**
```python
# Identify skewed keys
skewed_keys = ["key1", "key2"]  # Known skewed values

# Split data
df_normal = df.filter(~df.key.isin(skewed_keys))
df_skewed = df.filter(df.key.isin(skewed_keys))

# Process separately
result_normal = df_normal.groupBy("key").agg(sum("amount"))

# For skewed keys, use salting
df_skewed_salted = df_skewed.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)
result_skewed = df_skewed_salted.groupBy("salted_key").agg(sum("amount")) \
    .withColumn("key", split(col("salted_key"), "_")[0]) \
    .groupBy("key").agg(sum("amount"))

# Union results
result = result_normal.union(result_skewed)
```

### 5. Increase Partitions

**Simple Approach:**
```python
# More partitions = smaller partitions
df.repartition(1000, "key")  # Instead of 200
# Reduces impact of skew (but doesn't eliminate it)
```

**With Skewed Key Handling:**
```python
# More partitions for skewed keys
df = df.withColumn(
    "partition_key",
    when(df.key.isin(skewed_keys), 
         concat(df.key, lit("_"), (rand() * 100).cast("int")))
    .otherwise(df.key)
)
df.repartition(1000, "partition_key")
```

## 🎯 Practical Exercises

### Exercise 1: Detect Skew

```python
# 1. Create skewed data
df = spark.range(0, 1000000).withColumn(
    "key",
    when(col("id") < 100000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
)

# 2. Detect skew
is_skewed, ratio = detect_skew(df)
print(f"Skewed: {is_skewed}, Ratio: {ratio:.2f}")

# 3. Check key distribution
key_counts = df.groupBy("key").count().orderBy(col("count").desc())
key_counts.show(10)
```

### Exercise 2: Handle Skew with Salting

```python
# 1. Create skewed join scenario
df1 = spark.range(0, 1000000).withColumn(
    "key",
    when(col("id") < 100000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
)

df2 = spark.range(0, 100000).withColumn(
    "key",
    when(col("id") < 10000, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string")))
)

# 2. Join without handling skew
result1 = df1.join(df2, "key")
# Measure time and check Spark UI

# 3. Join with salting
# Implement salting technique
# Measure time and compare
```

### Exercise 3: Use AQE for Skew

```python
# 1. Enable AQE skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 2. Run skewed join
df1.join(df2, "key")

# 3. Check Spark UI:
#    - Look for skew join indicators
#    - Check if partitions were split
#    - Compare task execution times
```

## 💡 Best Practices for On-Premise

### 1. Proactive Skew Detection

**Regular Monitoring:**
- Weekly analysis of job performance
- Check for straggler tasks
- Monitor key distributions

**Prevention:**
- Choose balanced partition keys
- Monitor data distribution
- Set up alerts for skew

### 2. Skew Handling Strategy

**For Known Skew:**
- Use salting for specific keys
- Split skewed keys separately
- Pre-process to balance data

**For Unknown Skew:**
- Enable AQE (Spark 3.0+)
- Monitor and adjust
- Use adaptive techniques

### 3. Salting Best Practices

**Choose Salt Buckets:**
- Too few: Still skewed
- Too many: Overhead
- Sweet spot: 10-100 typically

**Test and Iterate:**
- Start with 10 buckets
- Measure improvement
- Adjust based on results

### 4. Combine Techniques

**Multi-Layer Approach:**
```python
# 1. Enable AQE (automatic)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 2. Use salting for known extreme skew
df_salted = add_salt(df, skewed_keys)

# 3. Increase partitions
df.repartition(1000, "salted_key")
```

## 🚨 Common Issues & Solutions

### Issue 1: Salting Overhead Too High

**Symptom**: Salting slower than original

**Root Cause**: Too many salt buckets or inefficient implementation

**Solution:**
```python
# Reduce salt buckets
salt_buckets = 5  # Instead of 100

# Or use AQE instead
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Issue 2: Skew Still Present After Salting

**Symptom**: Still seeing straggler tasks

**Root Cause**: Not enough salt buckets or wrong keys salted

**Solution:**
```python
# Increase salt buckets
salt_buckets = 50  # More buckets

# Or salt both sides of join
# (replicate smaller side)
```

### Issue 3: AQE Not Detecting Skew

**Symptom**: AQE enabled but skew not handled

**Root Cause**: Thresholds too high

**Solution:**
```python
# Lower thresholds
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
```

### Issue 4: OOM on Skewed Partitions

**Symptom**: OutOfMemoryError on large partitions

**Root Cause**: Skewed partition too large for memory

**Solution:**
```python
# Split skewed keys before processing
df_normal = df.filter(~df.key.isin(skewed_keys))
df_skewed = df.filter(df.key.isin(skewed_keys))

# Process skewed keys with more partitions
df_skewed.repartition(100, "key")
```

## 📝 Key Takeaways

1. **Data skew** causes straggler tasks and slow performance
2. **Detect skew** early through monitoring and analysis
3. **AQE** automatically handles skew (Spark 3.0+)
4. **Salting** distributes skewed keys across partitions
5. **Two-phase aggregation** reduces skew impact
6. **Split skewed keys** for extreme cases
7. **Monitor and adjust** skew handling strategies
8. **Combine techniques** for best results

## 🔗 Next Steps

- **Day 11**: Shuffle Optimization
- Practice: Detect skew in your data
- Experiment: Try different salting strategies
- Optimize: Handle skew in your production jobs

## 📚 Additional Resources

- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Data Skew Handling](https://www.databricks.com/blog/2020/07/29/a-look-under-the-hood-at-spark-3-0-sql-adaptive-execution-engine.html)
- [Skew Join Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

---

**Progress**: Day 10/30+ ✅

