# Day 6: Join Algorithms and Optimization

## 🎯 Learning Objectives
- Understand different join algorithms in Spark
- Learn when each join strategy is used
- Master join optimization techniques
- Diagnose and fix join performance issues
- Optimize joins for on-premise clusters

## 📚 Core Concepts

### 1. Join Types in Spark

**Inner Join:**
```python
df1.join(df2, "id")  # Only matching rows
# or
df1.join(df2, df1.id == df2.id, "inner")
```

**Left Join:**
```python
df1.join(df2, "id", "left")  # All rows from left
```

**Right Join:**
```python
df1.join(df2, "id", "right")  # All rows from right
```

**Full Outer Join:**
```python
df1.join(df2, "id", "outer")  # All rows from both
```

**Left Semi Join:**
```python
df1.join(df2, "id", "left_semi")  # Rows in left that match right
# Equivalent to: df1.filter(df1.id.isin(df2.select("id")))
```

**Left Anti Join:**
```python
df1.join(df2, "id", "left_anti")  # Rows in left that don't match right
# Equivalent to: df1.filter(~df1.id.isin(df2.select("id")))
```

### 2. Join Algorithms

Spark uses different join algorithms based on:
- Table sizes
- Available memory
- Join keys distribution
- Configuration settings

**Main Algorithms:**
1. **Broadcast Hash Join** (BHJ)
2. **Shuffle Hash Join** (SHJ)
3. **Sort Merge Join** (SMJ)
4. **Broadcast Nested Loop Join** (BNLJ)
5. **Cartesian Join**

## 🔍 Deep Dive: Join Algorithms

### 1. Broadcast Hash Join (BHJ)

**How It Works:**
1. Small table broadcasted to all executors
2. Builds hash table from broadcasted table
3. Large table scanned, hash lookup for matches
4. No shuffle required

**When Used:**
- One table is small (< `spark.sql.autoBroadcastJoinThreshold`)
- Join key distribution is even
- Default threshold: 10MB

**Example:**
```python
# Automatic broadcast
df_large.join(df_small, "id")
# Spark automatically broadcasts df_small if < 10MB

# Manual broadcast hint
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
```

**Advantages:**
- No shuffle (fastest)
- No network I/O for small table
- Efficient for small dimension tables

**Limitations:**
- Small table must fit in executor memory
- All executors get full copy
- Not suitable for large tables

**Configuration:**
```python
# Adjust broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # Default: 10MB

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### 2. Shuffle Hash Join (SHJ)

**How It Works:**
1. Both tables partitioned by join key (shuffle)
2. Hash table built from smaller partition
3. Other partition scanned, hash lookup
4. Requires shuffle for both tables

**When Used:**
- Tables are similar size
- Neither table is small enough to broadcast
- Join keys are evenly distributed
- Less common in Spark 2.0+ (prefers Sort Merge Join)

**Example:**
```python
# Spark may choose SHJ for medium-sized tables
df1.join(df2, "id")
# Check explain plan to see which algorithm used
```

**Advantages:**
- Works for any table size
- No memory constraint for entire table

**Limitations:**
- Requires shuffle (expensive)
- Both tables shuffled
- Network I/O intensive

### 3. Sort Merge Join (SMJ) - Default

**How It Works:**
1. Both tables sorted by join key (shuffle + sort)
2. Merge step: scan both sorted tables
3. Match rows with same key
4. Efficient for large tables

**When Used:**
- Default for large table joins
- Tables don't fit broadcast threshold
- Most common join algorithm in Spark

**Example:**
```python
# Default for large tables
df_large1.join(df_large2, "id")
# Uses Sort Merge Join
```

**Advantages:**
- Efficient for large tables
- Predictable performance
- Works with any table size
- No memory constraint

**Limitations:**
- Requires shuffle and sort (expensive)
- Both tables shuffled
- Slower than broadcast for small tables

**Optimization:**
```python
# Pre-sort data to avoid sort during join
df1_sorted = df1.sort("id")
df2_sorted = df2.sort("id")
df1_sorted.join(df2_sorted, "id")
# May skip sort step if data already sorted
```

### 4. Broadcast Nested Loop Join (BNLJ)

**How It Works:**
1. One table broadcasted
2. Nested loop: for each row in large table, check all rows in broadcast
3. No hash table, just iteration

**When Used:**
- Non-equi joins (>, <, BETWEEN, etc.)
- One table is small
- Fallback when hash join not possible

**Example:**
```python
# Non-equi join
df1.join(df2, df1.start <= df2.end, "inner")
# Uses Broadcast Nested Loop Join
```

**Advantages:**
- Works for non-equi joins
- No shuffle for large table

**Limitations:**
- O(n*m) complexity (slow)
- Only for small broadcast table
- Not efficient for large datasets

### 5. Cartesian Join

**How It Works:**
- Every row from left matched with every row from right
- No join condition (or always true condition)

**When Used:**
- Explicit cross join
- Usually indicates a bug

**Example:**
```python
# Explicit cross join
df1.crossJoin(df2)

# Accidental cartesian (bad!)
df1.join(df2)  # Missing join condition!
```

**Warning:**
- Extremely expensive
- Produces huge result set
- Usually a mistake

## 💡 Join Optimization Strategies

### 1. Broadcast Join Optimization

**When to Use:**
- Small dimension tables (< 100MB typically)
- Frequently joined tables
- Lookup/reference tables

**Implementation:**
```python
# Automatic (if under threshold)
df_large.join(df_small, "id")

# Manual hint (force broadcast)
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")

# Adjust threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

**Best Practices:**
- Monitor broadcast table size
- Don't broadcast tables > 200MB (risk OOM)
- Use for dimension tables in star schema
- Cache frequently used broadcast tables

### 2. Bucket Join Optimization

**How It Works:**
- Both tables bucketed by join key
- Matching buckets joined locally
- No shuffle required

**Creating Bucketed Tables:**
```python
# Write bucketed table
df.write.bucketBy(10, "id").sortBy("id").saveAsTable("bucketed_table")

# Join bucketed tables
df1 = spark.table("bucketed_table1")
df2 = spark.table("bucketed_table2")
df1.join(df2, "id")  # No shuffle if both bucketed by same key
```

**Benefits:**
- No shuffle for bucketed joins
- Faster than sort merge join
- Pre-sorted data

**Requirements:**
- Both tables must be bucketed
- Same number of buckets
- Same join key
- Tables must be saved (not temporary)

### 3. Skew Join Optimization

**The Problem:**
- Some join keys have many more rows than others
- Causes straggler tasks
- Slow join performance

**Detection:**
```python
# Check for skew in Spark UI
# Look for:
# - Uneven task execution times
# - Some tasks much larger
# - Skew metrics in stage details
```

**Solutions:**

**Method 1: Adaptive Query Execution (Spark 3.0+)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Automatically handles skew
df1.join(df2, "id")
```

**Method 2: Salting**
```python
from pyspark.sql.functions import col, concat, lit, rand

# Add random salt to skewed key
df1_salted = df1.withColumn("salted_key", 
    concat(col("key"), lit("_"), (rand() * 10).cast("int")))

df2_salted = df2.withColumn("salted_key", 
    concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# Join on salted key
result = df1_salted.join(df2_salted, "salted_key")

# Remove salt from result
result = result.drop("salted_key").withColumn("key", 
    split(col("salted_key"), "_")[0])
```

**Method 3: Split Skewed Keys**
```python
# For known skewed keys, handle separately
skewed_keys = ["key1", "key2"]  # Known skewed values

# Split into two parts
df1_normal = df1.filter(~df1.key.isin(skewed_keys))
df1_skewed = df1.filter(df1.key.isin(skewed_keys))

df2_normal = df2.filter(~df2.key.isin(skewed_keys))
df2_skewed = df2.filter(df2.key.isin(skewed_keys))

# Join separately
result_normal = df1_normal.join(df2_normal, "key")
result_skewed = df1_skewed.join(df2_skewed, "key")

# Union results
result = result_normal.union(result_skewed)
```

### 4. Join Order Optimization

**Impact:**
- Join order affects performance
- Catalyst optimizer reorders joins
- Cost-based optimization helps

**Best Practices:**
```python
# Let Catalyst optimize (usually best)
df1.join(df2, "id").join(df3, "id")

# Or use hints for specific order
df1.join(df2.hint("broadcast"), "id").join(df3, "id")
```

**Cost-Based Optimization:**
```python
# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Collect statistics
spark.sql("ANALYZE TABLE table1 COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE table2 COMPUTE STATISTICS FOR ALL COLUMNS")

# Catalyst will choose better join order
df1.join(df2, "id").join(df3, "id")
```

### 5. Filter Before Join

**Principle:**
- Reduce data size before join
- Less data shuffled
- Faster join

**Example:**
```python
# Bad: Join then filter
df1.join(df2, "id").filter(df1.status == "active")

# Good: Filter then join
df1.filter(df1.status == "active").join(df2, "id")
# Less data to join
```

## 🔍 Deep Dive: Join Plan Analysis

### Understanding Join Plans

**View Join Plan:**
```python
df1.join(df2, "id").explain(extended=True)
```

**Plan Output Example:**
```
== Physical Plan ==
*(5) SortMergeJoin [id#10], [id#20], Inner
:- *(2) Sort [id#10 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(id#10, 200)
:     +- *(1) Project [id#10, ...]
+- *(4) Sort [id#20 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(id#20, 200)
      +- *(3) Project [id#20, ...]
```

**Reading the Plan:**
- `SortMergeJoin`: Algorithm used
- `Exchange hashpartitioning`: Shuffle operation
- `Sort`: Sort operation before merge
- Numbers in `*(N)`: Code generation enabled

**Broadcast Join Plan:**
```
== Physical Plan ==
*(2) BroadcastHashJoin [id#10], [id#20], Inner, BuildRight
:- *(1) Project [id#10, ...]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false]))
   +- *(1) Project [id#20, ...]
```

**Key Indicators:**
- `BroadcastHashJoin`: Broadcast join used
- `BroadcastExchange`: Table being broadcasted
- `BuildRight`/`BuildLeft`: Which side is broadcasted

## 🎯 Practical Exercises

### Exercise 1: Compare Join Algorithms

```python
# 1. Create test data
df_large = spark.range(0, 10000000).withColumn("key", col("id") % 1000)
df_small = spark.range(0, 1000).withColumn("key", col("id"))

# 2. Join and check plan
result = df_large.join(df_small, "key")
result.explain(extended=True)
# Note: Which algorithm used?

# 3. Force broadcast
from pyspark.sql.functions import broadcast
result_broadcast = df_large.join(broadcast(df_small), "key")
result_broadcast.explain(extended=True)
# Compare plans

# 4. Measure performance
# Time both approaches
```

### Exercise 2: Optimize Join with Filtering

```python
# 1. Create data
df1 = spark.range(0, 1000000).withColumn("status", 
    when(col("id") % 10 == 0, "active").otherwise("inactive"))
df2 = spark.range(0, 100000)

# 2. Bad: Join then filter
result_bad = df1.join(df2, "id").filter(col("status") == "active")
result_bad.explain()

# 3. Good: Filter then join
result_good = df1.filter(col("status") == "active").join(df2, "id")
result_good.explain()

# 4. Compare:
#    - Shuffle size
#    - Execution time
#    - Data scanned
```

### Exercise 3: Handle Join Skew

```python
# 1. Create skewed data
df1 = spark.range(0, 1000000).withColumn("key",
    when(col("id") < 100, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string"))))

df2 = spark.range(0, 100000).withColumn("key",
    when(col("id") < 100, lit("skewed"))
    .otherwise(concat(lit("normal_"), (col("id") % 100).cast("string"))))

# 2. Join and observe skew
result = df1.join(df2, "key")
# Check Spark UI for uneven task times

# 3. Enable AQE skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result_optimized = df1.join(df2, "key")
# Compare performance
```

## 💡 Best Practices for On-Premise

### 1. Broadcast Small Tables

```python
# Identify small dimension tables
# Broadcast them explicitly
from pyspark.sql.functions import broadcast

# Common pattern: Fact table join dimension table
fact_df.join(broadcast(dim_df), "dim_id")
```

### 2. Monitor Join Performance

**Key Metrics:**
- Join algorithm used
- Shuffle size
- Execution time
- Task distribution (check for skew)

**Regular Review:**
- Weekly analysis of slow joins
- Identify optimization opportunities
- Track improvements

### 3. Use Bucketing for Frequent Joins

```python
# For tables frequently joined together
df1.write.bucketBy(100, "join_key").sortBy("join_key").saveAsTable("table1")
df2.write.bucketBy(100, "join_key").sortBy("join_key").saveAsTable("table2")

# Subsequent joins are faster
spark.table("table1").join(spark.table("table2"), "join_key")
```

### 4. Enable Adaptive Query Execution

```python
# Spark 3.0+ features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## 🚨 Common Issues & Solutions

### Issue 1: Broadcast Join OOM

**Symptom**: `OutOfMemoryError` during broadcast

**Root Cause**: Table too large to broadcast

**Solution:**
```python
# Reduce broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Or don't broadcast this table
# Remove broadcast hint, let Spark choose
df1.join(df2, "id")  # Instead of broadcast(df2)
```

### Issue 2: Slow Sort Merge Join

**Symptom**: Join takes very long, high shuffle

**Root Causes:**
- Large tables
- Data skew
- Too many shuffle partitions

**Solution:**
```python
# 1. Filter before join
df1.filter(...).join(df2.filter(...), "id")

# 2. Handle skew
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 4. Consider bucketing
df1.write.bucketBy(100, "id").saveAsTable("table1")
```

### Issue 3: Cartesian Join (Accidental)

**Symptom**: Extremely slow, huge result set

**Root Cause**: Missing join condition

**Solution:**
```python
# Always specify join condition
df1.join(df2, "id")  # Good
df1.join(df2)  # Bad! Creates cartesian

# Check explain plan - look for CartesianProduct
```

### Issue 4: Join Key Mismatch

**Symptom**: Wrong results or no matches

**Root Causes:**
- Different data types
- Null values
- Case sensitivity

**Solution:**
```python
# Ensure same data type
df1.join(df2, df1.id == df2.id.cast("int"), "inner")

# Handle nulls
df1.join(df2, (df1.id == df2.id) & df1.id.isNotNull(), "inner")

# Case sensitivity
df1.join(df2, lower(df1.key) == lower(df2.key), "inner")
```

### Issue 5: Multiple Joins Performance

**Symptom**: Chain of joins is slow

**Root Cause**: Each join causes shuffle

**Solution:**
```python
# 1. Broadcast small tables
df1.join(broadcast(df2), "id").join(broadcast(df3), "id")

# 2. Filter early
df1.filter(...).join(df2.filter(...), "id").join(df3.filter(...), "id")

# 3. Use bucketing for all tables
# All tables bucketed by join key
```

## 📝 Key Takeaways

1. **Broadcast Hash Join** is fastest (no shuffle) but only for small tables
2. **Sort Merge Join** is default for large tables (requires shuffle)
3. **Filter before join** to reduce data size
4. **Broadcast small dimension tables** explicitly
5. **Handle join skew** with AQE or salting
6. **Use bucketing** for frequently joined tables
7. **Monitor join plans** to understand what Spark is doing
8. **Enable AQE** for automatic optimizations (Spark 3.0+)

## 🔗 Next Steps

- **Day 7**: Caching and Persistence
- Practice: Analyze join plans in your queries
- Experiment: Try different join strategies
- Optimize: Improve join performance in your jobs

## 📚 Additional Resources

- [Spark Join Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)
- [Broadcast Join Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hash-join)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

---

**Progress**: Day 6/30+ ✅

