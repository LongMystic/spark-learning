# Day 12: Join Optimization

## 🎯 Learning Objectives
- Master join optimization techniques
- Learn to choose the right join strategy
- Optimize broadcast joins and sort-merge joins
- Handle join skew and performance issues
- Tune joins for on-premise clusters

## 📚 Core Concepts

### 1. Join Strategy Selection

**Factors Affecting Join Strategy:**
- Table sizes
- Available memory
- Join key distribution
- Join type (equi vs non-equi)
- Configuration settings

**Join Strategies:**
1. **Broadcast Hash Join** (BHJ) - Fastest, no shuffle
2. **Sort Merge Join** (SMJ) - Default for large tables
3. **Shuffle Hash Join** (SHJ) - Less common
4. **Broadcast Nested Loop Join** (BNLJ) - For non-equi joins

### 2. Broadcast Join Optimization

**When to Use:**
- Small dimension tables (< 100MB typically)
- Frequently joined tables
- Lookup/reference tables

**Configuration:**
```python
# Automatic broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # Default: 10MB

# Manual broadcast hint
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
```

**Best Practices:**
- Monitor broadcast table size
- Don't broadcast > 200MB (risk OOM)
- Use for dimension tables in star schema
- Cache frequently used broadcast tables

### 3. Sort Merge Join Optimization

**Default for Large Tables:**
- Requires shuffle and sort
- Both tables shuffled
- Efficient for large datasets

**Optimization:**
```python
# Pre-sort data to avoid sort during join
df1_sorted = df1.sort("id")
df2_sorted = df2.sort("id")
df1_sorted.join(df2_sorted, "id")
# May skip sort step if data already sorted
```

## 🔍 Deep Dive: Join Optimization Techniques

### 1. Filter Before Join

**Principle**: Reduce data size before join

**Implementation:**
```python
# Bad: Join then filter
df1.join(df2, "id").filter(df1.status == "active")

# Good: Filter then join
df1.filter(df1.status == "active").join(df2, "id")
# Less data to join
```

**Benefits:**
- Smaller shuffle size
- Faster join operation
- Less memory usage

### 2. Column Pruning

**Principle**: Only join needed columns

**Implementation:**
```python
# Bad: Join all columns
df1.join(df2, "id")

# Good: Select needed columns first
df1.select("id", "col1").join(
    df2.select("id", "col2"), "id")
```

**Benefits:**
- Reduces shuffle data size
- Faster network transfer
- Lower memory usage

### 3. Join Order Optimization

**Principle**: Join smaller tables first

**Automatic (Catalyst):**
```python
# Catalyst optimizer reorders joins
df1.join(df2, "id").join(df3, "id")
# Usually chooses optimal order
```

**Manual Hints:**
```python
from pyspark.sql.functions import broadcast

# Force broadcast for small table
df1.join(broadcast(df2), "id").join(df3, "id")
```

**Cost-Based Optimization:**
```python
# Enable CBO for better join order
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Collect statistics
spark.sql("ANALYZE TABLE table1 COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE table2 COMPUTE STATISTICS FOR ALL COLUMNS")

# Catalyst uses statistics for join order
df1.join(df2, "id").join(df3, "id")
```

### 4. Bucket Join Optimization

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

**Requirements:**
- Both tables must be bucketed
- Same number of buckets
- Same join key
- Tables must be saved (not temporary)

**Benefits:**
- No shuffle for bucketed joins
- Faster than sort merge join
- Pre-sorted data

### 5. Join Skew Handling

**Adaptive Query Execution (Spark 3.0+):**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Automatically handles skew
df1.join(df2, "key")
```

**Salting Technique:**
```python
from pyspark.sql.functions import col, concat, lit, rand

# Add salt to skewed side
df1_salted = df1.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand() * 10).cast("int"))
)

# Replicate and salt other side
from pyspark.sql.functions import explode, array
salt_array = array([lit(f"_{i}") for i in range(10)])
df2_salted = df2.withColumn("salt", explode(salt_array)) \
    .withColumn("salted_key", concat(col("key"), col("salt")))

# Join on salted key
result = df1_salted.join(df2_salted, "salted_key")
```

## 💡 Advanced Join Optimization

### 1. Multiple Join Optimization

**Chain Joins Efficiently:**
```python
# Let Catalyst optimize
df1.join(df2, "id").join(df3, "id").join(df4, "id")

# Or use hints for specific strategy
df1.join(broadcast(df2), "id") \
   .join(broadcast(df3), "id") \
   .join(df4, "id")
```

**Best Practice:**
- Broadcast small tables
- Let Catalyst optimize order
- Enable CBO for statistics

### 2. Join Type Selection

**Inner Join:**
```python
df1.join(df2, "id", "inner")  # Only matching rows
# Most efficient, default
```

**Left Join:**
```python
df1.join(df2, "id", "left")  # All rows from left
# Less efficient, requires null handling
```

**Full Outer Join:**
```python
df1.join(df2, "id", "outer")  # All rows from both
# Least efficient, largest result set
```

**Best Practice:**
- Use inner join when possible
- Avoid full outer join if not needed
- Consider left semi/anti joins

### 3. Non-Equi Join Optimization

**Challenge:**
- Non-equi joins (>, <, BETWEEN) can't use hash join
- Use Broadcast Nested Loop Join
- O(n*m) complexity

**Optimization:**
```python
# For range joins, consider:
# 1. Pre-filter to reduce data
df1.filter(...).join(df2, condition, "inner")

# 2. Use broadcast if one side is small
df1.join(broadcast(df2), condition, "inner")

# 3. Consider alternative approaches
# (e.g., window functions, self-joins)
```

## 🎯 Practical Exercises

### Exercise 1: Compare Join Strategies

```python
# 1. Create test data
df_large = spark.range(0, 10000000).withColumn("key", col("id") % 1000)
df_small = spark.range(0, 1000).withColumn("key", col("id"))

# 2. Join without broadcast
result1 = df_large.join(df_small, "key")
result1.explain()
# Note: Which algorithm used?

# 3. Join with broadcast
from pyspark.sql.functions import broadcast
result2 = df_large.join(broadcast(df_small), "key")
result2.explain()
# Note: Which algorithm used?

# 4. Measure performance
# Compare execution times
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

### Exercise 3: Test Bucket Join

```python
# 1. Create and save bucketed tables
df1 = spark.range(0, 1000000).withColumn("key", col("id") % 100)
df2 = spark.range(0, 100000).withColumn("key", col("id") % 100)

df1.write.bucketBy(10, "key").sortBy("key").saveAsTable("bucketed_table1")
df2.write.bucketBy(10, "key").sortBy("key").saveAsTable("bucketed_table2")

# 2. Join bucketed tables
df1_bucketed = spark.table("bucketed_table1")
df2_bucketed = spark.table("bucketed_table2")
result = df1_bucketed.join(df2_bucketed, "key")

# 3. Check explain plan
result.explain()
# Should show no shuffle

# 4. Compare with non-bucketed join
# Measure performance difference
```

## 💡 Best Practices for On-Premise

### 1. Broadcast Small Tables

**Identify Candidates:**
- Dimension tables in star schema
- Lookup/reference tables
- Small tables frequently joined

**Implementation:**
```python
from pyspark.sql.functions import broadcast

# Common pattern
fact_df.join(broadcast(dim_df), "dim_id")
```

**Monitor:**
- Check broadcast table size
- Watch for OOM errors
- Adjust threshold if needed

### 2. Optimize Join Order

**Enable CBO:**
```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Collect statistics regularly
spark.sql("ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS")
```

**Let Catalyst Optimize:**
- Usually chooses best order
- CBO improves with statistics
- Manual hints only when needed

### 3. Use Bucketing for Frequent Joins

**When to Use:**
- Tables frequently joined together
- Join key is stable
- Can pre-process data

**Implementation:**
```python
# Create bucketed tables
df1.write.bucketBy(100, "join_key").sortBy("join_key").saveAsTable("table1")
df2.write.bucketBy(100, "join_key").sortBy("join_key").saveAsTable("table2")

# Subsequent joins are faster
spark.table("table1").join(spark.table("table2"), "join_key")
```

### 4. Handle Join Skew

**Enable AQE:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Or Use Salting:**
- For known skewed keys
- Manual implementation
- More control but more complex

## 🚨 Common Issues & Solutions

### Issue 1: Broadcast Join OOM

**Symptom**: OutOfMemoryError during broadcast

**Root Cause**: Table too large to broadcast

**Solution:**
```python
# Reduce broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Or don't broadcast this table
df1.join(df2, "id")  # Instead of broadcast(df2)
```

### Issue 2: Slow Sort Merge Join

**Symptom**: Join takes very long

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
7. **Enable CBO** for better join order
8. **Monitor join plans** to understand what Spark is doing

## 🔗 Next Steps

- **Day 13**: Memory Optimization
- Practice: Analyze join plans in your queries
- Experiment: Try different join strategies
- Optimize: Improve join performance in your jobs

## 📚 Additional Resources

- [Join Optimization Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)
- [Broadcast Join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hash-join)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)

---

**Progress**: Day 12/30+ ✅

