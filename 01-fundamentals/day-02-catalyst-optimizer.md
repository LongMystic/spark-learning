# Day 2: Catalyst Optimizer Internals

## 🎯 Learning Objectives
- Understand how Catalyst optimizer works
- Learn optimization rules and strategies
- Master query plan analysis
- Apply optimization techniques in practice

## 📚 What is Catalyst Optimizer?

Catalyst is Spark's query optimization engine that:
- Transforms logical plans into optimized physical plans
- Applies rule-based and cost-based optimizations
- Generates efficient bytecode for execution

## 🔍 Optimization Phases

### Phase 1: Analysis
- Resolves table/column references
- Determines data types
- Validates SQL syntax

### Phase 2: Logical Optimization
- Applies optimization rules
- Simplifies expressions
- Pushes down predicates

### Phase 3: Physical Planning
- Selects execution strategies
- Generates physical operators
- Applies cost-based optimizations

### Phase 4: Code Generation
- Generates Java bytecode
- Optimizes for JVM execution

## 🎯 Key Optimization Rules

### 1. Predicate Pushdown
**What it does**: Moves filters closer to data source

**Example:**
```sql
-- Before optimization
SELECT * FROM (
  SELECT * FROM large_table
) WHERE status = 'active'

-- After optimization (predicate pushed down)
SELECT * FROM large_table WHERE status = 'active'
```

**Benefit**: Reduces data read from disk

### 2. Projection Pushdown
**What it does**: Only reads required columns

**Example:**
```sql
-- Only reads col1 and col2, not col3, col4, etc.
SELECT col1, col2 FROM large_table
```

### 3. Constant Folding
**What it does**: Evaluates constant expressions at compile time

**Example:**
```sql
-- Before: Calculated for each row
SELECT * FROM table WHERE age > 18 + 2

-- After: Optimized to
SELECT * FROM table WHERE age > 20
```

### 4. Column Pruning
**What it does**: Eliminates unused columns early

### 5. Join Reordering
**What it does**: Reorders joins for better performance

**Strategy**: Smaller tables join first when possible

## 💡 Viewing Query Plans

### Logical Plan
```python
df = spark.read.parquet("table/")
result = df.filter(df.age > 25).select("name", "age")

# View logical plan
result.explain(extended=True)
# or
print(result.queryExecution.logical)
```

### Optimized Logical Plan
```python
# View optimized logical plan
print(result.queryExecution.optimizedPlan)
```

### Physical Plan
```python
# View physical plan
print(result.queryExecution.executedPlan)
```

### Parsed Plan (SQL)
```python
# For SQL queries
spark.sql("SELECT * FROM table WHERE age > 25").explain(extended=True)
```

## 🔬 Understanding Plan Output

### Example Plan Analysis

```
== Physical Plan ==
*(1) Project [name#10, age#11]
+- *(1) Filter (age#11 > 25)
   +- *(1) ColumnarToRow
      +- FileScan parquet [name#10,age#11] Batched: true
```

**Reading the Plan:**
- `*(1)`: Indicates code generation (whole-stage codegen)
- `Project`: Selects columns
- `Filter`: Applies predicate
- `ColumnarToRow`: Converts columnar format to row format
- `FileScan`: Reads from parquet file

## 🎯 Optimization Strategies

### 1. Enable Cost-Based Optimization (CBO)

```python
# Enable CBO (requires table statistics)
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")

# Collect statistics
spark.sql("ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS")
```

**Benefits:**
- Better join order selection
- More accurate cost estimation
- Improved broadcast join decisions

### 2. Adaptive Query Execution (AQE)

```python
# Enable AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Benefits:**
- Dynamically adjusts partitions
- Handles data skew automatically
- Optimizes shuffle partitions at runtime

### 3. Broadcast Join Optimization

```python
# Automatic broadcast for small tables (< 10MB default)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

# Manual broadcast hint
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

## 🚨 Common Optimization Issues

### Issue 1: Missing Statistics
**Problem**: CBO can't optimize without statistics
**Solution**: Run `ANALYZE TABLE` regularly

### Issue 2: Large Broadcast Tables
**Problem**: Broadcasting large tables causes OOM
**Solution**: Adjust `spark.sql.autoBroadcastJoinThreshold`

### Issue 3: Skewed Joins
**Problem**: Data skew causes straggler tasks
**Solution**: Enable AQE skew join handling

## 🎯 Practical Exercises

### Exercise 1: Compare Plans
```python
# Write the same query in two ways and compare plans
# Method 1: DataFrame API
df1 = spark.read.parquet("table1/")
df2 = spark.read.parquet("table2/")
result1 = df1.filter(df1.age > 25).join(df2, "id")

# Method 2: SQL
result2 = spark.sql("""
  SELECT t1.*, t2.* 
  FROM table1 t1
  JOIN table2 t2 ON t1.id = t2.id
  WHERE t1.age > 25
""")

# Compare plans
result1.explain(extended=True)
result2.explain(extended=True)
```

### Exercise 2: Force Optimization
```python
# Disable an optimization and see the difference
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "false")
# Run query and compare performance
```

### Exercise 3: Analyze Statistics Impact
```python
# 1. Run query without statistics
# 2. Collect statistics: ANALYZE TABLE ...
# 3. Run same query and compare plans
```

## 📊 Monitoring Optimization

### Key Metrics
- **Query compilation time**: Time to build plan
- **Number of stages**: Fewer is often better
- **Shuffle size**: Optimizer should minimize shuffles
- **Broadcast joins**: Check if small tables are broadcast

### Spark UI Analysis
1. Check "SQL" tab for query plans
2. Compare execution times before/after optimization
3. Monitor shuffle read/write sizes

## 💡 Best Practices

1. **Collect Statistics Regularly**
   ```sql
   ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
   ```

2. **Enable AQE** (if Spark 3.0+)
   - Automatically handles many optimization scenarios

3. **Use Broadcast Hints Wisely**
   - Only for small tables (< 100MB typically)

4. **Monitor Query Plans**
   - Regularly check `explain()` output
   - Look for optimization opportunities

5. **Partition Pruning**
   - Use partitioned columns in WHERE clauses
   - Enables partition elimination

## 📝 Key Takeaways

1. **Catalyst** applies multiple optimization phases
2. **Predicate pushdown** reduces data reading
3. **CBO** requires statistics for best results
4. **AQE** provides runtime optimizations
5. **Query plans** reveal optimization opportunities

## 🔗 Next Steps

- **Day 3**: Memory Management and Garbage Collection
- Practice: Analyze query plans for your common queries
- Experiment: Enable/disable optimizations and measure impact

---

**Progress**: Day 2/30+ ✅

