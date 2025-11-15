# Day 1: Spark Execution Model Deep Dive

## 🎯 Learning Objectives
- Understand Spark's execution model at a granular level
- Learn how jobs, stages, and tasks are orchestrated
- Master the DAG (Directed Acyclic Graph) execution
- Understand lazy evaluation and its implications

## 📚 Core Concepts

### 1. Spark Execution Hierarchy

```
Application
  └── Job (triggered by action)
      └── Stage (bounded by shuffle)
          └── Task (one per partition)
```

### 2. Lazy Evaluation

**Key Points:**
- Transformations are lazy (map, filter, join, etc.)
- Actions trigger execution (collect, count, write, etc.)
- Catalyst optimizer builds optimized execution plan
- Physical plan is created only when action is called

**Example:**
```python
# This doesn't execute anything
df = spark.read.parquet("data/")
filtered = df.filter(df.age > 25)
joined = filtered.join(other_df, "id")

# This triggers execution
result = joined.collect()  # Action!
```

### 3. DAG (Directed Acyclic Graph)

Spark builds a DAG of operations:
- **Narrow Dependencies**: No shuffle required (map, filter)
- **Wide Dependencies**: Shuffle required (groupBy, join, repartition)

**Stage Boundaries:**
- Stages are separated by shuffle operations
- Tasks within a stage can run in parallel
- Stages execute sequentially

### 4. Task Execution

**Task Components:**
- **Input**: Reads data from partitions
- **Computation**: Applies transformations
- **Output**: Writes results to partitions

**Task Scheduling:**
- Tasks are scheduled on executors
- Data locality is preferred
- Failed tasks are retried

## 🔍 Deep Dive: How Spark Executes a Job

### Step-by-Step Execution Flow

1. **User Code** → Spark creates RDD/DataFrame
2. **Lazy Evaluation** → Builds logical plan
3. **Action Triggered** → Catalyst optimizer kicks in
4. **Logical Plan** → Optimized logical plan
5. **Physical Plan** → DAG of stages
6. **DAG Scheduler** → Splits into stages
7. **Task Scheduler** → Creates tasks for each partition
8. **Executor Execution** → Tasks run on cluster

### Example: Understanding Stages

```python
# Example that creates multiple stages
df1 = spark.read.parquet("table1/")
df2 = spark.read.parquet("table2/")

# Stage 1: Read and filter df1
filtered1 = df1.filter(df1.status == "active")

# Stage 1: Read and filter df2  
filtered2 = df2.filter(df2.category == "A")

# Stage 2: Join (requires shuffle)
joined = filtered1.join(filtered2, "id")

# Stage 3: GroupBy (requires shuffle)
grouped = joined.groupBy("category").agg(sum("amount"))

# Stage 4: Write (action)
grouped.write.parquet("output/")
```

**Stage Breakdown:**
- **Stage 0**: Read and filter df1 (narrow)
- **Stage 1**: Read and filter df2 (narrow)
- **Stage 2**: Join operation (wide - shuffle)
- **Stage 3**: GroupBy operation (wide - shuffle)
- **Stage 4**: Write operation (narrow)

## 💡 Key Insights for On-Premise

### 1. Resource Manager Integration (YARN)

- **Application Master**: Coordinates with YARN ResourceManager
- **Container Allocation**: YARN allocates containers for executors
- **Dynamic Allocation**: Can request/release executors based on load

### 2. Memory Management

**Executor Memory Structure:**
```
Total Executor Memory
├── Reserved Memory (300MB)
├── User Memory (for UDFs, aggregations)
└── Spark Memory
    ├── Storage Memory (caching)
    └── Execution Memory (shuffle, joins)
```

**Key Configurations:**
- `spark.executor.memory`: Total executor memory
- `spark.memory.fraction`: Fraction for Spark operations (default 0.6)
- `spark.memory.storageFraction`: Fraction for caching (default 0.5)

### 3. Shuffle Mechanics

**Shuffle Process:**
1. **Map Phase**: Each task writes shuffle files
2. **Fetch Phase**: Tasks read shuffle files from other nodes
3. **Network I/O**: High network traffic during shuffle

**Shuffle Files Location:**
- Stored in `spark.local.dir` (default: `/tmp`)
- Cleaned up after stage completion
- Can be persisted for fault tolerance

## 🎯 Practical Exercises

### Exercise 1: Analyze DAG
```python
# Run this and examine the DAG in Spark UI
df = spark.read.parquet("your_table/")
result = (df
    .filter(df.col1 > 100)
    .groupBy("category")
    .agg(sum("amount").alias("total"))
    .orderBy("total", ascending=False)
)

# Check Spark UI: http://driver:4040
# Navigate to "Jobs" → "Stages" → "DAG Visualization"
```

### Exercise 2: Count Stages
```python
# Write a query that creates exactly 3 stages
# Hint: Use operations that require shuffles
```

### Exercise 3: Monitor Task Execution
```python
# Run a job and observe:
# 1. Number of tasks per stage
# 2. Task execution time
# 3. Data locality (NODE_LOCAL, RACK_LOCAL, ANY)
```

## 📊 Spark UI Deep Dive

### Key Metrics to Monitor

1. **Jobs Tab:**
   - Total execution time
   - Number of stages
   - Success/failure status

2. **Stages Tab:**
   - Stage execution time
   - Input/output sizes
   - Shuffle read/write sizes
   - Task distribution

3. **Storage Tab:**
   - Cached RDDs/DataFrames
   - Memory usage
   - Disk usage

4. **Executors Tab:**
   - Active executors
   - Memory usage per executor
   - Task execution metrics

## 🚨 Common Issues & Solutions

### Issue 1: Too Many Small Tasks
**Symptom**: Thousands of tiny tasks
**Solution**: Increase partition size, use `coalesce()` instead of `repartition()`

### Issue 2: Stage Skipping
**Symptom**: Some stages show as "skipped"
**Cause**: Data already computed and cached
**Action**: Check Storage tab for cached data

### Issue 3: Long Shuffle Time
**Symptom**: Shuffle operations take too long
**Solution**: 
- Increase `spark.sql.shuffle.partitions`
- Optimize join strategies
- Consider broadcast joins for small tables

## 📝 Key Takeaways

1. **Lazy Evaluation** allows Spark to optimize the entire query plan
2. **Stages** are separated by shuffle operations
3. **Tasks** are the unit of parallel execution
4. **DAG** represents the execution plan visually
5. **Spark UI** is your best friend for understanding execution

## 🔗 Next Steps

- **Day 2**: Catalyst Optimizer Internals
- Practice: Run queries and analyze their DAGs in Spark UI
- Experiment: Create queries with different numbers of stages

## 📚 Additional Resources

- [Spark Architecture Documentation](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Understanding Spark Execution](https://spark.apache.org/docs/latest/job-scheduling.html)

---

**Progress**: Day 1/30+ ✅

