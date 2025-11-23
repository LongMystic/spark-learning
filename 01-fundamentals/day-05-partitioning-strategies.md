# Day 5: Partitioning Strategies

## 🎯 Learning Objectives
- Understand different partitioning strategies
- Learn when to use each partitioning approach
- Master partition pruning and optimization
- Optimize partitioning for Hive and Iceberg tables
- Diagnose partitioning-related performance issues

## 📚 Core Concepts

### 1. What is Partitioning?

**Definition:**
Partitioning divides data into separate directories/files based on column values. This allows Spark to skip reading irrelevant data (partition pruning).

**Benefits:**
- **Faster Queries**: Only read relevant partitions
- **Better Performance**: Less I/O, less data processed
- **Cost Savings**: Reduced compute resources

**Example:**
```
data/
├── year=2023/
│   ├── month=01/
│   ├── month=02/
│   └── month=03/
└── year=2024/
    ├── month=01/
    └── month=02/
```

### 2. Types of Partitioning

**1. File System Partitioning (Hive-Style)**
- Directory-based partitioning
- Used by Hive, Parquet, ORC
- Most common in on-premise clusters

**2. Table Partitioning**
- Managed by table format (Hive, Iceberg)
- Can be combined with file partitioning
- Enables partition pruning

**3. Bucketing**
- Hash-based partitioning within partitions
- Used for join optimization
- Different from directory partitioning

### 3. Hive-Style Partitioning

**Structure:**
```
table_name/
├── partition_col1=value1/
│   ├── partition_col2=value2/
│   │   └── data.parquet
│   └── partition_col2=value3/
│       └── data.parquet
└── partition_col1=value2/
    └── data.parquet
```

**Writing Partitioned Data:**
```python
# Method 1: partitionBy()
df.write.mode("overwrite").partitionBy("year", "month").parquet("output/")

# Method 2: insertInto() for Hive tables
df.write.mode("append").insertInto("hive_table")

# Method 3: Dynamic partition insert
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").insertInto("hive_table")
```

**Reading Partitioned Data:**
```python
# Automatic partition pruning
df = spark.read.parquet("output/")
filtered = df.filter((df.year == 2023) & (df.month == 1))
# Only reads year=2023/month=01/ directory

# For Hive tables
df = spark.table("hive_table")
filtered = df.filter((df.year == 2023) & (df.month == 1))
```

## 🔍 Deep Dive: Partition Pruning

### How Partition Pruning Works

**Process:**
1. **Query Analysis**: Spark analyzes WHERE clause
2. **Partition Discovery**: Lists available partitions
3. **Filter Application**: Matches partitions to predicates
4. **Pruning**: Skips irrelevant partitions
5. **Execution**: Only reads matching partitions

**Example:**
```python
# Table partitioned by (year, month, day)
# Query:
df.filter((df.year == 2023) & (df.month == 1) & (df.day == 15))

# Without pruning: Reads all partitions
# With pruning: Only reads year=2023/month=01/day=15/
# Performance improvement: 100x-1000x for large tables
```

### Partition Pruning Conditions

**Works With:**
- Equality predicates: `col == value`
- Range predicates: `col > value`, `col BETWEEN val1 AND val2`
- IN clauses: `col IN (val1, val2, val3)`
- Multiple partition columns: `col1 == val1 AND col2 == val2`

**Doesn't Work With:**
- Functions on partition columns: `YEAR(date_col) == 2023`
- Complex expressions: `col1 + col2 == value`
- UDFs on partition columns

**Best Practice:**
```python
# Bad: Function on partition column
df.filter(year(df.date) == 2023)  # No pruning!

# Good: Use partition column directly
df.filter(df.year == 2023)  # Pruning works!
```

## 💡 Partitioning Strategies

### 1. Choosing Partition Columns

**Good Partition Columns:**
- **High Cardinality**: Many distinct values
- **Query Filters**: Frequently used in WHERE clauses
- **Balanced Distribution**: Relatively even data distribution
- **Not Too Many**: 2-3 partition columns typically

**Bad Partition Columns:**
- **Low Cardinality**: Few distinct values (creates many small files)
- **Skewed**: One value has most data
- **Too Many**: 5+ partition columns (too many directories)

**Example:**
```python
# Good: Date partitioning
df.write.partitionBy("year", "month").parquet("output/")
# - High cardinality (many dates)
# - Common in queries
# - Relatively balanced

# Bad: User ID partitioning
df.write.partitionBy("user_id").parquet("output/")
# - Very high cardinality (millions of users)
# - Creates millions of small partitions
# - Poor performance
```

### 2. Partition Granularity

**Too Coarse (Few Partitions):**
- Large partitions, slow queries
- Less pruning benefit
- Example: Only `year` partition

**Too Fine (Many Partitions):**
- Many small files (small file problem)
- High metadata overhead
- Example: `year`, `month`, `day`, `hour`, `minute`

**Sweet Spot:**
- Balance between pruning and file size
- Typical: `year`, `month` or `year`, `month`, `day`
- Target: 100MB - 1GB per partition

### 3. Dynamic vs Static Partitioning

**Static Partitioning:**
```python
# Specify partition values explicitly
df.write.mode("overwrite").partitionBy("year", "month").parquet("output/")
# All partitions written

# For specific partition
df.filter((df.year == 2023) & (df.month == 1)) \
  .write.mode("overwrite").parquet("output/year=2023/month=01/")
```

**Dynamic Partitioning:**
```python
# Spark determines partitions from data
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("year", "month").parquet("output/")
# Only overwrites partitions present in data

# Benefits:
# - Only updates changed partitions
# - Faster for incremental updates
# - Less risk of data loss
```

## 🎯 Partitioning for Hive Tables

### Hive Table Partitioning

**Creating Partitioned Hive Table:**
```sql
CREATE TABLE sales (
    id INT,
    product STRING,
    amount DECIMAL(10,2)
) PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;

-- Insert data
INSERT INTO TABLE sales PARTITION(year=2023, month=1)
SELECT id, product, amount FROM source_table;
```

**Reading from Hive:**
```python
# Automatic partition pruning
df = spark.table("sales")
result = df.filter((df.year == 2023) & (df.month == 1))

# Check if pruning occurred
result.explain()
# Look for: PartitionFilters: [isnotnull(year#X), (year#X = 2023), ...]
```

### Hive Partition Management

**Adding Partitions:**
```sql
ALTER TABLE sales ADD PARTITION (year=2023, month=12);
```

**Dropping Partitions:**
```sql
ALTER TABLE sales DROP PARTITION (year=2022);
```

**Repairing Partitions:**
```sql
-- If partitions added outside Spark
MSCK REPAIR TABLE sales;
-- Or
ALTER TABLE sales RECOVER PARTITIONS;
```

## 🎯 Partitioning for Iceberg Tables

### Iceberg Partitioning

**Creating Partitioned Iceberg Table:**
```python
# Using PySpark
spark.sql("""
    CREATE TABLE iceberg_sales (
        id BIGINT,
        product STRING,
        amount DECIMAL(10,2),
        sale_date DATE
    ) USING ICEBERG
    PARTITIONED BY (years(sale_date), months(sale_date))
""")
```

**Iceberg Partition Evolution:**
```python
# Can change partitioning without rewriting all data
spark.sql("""
    ALTER TABLE iceberg_sales
    ADD PARTITION FIELD days(sale_date)
""")
# Only new data uses new partitioning
# Old data remains accessible
```

**Benefits of Iceberg:**
- **Hidden Partitioning**: Partition columns not in data
- **Partition Evolution**: Change partitioning over time
- **Better Pruning**: More efficient than Hive
- **Time Travel**: Query historical partitions

### Iceberg Partition Strategies

**Identity Partitioning:**
```python
PARTITIONED BY (year, month)  # Direct column values
```

**Transform Partitioning:**
```python
PARTITIONED BY (
    years(sale_date),      # Extract year
    months(sale_date),     # Extract month
    bucket(16, customer_id)  # Hash bucket
)
```

**Best Practices:**
- Use transforms for date columns
- Use bucket() for high-cardinality columns
- Combine identity and transforms

## 🔍 Deep Dive: Small File Problem

### The Problem

**Symptoms:**
- Many small files (< 10MB each)
- Slow query performance
- High metadata overhead
- Long listing operations

**Causes:**
- Too many partitions
- Small batch writes
- Frequent appends

**Impact:**
- Listing 10,000 files takes minutes
- Each file has overhead
- Poor I/O efficiency

### Solutions

**1. Coalesce Before Writing:**
```python
# Coalesce to reduce files
df.coalesce(10).write.partitionBy("year", "month").parquet("output/")
# Creates ~10 files per partition instead of many

# Or repartition
df.repartition(10, "year", "month").write.parquet("output/")
```

**2. Merge Small Files:**
```python
# For existing tables, merge small files
# Using Iceberg (has built-in compaction)
spark.sql("""
    CALL system.rewrite_data_files(
        table => 'db.iceberg_table',
        strategy => 'binpack',
        options => map('target-file-size-bytes', '134217728')
    )
""")

# For Hive/Parquet, use custom merge job
```

**3. Adjust Partition Granularity:**
```python
# Instead of: year, month, day, hour
# Use: year, month, day
# Then use repartition() for file size control
```

**4. Batch Writes:**
```python
# Instead of many small writes
# Batch data and write less frequently
df.write.mode("append").partitionBy("year", "month").parquet("output/")
# Write in larger batches
```

## 🎯 Practical Exercises

### Exercise 1: Analyze Partition Pruning

```python
# 1. Create partitioned table
df = spark.range(0, 1000000).withColumn("year", 
    (col("id") % 5) + 2020).withColumn("month", 
    (col("id") % 12) + 1)

df.write.mode("overwrite").partitionBy("year", "month").parquet("partitioned_data/")

# 2. Query with partition filter
result = spark.read.parquet("partitioned_data/") \
    .filter((col("year") == 2023) & (col("month") == 1))

# 3. Check explain plan
result.explain(extended=True)
# Look for: PartitionFilters in physical plan

# 4. Compare with non-partitioned query
# Measure performance difference
```

### Exercise 2: Optimize Partition Granularity

```python
# 1. Create data with different partition strategies
# Strategy 1: Only year
df.write.partitionBy("year").parquet("strategy1/")

# Strategy 2: year, month
df.write.partitionBy("year", "month").parquet("strategy2/")

# Strategy 3: year, month, day
df.write.partitionBy("year", "month", "day").parquet("strategy3/")

# 2. Query each and measure:
#    - Query time
#    - Files read
#    - Data scanned
#    - Number of partitions

# 3. Find optimal granularity for your use case
```

### Exercise 3: Handle Small Files

```python
# 1. Create table with many small files
df = spark.range(0, 100000)
# Write in many small batches
for i in range(100):
    df.filter(col("id") % 100 == i).write.mode("append") \
      .partitionBy("year", "month").parquet("small_files/")

# 2. Measure query performance
spark.read.parquet("small_files/").count()

# 3. Coalesce and rewrite
df_coalesced = spark.read.parquet("small_files/") \
    .repartition(10, "year", "month")
df_coalesced.write.mode("overwrite") \
    .partitionBy("year", "month").parquet("coalesced_files/")

# 4. Compare query performance
spark.read.parquet("coalesced_files/").count()
```

## 💡 Best Practices for On-Premise

### 1. Hive Table Partitioning

**Recommended Structure:**
```sql
-- Partition by date components
PARTITIONED BY (year INT, month INT, day INT)

-- Or single date partition
PARTITIONED BY (date STRING)  -- Format: '2023-01-15'
```

**Writing Best Practices:**
```python
# Use dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Coalesce before writing
df.coalesce(num_files_per_partition).write \
    .mode("overwrite").insertInto("hive_table")
```

### 2. Iceberg Table Partitioning

**Recommended:**
```python
# Use transform partitioning
PARTITIONED BY (
    years(sale_date),
    months(sale_date),
    bucket(16, customer_id)  # For join optimization
)
```

**Maintenance:**
```python
# Regular compaction
spark.sql("CALL system.rewrite_data_files('db.table')")

# Expire old snapshots
spark.sql("CALL system.expire_snapshots('db.table', timestamp '2023-01-01')")
```

### 3. Partition Column Selection

**Checklist:**
- [ ] Column used in WHERE clauses frequently
- [ ] Reasonable cardinality (not too high, not too low)
- [ ] Balanced data distribution
- [ ] Not too many partition columns (2-3 max)

### 4. Monitoring Partition Health

**Metrics to Track:**
- Number of partitions
- Average partition size
- Number of small files (< 10MB)
- Partition pruning effectiveness
- Query performance on partitioned tables

## 🚨 Common Issues & Solutions

### Issue 1: No Partition Pruning

**Symptom**: Query reads all partitions despite filter

**Root Causes:**
- Function on partition column
- Complex expression
- Partition column not in filter

**Solution:**
```python
# Bad
df.filter(year(df.date) == 2023)

# Good: Use partition column directly
df.filter(df.year == 2023)

# Or add partition column to data
df = df.withColumn("year", year(df.date))
df.write.partitionBy("year").parquet("output/")
```

### Issue 2: Too Many Small Files

**Symptom**: Thousands of small files, slow queries

**Root Cause**: Too fine-grained partitioning or many small writes

**Solution:**
```python
# Coalesce before writing
df.coalesce(10).write.partitionBy("year", "month").parquet("output/")

# Or use repartition
df.repartition(10, "year", "month").write.parquet("output/")

# For Iceberg: Use compaction
spark.sql("CALL system.rewrite_data_files('db.table')")
```

### Issue 3: Partition Skew

**Symptom**: Some partitions much larger than others

**Root Cause**: Uneven data distribution

**Solution:**
```python
# Option 1: Adjust partition columns
# Use more balanced column

# Option 2: Use bucketing within partitions
df.write.bucketBy(10, "skewed_column") \
    .partitionBy("year", "month").saveAsTable("table")

# Option 3: Manual repartitioning
df.repartition("year", "month", "other_column") \
    .write.partitionBy("year", "month").parquet("output/")
```

### Issue 4: Slow Partition Discovery

**Symptom**: Long time to list partitions

**Root Cause**: Too many partitions or slow filesystem

**Solution:**
```python
# For Hive: Use partition pruning early
df = spark.table("hive_table")
df.filter(df.year == 2023)  # Prune before other operations

# For Iceberg: Use metadata tables
spark.sql("SELECT * FROM db.table.partitions WHERE year = 2023")

# Consider reducing partition granularity
```

### Issue 5: Dynamic Partition Overwrite Issues

**Symptom**: Unexpected data loss or incomplete updates

**Root Cause**: Misconfigured partition overwrite mode

**Solution:**
```python
# Use dynamic mode for incremental updates
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Use static mode for full overwrites
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
df.write.mode("overwrite").parquet("output/")
```

## 📝 Key Takeaways

1. **Partitioning enables partition pruning** - huge performance boost
2. **Choose partition columns wisely** - balance cardinality and query patterns
3. **Avoid too many partitions** - causes small file problem
4. **Use partition columns directly** - functions prevent pruning
5. **Coalesce before writing** - avoid small files
6. **Monitor partition health** - track file sizes and counts
7. **Iceberg offers advantages** - hidden partitioning, evolution, better pruning
8. **Dynamic partition overwrite** - safer for incremental updates

## 🔗 Next Steps

- **Day 6**: Join Algorithms and Optimization
- Practice: Analyze partitioning in your tables
- Experiment: Try different partition strategies
- Optimize: Improve partition pruning in your queries

## 📚 Additional Resources

- [Spark Partitioning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html#partition-discovery)
- [Iceberg Partitioning](https://iceberg.apache.org/docs/latest/partitioning/)
- [Hive Partitioning](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables)

---

**Progress**: Day 5/30+ ✅

