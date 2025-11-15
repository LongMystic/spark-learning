# Spark Quick Reference Guide

## 🔧 Essential Configurations

### Executor Configuration
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.instances", "10")
spark.conf.set("spark.executor.memoryOverhead", "2g")
```

### Parallelism
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
```

### Memory Management
```python
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Shuffle Optimization
```python
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Cost-Based Optimization
```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
```

## 📊 Query Plan Analysis

### View Execution Plans
```python
# Logical plan
df.explain(extended=True)

# Optimized logical plan
print(df.queryExecution.optimizedPlan)

# Physical plan
print(df.queryExecution.executedPlan)
```

### Check Spark UI
- Default: `http://driver:4040`
- YARN: `http://resource-manager:8088`

## 🔍 Common Debugging Commands

### Check Configuration
```python
spark.sparkContext.getConf().getAll()
```

### Monitor Executors
```python
# Check active executors
spark.sparkContext.statusTracker.getExecutorInfos()
```

### Analyze Statistics
```sql
-- Collect table statistics
ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;

-- Show table statistics
DESCRIBE EXTENDED table_name;
```

## 🚨 Common Issues & Quick Fixes

### OutOfMemoryError
- Increase executor memory
- Reduce partition size
- Enable spill to disk

### Too Many Small Tasks
- Increase `spark.sql.shuffle.partitions`
- Use `coalesce()` instead of `repartition()`

### Data Skew
- Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true`
- Use salting technique
- Increase shuffle partitions

### Slow Joins
- Enable broadcast for small tables
- Collect statistics for CBO
- Check join strategy in plan

## 📈 Performance Tuning Checklist

- [ ] Executor memory and cores configured
- [ ] Shuffle partitions set appropriately
- [ ] AQE enabled (Spark 3.0+)
- [ ] Statistics collected for CBO
- [ ] Broadcast joins used for small tables
- [ ] Partitioning strategy optimized
- [ ] Caching used appropriately
- [ ] Compression enabled for shuffle

## 🔗 Useful Links

- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---

**Keep this handy for quick reference! 📌**

