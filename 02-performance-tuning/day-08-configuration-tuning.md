# Day 8: Configuration Tuning Fundamentals

## 🎯 Learning Objectives
- Master Spark configuration parameters
- Learn how to right-size executors and resources
- Understand parallelism and partition tuning
- Optimize configurations for on-premise clusters
- Create configuration templates for different workloads

## 📚 Core Concepts

### 1. Configuration Hierarchy

**Configuration Priority (Highest to Lowest):**
1. **SparkConf in code** (highest priority)
2. **spark-submit --conf**
3. **spark-defaults.conf**
4. **spark-env.sh**
5. **Default values** (lowest priority)

**Setting Configurations:**
```python
# In code (highest priority)
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# Or after session creation
spark.conf.set("spark.executor.memory", "8g")
```

### 2. Executor Configuration

**Key Parameters:**
- `spark.executor.memory`: Memory per executor
- `spark.executor.cores`: CPU cores per executor
- `spark.executor.instances`: Number of executors
- `spark.executor.memoryOverhead`: Off-heap memory

**Right-Sizing Executors:**
```python
# Example: 10 nodes, 64GB RAM, 16 cores each
# Leave 16GB for OS: 48GB available per node
# Use 3 executors per node: 16GB per executor
# Leave 1 core for OS: 5 cores per executor

spark.conf.set("spark.executor.memory", "14g")
spark.conf.set("spark.executor.cores", "5")
spark.conf.set("spark.executor.instances", "30")  # 10 nodes * 3
spark.conf.set("spark.executor.memoryOverhead", "2g")
```

**Best Practices:**
- **Memory**: 8-16GB per executor (sweet spot)
- **Cores**: 3-5 cores per executor
- **Instances**: Balance between parallelism and overhead
- **Overhead**: 10-20% of executor memory (min 384MB)

### 3. Driver Configuration

**Key Parameters:**
- `spark.driver.memory`: Driver memory
- `spark.driver.cores`: Driver CPU cores
- `spark.driver.maxResultSize`: Max result size to collect

**Configuration:**
```python
# For large result collections
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")

# For broadcast joins with large tables
spark.conf.set("spark.driver.memory", "8g")
```

### 4. Parallelism Configuration

**Key Parameters:**
- `spark.default.parallelism`: Default number of partitions
- `spark.sql.shuffle.partitions`: Partitions for shuffles
- `spark.sql.files.maxPartitionBytes`: Max bytes per partition

**Setting Parallelism:**
```python
# Rule of thumb: 2-3x number of cores
total_cores = spark.conf.get("spark.executor.cores") * \
              spark.conf.get("spark.executor.instances")
optimal_parallelism = total_cores * 2

spark.conf.set("spark.default.parallelism", str(optimal_parallelism))
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_parallelism))
```

**File Reading Parallelism:**
```python
# Control partition size when reading files
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB
```

## 🔍 Deep Dive: Configuration Strategies

### 1. Small Cluster Configuration (< 10 nodes)

**Characteristics:**
- Limited resources
- Need efficient resource usage
- Focus on maximizing utilization

**Configuration:**
```python
# Executors
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.instances", "6")  # 2-3 per node

# Parallelism
spark.conf.set("spark.default.parallelism", "48")  # cores * 2
spark.conf.set("spark.sql.shuffle.partitions", "48")

# Memory
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### 2. Medium Cluster Configuration (10-50 nodes)

**Characteristics:**
- More resources available
- Can optimize for specific workloads
- Balance between parallelism and efficiency

**Configuration:**
```python
# Executors
spark.conf.set("spark.executor.memory", "14g")
spark.conf.set("spark.executor.cores", "5")
spark.conf.set("spark.executor.instances", "100")  # 2-3 per node

# Parallelism
spark.conf.set("spark.default.parallelism", "500")  # cores * 2
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### 3. Large Cluster Configuration (> 50 nodes)

**Characteristics:**
- Abundant resources
- Focus on throughput
- Can afford more overhead

**Configuration:**
```python
# Executors
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.cores", "5")
spark.conf.set("spark.executor.instances", "200")  # 2-3 per node

# Parallelism
spark.conf.set("spark.default.parallelism", "1000")
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Memory
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### 4. Workload-Specific Configurations

**ETL Workloads (Read-Heavy):**
```python
# More memory for caching
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.6")

# Optimize for reading
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB
```

**Analytical Workloads (Compute-Heavy):**
```python
# More memory for execution
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")

# More parallelism
spark.conf.set("spark.sql.shuffle.partitions", "800")
```

**Streaming Workloads:**
```python
# Smaller executors for better scheduling
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.cores", "2")
spark.conf.set("spark.executor.instances", "20")

# Dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
```

## 💡 Advanced Configuration

### 1. Adaptive Query Execution (Spark 3.0+)

**Enable AQE:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")

# Skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Benefits:**
- Automatically adjusts partitions
- Handles data skew
- Optimizes shuffle partitions at runtime

### 2. Cost-Based Optimization (CBO)

**Enable CBO:**
```python
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

### 3. Shuffle Configuration

**Optimize Shuffle:**
```python
# Compression
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")  # Fast compression

# Buffer sizes
spark.conf.set("spark.shuffle.file.buffer", "64k")  # Default: 32k
spark.conf.set("spark.shuffle.spill.initialMemoryThreshold", "5m")

# Local directories (use multiple disks)
spark.conf.set("spark.local.dir", "/data1/spark,/data2/spark,/data3/spark")
```

### 4. Garbage Collection Tuning

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

## 🎯 Practical Exercises

### Exercise 1: Calculate Optimal Configuration

```python
# Given cluster specs:
# - 20 nodes
# - 64GB RAM per node
# - 16 cores per node
# - Leave 16GB for OS

# Calculate:
# 1. Executor memory
# 2. Executor cores
# 3. Executor instances
# 4. Parallelism settings

# Your calculations here:
nodes = 20
ram_per_node = 64
cores_per_node = 16
os_reserved_gb = 16

available_ram = (ram_per_node - os_reserved_gb) * nodes
executors_per_node = 3
total_executors = nodes * executors_per_node
executor_memory = (available_ram / total_executors) * 0.9  # 90% for executor
executor_cores = (cores_per_node - 1) // executors_per_node  # Leave 1 core for OS

print(f"Executor Memory: {executor_memory:.0f}GB")
print(f"Executor Cores: {executor_cores}")
print(f"Total Executors: {total_executors}")
print(f"Parallelism: {total_executors * executor_cores * 2}")
```

### Exercise 2: Create Configuration Template

```python
# Create a function that generates optimal config for your cluster
def get_spark_config(cluster_specs):
    """
    cluster_specs = {
        'nodes': 20,
        'ram_per_node_gb': 64,
        'cores_per_node': 16,
        'os_reserved_gb': 16,
        'workload_type': 'analytical'  # 'etl', 'analytical', 'streaming'
    }
    """
    # Your implementation here
    pass

# Test with different cluster sizes
```

### Exercise 3: Compare Configurations

```python
# 1. Run query with default config
# 2. Run same query with optimized config
# 3. Compare:
#    - Execution time
#    - Resource usage
#    - Shuffle size
#    - Memory usage
```

## 💡 Best Practices for On-Premise

### 1. Configuration Management

**Use Configuration Files:**
```bash
# spark-defaults.conf
spark.executor.memory 14g
spark.executor.cores 5
spark.executor.instances 30
spark.sql.shuffle.partitions 300
```

**Override in Code:**
```python
# Only override what's needed
spark.conf.set("spark.sql.shuffle.partitions", "400")  # For this job
```

### 2. Right-Sizing Strategy

**Step 1: Understand Your Cluster**
- Total nodes, RAM, cores
- Network bandwidth
- Disk I/O capacity

**Step 2: Calculate Base Configuration**
- Executors per node
- Memory per executor
- Cores per executor

**Step 3: Tune for Workload**
- Adjust memory fractions
- Set parallelism
- Configure shuffle

**Step 4: Test and Iterate**
- Run representative workloads
- Measure performance
- Adjust based on metrics

### 3. Monitoring Configuration Impact

**Key Metrics:**
- Job execution time
- Resource utilization
- Shuffle size and time
- Memory usage and GC time
- Task execution times

**Regular Review:**
- Weekly configuration review
- Compare before/after changes
- Document what works

### 4. Configuration Templates

**Create Templates for:**
- Small batch jobs
- Large analytical queries
- ETL pipelines
- Streaming applications
- Interactive queries

## 🚨 Common Issues & Solutions

### Issue 1: Too Many Small Executors

**Symptom**: High overhead, slow performance

**Solution:**
```python
# Increase executor size, reduce instances
spark.conf.set("spark.executor.memory", "16g")  # Instead of 4g
spark.conf.set("spark.executor.cores", "5")  # Instead of 1
spark.conf.set("spark.executor.instances", "20")  # Instead of 80
```

### Issue 2: Too Few Large Executors

**Symptom**: Underutilization, long GC pauses

**Solution:**
```python
# Reduce executor size, increase instances
spark.conf.set("spark.executor.memory", "8g")  # Instead of 32g
spark.conf.set("spark.executor.cores", "4")  # Instead of 16
spark.conf.set("spark.executor.instances", "40")  # Instead of 10
```

### Issue 3: Wrong Parallelism

**Symptom**: Too many small tasks or too few large tasks

**Solution:**
```python
# Calculate based on cores
total_cores = executor_cores * executor_instances
spark.conf.set("spark.sql.shuffle.partitions", str(total_cores * 2))

# Or use AQE to auto-adjust
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Issue 4: Memory Pressure

**Symptom**: Frequent spills, OOM errors

**Solution:**
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "16g")

# Or adjust memory fractions
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")  # Less for cache
```

## 📝 Key Takeaways

1. **Right-size executors** - balance between size and count
2. **Set parallelism** - 2-3x number of cores typically
3. **Enable AQE** - automatic optimizations (Spark 3.0+)
4. **Tune for workload** - ETL vs analytical vs streaming
5. **Monitor and adjust** - configuration is iterative
6. **Use templates** - standardize configurations
7. **Test changes** - measure impact before deploying

## 🔗 Next Steps

- **Day 9**: Resource Allocation and YARN Integration
- Practice: Calculate optimal config for your cluster
- Experiment: Try different executor sizes
- Document: Create configuration templates

## 📚 Additional Resources

- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
- [Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- [Best Practices](https://spark.apache.org/docs/latest/best-practices.html)

---

**Progress**: Day 8/30+ ✅

