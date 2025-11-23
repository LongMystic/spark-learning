# Day 9: Resource Allocation and YARN Integration

## 🎯 Learning Objectives
- Understand YARN resource allocation
- Master dynamic allocation in Spark
- Learn resource negotiation strategies
- Optimize resource usage for on-premise clusters
- Handle resource contention and scheduling

## 📚 Core Concepts

### 1. YARN Resource Model

**YARN Components:**
- **ResourceManager**: Global resource manager
- **NodeManager**: Per-node resource manager
- **ApplicationMaster**: Per-application coordinator
- **Container**: Resource allocation unit

**Resource Types:**
- **Memory**: Measured in MB/GB
- **CPU**: Measured in virtual cores (vcores)

**Resource Allocation:**
```python
# Spark requests resources from YARN
# YARN allocates containers
# Spark runs executors in containers
```

### 2. Static vs Dynamic Allocation

**Static Allocation:**
```python
# Fixed number of executors
spark.conf.set("spark.executor.instances", "30")
spark.conf.set("spark.dynamicAllocation.enabled", "false")
```

**Dynamic Allocation:**
```python
# Executors allocated based on demand
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

### 3. YARN Configuration

**Key YARN Settings:**
- `yarn.nodemanager.resource.memory-mb`: Memory per node
- `yarn.nodemanager.resource.cpu-vcores`: Cores per node
- `yarn.scheduler.maximum-allocation-mb`: Max container memory
- `yarn.scheduler.maximum-allocation-vcores`: Max container cores

**Spark-YARN Integration:**
```python
# Submit to YARN
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 14g \
  --executor-cores 5 \
  --num-executors 30 \
  app.py
```

## 🔍 Deep Dive: Dynamic Allocation

### How Dynamic Allocation Works

**Allocation Process:**
1. **Initial**: Start with `initialExecutors`
2. **Scale Up**: Add executors when tasks are pending
3. **Scale Down**: Remove idle executors after timeout
4. **Bounds**: Respect `minExecutors` and `maxExecutors`

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")

# Executor bounds
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")

# Scaling behavior
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "5s")
```

### When to Use Dynamic Allocation

**Good For:**
- Variable workloads
- Multiple concurrent applications
- Resource sharing
- Cost optimization

**Not Good For:**
- Predictable, consistent workloads
- Low-latency requirements
- When you need guaranteed resources

### Dynamic Allocation Behavior

**Scale Up Triggers:**
- Pending tasks in queue
- Backlog timeout exceeded
- Sustained backlog timeout exceeded

**Scale Down Triggers:**
- Executor idle for `executorIdleTimeout`
- No cached data (if `cachedExecutorIdleTimeout` not set)

**Example:**
```python
# Job starts with 10 executors
# Tasks queue up → scales to 30 executors
# Tasks complete → scales down to 5 executors (min)
# Cached data keeps executors alive
```

## 💡 Resource Allocation Strategies

### 1. Conservative Allocation

**Strategy**: Request fewer resources, scale up if needed

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

**Use Case**: Shared cluster, multiple users

### 2. Aggressive Allocation

**Strategy**: Request more resources upfront

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "20")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "50")
```

**Use Case**: Dedicated resources, performance critical

### 3. Static Allocation

**Strategy**: Fixed resources, no scaling

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "30")
```

**Use Case**: Predictable workloads, guaranteed resources

### 4. Hybrid Approach

**Strategy**: Static for base, dynamic for peaks

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "20")  # Base load
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")  # Peak load
```

## 🔍 Deep Dive: YARN Integration

### Container Allocation

**Container Size:**
```python
# Executor memory + overhead
executor_memory = 14 * 1024  # 14GB in MB
executor_memoryOverhead = 2 * 1024  # 2GB in MB
container_memory = executor_memory + executor_memoryOverhead  # 16GB

# Executor cores
executor_cores = 5
container_vcores = executor_cores
```

**YARN Constraints:**
- Container memory must be ≤ `yarn.scheduler.maximum-allocation-mb`
- Container vcores must be ≤ `yarn.scheduler.maximum-allocation-vcores`
- Total resources ≤ node capacity

### Resource Negotiation

**Request Process:**
1. Spark requests containers from YARN
2. YARN checks available resources
3. YARN allocates containers if available
4. Spark launches executors in containers
5. Executors register with Spark driver

**Common Issues:**
- **Insufficient Resources**: YARN can't allocate requested containers
- **Resource Fragmentation**: Resources available but not contiguous
- **Queue Limits**: Queue doesn't have enough capacity

### Queue Configuration

**YARN Queues:**
```python
# Submit to specific queue
spark-submit \
  --master yarn \
  --queue production \
  app.py

# Or in code
spark.conf.set("spark.yarn.queue", "production")
```

**Queue Properties:**
- **Capacity**: Percentage of cluster resources
- **Max Capacity**: Maximum percentage (can borrow from other queues)
- **User Limits**: Per-user resource limits

## 🎯 Practical Exercises

### Exercise 1: Configure Dynamic Allocation

```python
# 1. Enable dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")

# 2. Set bounds
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")

# 3. Run a workload
df = spark.read.parquet("large_table/")
result = df.groupBy("key").agg(sum("amount"))

# 4. Monitor in Spark UI:
#    - Executors tab: Watch executor count change
#    - Jobs tab: See scaling behavior
```

### Exercise 2: Compare Static vs Dynamic

```python
# Configuration 1: Static
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "20")
# Run query and measure time

# Configuration 2: Dynamic
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
# Run same query and measure time

# Compare:
# - Execution time
# - Resource utilization
# - Cost (if applicable)
```

### Exercise 3: Monitor Resource Allocation

```python
# 1. Run job with dynamic allocation
# 2. Check YARN ResourceManager UI
#    - Application details
#    - Container allocations
#    - Resource usage
# 3. Check Spark UI
#    - Executor count over time
#    - Resource usage per executor
# 4. Analyze:
#    - When executors were added/removed
#    - Resource utilization patterns
```

## 💡 Best Practices for On-Premise

### 1. YARN Configuration

**NodeManager Settings:**
```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>49152</value>  <!-- 48GB (64GB - 16GB for OS) -->
</property>
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>15</value>  <!-- 16 cores - 1 for OS -->
</property>
```

**Scheduler Settings:**
```xml
<property>
  <name>yarn.scheduler.maximum-allocation-mb</name>
  <value>16384</value>  <!-- 16GB max container -->
</property>
<property>
  <name>yarn.scheduler.maximum-allocation-vcores</name>
  <value>8</value>  <!-- 8 cores max container -->
</property>
```

### 2. Dynamic Allocation Tuning

**For Interactive Workloads:**
```python
# Keep executors longer for cached data
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "300s")
```

**For Batch Workloads:**
```python
# Aggressive scaling
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

**For Streaming:**
```python
# Static allocation recommended
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "20")
```

### 3. Queue Management

**Create Queues for:**
- Production workloads (guaranteed resources)
- Development workloads (shared resources)
- ETL pipelines (scheduled resources)
- Ad-hoc queries (best-effort resources)

**Queue Configuration:**
```xml
<!-- capacity-scheduler.xml -->
<queue name="production">
  <capacity>50</capacity>
  <maxCapacity>80</maxCapacity>
</queue>
<queue name="development">
  <capacity>30</capacity>
  <maxCapacity>50</maxCapacity>
</queue>
```

### 4. Resource Monitoring

**Key Metrics:**
- Container allocation rate
- Resource utilization per queue
- Pending applications
- Executor lifecycle (add/remove events)

**Tools:**
- YARN ResourceManager UI
- Spark UI Executors tab
- Cluster monitoring tools (Ganglia, Prometheus)

## 🚨 Common Issues & Solutions

### Issue 1: Executors Not Allocated

**Symptom**: Job stuck, no executors starting

**Root Causes:**
- Insufficient cluster resources
- Queue capacity limits
- YARN configuration issues

**Solution:**
```python
# Check available resources
# Reduce executor size
spark.conf.set("spark.executor.memory", "8g")  # Instead of 16g
spark.conf.set("spark.executor.cores", "4")  # Instead of 8

# Or use different queue
spark.conf.set("spark.yarn.queue", "development")
```

### Issue 2: Slow Executor Allocation

**Symptom**: Long wait time for executors

**Root Cause**: Resource contention, many pending applications

**Solution:**
```python
# Increase initial executors
spark.conf.set("spark.dynamicAllocation.initialExecutors", "20")

# Or use static allocation for critical jobs
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "30")
```

### Issue 3: Executors Removed Too Quickly

**Symptom**: Executors removed before next stage

**Root Cause**: Idle timeout too short

**Solution:**
```python
# Increase idle timeout
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "300s")

# Keep executors with cached data
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity")
```

### Issue 4: Not Scaling Up Enough

**Symptom**: Slow performance, pending tasks

**Root Cause**: Max executors too low or scaling too slow

**Solution:**
```python
# Increase max executors
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")

# Faster scaling
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "3s")
```

### Issue 5: Resource Fragmentation

**Symptom**: Resources available but containers not allocated

**Root Cause**: Fragmented resources, can't fit requested size

**Solution:**
```python
# Reduce container size
spark.conf.set("spark.executor.memory", "8g")  # Smaller containers
spark.conf.set("spark.executor.cores", "4")

# Or request smaller initial allocation
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
```

## 📝 Key Takeaways

1. **Dynamic allocation** adapts to workload demand
2. **YARN manages** cluster resources globally
3. **Container size** must fit YARN limits
4. **Queue configuration** affects resource availability
5. **Monitor allocation** to optimize performance
6. **Static allocation** for predictable workloads
7. **Dynamic allocation** for variable workloads
8. **Right-size containers** to avoid fragmentation

## 🔗 Next Steps

- **Day 10**: Data Skew Handling
- Practice: Configure dynamic allocation for your workloads
- Experiment: Compare static vs dynamic allocation
- Monitor: Track resource utilization patterns

## 📚 Additional Resources

- [Dynamic Allocation Guide](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
- [YARN Configuration](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
- [Resource Management](https://spark.apache.org/docs/latest/running-on-yarn.html)

---

**Progress**: Day 9/30+ ✅

