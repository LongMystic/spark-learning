# Day 14: Network Optimization

## 🎯 Learning Objectives
- Understand network's role in Spark performance
- Learn to optimize network I/O
- Master shuffle network optimization
- Handle network bottlenecks
- Tune network for on-premise clusters

## 📚 Core Concepts

### 1. Network in Spark

**Network Usage:**
- **Shuffle Operations**: Data transfer between executors
- **Broadcast Variables**: Distributing data to all executors
- **Block Transfer**: Reading data from remote nodes
- **RPC Communication**: Driver-executor communication

**Network Bottlenecks:**
- **Bandwidth**: Limited network capacity
- **Latency**: Network delay
- **Contention**: Multiple jobs competing
- **Topology**: Network layout and routing

### 2. Shuffle Network I/O

**Shuffle Process:**
1. **Map Phase**: Write shuffle files locally
2. **Network Transfer**: Fetch shuffle data over network
3. **Reduce Phase**: Read and process shuffle data

**Network Impact:**
- Shuffle is network-intensive
- Can be bottleneck for large shuffles
- Affects job performance significantly

**Optimization Goals:**
- Minimize shuffle data size
- Optimize network transfer
- Reduce network contention
- Improve data locality

### 3. Network Configuration

**Key Parameters:**
- `spark.reducer.maxSizeInFlight`: Max data in flight per reducer
- `spark.reducer.maxReqsInFlight`: Max concurrent requests
- `spark.network.timeout`: Network timeout
- `spark.shuffle.io.numConnectionsPerPeer`: Connections per peer

## 🔍 Deep Dive: Network Optimization Techniques

### 1. Reduce Shuffle Data Size

**Principle**: Less data = less network transfer

**Strategies:**
```python
# Column pruning
df.select("col1", "col2").join(...)  # Only needed columns

# Predicate pushdown
df.filter(...).join(...)  # Filter before shuffle

# Compression
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")
```

**Benefits:**
- Faster network transfer
- Less bandwidth usage
- Lower latency

### 2. Optimize Shuffle Fetch

**Configuration:**
```python
# Increase data in flight
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  # Default: 48m

# Limit concurrent requests (reduce overhead)
spark.conf.set("spark.reducer.maxReqsInFlight", "1")  # Default: Int.Max

# Connections per peer
spark.conf.set("spark.shuffle.io.numConnectionsPerPeer", "1")  # Default: 1
```

**Tuning Guidelines:**
- **maxSizeInFlight**: Increase if network is fast
- **maxReqsInFlight**: Reduce if network is slow
- Balance between throughput and overhead

### 3. Improve Data Locality

**Principle**: Prefer local data over network

**Strategies:**
```python
# Co-locate data and compute
# Use same partition key for data and operations

# Prefer node-local over rack-local
# Prefer rack-local over any
```

**Spark Automatically:**
- Prefers local data
- Falls back to network if needed
- Tracks data locality

### 4. Network Topology Awareness

**For On-Premise Clusters:**
- Understand network layout
- Rack awareness (if available)
- Optimize for network topology

**YARN Rack Awareness:**
```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>
```

**Benefits:**
- Prefers same rack
- Reduces cross-rack traffic
- Better network utilization

### 5. Broadcast Optimization

**Reduce Network Transfer:**
```python
# Broadcast small tables instead of shuffling
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
# Small table sent once, not shuffled
```

**Monitor Broadcast Size:**
```python
# Check broadcast table size
# Don't broadcast > 200MB (risk OOM)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
```

## 💡 Advanced Network Optimization

### 1. Shuffle Compression

**Enable Compression:**
```python
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")
```

**Codec Selection:**
- **lz4**: Fast compression, good for network
- **snappy**: Balanced
- **zstd**: Better compression, slightly slower

**Recommendation:**
```python
# For shuffle: Use lz4 (fast)
spark.conf.set("spark.io.compression.codec", "lz4")
```

### 2. Network Timeout Configuration

**Settings:**
```python
# Network timeout
spark.conf.set("spark.network.timeout", "300s")  # Default: 120s

# RPC timeout
spark.conf.set("spark.rpc.askTimeout", "300s")  # Default: spark.network.timeout
```

**Tuning:**
- Increase for slow networks
- Decrease for fast networks
- Balance between timeout and retry overhead

### 3. Connection Pooling

**Configuration:**
```python
# Connections per peer
spark.conf.set("spark.shuffle.io.numConnectionsPerPeer", "1")

# Connection timeout
spark.conf.set("spark.shuffle.io.connectionTimeout", "60s")
```

**Best Practice:**
- One connection per peer typically sufficient
- More connections = more overhead
- Adjust based on network capacity

### 4. Network Monitoring

**Key Metrics:**
- Network I/O during shuffle
- Bytes sent/received
- Network utilization
- Fetch wait time

**Tools:**
- Spark UI (Stages, Tasks tabs)
- Network monitoring tools
- Cluster monitoring (Ganglia, Prometheus)

## 🎯 Practical Exercises

### Exercise 1: Measure Network Impact

```python
# 1. Run query with shuffle
df = spark.read.parquet("large_table/")
result = df.groupBy("category").agg(sum("amount"))

# 2. Check Spark UI - Stages tab:
#    - Shuffle read size
#    - Remote bytes read
#    - Fetch wait time
#    - Network I/O metrics

# 3. Optimize shuffle size:
#    - Select only needed columns
#    - Filter before groupBy
#    - Compare network metrics
```

### Exercise 2: Compare Compression Codecs

```python
# 1. Test with lz4
spark.conf.set("spark.io.compression.codec", "lz4")
df.groupBy("key").count().collect()
# Measure: execution time, network I/O

# 2. Test with zstd
spark.conf.set("spark.io.compression.codec", "zstd")
df.groupBy("key").count().collect()
# Measure: execution time, network I/O

# 3. Compare:
#    - Compression ratio
#    - Network transfer time
#    - CPU usage
```

### Exercise 3: Optimize Shuffle Fetch

```python
# 1. Default settings
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")
df.groupBy("key").count().collect()
# Measure: execution time, network metrics

# 2. Increased in-flight data
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
df.groupBy("key").count().collect()
# Measure: execution time, network metrics

# 3. Compare performance
```

## 💡 Best Practices for On-Premise

### 1. Network Infrastructure

**Requirements:**
- **Bandwidth**: 10Gbps+ recommended
- **Low Latency**: < 1ms within datacenter
- **Reliability**: Redundant network paths
- **Monitoring**: Network utilization tracking

**Optimization:**
- Use dedicated network for shuffle if possible
- Ensure sufficient bandwidth
- Monitor network utilization
- Optimize network topology

### 2. Shuffle Network Configuration

**Optimal Settings:**
```python
# Shuffle compression
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")

# Shuffle fetch
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
spark.conf.set("spark.reducer.maxReqsInFlight", "1")

# Network timeout
spark.conf.set("spark.network.timeout", "300s")
```

### 3. Reduce Network Usage

**Strategies:**
- Minimize shuffle data size
- Use broadcast joins for small tables
- Optimize data locality
- Compress shuffle data

**Checklist:**
- [ ] Column pruning before shuffle
- [ ] Filter before joins/groupBy
- [ ] Use broadcast for small tables
- [ ] Enable compression
- [ ] Optimize partition count

### 4. Monitor Network Health

**Regular Review:**
- Network utilization during peak hours
- Shuffle network I/O
- Network-related errors
- Fetch wait times

**Alerts:**
- High network utilization (> 80%)
- Frequent network timeouts
- Slow shuffle operations
- Network errors

## 🚨 Common Issues & Solutions

### Issue 1: Network Bottleneck

**Symptom**: Slow shuffle, high network utilization

**Root Causes:**
- Insufficient bandwidth
- Network contention
- Large shuffle data

**Solution:**
```python
# 1. Reduce shuffle data size
df.select("col1", "col2").join(...)  # Column pruning
df.filter(...).join(...)  # Filter early

# 2. Enable compression
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")

# 3. Optimize network settings
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

# 4. Use broadcast joins
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

### Issue 2: Network Timeouts

**Symptom**: `TimeoutException` during shuffle

**Root Causes:**
- Slow network
- Large data transfers
- Network congestion

**Solution:**
```python
# Increase network timeout
spark.conf.set("spark.network.timeout", "600s")  # 10 minutes
spark.conf.set("spark.rpc.askTimeout", "600s")

# Or reduce data size
df.repartition(200)  # More partitions = smaller transfers
```

### Issue 3: Slow Shuffle Fetch

**Symptom**: Long fetch wait times

**Root Causes:**
- Network latency
- Insufficient bandwidth
- Too many concurrent requests

**Solution:**
```python
# Increase data in flight
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

# Reduce concurrent requests
spark.conf.set("spark.reducer.maxReqsInFlight", "1")

# Or optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Issue 4: Network Contention

**Symptom**: Multiple jobs competing for network

**Root Cause**: Too many concurrent shuffles

**Solution:**
```python
# Schedule jobs to avoid overlap
# Use queue management in YARN
spark.conf.set("spark.yarn.queue", "production")

# Or reduce shuffle data size
# (column pruning, filtering, compression)
```

### Issue 5: Cross-Rack Traffic

**Symptom**: High network usage, slow performance

**Root Cause**: Data and compute on different racks

**Solution:**
```python
# Enable rack awareness in YARN
# (Cluster configuration)

# Or optimize data placement
# Co-locate data and compute when possible
```

## 📝 Key Takeaways

1. **Network is critical** for shuffle operations
2. **Reduce shuffle data size** to minimize network transfer
3. **Compression** reduces network I/O significantly
4. **Optimize fetch settings** for your network
5. **Monitor network metrics** regularly
6. **Use broadcast joins** to avoid network transfer
7. **Data locality** reduces network usage
8. **Network infrastructure** matters for performance

## 🔗 Next Steps

- **Day 15**: Common Error Patterns (Troubleshooting Phase)
- Practice: Analyze network usage in your jobs
- Experiment: Try different network configurations
- Optimize: Reduce network I/O in your queries

## 📚 Additional Resources

- [Network Configuration](https://spark.apache.org/docs/latest/configuration.html#networking)
- [Shuffle Tuning](https://spark.apache.org/docs/latest/tuning.html#shuffle-behavior)
- [Network Optimization](https://spark.apache.org/docs/latest/tuning.html#network)

---

**Progress**: Day 14/30+ ✅

