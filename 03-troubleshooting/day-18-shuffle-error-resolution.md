# Day 18: Shuffle Error Resolution (FetchFailed)

## 🎯 Learning Objectives
- Understand what `FetchFailedException` really means and why it cascades
- Trace it back to its true cause (lost executor, disk, GC, network)
- Apply the external shuffle service and the right timeout/retry knobs
- Prevent shuffle failures by controlling partition size

## 📚 Core Concepts

### 1. What FetchFailed is
During a shuffle, reduce-side tasks **fetch** map-side shuffle blocks from other executors. A `FetchFailedException` means the reducer couldn't get a block:
```
FetchFailedException: Failed to connect to worker-3:7337
MetadataFetchFailedException: Missing an output location for shuffle 4
```
It is almost never a "shuffle bug" — it's a **downstream symptom** of something that killed or stalled the map-side executor holding those blocks.

### 2. Why it cascades
When blocks are lost, Spark must **recompute the map stage** to regenerate them, then retry the reduce stage. If the underlying cause persists (e.g. a node keeps OOMing), stages retry until `spark.stage.maxConsecutiveAttempts` is exhausted and the job dies.

### 3. Root causes ranked
1. **Executor lost** — OOM/YARN kill on the map side (most common). Fix per Day 16.
2. **Long GC pause** — executor unresponsive past the network timeout.
3. **Disk full** on `spark.local.dir` — shuffle files can't be written/served.
4. **Network** — genuine connectivity/firewall issue between nodes.
5. **Executor removed by dynamic allocation** while its shuffle files were still needed → use the **external shuffle service**.

## 🔍 Deep Dive: Fixes

### Enable the external shuffle service (on-prem essential)
```bash
# NodeManager serves shuffle files independently of executors, so losing/removing
# an executor doesn't lose its shuffle output.
--conf spark.shuffle.service.enabled=true
--conf spark.dynamicAllocation.enabled=true   # safe to scale down now
```
(Requires the NodeManager aux-service configured cluster-side — a one-time admin step.)

### Loosen timeouts for GC-heavy jobs
```bash
--conf spark.network.timeout=300s
--conf spark.shuffle.io.maxRetries=5
--conf spark.shuffle.io.retryWait=10s
```

### Attack the real cause — partition size
Oversized shuffle partitions cause the map-side memory pressure that kills executors. Keep partitions ~100–200MB:
```python
spark.conf.set("spark.sql.shuffle.partitions", 800)   # more, smaller partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")   # AQE coalesces after the fact
```

## 💡 Key Insights for On-Premise

### 1. Dynamic allocation without the shuffle service = FetchFailed factory
If executors are scaled down but hold shuffle files, reducers fetch from dead executors. **Always** pair dynamic allocation with `spark.shuffle.service.enabled=true`.

### 2. `spark.local.dir` disk hygiene
Point it at large, fast, **multiple** disks (`/data1/spark,/data2/spark`) and monitor free space. A full shuffle disk manifests as FetchFailed/IOException, not "disk full."

## 🎯 Practical Exercises

### Exercise 1: Read a FetchFailed trace
```python
# See exercises/troubleshooting/exercise-18-fetchfailed.py
# Given a trace, identify which map executor died and why the reduce stage retried.
```

### Exercise 2: Partition sizing
```python
# Run a heavy shuffle at shuffle.partitions=8 vs 400; compare per-partition size and stability.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Removed/lost executors** timeline around the failure.
2. **Shuffle Read/Write size per task** — huge = oversized partitions.
3. **GC time %** on map-side executors just before the fetch failure.

### Spark UI Analysis
- Correlate the FetchFailed timestamp with an executor removal in the Executors tab.
- Stage retry count climbing = the cause is unresolved.

## 🚨 Common Issues & Solutions

### Issue 1: FetchFailed only with dynamic allocation
**Symptom**: fine with fixed executors, fails when scaling down.
**Solution**: enable the external shuffle service.

### Issue 2: "Missing an output location for shuffle N"
**Symptom**: reduce stage can't find map output.
**Solution**: the map executor is gone — find *why it died* (OOM/kill) and fix that; the fetch error is downstream.

## 📝 Key Takeaways
1. FetchFailed is a **symptom** — the cause is a lost/stalled map-side executor.
2. External shuffle service is mandatory with dynamic allocation.
3. Right-size shuffle partitions (~100–200MB) to prevent the OOM that triggers it.
4. Loosen `spark.network.timeout` for GC-heavy workloads.
5. Watch disk space on `spark.local.dir`.

## 🔗 Next Steps
- **Day 19**: Serialization & UDF Issues
- Practice: for a real FetchFailed, find the matching executor-loss event.

## 📚 Additional Resources
- Spark Shuffle & External Shuffle Service docs
- Network configuration reference

---

**Progress**: Day 18/40 ✅
