# Day 18: Shuffle Error Resolution (FetchFailed)

## 🎯 Learning Objectives
- Understand what `FetchFailedException` really means and why it cascades
- Trace it back to its true cause (lost executor, disk, GC, network)
- Apply shuffle tracking / decommissioning and the right timeout/retry knobs (there is **no external shuffle service** on Kubernetes)
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
1. **Executor lost** — OOM / pod `OOMKilled` (exit 137) on the map side (most common). Fix per Day 16.
2. **Long GC pause** — executor unresponsive past the network timeout.
3. **Disk full** on `spark.local.dir` — shuffle files can't be written/served (on K8S this is the pod's `emptyDir`/PVC scratch space).
4. **Network** — genuine connectivity/firewall issue between pods/nodes.
5. **Executor removed by dynamic allocation** while its shuffle files were still needed. On YARN the NodeManager's external shuffle service kept serving those files; **Kubernetes has no external shuffle service**, so you need **shuffle tracking** (keep shuffle-holding executors alive) or **decommissioning with block migration** instead.

## 🔍 Deep Dive: Fixes

### Protect shuffle output without an external shuffle service (Kubernetes)
On YARN the NodeManager ran an external shuffle service that served an executor's shuffle files even after that executor died, so dynamic allocation could scale down freely. **Kubernetes has no external shuffle service.** Instead you keep shuffle output reachable one of two ways:
```bash
# Option A: shuffle tracking — keep executors that still hold needed shuffle blocks alive.
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.shuffleTracking.enabled=true    # REQUIRED on K8S
--conf spark.dynamicAllocation.shuffleTracking.timeout=30m     # how long to keep holders

# Option B: executor decommissioning — migrate shuffle/cache blocks off an executor
# BEFORE it is removed, so nothing is lost.
--conf spark.decommission.enabled=true
--conf spark.storage.decommission.enabled=true
--conf spark.storage.decommission.shuffleBlocks.enabled=true
```
Optionally mount a PVC for shuffle data (`spark.kubernetes.executor.volumes...`) so shuffle files survive a pod restart. No cluster-side aux-service step exists on K8S — these are all Spark-side configs.

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

### 1. Dynamic allocation without shuffle tracking = FetchFailed factory
If executors are scaled down but hold shuffle files, reducers fetch from dead pods. With no external shuffle service on K8S, **always** pair dynamic allocation with `spark.dynamicAllocation.shuffleTracking.enabled=true` (or decommissioning with block migration).

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
**Solution**: enable shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled=true`) — there's no external shuffle service on K8S — or use decommissioning with block migration.

### Issue 2: "Missing an output location for shuffle N"
**Symptom**: reduce stage can't find map output.
**Solution**: the map executor is gone — find *why it died* (OOM/kill) and fix that; the fetch error is downstream.

## 📝 Key Takeaways
1. FetchFailed is a **symptom** — the cause is a lost/stalled map-side executor.
2. On K8S there's no external shuffle service — shuffle tracking (or decommissioning) is mandatory with dynamic allocation.
3. Right-size shuffle partitions (~100–200MB) to prevent the OOM that triggers it.
4. Loosen `spark.network.timeout` for GC-heavy workloads.
5. Watch disk space on `spark.local.dir`.

## 🔗 Next Steps
- **Day 19**: Serialization & UDF Issues
- Practice: for a real FetchFailed, find the matching executor-loss event.

## 📚 Additional Resources
- Spark on Kubernetes: dynamic allocation with shuffle tracking; executor decommissioning & block migration
- Network configuration reference

---

**Progress**: Day 18/40 ✅
