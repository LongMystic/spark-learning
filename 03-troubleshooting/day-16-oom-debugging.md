# Day 16: OOM Debugging (Driver vs Executor)

## 🎯 Learning Objectives
- Distinguish **driver** OOM from **executor** OOM — they have opposite fixes
- Map an OOM to a specific memory region (heap, off-heap/overhead, user)
- Recognize the pod **OOMKilled** (exit 137) "cgroup killed the container for exceeding its memory limit" case
- Apply the right fix instead of blindly bumping `--executor-memory`

## 📚 Core Concepts

### 1. Two very different OOMs

| | Driver OOM | Executor OOM |
|---|---|---|
| Trigger | `collect()`, `toPandas()`, huge broadcast, giant plan | skew, large shuffle/agg, wide rows, caching |
| Log location | driver pod | executor pod |
| Typical fix | stop pulling data to driver; raise `maxResultSize`/driver mem | reduce per-task data; more partitions; fix skew |
| Wrong fix | adding executors (does nothing) | adding driver memory (does nothing) |

### 2. Executor memory regions (recap → apply)
```
--executor-memory (JVM heap)          --executor-memory-overhead (off-heap)
├── Reserved (300MB)                   ├── Python worker processes (PySpark!)
├── User memory (UDF state, etc.)      ├── netty shuffle buffers
└── Unified memory (spark.memory.*)    └── native libs / direct buffers
    ├── Execution (shuffle/join/agg)
    └── Storage (cache)
```
- `java.lang.OutOfMemoryError: Java heap space` → **heap** too small for the region under pressure.
- Pod **OOMKilled** (exit code **137**, `kubectl describe pod` shows `Reason: OOMKilled`) → the executor's total RSS exceeded its **pod memory limit**, so the kubelet/cgroup killed it. On K8S the pod limit = `spark.executor.memory` + `spark.executor.memoryOverhead`, so an OOMKill with the heap *not* full means **overhead/off-heap** too small (very common with PySpark, which runs Python *outside* the heap) — NOT the heap. Same root cause the YARN "container killed for exceeding memory" message pointed at; different signal.

### 3. The signals
- `OutOfMemoryError: GC overhead limit exceeded` → heap almost full, JVM spends all time in GC.
- Spill (Memory/Disk) huge in the Stages tab → execution memory pressure (often precedes OOM).
- One task reads 100× the others → skew driving the OOM (see Day 10 / Day 18).

## 🔍 Deep Dive: Fixing each case

### Driver OOM
```python
# BAD: pulls the whole result to the driver JVM
rows = big_df.collect()            # or big_df.toPandas()

# GOOD: keep it distributed; write out, or sample/limit for inspection
big_df.write.parquet("out/")
big_df.limit(1000).toPandas()      # bounded
```
Also: over-large auto-broadcast (`spark.sql.autoBroadcastJoinThreshold` too high) materializes a big table on the driver → lower it or disable for that query.

### Executor OOM — order of remedies
1. **More partitions / higher `spark.sql.shuffle.partitions`** → smaller per-task footprint (cheapest, try first).
2. **Fix skew** (salting / AQE skew join) if one partition is huge.
3. **Raise memoryOverhead** if the pod is *OOMKilled* while the heap isn't full (esp. PySpark, Pandas UDFs).
4. **Raise executor heap** only if genuinely needed and the node has RAM.
5. **Reduce columns/rows early** (project + filter before wide ops).

```bash
spark-submit \
  --conf spark.sql.shuffle.partitions=600 \
  --conf spark.executor.memoryOverhead=3g \
  --executor-memory 12g --executor-cores 4 ...
```

## 💡 Key Insights for On-Premise

### 1. PySpark pays the overhead tax
Each executor spawns Python workers **outside** the JVM heap. Pandas/Arrow UDFs and big Python objects live there → the killer is usually `memoryOverhead`, not `--executor-memory`. Start overhead at ~15–20% for PySpark-heavy jobs.

### 2. "Fat" executors GC badly
A 64GB executor heap can suffer long G1 pauses that look like hangs and trigger `FetchFailed`. Prefer several medium executors (e.g. 4–5 cores, ~16–24GB) over one giant one.

## 🎯 Practical Exercises

### Exercise 1: Reproduce driver OOM safely
```python
# See exercises/troubleshooting/exercise-16-oom-debugging.py
# Compare collect() footprint vs write-out; observe driver memory in the UI.
```

### Exercise 2: Overhead vs heap
```python
# Run a Pandas UDF job; classify whether pressure is heap or overhead using the logs.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Executors tab**: Peak JVM memory, GC time, and *off-heap* usage.
2. **Stages tab**: Spill (Memory) and Spill (Disk) — precursors to OOM.
3. **`kubectl describe pod`**: `Reason: OOMKilled` / exit code 137, and the pod's memory limit vs `kubectl top pod` usage at kill time.

### Spark UI Analysis
- A stage where a few tasks have massive input/shuffle-read → skew-driven OOM.
- Rising GC time % across a stage → heap pressure building.

## 🚨 Common Issues & Solutions

### Issue 1: "I gave it 32g and it still OOMs"
**Symptom**: heap raised, pod still `OOMKilled` (exit 137).
**Solution**: it's **overhead**, not heap. Raise `spark.executor.memoryOverhead` (which raises the pod memory limit).

### Issue 2: Job dies at the very end on the driver
**Symptom**: all stages green, then OOM.
**Solution**: a `collect()`/`toPandas()`/`show(huge)` at the end. Bound it or write to storage.

## 📝 Key Takeaways
1. First decide **driver vs executor** — the fixes are opposite.
2. Pod **OOMKilled** with the heap not full = **overhead/off-heap**, not heap.
3. PySpark OOMs are usually overhead (Python lives off-heap).
4. More partitions and skew fixes beat brute-force memory bumps.
5. Prefer medium executors over one giant GC-prone executor.

## 🔗 Next Steps
- **Day 17**: Task Failure & Retry Analysis
- Practice: take one OOM at work and identify the exact region before changing configs.

## 📚 Additional Resources
- Spark Memory Management / Tuning docs
- Kubernetes pod memory requests/limits + OOMKilled behavior (cgroup limits); Spark's `spark.kubernetes.memoryOverheadFactor`

---

**Progress**: Day 16/40 ✅
