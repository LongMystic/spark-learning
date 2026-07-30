# Day 16: OOM Debugging (Driver vs Executor)

## 🎯 Learning Objectives
- Distinguish **driver** OOM from **executor** OOM — they have opposite fixes
- Map an OOM to a specific memory region (heap, off-heap/overhead, user)
- Recognize the pod **OOMKilled** (exit 137) "cgroup killed the container for exceeding its memory limit" case
- Apply the right fix instead of blindly bumping `--executor-memory`
- Build a systematic order-of-remedies for executor OOM instead of guessing

## 📚 Core Concepts

### 1. Two very different OOMs

The single most common mistake in OOM debugging is treating "OOM" as one problem. It's actually two almost-unrelated problems that happen to share an exception name.

| | Driver OOM | Executor OOM |
|---|---|---|
| Trigger | `collect()`, `toPandas()`, huge broadcast, giant plan | skew, large shuffle/agg, wide rows, caching |
| Log location | driver pod | executor pod |
| Typical fix | stop pulling data to driver; raise `maxResultSize`/driver mem | reduce per-task data; more partitions; fix skew |
| Wrong fix | adding executors (does nothing) | adding driver memory (does nothing) |
| Frequency in practice | rare, usually one bad line of code | common, usually a data-shape problem |

**Key Points:**
- Before touching any config, answer: **which pod's log shows the OOM?** That single fact tells you which half of the table applies.
- Driver OOM is almost always a code smell — something is materializing full-table data on a single JVM that was never sized for it.
- Executor OOM is almost always a data-shape problem — skew, an oversized partition, or a wide row — not "the cluster needs more RAM" in general.

### 2. Executor memory regions (recap → apply)

Understanding where memory actually lives inside an executor pod is what turns "just raise executor-memory" into a specific, correct fix.

```
--executor-memory (JVM heap)          --executor-memory-overhead (off-heap)
├── Reserved (300MB)                   ├── Python worker processes (PySpark!)
├── User memory (UDF state, etc.)      ├── netty shuffle buffers
└── Unified memory (spark.memory.*)    └── native libs / direct buffers
    ├── Execution (shuffle/join/agg)
    └── Storage (cache)
```

**Key Points:**
- `java.lang.OutOfMemoryError: Java heap space` → **heap** too small for the region under pressure (usually execution memory during a shuffle/aggregate/sort).
- Pod **OOMKilled** (exit code **137**, `kubectl describe pod` shows `Reason: OOMKilled`) → the executor's total RSS exceeded its **pod memory limit**, so the kubelet/cgroup killed it. On K8s the pod limit = `spark.executor.memory` + `spark.executor.memoryOverhead`, so an OOMKill with the heap *not* full means **overhead/off-heap** too small (very common with PySpark, which runs Python *outside* the heap) — NOT the heap. Same root cause the YARN "container killed for exceeding memory" message pointed at; different signal.
- `spark.memory.fraction` (default 0.6) splits the JVM heap between unified memory (execution + storage) and user memory; `spark.memory.storageFraction` (default 0.5) further splits unified memory between storage (cache) and execution — execution can borrow from storage under pressure but not vice versa when storage is pinned by cached data.

**Example:**
```python
# Given an executor with:
#   --executor-memory 8g
#   --conf spark.executor.memoryOverhead=1g
# The pod's total memory LIMIT on Kubernetes is roughly 8g + 1g = 9g (plus a small
# fixed buffer). If RSS crosses 9g, the kubelet kills the pod regardless of what
# the JVM heap itself reports as "used."
```

### 3. The signals

Each OOM variant leaves a distinct fingerprint in the logs and UI — learn to recognize them without reading the entire trace.

- `java.lang.OutOfMemoryError: Java heap space` → heap genuinely exhausted; the JVM couldn't allocate.
- `OutOfMemoryError: GC overhead limit exceeded` → heap almost full, JVM spends nearly all CPU time in garbage collection with little memory reclaimed — a slow-motion version of the same problem.
- Pod terminated, `kubectl describe pod` → `State: Terminated, Reason: OOMKilled, Exit Code: 137` — the container manager killed it; the JVM itself may never have logged an `OutOfMemoryError` at all.
- Spill (Memory/Disk) huge in the Stages tab → execution memory pressure (often precedes OOM); the job may still succeed by spilling, but it's a warning sign.
- One task reads 100× the others → skew driving the OOM (see Day 10 / Day 18) — the OOM is really a skew problem wearing a memory costume.
- `spark.sql.autoBroadcastJoinThreshold` triggering a broadcast of a table that's much bigger than expected → driver (during broadcast collection) or executor (holding the broadcast) OOM.

### 4. Broadcast joins are a special driver+executor hybrid case

`spark.sql.autoBroadcastJoinThreshold` doesn't just risk driver OOM — the broadcast table is first collected **to the driver**, then sent to **every executor**, so an oversized broadcast can OOM either side.

```python
# The build side of the broadcast is materialized on the driver first,
# THEN pushed out to every executor's memory. If "products" has grown
# past the threshold's assumption, both sides are at risk.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # 10MB
txns.join(products, "product_id")   # if products is now 2GB, this is a double OOM risk
```

**Key Points:**
- `org.apache.spark.sql.errors.QueryExecutionErrors` / `SparkException: Could not execute broadcast in ... secs` after a broadcast timeout (`spark.sql.broadcastTimeout`, default 300s) is a related but distinct symptom — the broadcast build is taking too long, often because the "small" side is no longer small.
- Explicit `broadcast(df)` hints do not protect you from this — they force the broadcast strategy regardless of actual size, so an explicit hint on a table that grew unexpectedly is a self-inflicted OOM.
- Always re-verify broadcast candidates' actual size (`df.explain("cost")` or the Scan node's bytes-read metric) rather than trusting a hint or threshold set months ago.

## 🔍 Deep Dive: Fixing each case

### Step-by-Step Process
1. **Identify which pod logged the OOM** — driver pod or an executor pod. This is the fork in the road.
2. **If driver**: grep the driver log and your own code for `collect()`, `toPandas()`, `show(N)` with huge N, or a broadcast join on an unexpectedly large table.
3. **If executor**: check `kubectl describe pod` for `OOMKilled`/exit 137 vs a plain `OutOfMemoryError: Java heap space` in the log — this tells you overhead vs heap.
4. **Check the Stages tab** for skew (max task time/input ≫ median) before touching any memory config — skew fixes are usually cheaper and more durable than memory bumps.
5. **Apply exactly one remedy**, from the ordered list below, and re-run.
6. **Confirm** the fix by comparing the same metric (peak memory, spill, GC time) before and after.

### Driver OOM
```python
# BAD: pulls the whole result to the driver JVM
rows = big_df.collect()            # or big_df.toPandas()

# GOOD: keep it distributed; write out, or sample/limit for inspection
big_df.write.parquet("out/")
big_df.limit(1000).toPandas()      # bounded
```
Also: over-large auto-broadcast (`spark.sql.autoBroadcastJoinThreshold` too high) materializes a big table on the driver → lower it or disable for that query. If a genuine large result must reach the driver (rare), raise `spark.driver.maxResultSize` deliberately and size driver memory to match — but treat this as an exception, not a default fix.

```bash
# Deliberately allow a larger (but still bounded) result to reach the driver
--conf spark.driver.maxResultSize=4g
--driver-memory 8g
```

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

### Example: telling heap OOM from overhead OOMKill apart

```
# Case A - heap exhausted (JVM itself threw it):
executor 4: java.lang.OutOfMemoryError: Java heap space
    at org.apache.spark.util.collection.ExternalAppendOnlyMap...
-> Heap too small for the execution-memory region under pressure.
   Fix: more partitions first, then raise --executor-memory if still needed.

# Case B - pod killed by the kubelet, heap logs show nothing alarming:
$ kubectl -n spark-jobs describe pod daily-etl-abc-exec-4
    Last State:  Terminated
    Reason:      OOMKilled
    Exit Code:   137
-> RSS (heap + off-heap: Python workers, netty buffers) exceeded the pod's
   memory LIMIT (executor-memory + memoryOverhead). If the heap wasn't full,
   raise spark.executor.memoryOverhead, not --executor-memory.
```

**Analysis:**
- Case A is visible entirely inside the JVM's own error output — no need for `kubectl` at all.
- Case B requires `kubectl describe pod`, because the JVM may never get the chance to log anything before the kubelet sends SIGKILL.
- For PySpark and Pandas/Arrow UDF workloads, Case B is far more common than Case A, because Python worker processes and Arrow buffers live entirely in the "overhead" region, invisible to the JVM heap's own accounting.

### Example: skew-driven executor OOM, diagnosed and fixed

```python
from pyspark.sql.functions import col, concat, lit, rand

# Symptom: executor OOM only on the groupBy stage; Stages tab shows one task
# reading 40x the shuffle-read bytes of the median task for that stage.
skewed = spark.read.parquet("data/transactions_skewed")
result = skewed.groupBy("customer_id").agg({"amount": "sum"})   # OOMs on the hot key

# Step 1 (cheapest): more partitions -- may not be enough if ONE key is huge.
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Step 2: let AQE handle it automatically (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Step 3 (manual salting, if AQE's skew handling doesn't cover this shape
# of aggregation): spread the hot key across N sub-keys, aggregate, then
# combine.
salted = skewed.withColumn(
    "salted_key", concat(col("customer_id"), lit("_"), (rand() * 20).cast("int"))
)
partial = salted.groupBy("salted_key", "customer_id").agg({"amount": "sum"})
final = partial.groupBy("customer_id").agg({"sum(amount)": "sum"})
```

**Analysis:**
- Step 1 alone often isn't enough for true single-key skew: no matter how many partitions you configure, the one partition holding the hot key's data doesn't shrink until that key's data is itself split.
- AQE's skew join handling (Step 2) targets **joins**, not all aggregations — for a `groupBy` with a genuinely dominant key, manual salting (Step 3) may still be needed: it explicitly breaks the hot key into sub-keys for a first-pass partial aggregation, then recombines.
- This is the same skew root cause discussed in Day 10 and Day 18 wearing a memory-symptom costume — recognizing "skew" as the actual disease, rather than treating each symptom (OOM here, FetchFailed elsewhere) as a separate disease, is the point of this whole phase.

## 💡 Key Insights for On-Premise

### 1. PySpark pays the overhead tax
Each executor spawns Python workers **outside** the JVM heap. Pandas/Arrow UDFs and big Python objects live there → the killer is usually `memoryOverhead`, not `--executor-memory`. Start overhead at ~15–20% for PySpark-heavy jobs (Spark's default `spark.executor.memoryOverheadFactor` is 0.1, i.e. 10% — often too low for heavy PySpark).

```bash
--conf spark.executor.memoryOverheadFactor=0.2   # 20% of executor-memory
# or an absolute value once you know your workload's real off-heap footprint:
--conf spark.executor.memoryOverhead=4g
```

### 2. "Fat" executors GC badly
A 64GB executor heap can suffer long G1 pauses that look like hangs and trigger `FetchFailed`. Prefer several medium executors (e.g. 4–5 cores, ~16–24GB) over one giant one. This also limits the "blast radius" of an OOM kill — losing one 16GB executor recomputes far less shuffle output than losing one 64GB executor.

### 3. Requests vs limits matter on Kubernetes
The spark-operator sets both a memory *request* and a memory *limit* on executor pods (derived from `spark.executor.memory` + overhead, times `spark.kubernetes.memoryOverheadFactor` where applicable). If requests and limits diverge significantly, the node can still schedule more pods than it has headroom for, increasing the chance of a node-wide memory squeeze that kills your executor even though your own job's math looked fine.

### 4. Don't confuse driver OOM with driver pod eviction
A driver pod can also be evicted by the node (not the JVM OOM'ing) if the node itself is under memory pressure from other workloads. `kubectl describe pod <driver-pod>` distinguishing `OOMKilled` from `Evicted` tells you whether the problem is your driver's own memory use or the node's overall capacity.

## 🎯 Practical Exercises

### Exercise 1: Reproduce driver OOM safely
```python
# See exercises/troubleshooting/exercise-16-oom-debugging.py
# Compare collect() footprint vs write-out; observe driver memory in the UI.
# On a laptop-sized dataset it won't truly OOM -- the point is to reason
# about WHERE the memory goes (driver JVM vs distributed executors) and to
# practice the safe, bounded patterns (limit().toPandas(), write() instead
# of collect()).
```

### Exercise 2: Overhead vs heap
```python
# Run a Pandas UDF job; classify whether memory pressure would be heap or
# overhead using the logs/reasoning, without needing an actual crash:
#   - Pandas UDF holds whole-column batches as Arrow buffers -> overhead.
#   - A wide groupBy aggregation building large hash maps -> heap (execution memory).
# See exercise-16-oom-debugging.py's "Where to look when it DOES OOM" section
# for the exact log signatures to search for.
```

### Exercise 3: Right-size memoryOverhead for a PySpark job
```bash
# Starting point for a PySpark-heavy job with Pandas UDFs:
spark-submit \
  --executor-memory 12g \
  --conf spark.executor.memoryOverheadFactor=0.2 \
  --conf spark.sql.shuffle.partitions=400 \
  your_job.py
# Watch Executors tab "Peak Execution Memory" and kubectl top pod during the
# run; if RSS approaches the limit without heap OOM, overhead was the right lever.
```

### Exercise 4: Diagnose a broadcast-driven OOM
```python
# Verify the actual size of a join's "small" side before trusting the
# threshold or an explicit hint:
products = read_table(spark, "products")
print(products.count(), len(products.columns))
products.explain("cost")   # look for sizeInBytes in the stats-based plan
# If it's grown well past spark.sql.autoBroadcastJoinThreshold, lower the
# threshold (or remove an explicit broadcast() hint) so Spark falls back
# to a SortMergeJoin instead of risking a broadcast OOM.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Executors tab**: Peak JVM memory, GC time, and *off-heap* usage.
2. **Stages tab**: Spill (Memory) and Spill (Disk) — precursors to OOM.
3. **`kubectl describe pod`**: `Reason: OOMKilled` / exit code 137, and the pod's memory limit vs `kubectl top pod` usage at kill time.
4. **GC time % of task time** — sustained values above ~10-15% indicate heap pressure building well before an actual OOM.
5. **Driver memory in the UI's Executors tab** (the driver appears as an entry too) — watch it climb during/after any `collect()`-like operation.

### Spark UI Analysis
- A stage where a few tasks have massive input/shuffle-read → skew-driven OOM.
- Rising GC time % across a stage → heap pressure building.
- Executors tab "Storage Memory" column climbing with heavy `.cache()`/`.persist()` usage → competing with execution memory for the same unified pool.
- Compare "Peak Execution Memory" across executors for the same stage — a single outlier confirms skew rather than a uniformly undersized cluster.

## 🚨 Common Issues & Solutions

### Issue 1: "I gave it 32g and it still OOMs"
**Symptom**: heap raised, pod still `OOMKilled` (exit 137).
**Root Cause**: the pressure is off-heap (PySpark workers, Arrow buffers, netty), which raising `--executor-memory` does not touch at all.
**Solution**: it's **overhead**, not heap. Raise `spark.executor.memoryOverhead` (which raises the pod memory limit).

### Issue 2: Job dies at the very end on the driver
**Symptom**: all stages green, then OOM.
**Root Cause**: a `collect()`/`toPandas()`/`show(huge)` at the end pulls the full result onto a single JVM sized for orchestration, not data.
**Solution**: bound it or write to storage; use `limit()` before any driver-side materialization.

### Issue 3: OOM only happens on the largest daily run, not smaller test data
**Symptom**: passes on staging/sample data, OOMs in production on the full dataset.
**Root Cause**: usually skew that only becomes severe at real data volume, or a broadcast join whose "small" side has grown past `autoBroadcastJoinThreshold` in production.
**Solution**: check Stages tab task duration/input distribution for the production run specifically; verify the broadcast side's actual size with `df.explain("cost")`, not an assumption from months ago.

### Issue 4: Executor OOMs immediately, before any real work is logged
**Symptom**: the pod dies within seconds of starting, with little to no task output.
**Root Cause**: often an oversized broadcast variable or a cached/loaded lookup table being pulled into every executor at startup, sized for a much smaller dataset than production now has.
**Solution**: check `spark.sql.autoBroadcastJoinThreshold` and any explicit `broadcast()` calls; verify the broadcast side's current size, not its size when the threshold was originally tuned.

### Issue 5: Memory looks fine in the UI but the pod still gets OOMKilled
**Symptom**: Spark's own "Peak Execution Memory" metric looks well within limits, yet `kubectl describe pod` shows `OOMKilled`.
**Root Cause**: the JVM heap metrics Spark reports don't include off-heap Python/Arrow memory or native library allocations — RSS as seen by the kernel/cgroup can be significantly higher than what the Spark UI tracks.
**Solution**: cross-check with `kubectl top pod` at (or just before) the kill time, and treat any PySpark/Pandas UDF-heavy job as needing generous `memoryOverhead` regardless of what the JVM-side metrics claim.

## 📝 Key Takeaways
1. First decide **driver vs executor** — the fixes are opposite.
2. Pod **OOMKilled** with the heap not full = **overhead/off-heap**, not heap.
3. PySpark OOMs are usually overhead (Python lives off-heap).
4. More partitions and skew fixes beat brute-force memory bumps — try them first.
5. Prefer medium executors over one giant GC-prone executor.
6. `kubectl describe pod` is often more informative than the JVM log for a memory kill, since the kubelet may kill the container before the JVM logs anything.
7. Requests/limits and node-level memory pressure can evict a pod even when your own job's math looks correct.

## 🔗 Next Steps
- **Day 17**: Task Failure & Retry Analysis
- Practice: take one OOM at work and identify the exact region (driver/executor, heap/overhead) before changing configs.
- Experiment: intentionally under-provision `memoryOverhead` on a PySpark Pandas UDF job in a test namespace and observe the OOMKilled signature end to end.

## 📚 Additional Resources
- Spark Memory Management / Tuning docs
- Kubernetes pod memory requests/limits + OOMKilled behavior (cgroup limits); Spark's `spark.kubernetes.memoryOverheadFactor`
- `spark.driver.maxResultSize` and broadcast join threshold configuration reference

---

**Progress**: Day 16/40 ✅
