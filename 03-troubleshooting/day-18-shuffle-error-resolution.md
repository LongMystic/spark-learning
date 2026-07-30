# Day 18: Shuffle Error Resolution (FetchFailed)

## 🎯 Learning Objectives
- Understand what `FetchFailedException` really means and why it cascades
- Trace it back to its true cause (lost executor, disk, GC, network)
- Apply shuffle tracking / decommissioning and the right timeout/retry knobs (there is **no external shuffle service** on Kubernetes)
- Prevent shuffle failures by controlling partition size
- Recognize the difference between `FetchFailedException` and `MetadataFetchFailedException`

## 📚 Core Concepts

### 1. What FetchFailed is

During a shuffle, reduce-side tasks **fetch** map-side shuffle blocks from other executors. A `FetchFailedException` means the reducer couldn't get a block:
```
FetchFailedException: Failed to connect to worker-3:7337
MetadataFetchFailedException: Missing an output location for shuffle 4
```

**Key Points:**
- It is almost never a "shuffle bug" — it's a **downstream symptom** of something that killed or stalled the map-side executor holding those blocks.
- `FetchFailedException` (couldn't connect / connection reset) means the reducer knew *where* to fetch from but the target executor was unreachable — usually because it's dead or GC-paused.
- `MetadataFetchFailedException: Missing an output location for shuffle N` means the driver's map-output tracker doesn't even have a *location* for that shuffle block anymore — the executor that produced it was already removed from the cluster's bookkeeping entirely.
- Unlike most task failures, a `FetchFailedException` triggers a **stage-level** retry, not just a task-level one — because the missing data belongs to an entire upstream stage, not just one task's input.

### 2. Why it cascades

**Key Points:**
- When blocks are lost, Spark must **recompute the map stage** to regenerate them, then retry the reduce stage.
- If the underlying cause persists (e.g. a node keeps OOMing), stages retry until `spark.stage.maxConsecutiveAttempts` is exhausted and the job dies.
- Because recomputing the map stage re-does real work (not just a quick retry), a single `FetchFailedException` can cost minutes to tens of minutes on a large job — this is why chasing the *root cause* (why the executor died) matters far more than tuning fetch retries.
- A cascade can compound: if the same underlying issue (say, a consistently OOMing node) kills a *new* map-side executor during the recompute, you get another `FetchFailedException`, and the stage retries again — potentially exhausting `maxConsecutiveAttempts` even though each individual retry "made progress."

### 3. Root causes ranked

1. **Executor lost** — OOM / pod `OOMKilled` (exit 137) on the map side (most common). Fix per Day 16.
2. **Long GC pause** — executor unresponsive past the network timeout; from the reducer's perspective this looks identical to a dead executor.
3. **Disk full** on `spark.local.dir` — shuffle files can't be written/served (on K8s this is the pod's `emptyDir`/PVC scratch space).
4. **Network** — genuine connectivity/firewall issue between pods/nodes.
5. **Executor removed by dynamic allocation** while its shuffle files were still needed. On YARN the NodeManager's external shuffle service kept serving those files; **Kubernetes has no external shuffle service**, so you need **shuffle tracking** (keep shuffle-holding executors alive) or **decommissioning with block migration** instead.

**Example:**
```
# Reducer-side trace (what you see first):
org.apache.spark.shuffle.FetchFailedException: Failed to connect to worker-3:7337

# What actually happened, minutes earlier, on worker-3 (map side):
$ kubectl -n spark-jobs describe pod daily-etl-abc-exec-9
    Last State:   Terminated
    Reason:       OOMKilled
    Exit Code:    137
# -> the map-side executor died from OOM; the FetchFailed on the reduce
#    side is purely a downstream consequence, minutes later.
```

### 4. FetchFailed vs other shuffle-adjacent exceptions

Not every shuffle-stage error is a `FetchFailedException` — telling them apart correctly saves you from chasing the wrong fix.

| Exception | Meaning | Typical fix |
|---|---|---|
| `FetchFailedException: Failed to connect to X:Y` | Reducer knows the block's location but can't reach that executor | Find why the map-side executor died/stalled (Day 16) |
| `MetadataFetchFailedException: Missing an output location for shuffle N` | Driver's map-output tracker has no location at all for this shuffle | Same root cause, one step further gone — executor was fully removed |
| `org.apache.spark.shuffle.FetchFailedException: ... Failed to get block(s)` after retries exhausted | Target was reachable but never returned the block within `spark.shuffle.io.maxRetries` × `spark.shuffle.io.retryWait` | Often a genuine GC pause; loosen timeouts as a complement to fixing GC |
| `java.io.IOException: No space left on device` on `spark.local.dir` | Disk literally full | Expand/clean `spark.local.dir`; use multiple disks |

**Key Points:**
- All of these ultimately funnel into the same stage-retry mechanism, but the *fix* differs: a connection failure points you at "why did the executor die," while a disk-full error points you at storage capacity directly, with no executor-loss investigation needed at all.
- Spark's retry configs (`spark.shuffle.io.maxRetries`, `spark.shuffle.io.retryWait`) only help the "reachable but slow" case — they do nothing for a genuinely dead executor or a full disk.

## 🔍 Deep Dive: Fixes

### Step-by-Step Process
1. **Note the FetchFailed timestamp and the target host/executor** it was trying to reach (e.g. `worker-3:7337`).
2. **Open the Executors tab (or History Server)** and find that executor — was it removed/lost around that timestamp?
3. **Check why it was removed**: `kubectl describe pod` for `OOMKilled`, or check GC time in its last recorded metrics, or check disk usage on its node for `spark.local.dir`.
4. **Fix the root cause** (Day 16 for OOM, disk cleanup/expansion for disk-full, node/network investigation for connectivity).
5. **Prevent recurrence structurally**: right-size shuffle partitions, enable shuffle tracking or decommissioning if dynamic allocation is on, and loosen network timeouts only for genuinely GC-heavy jobs (not as a substitute for fixing the GC).

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
Treat this as a *symptom mitigation* for a job you know is GC-heavy (e.g. very large heaps, heavy caching) — it buys the map-side executor more time before the reducer gives up on it. It does **not** fix the underlying GC pressure; pair it with the memory/partition fixes below.

### Attack the real cause — partition size
Oversized shuffle partitions cause the map-side memory pressure that kills executors. Keep partitions ~100–200MB:
```python
spark.conf.set("spark.sql.shuffle.partitions", 800)   # more, smaller partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")   # AQE coalesces after the fact
```

### Example: sizing shuffle partitions from real data volume

```python
# Estimate the shuffle input size, then choose a partition count targeting
# ~100-200MB per partition after shuffle.
shuffle_input_bytes = 400 * 1024 * 1024 * 1024   # e.g. 400GB, from the Stages tab
target_partition_bytes = 150 * 1024 * 1024       # 150MB target
partitions = shuffle_input_bytes // target_partition_bytes
print(f"suggested spark.sql.shuffle.partitions ~= {partitions}")   # ~2730

spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
# Then let AQE coalesce any resulting tiny partitions automatically:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Analysis:**
- Estimating partition count from actual shuffle-input bytes (visible in the Stages tab's "Shuffle Write" column of the upstream stage) beats guessing a round number.
- Combining a deliberately high partition count with AQE coalescing gives you the best of both: no single partition is dangerously large, but you don't pay huge per-task scheduling overhead for thousands of tiny final partitions.

### Example: GC-pause-driven FetchFailed, diagnosed and mitigated

```
# Reducer side:
FetchFailedException: Failed to connect to worker-5:7337

# worker-5's executor log (still alive, just unresponsive during the fetch window):
25/07/30 02:14:01 WARN executor.Executor: Managed memory leak detected...
25/07/30 02:14:22 INFO GCUtil: [Full GC (Allocation Failure) 14G->13.8G, 21.4 secs]
25/07/30 02:14:47 INFO GCUtil: [Full GC (Allocation Failure) 13.9G->13.7G, 23.1 secs]
```

**Analysis:**
- Unlike the OOMKilled case, the executor never actually died — `kubectl describe pod` would show it still `Running`. The reducer simply couldn't get a response within `spark.network.timeout` while the JVM was buried in repeated full GCs.
- The structural fix is the same as any heap-pressure problem: more/smaller partitions, or a genuinely oversized heap for the workload (see Day 16's fat-executor guidance — very large heaps make full GCs slower, compounding this exact failure mode).
- Loosening `spark.network.timeout` and `spark.shuffle.io.maxRetries` is a legitimate *complementary* mitigation here (the executor is genuinely still alive and will eventually respond), unlike the OOMKilled case where no amount of waiting helps because the process is gone.

## 💡 Key Insights for On-Premise

### 1. Dynamic allocation without shuffle tracking = FetchFailed factory
If executors are scaled down but hold shuffle files, reducers fetch from dead pods. With no external shuffle service on K8S, **always** pair dynamic allocation with `spark.dynamicAllocation.shuffleTracking.enabled=true` (or decommissioning with block migration).

### 2. `spark.local.dir` disk hygiene
Point it at large, fast, **multiple** disks (`/data1/spark,/data2/spark`) and monitor free space. A full shuffle disk manifests as FetchFailed/IOException, not "disk full." On Kubernetes, confirm whether `spark.local.dir` is backed by an `emptyDir` (ephemeral, tied to the node's disk) or a dedicated PVC — an `emptyDir` sharing the node's root disk with other pods can fill up from causes entirely outside your Spark job.

### 3. Correlate FetchFailed timing with cluster-wide events
On a shared on-premise cluster, a wave of `FetchFailedException`s across multiple unrelated jobs at the same time usually means a node-level or network-level event (a node going `NotReady`, a network partition, a shared disk filling up) rather than anything specific to your job. `kubectl get events -A` and `kubectl get nodes` around the failure window are worth checking before diving into your own job's configuration.

### 4. Don't confuse "no external shuffle service" with "shuffle is unsafe on K8S"
Spark-on-Kubernetes is fully production-capable for shuffle-heavy workloads — it simply requires you to *actively* choose shuffle tracking or decommissioning instead of getting an external shuffle service "for free" the way YARN clusters did historically. Treat this as a required checklist item whenever dynamic allocation is enabled, not an exotic edge case.

## 🎯 Practical Exercises

### Exercise 1: Read a FetchFailed trace
```python
# See exercises/troubleshooting/exercise-18-fetchfailed.py
# Given a trace, identify which map executor died and why the reduce stage retried.
# Practice distinguishing FetchFailedException (connection-level) from
# MetadataFetchFailedException (the driver's map-output tracker has no
# location at all for that shuffle block).
```

### Exercise 2: Partition sizing
```python
# Run a heavy shuffle at shuffle.partitions=1 vs 8 vs 200 (disable AQE so your
# chosen count sticks) and compare per-partition size and stability:
spark.conf.set("spark.sql.adaptive.enabled", "false")
for parts in [1, 8, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    agg = txns.groupBy("customer_id").agg(spark_sum("amount").alias("total"))
    print(parts, agg.rdd.getNumPartitions())
# Fewer partitions => bigger each => more map-side memory pressure => the
# OOM/GC that gets an executor killed => reducers see FetchFailed.
```

### Exercise 3: Configure shuffle tracking for dynamic allocation
```bash
# Confirm your job's submit config includes shuffle tracking whenever
# dynamic allocation is on (there is no external shuffle service on K8S):
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.timeout=30m \
  your_job.py
# Then scale executors down mid-job (reduce load) and confirm no FetchFailed
# occurs for in-flight shuffle blocks held by a scaled-down executor.
```

### Exercise 4: Classify a shuffle exception before fixing anything
```python
# For each exception below, decide: (a) which row of the table in Core
# Concepts section 4 it matches, (b) whether kubectl describe pod would show
# OOMKilled, Evicted, or the pod still Running, and (c) the correct first fix.
#   1. FetchFailedException: Failed to connect to worker-3:7337
#   2. MetadataFetchFailedException: Missing an output location for shuffle 4
#   3. java.io.IOException: No space left on device
#   4. FetchFailedException ... after spark.shuffle.io.maxRetries exhausted,
#      with the target executor's log showing repeated long Full GCs
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Removed/lost executors** timeline around the failure.
2. **Shuffle Read/Write size per task** — huge = oversized partitions.
3. **GC time %** on map-side executors just before the fetch failure.
4. **Stage retry count** — a stage id climbing (`8.0` → `8.1` → `8.2`) means the cause is still unresolved and each retry is recomputing the map stage from scratch.
5. **Disk usage on `spark.local.dir`** at the node/pod level — approaching full is a leading indicator, not just a post-mortem fact.

### Spark UI Analysis
- Correlate the FetchFailed timestamp with an executor removal in the Executors tab.
- Stage retry count climbing = the cause is unresolved.
- The failed stage's "Shuffle Read" column, compared across its retries, tells you whether the recompute is even making progress or hitting the same wall each time.
- History Server: compare the shuffle write size of the *map* stage between a healthy run and the failing run — a sudden jump often means an upstream data-volume change is now producing oversized partitions.

## 🚨 Common Issues & Solutions

### Issue 1: FetchFailed only with dynamic allocation
**Symptom**: fine with fixed executors, fails when scaling down.
**Root Cause**: an executor holding needed shuffle blocks was removed by dynamic allocation before those blocks were consumed, and there's no external shuffle service on K8S to serve them after the fact.
**Solution**: enable shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled=true`) or use decommissioning with block migration.

### Issue 2: "Missing an output location for shuffle N"
**Symptom**: reduce stage can't find map output; the exception is `MetadataFetchFailedException`, not a connection error.
**Root Cause**: the map executor is gone, and the driver's map-output tracker has no recorded location for that shuffle at all — a step further gone than a simple connection failure.
**Solution**: find *why it died* (OOM/kill) and fix that; the fetch error is downstream. Check `kubectl describe pod` for the relevant executor around the shuffle-write time.

### Issue 3: Stage keeps retrying and never succeeds
**Symptom**: stage attempt number climbs (`.0`, `.1`, `.2`, `.3`) and then the job fails outright once `spark.stage.maxConsecutiveAttempts` is exhausted.
**Root Cause**: the underlying cause (e.g. a node that keeps OOMing under the same partition size) recurs on every retry because nothing structural changed between attempts.
**Solution**: don't just resubmit — fix the actual driver (right-size partitions, raise memoryOverhead, or address the disk/network issue) before retrying again.

### Issue 4: FetchFailed correlates with a specific time of day
**Symptom**: the same job fails with FetchFailed intermittently, often around when other jobs also run on the shared cluster.
**Root Cause**: contention on a shared node or shared network segment — another tenant's workload is starving your executor's CPU/memory/network enough to trigger GC pauses or timeouts.
**Solution**: check cluster-wide node utilization (`kubectl top nodes`) during the failure window; consider resource requests/limits and node affinity/anti-affinity if a specific noisy neighbor is identifiable.

### Issue 5: Loosening `spark.network.timeout` didn't help
**Symptom**: increased timeout and retry counts, but FetchFailed still occurs at roughly the same rate.
**Root Cause**: the map-side executor isn't slow — it's actually dead (OOMKilled), so no amount of waiting will let it respond; timeout tuning only helps the *GC-pause* variant of this problem, not an outright kill.
**Solution**: check `kubectl describe pod` for the target executor first; if it shows `OOMKilled`, address memory/partition sizing (Day 16) instead of network timeouts.

## 📝 Key Takeaways
1. FetchFailed is a **symptom** — the cause is a lost/stalled map-side executor.
2. On K8S there's no external shuffle service — shuffle tracking (or decommissioning) is mandatory with dynamic allocation.
3. Right-size shuffle partitions (~100–200MB) to prevent the OOM that triggers it.
4. Loosen `spark.network.timeout` for GC-heavy workloads — but only as a complement to fixing the GC pressure, not a substitute.
5. Watch disk space on `spark.local.dir`.
6. `MetadataFetchFailedException` means the block's location is gone entirely from the driver's tracker — a step beyond a simple connection failure.
7. A climbing stage retry count without a config change will keep failing the same way; fix the root cause before resubmitting.

## 🔗 Next Steps
- **Day 19**: Serialization & UDF Issues
- Practice: for a real FetchFailed, find the matching executor-loss event.
- Experiment: deliberately undersize `spark.sql.shuffle.partitions` on a large shuffle in a test namespace and observe the resulting executor OOM cascade into FetchFailed.

## 📚 Additional Resources
- Spark on Kubernetes: dynamic allocation with shuffle tracking; executor decommissioning & block migration
- Network configuration reference
- Spark shuffle internals (map-output tracker, `BlockManager`)

---

**Progress**: Day 18/40 ✅
