# Day 20: Performance Debugging (Spark UI & SQL Tab)

## 🎯 Learning Objectives
- Turn "the job is slow" into a specific, measured bottleneck
- Read the SQL tab's query plan and per-node metrics
- Identify the four classic bottlenecks: skew, spill, small files, and bad joins
- Use `explain()` metrics to confirm a fix before/after
- Separate real Spark execution time from Kubernetes scheduling/queueing delay

## 📚 Core Concepts

### 1. A method, not a vibe

"The job is slow" is not a diagnosis — it's a starting point. Slowness is one (or more) of a small, fixed set of root causes, and the whole point of this lesson is to replace guessing with measurement.

**Key Points:**
- **Skew** — a few tasks dominate stage time.
- **Spill** — not enough execution memory; data goes to disk mid-stage.
- **I/O** — too many small files, or reading columns/partitions you don't need.
- **Wrong join** — SortMergeJoin where a broadcast would do, or vice-versa.
- **Too many/few partitions** — tiny tasks (scheduling overhead) or huge tasks (spill/OOM).
- Any of these can compound — e.g. skew often *causes* spill on the one oversized partition, so fixing skew alone can also eliminate spill you thought was a separate problem.

### 2. The SQL tab is the best tool

UI → **SQL / DataFrame** → click the query. You get the plan as a graph with **live metrics per operator**: rows, data size, spill, and time. This is where you *see* which operator is expensive, not guess.

**Key Points:**
- Key nodes to recognize:
  - `Scan parquet` — check "number of files read", "pushed filters", partition pruning.
  - `Exchange` — a shuffle; check its size.
  - `SortMergeJoin` / `BroadcastHashJoin` — the join strategy actually chosen.
  - `HashAggregate` — spill here means execution-memory pressure.
  - `AQEShuffleRead` — AQE coalesced/split partitions.
- The SQL tab shows metrics for a *completed or running* query for the life of that Spark application; after the app exits, the same information is available from the History Server as long as `spark.eventLog.enabled=true` and `spark.eventLog.dir` points at a durable location (e.g. `s3a://spark-events`).
- Every number shown in the plan graph (rows, size, time, spill) is an *actual runtime metric*, not an estimate — this is different from `explain("cost")`'s estimates, which come from table statistics and may be stale or missing.

### 3. Reading `explain`
```python
df.explain("formatted")     # readable plan with per-operator details
df.explain("cost")          # includes stats-based size estimates (needs ANALYZE)
```

**Key Points:**
- `explain("formatted")` groups the physical plan into numbered nodes with a legend below — easier to read than the default tree for wide plans.
- `explain("cost")` only shows meaningful size estimates if the tables involved have been `ANALYZE TABLE ... COMPUTE STATISTICS` (and ideally `FOR COLUMNS` for the columns used in filters/joins) — without stats, Spark falls back to conservative defaults that can pick the wrong join strategy.
- `df.explain()` (no argument) is the fast default — a plain physical plan tree; use it for a quick sanity check before reaching for the more detailed modes.

### 4. Adaptive Query Execution (AQE) changes the plan while it runs

AQE (on by default since Spark 3.2, `spark.sql.adaptive.enabled=true`) re-optimizes the plan **between stages**, using actual runtime statistics instead of only the pre-run estimates. This means the plan you see in `explain()` before running can differ from what actually executed.

**Key Points:**
- `spark.sql.adaptive.coalescePartitions.enabled=true` merges small post-shuffle partitions automatically — this is why you may configure a high `spark.sql.shuffle.partitions` for safety and still not pay for excessive tiny tasks.
- `spark.sql.adaptive.skewJoin.enabled=true` detects an oversized partition **after the shuffle has run** and splits it into smaller sub-partitions on the fly, without you salting anything manually.
- `spark.sql.adaptive.localShuffleReader.enabled` (default true) avoids a network shuffle read when AQE determines the data can be read locally after a broadcast join conversion.
- The SQL tab shows both the **initial** plan and, after execution, the **final adaptive** plan — always check the *final* plan's per-node metrics, not the initial one, since AQE may have changed join strategies or partition counts based on runtime stats.

## 🔍 Deep Dive: The 6-minute performance triage

### Step-by-Step Process
1. **UI → SQL tab → slowest query.** Find the operator consuming most time.
2. **Stages tab → slowest stage → task duration min/median/max.** Max ≫ median ⇒ **skew**.
3. **Same stage → Spill (Memory/Disk).** Non-zero ⇒ **memory pressure** (raise partitions/memory).
4. **Scan node → files read + bytes read.** Huge for a small result ⇒ **no pruning / small files**.
5. **Join node → strategy.** Big SortMergeJoin with one small side ⇒ **broadcast it**.
6. **Confirm**: apply one change, re-run, compare the same metric.

### Example: proving small-file pain
```python
scan = spark.read.parquet("data/transactions")
print("files:", scan.rdd.getNumPartitions())    # ~1 partition per file
# 10k tiny files -> 10k tasks -> scheduling overhead dominates -> compact (Day 26/34)
```

### Example: end-to-end triage of a genuinely slow query

```python
from pyspark.sql import functions as F

txns = spark.read.parquet("data/transactions")
products = spark.read.parquet("data/products")

result = (
    txns.join(products, "product_id")
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("total"))
)
result.explain("formatted")
result.write.mode("overwrite").parquet("out/slow_query")
```
Working the checklist:
1. **SQL tab**: the graph shows `Exchange` nodes on both sides of the join and again before the `HashAggregate` — two shuffles for what should be a small-dimension join.
2. **Stages tab**: the aggregation stage shows max task duration 6 minutes vs a 20-second median — **skew** on `customer_id`.
3. **Same stage, Spill (Disk)**: 40GB — the skewed partition's hash aggregation is spilling.
4. **Scan node** for `products`: 50MB total, well under the default `spark.sql.autoBroadcastJoinThreshold` (10MB by default, often raised) — it *should* have been broadcast.
5. **Join node**: shows `SortMergeJoin`, not `BroadcastHashJoin` — confirms the broadcast didn't happen.

**Analysis:**
- The join strategy is the first fix: `products` is small enough to broadcast; forcing it removes one `Exchange` entirely and often changes the *shape* of the downstream stage.
- Skew on `customer_id` during the aggregation is a separate, second problem — enabling `spark.sql.adaptive.skewJoin.enabled` (for the join) and checking whether AQE's skewed-partition handling applies to the aggregation, or salting the hot key, addresses it.
- Fixing the join alone might reduce the spill too, since less shuffled data (no second exchange feeding the aggregate) can mean less pressure — but re-measure after each change rather than assuming.

### Example: a spill-dominated stage, diagnosed and fixed

```python
# Symptom: a HashAggregate stage takes 8 minutes; SQL tab shows
# "spill size (disk): 120 GB" on that operator.
big = spark.read.parquet("data/transactions")
result = big.groupBy("customer_id", "category").agg(F.sum("amount"), F.avg("amount"))
result.write.mode("overwrite").parquet("out/agg")
```
Triage:
1. **SQL tab**: `HashAggregate` node shows a large "spill size (disk)" metric directly — no guessing needed.
2. **Stages tab**: for that stage, "Shuffle Read Size / Records" is large per task; task duration is fairly even (median ≈ max) — this rules out skew as the primary cause.
3. **Diagnosis**: too few partitions for the amount of data being aggregated per task, causing the in-memory hash map to exceed `spark.sql.shuffle.partitions`-driven per-task memory and spill to disk.
```python
# Fix: more, smaller partitions so each task's aggregation state fits in memory.
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.adaptive.enabled", "true")   # let AQE coalesce afterward if needed
```

**Analysis:**
- Even task durations (median ≈ max) with high spill is a different signature than skew (max ≫ median) — same symptom category ("slow aggregation") but a different root cause and a different fix.
- Raising shuffle partitions directly attacks the per-task memory footprint, unlike a skew fix (salting/AQE skew join) which targets one disproportionate key.
- Re-running with the new partition count and confirming spill drops to near-zero on the same operator is the "confirm" step — don't assume the fix worked without re-measuring the same metric.

## 💡 Key Insights for On-Premise
### 1. Wall-clock ≠ work
A job "taking 20 min" might be 18 min with executor pods stuck `Pending` waiting for the scheduler or blocked by the namespace quota. Check the app's *submit → first task* gap and `kubectl get pods` / `kubectl describe pod` (and the namespace `ResourceQuota`) before blaming Spark.

```bash
# See how long executor pods sat Pending before running
kubectl -n spark-jobs get pods -l spark-role=executor -o wide
kubectl -n spark-jobs describe pod <pending-exec-pod>   # look at Events: for scheduling reasons
kubectl -n spark-jobs describe resourcequota             # is the namespace at its pod/CPU/memory cap?
```

### 2. Locality levels matter
Tasks running at `ANY` instead of `NODE_LOCAL` mean data is fetched across the network. `spark.locality.wait` trades a short scheduling delay for better locality on busy clusters.

### 3. History Server as a baseline
Because on-premise jobs typically run on a recurring schedule (via Airflow), the History Server holding weeks of past runs is one of your best diagnostic tools: compare today's slow run's stage times, shuffle sizes, and task counts against last week's healthy run of the *same* DAG task to see exactly what changed, rather than debugging from a cold start.

### 4. Shared-cluster contention looks like a Spark problem but isn't
On multi-tenant Kubernetes, CPU throttling (from a pod's CPU *limit*, distinct from its *request*) can silently slow every task in a stage uniformly, without showing up as skew or spill at all — it just looks like "everything is a bit slower than usual." `kubectl top pod` and checking for CPU throttling metrics (if your monitoring stack exposes `container_cpu_cfs_throttled_seconds_total` from cAdvisor/Prometheus) can reveal this before you spend time on a Spark-side fix that won't help.

## 🎯 Practical Exercises

### Exercise 1: Find the bottleneck
```python
# See exercises/troubleshooting/exercise-20-perf-debugging.py
# It runs three distinct patterns against generated tables:
#   A) groupBy on a skewed key (transactions_skewed) -- classify via
#      max/median task duration ratio in the Stages tab.
#   B) a query filtered on a partition column vs an unfiltered read --
#      compare "number of files read" at the Scan node via explain().
#   C) a join with broadcast disabled vs enabled -- compare Exchange nodes.
# Classify each pattern using the checklist above before reading the
# analysis questions at the end of the script.
```

### Exercise 2: Before/after
```python
# Broadcast a small dimension; compare the join node and stage time before vs after.
products = read_table(spark, "products")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
txns.join(products, "product_id").explain()   # SortMergeJoin, shuffles both sides

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
txns.join(products, "product_id").explain()   # BroadcastHashJoin, no fact-table shuffle
```

### Exercise 3: Separate scheduling delay from compute time
```bash
# For a job that "feels slow", compare submit time to first-task-start time:
kubectl -n spark-jobs get pods -l spark-role=executor \
  -o custom-columns=NAME:.metadata.name,START:.status.startTime,PHASE:.status.phase
# A large gap between pod creation and Running phase, or many pods stuck
# Pending, points at scheduling/quota, not query logic.
```

### Exercise 4: Compare the initial plan to the final adaptive plan
```python
df = spark.read.parquet("data/transactions").join(
    spark.read.parquet("data/products"), "product_id"
).groupBy("category").agg(F.sum("amount"))

print("--- initial plan (pre-execution estimate) ---")
df.explain("formatted")

df.collect()   # actually run it so AQE re-optimizes with runtime stats

# In the SQL tab, open this query and compare the "final" plan graph
# (post-AQE) against the explain() output above -- note any join strategy
# changes or coalesced partition counts that only appear after execution.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Task duration distribution** per stage (skew).
2. **Spill (Memory/Disk)** per stage.
3. **Input files / bytes read** at scan nodes (I/O & pruning).
4. **Shuffle read/write** at exchanges.
5. **Scheduler delay / launch delay** in the stage's "Event Timeline" — time spent waiting to be scheduled, not computing.
6. **Executor CPU utilization** vs allocated cores — low utilization with long duration can indicate throttling or I/O wait rather than genuine compute-bound work.

### Spark UI Analysis
- SQL tab per-node metrics pinpoint the costly operator.
- "Event Timeline" on the stage page shows scheduler delay, GC, and compute split.
- The Stages tab's "Summary Metrics" table (min/25th/median/75th/max) is the fastest way to quantify skew numerically instead of eyeballing a chart.
- Compare "Shuffle Write Size" at the upstream stage against "Shuffle Read Size" at the downstream stage — they should roughly match; a big mismatch suggests something odd happened between stages (e.g. AQE re-optimization or a partial re-run).

## 🚨 Common Issues & Solutions

### Issue 1: Median task 2s, max task 5min
**Symptom**: one straggler dominates.
**Root Cause**: skew — a single key/partition holds disproportionately more data than the rest.
**Solution**: salt / AQE skew join / isolate the hot key (Day 10, 18).

### Issue 2: Reads 500GB to return 2 rows
**Symptom**: no partition pruning / predicate pushdown.
**Root Cause**: the filter isn't expressed in a form Catalyst can push down to the scan — often because the filter column isn't a partition column, or it's wrapped in a function (`CAST`, string manipulation) before comparison.
**Solution**: filter on partition columns; check "pushed filters" at the scan; avoid wrapping columns in functions that block pushdown.

### Issue 3: Job is uniformly a bit slower than usual, no clear hot spot
**Symptom**: every stage takes somewhat longer than the historical baseline; no single task or operator stands out.
**Root Cause**: often shared-cluster contention — CPU throttling from pod CPU limits, node-level resource pressure from other tenants, or a busier network segment — rather than anything in the query itself.
**Solution**: check `kubectl top nodes`/`kubectl top pod` and, if available, CPU throttling metrics for the time window; compare against the History Server's baseline for the same job on a quieter day.

### Issue 4: Adding a broadcast hint had no effect
**Symptom**: `broadcast(df)` used, but the SQL tab still shows `SortMergeJoin`.
**Root Cause**: the "small" side is actually larger than `spark.sql.autoBroadcastJoinThreshold`, or the broadcast side is on the wrong side of an outer join type that Spark cannot broadcast safely (e.g. broadcasting the left side of a left outer join is invalid).
**Solution**: check the actual size of the broadcast candidate via the Scan node's bytes-read metric, and confirm the join type actually permits broadcasting on that side.

### Issue 5: Small-file scan dominates a query that reads very little data
**Symptom**: thousands of tasks, most doing almost no work, for a scan that should be quick.
**Root Cause**: the source table has far more files than necessary for its data volume — each task has fixed per-task scheduling overhead, so many tiny files means overhead dominates actual work.
**Solution**: compact the table (see Day 26/34 for compaction strategies on Iceberg/Hive tables); in the interim, consider `spark.sql.files.maxPartitionBytes` tuning to combine small files into fewer read tasks.

## 📝 Key Takeaways
1. Debug with the **SQL tab**, not intuition — it shows per-operator cost.
2. Max-≫-median task time = skew; non-zero spill = memory pressure.
3. Check files-read and pushed-filters for I/O problems.
4. Verify the join strategy actually chosen.
5. Separate pod-scheduling/quota wait (Pending) from real Spark execution time.
6. `explain("cost")` needs table statistics to be meaningful — collect them with `ANALYZE TABLE`.
7. Use the History Server to compare against a known-healthy baseline run before assuming what changed.

## 🔗 Next Steps
- **Day 21**: Production Incident Response
- Practice: profile one slow production query end-to-end via the SQL tab.
- Experiment: run `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` on a table you query often and compare `explain("cost")` before and after.

## 📚 Additional Resources
- Spark Web UI (SQL tab) documentation
- `EXPLAIN` / cost-based stats docs
- Kubernetes CPU requests/limits and CFS throttling behavior

---

**Progress**: Day 20/40 ✅
