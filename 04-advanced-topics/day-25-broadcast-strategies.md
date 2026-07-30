# Day 25: Broadcast Strategies & AQE Deep Dive

## 🎯 Learning Objectives
- Master when and how broadcast joins win (and precisely when they backfire)
- Understand Adaptive Query Execution's three pillars: coalesce, skew join, and dynamic join switching
- Read AQE's effects in the plan and the Spark UI, including the initial-vs-final plan distinction
- Tune the thresholds that drive both broadcast decisions and AQE behavior

## 📚 Core Concepts

### 1. Broadcast hash join mechanics

```python
from pyspark.sql.functions import broadcast
txns.join(broadcast(products), "product_id")     # explicit hint
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # auto if estimated size < 10MB
```

**Key Points:**
- The small side is **collected to the driver**, wrapped into a `HashedRelation`, and then **broadcast** to every executor using Spark's torrent-style block broadcast (executors pull it peer-to-peer, not all from the driver at once) — so the large side of the join never shuffles at all.
- **Win**: large ⨝ small avoids the big shuffle entirely — no `Exchange` on the large side, no sort, no network movement of the fact table.
- **Backfire**: if the "small" side isn't actually small, collecting it can OOM the **driver**, and holding the broadcast `HashedRelation` in every executor can OOM the **executors** simultaneously — a single bad broadcast can take down the whole job at once, unlike a slow shuffle that merely runs long.
- Broadcasting is also bounded by `spark.driver.maxResultSize` (the collect step) and `spark.sql.broadcastTimeout` (default 300s — a large or slow-to-materialize side can time out before it even finishes broadcasting).

**Example: SQL join hints**
```sql
SELECT /*+ BROADCAST(products) */ t.*, p.category
FROM transactions t JOIN products p ON t.product_id = p.product_id
-- equivalent legacy syntax: /*+ BROADCASTJOIN(products) */ or /*+ MAPJOIN(products) */
```

### 2. Join hints beyond broadcast

**Key Points:**
- `BROADCAST` isn't the only join strategy hint — Spark supports a full set, each forcing a specific physical join operator regardless of size estimates:

| Hint | Forces |
|---|---|
| `BROADCAST` / `BROADCASTJOIN` / `MAPJOIN` | `BroadcastHashJoin` |
| `MERGE` / `SHUFFLE_MERGE` / `MERGEJOIN` | `SortMergeJoin` |
| `SHUFFLE_HASH` | `ShuffledHashJoin` (build a hash table per shuffled partition, no sort) |
| `SHUFFLE_REPLICATE_NL` | `BroadcastNestedLoopJoin`-style replication for non-equi joins without a small side |

```sql
SELECT /*+ SHUFFLE_HASH(t) */ t.*, p.category
FROM transactions t JOIN products p ON t.product_id = p.product_id
```
- `SHUFFLE_HASH` is worth knowing about specifically: it still shuffles both sides (unlike broadcast) but skips the **sort** step of `SortMergeJoin`, trading memory (building a full hash table per partition) for avoiding a sort — useful when one side is too big to broadcast but still small enough per-partition to hash cheaply.
- Hints are a **directive**, not a suggestion — Catalyst will honor a valid hint even when its own cost model would have picked differently; this is exactly why a stale hint can quietly become a performance regression as data grows (Issue 4).

### 3. AQE — re-optimizing at runtime

`spark.sql.adaptive.enabled=true` (default on since Spark 3.2) lets Spark use **actual** shuffle statistics — measured after a stage completes, not just estimated beforehand — to re-plan the *next* stage:

**Key Points — the three AQE optimizations:**
1. **Coalesce partitions**: merges tiny post-shuffle partitions into fewer, right-sized ones (kills the "200 tiny tasks" problem from a fixed `shuffle.partitions`).
2. **Skew join handling**: detects an oversized post-shuffle partition and splits it into several sub-partitions processed independently, then unions the results.
3. **Dynamic join strategy switch**: converts a planned `SortMergeJoinExec` into a `BroadcastHashJoinExec` (or a shuffled hash join) when a side turns out to be small **after filtering/aggregation**, something the compile-time estimate couldn't know.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")               # >5x median size...
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") # ...and >256MB is "skewed"
```

### 4. Why AQE reduces the need for manual tuning

**Key Points:**
- Pre-AQE, you hand-set `spark.sql.shuffle.partitions` per query, trying to guess a number that's neither too small (huge partitions, spill) nor too large (task-scheduling overhead, tiny output files). With AQE, set a **generous** number and let coalescing right-size it after the fact using real post-shuffle bytes.
- Skew that once required manual salting (Day 10, Day 23) is often handled **automatically** for joins by AQE's skew-join split — though extreme skew, and **window/aggregation** skew (Day 23), still need manual work since AQE's skew handling is join-specific.
- The dynamic join switch means you can write a plain `SortMergeJoin`-shaped query and still get a broadcast join at runtime if a `WHERE` clause happens to shrink one side enough — you don't have to predict selectivity at write time.

## 🔍 Deep Dive: Reading AQE in plans

### Step-by-Step Process
1. Call `.explain()` **before execution** — this shows the *initial* plan, wrapped in an `AdaptiveSparkPlan` node with `isFinalPlan=false`.
2. Trigger execution (`.count()`, `.collect()`, a write action).
3. Call `.explain()` **again** (or check the SQL tab) — the plan now shows `isFinalPlan=true` and reflects what AQE actually decided after seeing real stage output sizes.
4. Look for `AQEShuffleRead` nodes — annotated `coalesced` or `skewed` depending on which optimization fired.

### Example: Practical Example
```python
q = txns.filter("status='active'").join(products, "product_id").groupBy("category").count()
q.explain()             # initial plan: AdaptiveSparkPlan isFinalPlan=false
q.count()                # triggers execution; AQE re-optimizes stage-by-stage
q.explain()              # final plan: AdaptiveSparkPlan isFinalPlan=true
```

**Analysis:**
- If the initial plan shows `SortMergeJoin` but the **final** plan shows `BroadcastHashJoin`, AQE's dynamic join switch fired — the `products` side turned out small enough after the `status='active'` filter upstream shrank the join input's estimated size mid-query.
- `AQEShuffleRead (coalesced)` confirms partition coalescing ran; `AQEShuffleRead (skewed)` confirms a skewed partition was split — check the SQL tab's node details for before/after partition counts.

### Example: Dynamic join strategy switch in action
```python
# Planned (compile-time) as a SortMergeJoin because the optimizer doesn't
# know how selective the filter is without stats:
q = txns.join(products.where("category = 'rare_category'"), "product_id")
q.explain()             # initial: SortMergeJoin (products' unfiltered size looked too big to broadcast)
q.count()
q.explain()             # final: often BroadcastHashJoin once AQE sees the filtered side is tiny
```
**Analysis:** the filter's real selectivity is invisible to the compile-time estimate (no histogram, Day 28) but is fully known once the filter stage actually runs — AQE's dynamic join switch uses that *measured* size to swap in a broadcast join for the next stage, something no amount of pre-run tuning could achieve without either stats or a manual hint.

## 💡 Key Insights for On-Premise

### 1. Broadcast threshold vs reality
Auto-broadcast uses the optimizer's **size estimate**, which without stats (Day 28) can be based on rough heuristics (e.g. file size on disk, which for compressed Parquet can badly under- or over-estimate in-memory row size). A "small" table may fail to auto-broadcast, or a deceptively large one may be attempted and OOM. `ANALYZE TABLE` improves the estimate; the explicit `broadcast()` hint (or SQL hint) overrides the estimate entirely and should be used deliberately, not as a reflex.

### 2. AQE and the number of output files
Coalescing post-shuffle partitions also reduces the number of output files on write — a free small-file win for on-prem object storage (s3a/MinIO), where many tiny objects hurt LIST/GET throughput just as small files historically hurt HDFS NameNode load. Setting a sane `spark.sql.adaptive.advisoryPartitionSizeInBytes` (e.g. 128-256MB, matched to your typical file size target) is one of the highest-leverage single settings on an on-prem cluster.

### 3. `SHUFFLE_HASH` as a middle ground on memory-constrained executors
On a Kubernetes cluster where executor pods have modest, fixed memory limits, `SortMergeJoin`'s sort step and `BroadcastHashJoin`'s full-copy-per-executor both have failure modes at the extremes (spill vs OOM). `SHUFFLE_HASH` sidesteps the sort but still needs the smaller side's *per-partition* slice to fit in memory as a hash table — a reasonable middle ground when a side is too big to broadcast (in full) but still comfortably small once shuffled into `spark.sql.shuffle.partitions` pieces.

### 4. Broadcast timeout and driver sizing are coupled decisions
On a resource-constrained Kubernetes cluster, the driver pod's memory limit directly caps what `spark.driver.maxResultSize` can safely be raised to. If a dimension table legitimately needs to grow past the default 10MB broadcast threshold, raising the threshold means the driver pod must also have enough headroom to collect and hold that data — size the driver's `spark.driver.memory`/`spark.kubernetes.driver.limit.cores` accordingly, don't just bump the threshold in isolation.

## 🎯 Practical Exercises

### Exercise 1: Broadcast on/off (see `exercises/advanced/exercise-25-broadcast-aqe.py`)
```python
# Broadcast OFF -> SortMergeJoin (both sides shuffle)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
txns.join(products, "product_id").explain()

# Broadcast ON (auto) -> BroadcastHashJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
txns.join(products, "product_id").explain()

# Explicit broadcast() hint overrides the estimate
txns.join(broadcast(products), "product_id").explain()
```

### Exercise 2: AQE coalesce & skew
```python
skew = read_table(spark, "transactions_skewed")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")   # AQE will coalesce these down
res = skew.groupBy("customer_id").agg(F.sum("amount").alias("t"))
res.count()
res.explain()   # look for AdaptiveSparkPlan and AQEShuffleRead (coalesced)
```

### Exercise 3: Force each join hint and compare
```python
from pyspark.sql.functions import broadcast

txns.createOrReplaceTempView("t")
products.createOrReplaceTempView("p")

for hint in ["BROADCAST", "MERGE", "SHUFFLE_HASH"]:
    print(f"--- {hint} ---")
    spark.sql(f"""
        SELECT /*+ {hint}(p) */ t.*, p.category
        FROM t JOIN p ON t.product_id = p.product_id
    """).explain()
# 1. Confirm each hint forces the operator named in Core Concept 2's table.
# 2. Which hint produces the smallest shuffle for THIS data size? Why?
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Join node type actually chosen** (`BroadcastHashJoin` vs `SortMergeJoin`) in the *final* plan versus the initial one.
2. **`AQEShuffleRead` coalesced/split partition counts** — how far AQE moved from the configured `shuffle.partitions`.
3. **Post-shuffle task-time balance** — max vs median task duration in the stage, to confirm skew was actually handled.
4. **Driver memory during broadcast** — watch the driver's peak memory in the Spark UI/Kubernetes pod metrics during the collect-and-broadcast step.
5. **Output file count and average size** — a proxy for whether coalescing is helping small-file behavior on MinIO/S3.

### Spark UI Analysis
- **SQL tab**: shows the final adaptive plan with per-node metrics after execution; compare it against the plan captured via `explain()` before execution to see exactly what AQE changed.
- **Stages tab**: for a coalesced stage, the number of tasks in the post-shuffle stage will be fewer than `spark.sql.shuffle.partitions` — confirms coalescing engaged.
- **Executors tab**: a spike in one executor's memory during a broadcast join's build phase is expected; a spike on **all** executors simultaneously followed by failures signals the "small" side wasn't small.

## 🚨 Common Issues & Solutions

### Issue 1: Broadcast join causes driver OOM
**Symptom**: `OutOfMemoryError` (or a `maxResultSize exceeded` error) while collecting the broadcast side.
**Root Cause**: The side being broadcast wasn't actually small — a stale estimate, a missing filter, or an over-eager manual `broadcast()` hint on a table that grew.
**Solution**: Lower `spark.sql.autoBroadcastJoinThreshold` so auto-broadcast doesn't attempt it, remove the manual hint and let AQE's dynamic join switch decide instead, or pre-aggregate/filter the side down before the join.

### Issue 2: AQE enabled but skew persists
**Symptom**: One straggler task remains despite `spark.sql.adaptive.skewJoin.enabled=true`.
**Root Cause**: The skew is in a *window* or *aggregation* operator, not a `SortMergeJoinExec` — AQE's skew-join optimization only rewrites join inputs.
**Solution**: Salt the hot key manually (Day 10/23) for the aggregation or window operator; AQE cannot help outside joins.

### Issue 3: Broadcast times out
**Symptom**: Job fails with a broadcast timeout error after ~300 seconds.
**Root Cause**: The side being broadcast is large enough that collecting, serializing, and distributing it takes longer than `spark.sql.broadcastTimeout`.
**Solution**: Either the table is too big to broadcast (drop the hint, let it shuffle-join instead) or genuinely just slow to materialize (e.g. an expensive upstream computation) — increase `spark.sql.broadcastTimeout` only after confirming the size is actually appropriate for broadcast.

### Issue 4: Broadcast hint silently ignored
**Symptom**: `explain()` still shows `SortMergeJoin` even with an explicit `broadcast()`/`/*+ BROADCAST */` hint.
**Root Cause**: Certain join types can't broadcast the required side — e.g. a `FULL OUTER JOIN` cannot broadcast either side safely (both need full retention), and a `RIGHT OUTER JOIN` can't broadcast the left/outer-preserved side.
**Solution**: Check the join type; hints are only honored where the join semantics permit that side to be the broadcast (build) side. Restructure the query or accept the shuffle join for outer joins where broadcast isn't valid.

### Issue 5: Initial plan and final plan disagree, confusing on-call debugging
**Symptom**: A `explain()` captured in application logs at submit-time shows one join strategy, but the Spark UI after the fact shows another.
**Root Cause**: This is expected AQE behavior — the initial plan is a compile-time guess; the final plan reflects runtime re-optimization. Treating the pre-execution `explain()` as the source of truth for a running job is the actual mistake.
**Solution**: Always check the **final** adaptive plan (`isFinalPlan=true`, or the SQL tab after the query finishes) when debugging actual runtime behavior, not just the plan captured before execution.

### Issue 6: `SHUFFLE_HASH` join OOMs on one executor
**Symptom**: A single executor fails with an OOM during a `/*+ SHUFFLE_HASH */`-hinted join, while a plain `SortMergeJoin` on the same data succeeded.
**Root Cause**: `ShuffledHashJoin` builds a full in-memory hash table for the smaller side's shuffled **partition** — if that partition is skewed (Day 10) or `spark.sql.shuffle.partitions` is set too low (making each partition larger), the hash table for one task can exceed executor memory even though the *total* data size looked reasonable.
**Solution**: Increase `spark.sql.shuffle.partitions` to shrink each partition's hash table, address any skew in the join key first, or fall back to `SortMergeJoin` (which spills to disk instead of OOMing) for that specific join.

## 📝 Key Takeaways
1. Broadcast wins large⨝small by skipping the shuffle entirely, but OOMs the driver and/or every executor at once if the "small" side isn't.
2. AQE re-optimizes with real post-shuffle stats: coalesce, skew-split, and dynamic broadcast switching are its three pillars.
3. With AQE on, set a generous `shuffle.partitions` and a sane `advisoryPartitionSizeInBytes`, and let coalescing right-size partitions.
4. Good stats (Day 28) make auto-broadcast estimates trustworthy; without them, prefer explicit hints for known-small dimension tables.
5. AQE skew handling is join-only — windows/aggregations still need manual salting.
6. Always read the **final** adaptive plan, not just the pre-execution one, when debugging what actually ran.

## 🔗 Next Steps
- **Day 26**: Bucketing Techniques

## 📚 Additional Resources
- Spark Adaptive Query Execution docs (`spark.sql.adaptive.*` configuration reference)
- Join hints reference (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`)
- `spark.sql.autoBroadcastJoinThreshold`, `spark.sql.broadcastTimeout`, `spark.driver.maxResultSize`

---

**Progress**: Day 25/40 ✅
