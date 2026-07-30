# Day 27: Dynamic Partition Pruning (DPP)

## 🎯 Learning Objectives
- Understand DPP and why it dramatically speeds up star-schema joins
- Recognize the exact query shape that triggers it, and why broadcast reuse makes it nearly free
- Verify DPP fired in the plan and the Spark UI
- Know its prerequisites, its relationship to static pruning and bucketing, and common blockers

## 📚 Core Concepts

### 1. The problem DPP solves

Static partition pruning works when you filter the partition column **directly** on the table you're scanning:
```python
txns.where("txn_date = '2026-07-01'")   # prunes at compile time — Catalyst sees the literal
```
This is resolved entirely in the optimizer, before any job runs — Spark just doesn't list/read the other partitions' files.

**Key Points:**
- The hard case is a **star join** where you filter the **dimension**, not the fact:
```python
# You want only "north" region sales, but region lives in `stores`, and the
# fact table `txns` is partitioned by store-related keys, not filtered directly.
txns.join(stores.where("region = 'north'"), "store_id")
```
- Without DPP, Spark must read **all** fact partitions, then perform the join, then discard most of the joined rows — the filter's selectivity is completely wasted at scan time.

### 2. What DPP does

**Key Points:**
- At runtime, Spark evaluates the filtered dimension's join keys **first** (as a small broadcastable relation), then injects those keys as a **dynamic filter** on the fact table's partition column — turning what would be a full scan into a pruned scan, driven by the *other* side of the join.
- Concretely, Catalyst inserts a `DynamicPruningSubquery`/`dynamicpruningexpression` into the fact scan's `PartitionFilters`. When the dimension side is **already being broadcast** for the join itself, Spark **reuses that same broadcast exchange** (`ReuseExchange`) to drive the pruning filter — meaning DPP effectively costs nothing extra beyond the join's own broadcast.
- If the dimension isn't otherwise broadcast, Spark can still run a small duplicate subquery to compute the pruning keys, but only when its own cost/benefit heuristic judges the fact table's estimated savings worth the extra subquery scan (governed by `spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio`).

### 3. Trigger conditions

**Key Points:**
- `spark.sql.optimizer.dynamicPartitionPruning.enabled=true` (default on in Spark 3+).
- The fact table is **partitioned** on the join key (or a column functionally tied to it) — DPP prunes **partitions**, so an unpartitioned fact table gets no benefit from DPP at all (bucketing/file-level stats are the alternative there, Day 26/28).
- The dimension side is **filtered**, and its filtered result is small enough to broadcast — `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly` (default `true`) restricts automatic DPP to cases where a broadcast of that side is already happening for the join, which is also the cheapest and most reliable case to get.
- The join is an **equi-join** on the partition column (or a column the partition column is derived from) — DPP cannot fire through a non-equi join condition.

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
```

### 4. DPP vs bucketing vs CBO — three different problems that look similar

**Key Points:**
- **DPP** (this lesson) reduces **I/O** at scan time by skipping whole fact-table **partitions** the join could never match — it says nothing about whether the join itself shuffles.
- **Bucketing** (Day 26) reduces **shuffle** at join time by pre-co-locating matching keys into aligned files — it says nothing about which partitions get scanned.
- **CBO** (Day 28) picks a better **join order/strategy** at compile time using stored statistics — it doesn't touch scan-time pruning or shuffle elimination directly, though its stats often *enable* DPP and bucketing decisions to be made correctly (e.g. keeping a dimension's estimated size small enough to stay broadcastable).
- These three are complementary, not overlapping: a well-tuned star-schema pipeline uses all three — CBO to pick good broadcast/join decisions, DPP to prune fact partitions using the broadcast dimension, and bucketing (where the join key isn't the partition key) to eliminate shuffle on top.

## 🔍 Deep Dive: Verifying DPP

### Step-by-Step Process
1. Write the star join with the dimension side **filtered** and (ideally) **explicitly broadcast**.
2. Call `.explain()` and search the fact table's `Scan` node for `PartitionFilters` containing `dynamicpruningexpression`.
3. Run the query and check the Spark UI SQL tab — the fact scan node's metrics should show "number of partitions read" far smaller than the table's total partition count.
4. Compare against the same query with DPP disabled (`spark.sql.optimizer.dynamicPartitionPruning.enabled=false`) to see the difference in partitions/files/bytes read.

### Example: Practical Example
```python
plan = txns.join(broadcast(stores.where("region='north'")), "store_id")
plan.explain()
# Look for: "PartitionFilters: [... dynamicpruning#... ]" or a
# "SubqueryBroadcast"/"dynamicpruningexpression" node feeding the fact-table scan.
```

**Analysis:**
- In the SQL tab, the fact `Scan` node's "number of partitions read" should be far smaller than the total — this is the single clearest confirmation DPP actually fired, more reliable than reading the text plan alone.
- Because `reuseBroadcastOnly` defaults to `true`, explicitly broadcasting the filtered dimension (as above) is the most dependable way to *guarantee* DPP engages, rather than hoping the cost-based fallback subquery path kicks in.

> Note: our sample fact table (`transactions`) is partitioned by `txn_date`, so to *see* DPP locally, filter a small dimension whose join key maps to `txn_date` (e.g. a "recent dates" dimension). In real star schemas the fact table is often partitioned by a dimension foreign key directly (e.g. `store_id`, `region`) — that's the textbook DPP case this lesson describes conceptually.

### Example: the non-broadcast fallback path
```python
# If the filtered dimension is NOT broadcast (e.g. it's too large, or
# reuseBroadcastOnly forces Spark to decide against it), DPP can still fire
# via a duplicated subquery -- but only if the estimated savings clear the
# fallbackFilterRatio cost/benefit bar:
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
q = txns.join(stores.where("region = 'north'"), "store_id")   # no explicit broadcast() hint
q.explain()   # look for a duplicated small scan feeding a SubqueryBroadcast/dynamicpruning filter
```
**Analysis:** this path costs an *extra* small scan of the dimension (to compute the pruning keys separately from the join itself), so Spark only takes it when the fact table's estimated pruning savings are large enough, per `fallbackFilterRatio`, to be worth that extra cost. In practice, explicitly broadcasting the filtered dimension (as in the main example above) is simpler, cheaper, and more predictable than relying on this fallback.

## 💡 Key Insights for On-Premise

### 1. DPP and broadcast are best friends
DPP most reliably fires when the filtered dimension is broadcast — the keys are then cheaply available (already sitting in every executor) to reuse as a pruning filter with zero extra scan cost. Keep dimension tables small, filtered early, and broadcastable (good stats, Day 28) to get DPP "for free" as a side effect of a broadcast join you were already going to do.

### 2. It only helps if the fact is partitioned on the right column
DPP prunes **partitions**, full stop. If your huge fact table isn't partitioned on the foreign key you actually filter through (e.g. it's partitioned by `txn_date` but your query filters by `region`), DPP simply has nothing to prune — consider re-partitioning the fact table by the dimension key you filter most often, bucketing by it instead (Day 26) for join-shuffle elimination, or relying on file-level min/max statistics (Parquet row-group stats, or Iceberg's file-level metadata) for a coarser form of pruning.

### 3. DPP reduces I/O against MinIO/S3, which matters more than on HDFS
Every fact partition DPP skips is one less set of LIST/GET calls against the on-prem object store. Because s3a-style object storage has higher per-request latency than local HDFS block reads, DPP's I/O savings compound more visibly on an on-prem MinIO-backed lakehouse than they would on a tightly-coupled HDFS cluster — making DPP-friendly fact table partitioning a high-value design choice for star schemas served off MinIO.

## 🎯 Practical Exercises

### Exercise 1: See DPP fire (see `exercises/advanced/exercise-27-dpp.py`)
```python
dates = (txns.select("txn_date").distinct()
         .withColumn("is_recent", F.col("txn_date") >= F.date_sub(F.current_date(), 7)))
recent = dates.where("is_recent").select("txn_date")

spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
q = txns.join(broadcast(recent), "txn_date").agg(F.sum("amount"))
q.explain()   # look for 'dynamicpruningexpression' / 'PartitionFilters' on the fact Scan
q.count()
```

### Exercise 2: Break it
```python
# Disable DPP and compare partitions/files read on the exact same query
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
q2 = txns.join(broadcast(recent), "txn_date").agg(F.sum("amount"))
q2.explain()
q2.count()
# Compare the fact Scan node's "number of partitions read" between q and q2.
```

### Exercise 3: DPP vs bucketing — which one actually removed the shuffle?
```python
# 1. Run the DPP-enabled join from Exercise 1 and check for an Exchange
#    feeding the join itself (separately from partition pruning on the scan):
q.explain()
# 2. Answer: did DPP remove the join's shuffle, or only reduce the fact
#    scan's partitions-read? (Hint: re-read Core Concept 4.)
# 3. If the join key were customer_id instead of the partition column txn_date,
#    would DPP help at all? What would help instead (Day 26)?
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Fact-scan "partitions read"** with DPP enabled vs disabled — the core signal of whether pruning engaged and how much it saved.
2. **Bytes/files read at the fact `Scan` node**, visible in the SQL tab's node metrics.
3. **Presence of `dynamicpruningexpression`/`SubqueryBroadcast`** in `PartitionFilters` on the fact scan.
4. **Whether the dimension's broadcast is reused** (`ReuseExchange`) rather than re-computed as a separate subquery — reused broadcasts mean DPP is essentially free.

### Spark UI Analysis
- **SQL tab scan node**: `dynamicpruningexpression` inside `PartitionFilters` confirms DPP is active for that scan; the node's "number of partitions read" metric quantifies the actual savings.
- **Stages tab**: fewer/smaller tasks reading the fact table (vs a full scan) is the downstream, visible effect of successful pruning.

## 🚨 Common Issues & Solutions

### Issue 1: DPP not firing
**Symptom**: Full fact scan despite a filtered dimension in the query.
**Root Cause**: The fact table isn't partitioned on the join key, the dimension side isn't filtered/broadcastable, the join isn't an equi-join on the partition column, or `dynamicPartitionPruning.enabled` is false.
**Solution**: Confirm the fact table's partition column matches the join key, ensure the dimension is filtered and small enough to broadcast (explicitly hint `broadcast()` if needed), and check the config flag is on (it's the default).

### Issue 2: Worked in dev, not in prod
**Symptom**: The same query plan behaves differently between environments.
**Root Cause**: Stats/size estimates differ between environments — in prod the dimension may have grown past the broadcast threshold, so it's no longer broadcast and `reuseBroadcastOnly` prevents DPP from firing.
**Solution**: Run `ANALYZE TABLE` on the dimension regularly (Day 28) and monitor its size over time; if it's structurally growing past broadcast size, consider an explicit `broadcast()` hint with a raised threshold, or accept that DPP will stop applying and plan for the fact table's partitioning to be pruned some other way.

### Issue 3: DPP fires but savings are smaller than expected
**Symptom**: Partition count drops, but query time doesn't improve proportionally.
**Root Cause**: The remaining partitions are still large (few, but big, partitions), or the bottleneck has shifted elsewhere (e.g. the join's build side, downstream aggregation shuffle).
**Solution**: Check whether the fact table's partitioning granularity matches actual filter selectivity — very coarse partitions (e.g. by year) prune fewer bytes per partition skipped than fine-grained ones (by day); re-examine the end-to-end plan rather than assuming the scan was the only cost.

### Issue 4: DPP mistaken for a fix to join-shuffle cost
**Symptom**: Team assumes DPP eliminates the shuffle in a fact-dimension join, but a large `Exchange` is still present.
**Root Cause**: DPP only prunes **which fact partitions get scanned** — it says nothing about whether the join itself still needs a shuffle. That's a separate concern addressed by broadcast joins (Day 25) or bucketing (Day 26).
**Solution**: Treat DPP as an I/O-reduction optimization layered on top of, not a replacement for, correct join-strategy selection — verify both the scan's partition count *and* the join's operator type in the plan.

### Issue 5: Non-equi or complex join condition blocks DPP
**Symptom**: A range or expression-based join condition on the partition column never triggers DPP, even with a filtered, broadcastable dimension.
**Root Cause**: DPP's dynamic filter injection requires an equi-join on the partition column (or a simple derived expression of it) — it cannot construct a pruning predicate from an arbitrary non-equi condition.
**Solution**: Where possible, express the relationship as an equi-join on the actual partition column (e.g. join on `txn_date` directly rather than a `BETWEEN` range against two separate dimension columns), or accept that non-equi star joins won't benefit from DPP and rely on file-level statistics instead.

## 📝 Key Takeaways
1. DPP prunes fact-table partitions at runtime using a dynamic filter derived from the joined-and-filtered dimension.
2. It shines in star-schema joins where the filter naturally lands on the dimension, not the fact table's own partition column.
3. Prerequisites: fact partitioned on the join key, dimension filtered and (ideally) broadcast, and an equi-join between them.
4. When the dimension's broadcast is reused for pruning, DPP is essentially free — this is the design's key efficiency.
5. Verify via `dynamicpruningexpression` in `PartitionFilters` and a measurably reduced "partitions read" in the SQL tab.
6. DPP reduces scan I/O only — it doesn't replace correct join-strategy selection (broadcast/bucketing) for shuffle elimination.

## 🔗 Next Steps
- **Day 28**: Cost-Based Optimization

## 📚 Additional Resources
- Spark Dynamic Partition Pruning design docs (SPARK-11150)
- `spark.sql.optimizer.dynamicPartitionPruning.*` configuration reference
- Spark SQL partition pruning and predicate pushdown documentation

---

**Progress**: Day 27/40 ✅
