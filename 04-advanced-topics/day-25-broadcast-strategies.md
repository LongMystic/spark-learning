# Day 25: Broadcast Strategies & AQE Deep Dive

## 🎯 Learning Objectives
- Master when and how broadcast joins win (and when they backfire)
- Understand Adaptive Query Execution: coalesce, skew join, and dynamic join switch
- Read AQE's effects in the plan and UI
- Tune the thresholds that drive both

## 📚 Core Concepts

### 1. Broadcast hash join
The small side is collected to the driver and **broadcast** to every executor, so the large side never shuffles.
```python
from pyspark.sql.functions import broadcast
txns.join(broadcast(products), "product_id")     # explicit hint
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # auto if < 10MB
```
- **Win**: large ⨝ small, avoids the big shuffle entirely.
- **Backfire**: "small" side isn't small → driver OOM broadcasting it, or executor memory blows up holding it. Broadcasting is bounded by `spark.driver.maxResultSize` too.

### 2. AQE — re-optimizing at runtime
`spark.sql.adaptive.enabled=true` lets Spark use **actual** shuffle statistics to re-plan between stages:
- **Coalesce partitions**: merges tiny post-shuffle partitions (kills the "200 tiny tasks" problem).
- **Skew join**: splits an oversized partition into sub-partitions automatically.
- **Dynamic join switch**: converts a planned SortMergeJoin into a broadcast join when a side turns out to be small after filtering.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
```

### 3. Why AQE reduces the need for manual tuning
Pre-AQE you hand-set `shuffle.partitions` per query. With AQE, set a generous number and let coalescing right-size it. Skew that once required manual salting is often handled automatically — though extreme skew and *window* skew still need manual work.

## 🔍 Deep Dive: Reading AQE in plans
```python
q = txns.filter("status='active'").join(products, "product_id").groupBy("category").count()
q.explain()      # look for: AdaptiveSparkPlan, AQEShuffleRead (coalesced/skewed), BroadcastHashJoin
```
`AdaptiveSparkPlan isFinalPlan=true` after execution shows the *final* re-optimized plan. The initial `explain()` before running shows the *planned* one — they can differ.

## 💡 Key Insights for On-Premise
### 1. Broadcast threshold vs reality
Auto-broadcast uses the optimizer's **size estimate**. Without stats (Day 28) the estimate can be wrong — a "small" table may not broadcast, or a big one may be attempted. `ANALYZE TABLE` improves the estimate; the explicit `broadcast()` hint overrides it.

### 2. AQE and the number of output files
Coalescing post-shuffle partitions also reduces the number of output files on write — a free small-file win for on-prem HDFS.

## 🎯 Practical Exercises

### Exercise 1: Broadcast on/off
```python
# See exercises/advanced/exercise-25-broadcast-aqe.py
# Toggle autoBroadcastJoinThreshold and the broadcast() hint; watch the join node change.
```

### Exercise 2: AQE coalesce & skew
```python
# Run a skewed join with AQE off vs on; observe AQEShuffleRead and task-time balance.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Join node type actually chosen (BroadcastHashJoin vs SortMergeJoin).
2. `AQEShuffleRead` coalesced/split partition counts.
3. Post-shuffle task-time balance (skew handled?).

### Spark UI Analysis
- SQL tab shows the final adaptive plan and per-node metrics; compare initial vs final.

## 🚨 Common Issues & Solutions

### Issue 1: Broadcast join causes driver OOM
**Symptom**: OOM while broadcasting.
**Solution**: the side wasn't small — lower `autoBroadcastJoinThreshold`, remove the hint, or pre-aggregate it.

### Issue 2: AQE enabled but skew persists
**Symptom**: still one straggler.
**Solution**: it's a *window* or aggregation skew (AQE skew join only covers joins) → salt manually.

## 📝 Key Takeaways
1. Broadcast wins large⨝small but OOMs if the "small" side isn't.
2. AQE re-optimizes with real stats: coalesce, skew-split, dynamic broadcast switch.
3. With AQE, set generous `shuffle.partitions` and let it coalesce.
4. Stats (Day 28) make auto-broadcast estimates trustworthy.
5. AQE skew handling is join-only — windows/aggs still need salting.

## 🔗 Next Steps
- **Day 26**: Bucketing Techniques

## 📚 Additional Resources
- Spark Adaptive Query Execution docs
- Join hints reference

---

**Progress**: Day 25/40 ✅
