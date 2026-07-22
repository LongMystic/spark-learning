# Day 27: Dynamic Partition Pruning (DPP)

## 🎯 Learning Objectives
- Understand DPP and why it dramatically speeds up star-schema joins
- Recognize the query shape that triggers it
- Verify DPP fired in the plan and UI
- Know its prerequisites and common blockers

## 📚 Core Concepts

### 1. The problem DPP solves
Static partition pruning works when you filter the partition column directly:
```python
txns.where("txn_date = '2026-07-01'")   # prunes at compile time
```
But in a star join you often filter the **dimension**, not the fact:
```python
# You want only "north" region sales, but region lives in `stores`, and the
# fact table `txns` is partitioned by txn_date, not region.
txns.join(stores.where("region = 'north'"), "store_id")
```
Without DPP, Spark reads **all** fact partitions, then joins, then discards most rows.

### 2. What DPP does
At runtime, Spark computes the filtered dimension's join keys **first**, then injects them as a **dynamic filter** on the fact table's partition column — so only the relevant fact partitions are read. It turns a full-table scan into a pruned scan, driven by the other side of the join.

### 3. Trigger conditions
- `spark.sql.optimizer.dynamicPartitionPruning.enabled=true` (default on in Spark 3+).
- The fact table is **partitioned** on the join key (or a column functionally tied to it).
- The dimension side is **filtered** and small enough (often broadcastable).
- The join is on the partition/broadcast key.

## 🔍 Deep Dive: Verifying DPP
```python
plan = txns.join(broadcast(stores.where("region='north'")), "store_id")
plan.explain()
# Look for: "PartitionFilters: [... dynamicpruning#... ]" or a
# "SubqueryBroadcast"/"dynamicpruningexpression" node on the fact-table scan.
```
In the SQL tab, the fact `Scan` node's "number of partitions read" should be far smaller than the total.

> Note: our sample fact table is partitioned by `txn_date`, so to *see* DPP locally,
> filter a dimension that maps to `txn_date`. In real star schemas the fact is often
> partitioned by a dimension FK — that's the ideal DPP case.

## 💡 Key Insights for On-Premise
### 1. DPP + broadcast are best friends
DPP most reliably fires when the filtered dimension is broadcast (the keys are cheaply available to inject). Keep dimensions broadcastable (small, with stats) to get DPP for free.

### 2. It only helps if the fact is partitioned on the right column
DPP prunes **partitions**. If your huge fact table isn't partitioned on the FK you filter through, DPP can't prune — consider partitioning (or bucketing) accordingly, or rely on file-level min/max stats instead.

## 🎯 Practical Exercises

### Exercise 1: See DPP fire
```python
# See exercises/advanced/exercise-27-dpp.py
# Join a filtered dimension to the partitioned fact; find dynamicpruning in the plan.
```

### Exercise 2: Break it
```python
# Disable DPP (spark.sql.optimizer.dynamicPartitionPruning.enabled=false) and compare
# partitions/files read.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Fact-scan "partitions read" with vs without DPP.
2. Bytes read at the fact `Scan` node.

### Spark UI Analysis
- SQL tab scan node: `dynamicpruningexpression` in PartitionFilters confirms DPP.

## 🚨 Common Issues & Solutions

### Issue 1: DPP not firing
**Symptom**: full fact scan despite a filtered dimension.
**Solution**: fact not partitioned on the join key, dimension not broadcastable/filtered, or DPP disabled.

### Issue 2: Helped in dev, not in prod
**Symptom**: different behavior.
**Solution**: stats/size estimates differ (dimension no longer broadcast). Run `ANALYZE TABLE` and keep the dimension small.

## 📝 Key Takeaways
1. DPP prunes fact partitions using a runtime filter from the joined dimension.
2. It shines in star-schema joins where you filter the dimension.
3. Needs: fact partitioned on the join key + filtered (usually broadcast) dimension.
4. Verify via `dynamicpruning` in PartitionFilters and reduced partitions read.
5. Good stats keep the dimension broadcastable so DPP fires reliably.

## 🔗 Next Steps
- **Day 28**: Cost-Based Optimization

## 📚 Additional Resources
- Spark Dynamic Partition Pruning design/docs

---

**Progress**: Day 27/40 ✅
