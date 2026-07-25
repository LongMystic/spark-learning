# Day 20: Performance Debugging (Spark UI & SQL Tab)

## 🎯 Learning Objectives
- Turn "the job is slow" into a specific, measured bottleneck
- Read the SQL tab's query plan and per-node metrics
- Identify the four classic bottlenecks: skew, spill, small files, and bad joins
- Use `explain()` metrics to confirm a fix before/after

## 📚 Core Concepts

### 1. A method, not a vibe
Slowness is one (or more) of:
- **Skew** — a few tasks dominate stage time.
- **Spill** — not enough execution memory; data goes to disk mid-stage.
- **I/O** — too many small files, or reading columns/partitions you don't need.
- **Wrong join** — SortMergeJoin where a broadcast would do, or vice-versa.
- **Too many/few partitions** — tiny tasks (scheduling overhead) or huge tasks (spill/OOM).

### 2. The SQL tab is the best tool
UI → **SQL / DataFrame** → click the query. You get the plan as a graph with **live metrics per operator**: rows, data size, spill, and time. This is where you *see* which operator is expensive, not guess.

Key nodes to recognize:
- `Scan parquet` — check "number of files read", "pushed filters", partition pruning.
- `Exchange` — a shuffle; check its size.
- `SortMergeJoin` / `BroadcastHashJoin` — the join strategy actually chosen.
- `HashAggregate` — spill here means execution-memory pressure.
- `AQEShuffleRead` — AQE coalesced/split partitions.

### 3. Reading `explain`
```python
df.explain("formatted")     # readable plan with per-operator details
df.explain("cost")          # includes stats-based size estimates (needs ANALYZE)
```

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

## 💡 Key Insights for On-Premise
### 1. Wall-clock ≠ work
A job "taking 20 min" might be 18 min with executor pods stuck `Pending` waiting for the scheduler or blocked by the namespace quota. Check the app's *submit → first task* gap and `kubectl get pods` / `kubectl describe pod` (and the namespace `ResourceQuota`) before blaming Spark.

### 2. Locality levels matter
Tasks running at `ANY` instead of `NODE_LOCAL` mean data is fetched across the network. `spark.locality.wait` trades a short scheduling delay for better locality on busy clusters.

## 🎯 Practical Exercises

### Exercise 1: Find the bottleneck
```python
# See exercises/troubleshooting/exercise-20-perf-debugging.py
# One query has skew, one has spill, one has small files — classify each from the UI/metrics.
```

### Exercise 2: Before/after
```python
# Broadcast a small dimension; compare the join node and stage time before vs after.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Task duration distribution** per stage (skew).
2. **Spill (Memory/Disk)** per stage.
3. **Input files / bytes read** at scan nodes (I/O & pruning).
4. **Shuffle read/write** at exchanges.

### Spark UI Analysis
- SQL tab per-node metrics pinpoint the costly operator.
- "Event Timeline" on the stage page shows scheduler delay, GC, and compute split.

## 🚨 Common Issues & Solutions

### Issue 1: Median task 2s, max task 5min
**Symptom**: one straggler dominates.
**Solution**: skew — salt / AQE skew join / isolate the hot key (Day 10, 18).

### Issue 2: Reads 500GB to return 2 rows
**Symptom**: no partition pruning / predicate pushdown.
**Solution**: filter on partition columns; check "pushed filters" at the scan; avoid wrapping columns in functions that block pushdown.

## 📝 Key Takeaways
1. Debug with the **SQL tab**, not intuition — it shows per-operator cost.
2. Max-≫-median task time = skew; non-zero spill = memory pressure.
3. Check files-read and pushed-filters for I/O problems.
4. Verify the join strategy actually chosen.
5. Separate pod-scheduling/quota wait (Pending) from real Spark execution time.

## 🔗 Next Steps
- **Day 21**: Production Incident Response
- Practice: profile one slow production query end-to-end via the SQL tab.

## 📚 Additional Resources
- Spark Web UI (SQL tab) documentation
- `EXPLAIN` / cost-based stats docs

---

**Progress**: Day 20/40 ✅
