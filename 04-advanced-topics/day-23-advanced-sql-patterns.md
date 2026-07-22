# Day 23: Advanced SQL & Window Functions

## 🎯 Learning Objectives
- Master window functions and how they execute (partition → sort → frame)
- Use grouping sets / rollup / cube for multi-level aggregation in one pass
- Apply higher-order functions on arrays/maps without exploding
- Write SQL that the optimizer can actually make fast

## 📚 Core Concepts

### 1. Window functions
```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy(F.col("txn_ts"))
df = (txns
   .withColumn("running_total", F.sum("amount").over(w))
   .withColumn("rn", F.row_number().over(w))
   .withColumn("prev_amount", F.lag("amount").over(w)))
```
Execution: a window requires a **shuffle by the partition key**, then a **sort by the order key**, then the frame computation. `partitionBy()` with a skewed key → skewed window (same remedies as Day 10).

### 2. Frames
```python
# Rolling 3-row window
w3 = Window.partitionBy("customer_id").orderBy("txn_ts").rowsBetween(-2, 0)
# Range frame (by value, not row count)
wr = Window.partitionBy("customer_id").orderBy("txn_ts").rangeBetween(Window.unboundedPreceding, 0)
```
Ranking (`row_number/rank/dense_rank`) vs aggregate-over-window (`sum/avg`) vs offset (`lag/lead`) behave differently at ties and frame edges — know which you need.

### 3. Multi-dimensional aggregation in one pass
```python
txns.cube("category", "status").agg(F.sum("amount"))       # all combinations
txns.rollup("txn_date", "category").agg(F.sum("amount"))    # hierarchy
txns.groupBy("category").agg(
    F.sum(F.when(F.col("status") == "active", F.col("amount"))).alias("active_amt"))  # pivot-ish
```
`ROLLUP`/`CUBE`/`GROUPING SETS` compute several aggregation levels in a single scan+shuffle instead of N separate queries.

### 4. Higher-order functions (avoid explode when you can)
```python
df.select(F.transform("items", lambda x: x * 2).alias("doubled"))
df.select(F.filter("items", lambda x: x > 10).alias("big"))
df.select(F.aggregate("items", F.lit(0), lambda acc, x: acc + x).alias("sum"))
```
These operate on arrays in-place — cheaper than `explode` → aggregate → collect.

## 🔍 Deep Dive: Top-N per group (a very common pattern)
```python
w = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = txns.withColumn("rn", F.row_number().over(w)).where("rn <= 3")
```
Watch: this shuffles the whole table by `category`. If you only need top-N and the table is huge, a pre-aggregation or a broadcast of the small side can cut the shuffle.

## 💡 Key Insights for On-Premise
### 1. One window spec, many columns
Define a `Window` once and reuse it for several columns — Spark computes them in a **single** window operator rather than repeating the shuffle/sort per column.

### 2. Skewed window partitions
`Window.partitionBy(hot_key)` skews exactly like a groupBy/join. AQE skew handling does **not** apply to windows — you must salt or restructure.

## 🎯 Practical Exercises

### Exercise 1: Windows & top-N
```python
# See exercises/advanced/exercise-23-advanced-sql.py
# Running totals, row_number top-N per category, lag/lead deltas.
```

### Exercise 2: rollup vs N queries
```python
# Compute per-date, per-category, and grand totals in one rollup; compare plan to 3 groupBys.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Number of `Window`/`Exchange` operators in the plan (fewer = better).
2. Task-time skew on the window's partition key.

### Spark UI Analysis
- SQL tab: a single `Window` node for multiple reused columns confirms the shuffle isn't repeated.

## 🚨 Common Issues & Solutions

### Issue 1: Window query is very slow
**Symptom**: one big shuffle + sort dominates.
**Solution**: reduce columns first, reuse one window spec, and fix partition-key skew.

### Issue 2: Wrong running total at ties
**Symptom**: unexpected values with equal order keys.
**Solution**: add a tiebreaker to `orderBy`, and choose `rows` vs `range` frame deliberately.

## 📝 Key Takeaways
1. Windows = shuffle(partition) + sort(order) + frame; skew applies.
2. Reuse one `Window` spec across columns.
3. `ROLLUP`/`CUBE`/`GROUPING SETS` do multi-level aggregation in one pass.
4. Higher-order functions beat explode for array work.
5. AQE skew handling doesn't cover windows — salt manually.

## 🔗 Next Steps
- **Day 24**: UDF/UDAF & Pandas/Arrow UDF Performance

## 📚 Additional Resources
- Spark SQL window functions & higher-order functions docs

---

**Progress**: Day 23/40 ✅
