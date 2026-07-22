# Day 26: Bucketing Techniques

## 🎯 Learning Objectives
- Understand bucketing and how it eliminates shuffle for repeated joins/aggregations
- Create and use bucketed tables correctly
- Know bucketing's constraints and when it pays off vs partitioning
- Diagnose why a bucketed join still shuffles

## 📚 Core Concepts

### 1. What bucketing does
Bucketing hash-distributes rows into a fixed number of files (**buckets**) by a key, and stores them sorted. Two tables bucketed the **same way** on the join key can be joined **without a shuffle** — the matching buckets are already co-located and sorted.

```python
(txns.write
    .bucketBy(64, "customer_id")
    .sortBy("customer_id")
    .mode("overwrite")
    .saveAsTable("txns_bucketed"))
```

### 2. Partitioning vs bucketing
| | Partitioning | Bucketing |
|---|---|---|
| Mechanism | directory per value (`txn_date=.../`) | fixed # of hashed files |
| Good for | pruning by a low-cardinality filter column | joins/aggregations on a high-cardinality key |
| Cardinality | low (dates, regions) | high (ids) |
| Risk | too many tiny partitions | wrong bucket count / skewed key |

They're **complementary**: partition by `txn_date`, bucket by `customer_id`.

### 3. When a bucketed join avoids shuffle
All must hold:
- Both tables bucketed by the **same** column(s), with the **same number of buckets**.
- Join key = bucket key.
- Both read as **tables** (via the metastore), not as raw file paths.

## 🔍 Deep Dive: Verifying no-shuffle
```python
a = spark.table("txns_bucketed")
b = spark.table("customers_bucketed")   # also bucketBy(64, "customer_id")
a.join(b, "customer_id").explain()
# Look for: NO Exchange on the join inputs. If you see Exchange, a precondition failed.
```

### Common shuffle-reintroducers
- Different bucket counts → Spark shuffles one side to match.
- Reading via `spark.read.parquet(path)` instead of `spark.table(name)` → bucketing metadata is lost.
- `spark.sql.sources.bucketing.enabled=false`.
- Filtering/transforming the bucket column before the join.

## 💡 Key Insights for On-Premise
### 1. Bucketing is a write-time investment
It costs a shuffle **once** at write. It pays off when the table is joined/aggregated on that key **many times** (e.g. a dimension joined by every downstream job). For a one-off join, broadcast or plain SMJ is simpler.

### 2. Choose bucket count with future data in mind
Aim for buckets of ~100–200MB. Too few → giant buckets/spill; too many → small files. You can't change the count without rewriting the table, so size for growth.

### 3. Skewed bucket keys still skew
Hashing spreads distinct values, but a single hot value lands in one bucket → that bucket is huge. Bucketing does not fix value-level skew.

## 🎯 Practical Exercises

### Exercise 1: Bucketed join
```python
# See exercises/advanced/exercise-26-bucketing.py
# Bucket two tables by customer_id; confirm the join plan has no Exchange.
# (Requires catalog support — falls back to SMJ locally with a note.)
```

### Exercise 2: Break it
```python
# Use mismatched bucket counts and observe the reintroduced shuffle.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Presence/absence of `Exchange` on join inputs.
2. Bucket file sizes (evenness).

### Spark UI Analysis
- SQL tab: a bucketed join shows `SortMergeJoin` with **no** child `Exchange`.

## 🚨 Common Issues & Solutions

### Issue 1: Bucketed join still shuffles
**Symptom**: `Exchange` present.
**Solution**: mismatched bucket counts, read via path not table, or bucketing disabled — check all preconditions.

### Issue 2: One bucket is huge
**Symptom**: straggler on a bucketed table.
**Solution**: value-level skew in the bucket key — combine with salting for that hot value.

## 📝 Key Takeaways
1. Bucketing removes shuffle for repeated joins/aggs on a high-cardinality key.
2. Partition (low-card filter col) + bucket (high-card join key) together.
3. Preconditions: same buckets, read as tables, bucketing enabled.
4. It's a one-time write cost that pays off over many reads.
5. Bucketing doesn't fix value-level skew.

## 🔗 Next Steps
- **Day 27**: Dynamic Partition Pruning

## 📚 Additional Resources
- Spark bucketing / `bucketBy` docs

---

**Progress**: Day 26/40 ✅
