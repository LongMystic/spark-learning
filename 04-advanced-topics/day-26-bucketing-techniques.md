# Day 26: Bucketing Techniques

## 🎯 Learning Objectives
- Understand bucketing and precisely how it eliminates shuffle for repeated joins/aggregations
- Create and use bucketed tables correctly, including bucket-count coalescing across mismatched tables
- Know bucketing's constraints and when it pays off vs plain partitioning
- Diagnose why a bucketed join still shuffles, and how Iceberg's bucketing differs from Spark's

## 📚 Core Concepts

### 1. What bucketing does

Bucketing hash-distributes rows into a **fixed number of files** (buckets) by a key, using Spark's internal hash function on the bucket column(s), and can additionally store each bucket **sorted**. Two tables bucketed the **same way** on the join key can be joined **without a shuffle** — the matching buckets are already co-located file-by-file and (if sorted) pre-sorted for the merge step.

```python
(txns.write
    .bucketBy(64, "customer_id")
    .sortBy("customer_id")
    .mode("overwrite")
    .saveAsTable("txns_bucketed"))
```

**Key Points:**
- The bucket a row lands in is `hash(bucket_col) % numBuckets` — deterministic, so the *same* key always lands in the *same* bucket number across any table bucketed the same way.
- `sortBy` is optional but valuable for joins: without it, Spark still skips the shuffle but must still sort each bucket pair at join time; with it, the join is closer to a pure merge.
- Bucketing metadata (bucket count, columns, sort columns) is stored in the **metastore**, not inferred from the files — this is why bucketing requires `saveAsTable`/catalog tables, not raw paths.

### 2. Partitioning vs bucketing

| | Partitioning | Bucketing |
|---|---|---|
| Mechanism | directory per value (`txn_date=.../`) | fixed number of hashed files per partition (or per table) |
| Good for | pruning by a low-cardinality filter column | joins/aggregations on a high-cardinality key |
| Cardinality | low (dates, regions) | high (customer/user ids) |
| Risk | too many tiny partitions (small-file problem) | wrong bucket count, or value-level skew in the key |
| Changeable later? | can add/drop partitions incrementally | bucket count is fixed at write time — changing it requires a full rewrite |

**Key Points:**
- They're **complementary**, not competing: partition by `txn_date` (coarse, filter-friendly) *and* bucket by `customer_id` (fine-grained, join-friendly) within each partition.
- A bucketed table's directory-per-partition still contains exactly `numBuckets` files per partition — bucketing and partitioning stack, they don't replace each other.

### 3. When a bucketed join avoids shuffle

All of the following must hold:
- Both tables bucketed by the **same** column(s), with the **same number of buckets** (or a coalescable ratio — see below).
- The join key **equals** the bucket key.
- Both sides are read as **catalog tables** (`spark.table(name)`), not raw file paths — bucketing metadata only survives through the metastore.
- `spark.sql.sources.bucketing.enabled` is `true` (the default).

**Key Points — bucket count coalescing (Spark 3.1+):**
```python
spark.conf.set("spark.sql.sources.bucketing.coalesceBucketsInJoin.enabled", "true")
spark.conf.set("spark.sql.sources.bucketing.coalesceBucketsInJoin.maxBucketRatio", "4")
```
If one table has 64 buckets and the other has 16 (a clean multiple, within `maxBucketRatio`), Spark can **coalesce** the 64-bucket side's buckets in groups of 4 to line up with the 16-bucket side — still avoiding a full shuffle even though the counts don't match exactly. Outside that ratio, Spark falls back to shuffling the mismatched side.

**Key Points — bucket pruning:**
A filter on the bucket column (e.g. `WHERE customer_id = 12345`) can let Spark compute which single bucket the value would hash into and skip reading the other buckets entirely — conceptually similar to partition pruning, but operating at the file level within a bucketed table.

### 4. Bucketing also eliminates shuffle for aggregations

```python
# groupBy on the bucket column can skip the shuffle too, not just joins
spark.table("txns_bkt").groupBy("customer_id").agg(F.sum("amount")).explain()
# Look for: no Exchange before the aggregate's partial/final HashAggregate.
```

**Key Points:**
- The same co-location property that lets a join skip shuffling also lets a `groupBy`/aggregation on the **bucket column** skip it — each bucket already contains all rows for its hashed keys, so partial aggregation within a bucket followed by a final aggregation needs no cross-executor shuffle.
- This applies to plain `groupBy(bucket_col).agg(...)`, not to aggregations on a *different* column — grouping by a non-bucket column still shuffles normally.

## 🔍 Deep Dive: Verifying no-shuffle

### Step-by-Step Process
1. Write both tables with the same `bucketBy(N, key)` (and ideally the same `sortBy(key)`), then `saveAsTable`.
2. Read both back via `spark.table(...)`, never via `spark.read.parquet(path)`.
3. Join on the bucket key and call `.explain()`.
4. Confirm there is **no** `Exchange` operator directly feeding the join's `SortMergeJoin` (or `ShuffledHashJoin`) — you should see the table scans feeding the join almost directly, with at most a per-bucket sort if `sortBy` wasn't used.

### Example: Practical Example
```python
a = spark.table("txns_bucketed")
b = spark.table("customers_bucketed")   # also bucketBy(64, "customer_id")
a.join(b, "customer_id").explain()
# Look for: NO Exchange feeding the join inputs.
# If you see Exchange hashpartitioning(...), a precondition failed.
```

**Analysis:**
- The absence of `Exchange` is the entire point — it means the (expensive, one-time) shuffle cost was paid **once**, at write time, and every subsequent join reuses that layout for free.
- If `sortBy` was used at write time, the plan may also skip the per-bucket `Sort` node — check for it explicitly; without pre-sorting, `SortMergeJoin` still needs a `Sort` per bucket even though there's no `Exchange`.

### Common shuffle-reintroducers
- Different bucket counts beyond the coalescable ratio → Spark shuffles one (or both) sides to reconcile.
- Reading via `spark.read.parquet(path)` instead of `spark.table(name)` → bucketing metadata is lost entirely; Spark has no way to know the files are pre-bucketed.
- `spark.sql.sources.bucketing.enabled=false` (globally disables the optimization).
- Filtering/transforming the bucket column *before* the join in a way that changes its value (e.g. `withColumn("customer_id", col("customer_id") + 0)` can sometimes defeat the optimizer's ability to prove the bucket key is unchanged, depending on the expression).
- A `UNION` or repartition inserted between the table read and the join, which invalidates the known bucketing.

## 💡 Key Insights for On-Premise

### 1. Bucketing is a write-time investment
It costs a full shuffle **once**, at write time. It pays off when the table is joined/aggregated on that key **many times** afterward — e.g. a dimension or fact table joined by every downstream ETL job, dashboard query, or DBT model that runs daily. For a one-off ad-hoc join, broadcast (Day 25) or a plain sort-merge join is simpler and doesn't require a dedicated write step.

### 2. Choose bucket count with future data growth in mind
Aim for individual bucket files of roughly ~100-200MB, matched to your typical HDFS-block-equivalent / object-storage part-size target. Too few buckets → giant files, memory pressure, and spill during the per-bucket sort; too many → the small-file problem that hurts MinIO/S3 LIST/GET throughput just as much as it hurts HDFS. Because the bucket count **cannot** be changed without rewriting the whole table, size it for the table's expected growth over its lifetime, not its current size.

### 3. Skewed bucket keys still skew
Hashing spreads *distinct* values roughly evenly across buckets, but a single hot value (e.g. a "walk-in customer" placeholder id used by millions of rows) still lands entirely in **one** bucket — that bucket becomes huge regardless of bucket count. Bucketing does not fix value-level skew (Day 10); combine with salting for the specific hot value if it's a known, persistent skew source.

### 4. Iceberg tables bucket differently from Spark's `bucketBy`
On Hive tables, `bucketBy`/`sortBy` metadata lives in the Hive metastore and Spark's classic bucketed-join optimization (described above) applies directly. **Iceberg tables use a different mechanism**: bucketing is expressed as a *hidden partition transform* in the table's partition spec (`PARTITIONED BY (bucket(N, customer_id))`), which drives file layout and partition-level pruning, but it is **not** the same code path as Spark's `bucketBy`-based shuffle-free join elimination — don't assume an Iceberg bucket-partitioned table automatically gets the exact same no-shuffle join guarantee as a Hive `bucketBy` table without verifying the plan.

## 🎯 Practical Exercises

### Exercise 1: Bucketed join (see `exercises/advanced/exercise-26-bucketing.py`)
```python
N_BUCKETS = 16
for name, df in [("txns_bkt", txns.select("customer_id", "amount")),
                 ("cust_bkt", customers.select("customer_id", "segment"))]:
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    (df.write.mode("overwrite")
       .bucketBy(N_BUCKETS, "customer_id")
       .sortBy("customer_id")
       .saveAsTable(name))

a = spark.table("txns_bkt")
b = spark.table("cust_bkt")
a.join(b, "customer_id").explain()   # expect NO Exchange on the join inputs
```

### Exercise 2: Break it
```python
# Mismatched bucket counts reintroduce a shuffle
(customers.select("customer_id", "segment").write.mode("overwrite")
    .bucketBy(8, "customer_id").sortBy("customer_id").saveAsTable("cust_bkt8"))
a.join(spark.table("cust_bkt8"), "customer_id").explain()
# Note the Exchange that appears due to the 16-vs-8 mismatch (test coalesceBucketsInJoin
# with a ratio of 2 to see it avoided instead).
```

### Exercise 3: Bucketed aggregation and bucket pruning
```python
# 1. groupBy on the bucket column should show no Exchange:
spark.table("txns_bkt").groupBy("customer_id").agg(F.sum("amount")).explain()

# 2. Filter on a single bucket-column value and check bytes/files read
# in the SQL tab vs an unfiltered scan of the same table:
spark.table("txns_bkt").where("customer_id = 12345").explain()
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Presence/absence of `Exchange`** feeding the join's `SortMergeJoin`/`ShuffledHashJoin` inputs.
2. **Bucket file size distribution** — evenness across buckets (a proxy for hash-level skew) and absolute size (too small = small-file problem, too large = spill risk).
3. **Write-time shuffle cost** for the initial bucketed write, weighed against the cumulative shuffle cost it saves across all downstream reads.
4. **Bucket-pruning effectiveness** — bytes/files read when filtering on the bucket column vs a full table scan.

### Spark UI Analysis
- **SQL tab**: a bucketed join shows `SortMergeJoin` (or `ShuffledHashJoin`) with **no** child `Exchange` node — this is the single clearest signal bucketing is working.
- **Stages tab**: for the initial bucketed write, expect one shuffle stage; for every subsequent join, expect **zero** shuffle stages on the bucketed tables' side.

## 🚨 Common Issues & Solutions

### Issue 1: Bucketed join still shuffles
**Symptom**: `Exchange` present in the join's physical plan despite both tables being bucketed.
**Root Cause**: Mismatched bucket counts beyond the coalescable ratio, reading via path instead of `spark.table()`, `spark.sql.sources.bucketing.enabled=false`, or a transform/union between the read and the join that erased the bucketing metadata.
**Solution**: Verify all four preconditions explicitly — same bucket count (or within `coalesceBucketsInJoin.maxBucketRatio`), catalog-table reads, bucketing enabled, and a direct read-to-join path with no intervening repartition/union.

### Issue 2: One bucket is huge
**Symptom**: A single straggler task on an otherwise well-bucketed table.
**Root Cause**: Value-level skew in the bucket key — one hot value dominates a bucket regardless of hash distribution.
**Solution**: Combine bucketing with salting for that specific hot value (Day 10/23), or bucket on a composite key that spreads the hot value's rows across more buckets.

### Issue 3: Bucket count chosen too small or too large
**Symptom**: Either very large per-bucket files causing spill during sorts, or thousands of tiny bucket files hurting MinIO/S3 LIST/GET throughput.
**Root Cause**: Bucket count was sized for the table's size at creation time without accounting for growth, and it cannot be changed without a full rewrite.
**Solution**: Re-bucket (full table rewrite with a new `bucketBy(N, ...)`) sized to the table's *projected* size, targeting ~100-200MB per bucket file; treat this as a planned maintenance operation, not a quick config change.

### Issue 4: Bucketing silently has no effect on a dynamic-overwrite write
**Symptom**: Expected bucket layout doesn't materialize as expected after a partitioned + bucketed write with dynamic partition overwrite mode.
**Root Cause**: Interactions between `spark.sql.sources.partitionOverwriteMode=dynamic` and bucketed writes can behave unexpectedly depending on Spark version and writer path — always verify the actual written files' bucket layout, don't assume it from the write call alone.
**Solution**: After any bucketed write, validate with `DESCRIBE EXTENDED table_name` (check the "Num Buckets"/"Bucket Columns" fields) and a follow-up `explain()` on a join, rather than trusting the write code in isolation.

### Issue 5: Iceberg "bucket" partition transform doesn't behave like Spark `bucketBy`
**Symptom**: A join on an Iceberg table partitioned with `bucket(N, col)` still shows an `Exchange`, contrary to the Hive-bucketing mental model.
**Root Cause**: Iceberg's bucket transform is a partitioning/pruning mechanism, not the same shuffle-elimination optimization Spark applies to Hive `bucketBy` tables.
**Solution**: Don't assume feature parity between the two — verify with `explain()` on the actual table type in use, and treat Iceberg's bucket transform primarily as a file-pruning tool rather than a join-shuffle eliminator.

### Issue 6: Bucketed scan optimization skipped even though the table is bucketed
**Symptom**: `DESCRIBE EXTENDED` confirms bucketing metadata exists, but a query that should benefit shows no bucketing-related plan difference.
**Root Cause**: Since Spark 3.2, `spark.sql.sources.bucketing.autoBucketedScan.enabled` (default `true`) lets Spark **disable** the bucketed read path automatically when the query doesn't actually need bucket-ordered output (e.g. a plain full-table scan with no join/aggregation on the bucket column) — this is a deliberate optimization, not a bug, since maintaining bucket order has its own cost.
**Solution**: Confirm the query genuinely joins or aggregates on the bucket column before expecting bucketed-scan behavior; for queries that don't, the auto-disable is correct and desired.

## 📝 Key Takeaways
1. Bucketing removes shuffle for repeated joins/aggregations on a high-cardinality key by co-locating matching hash buckets at write time.
2. Partition (low-cardinality filter column) + bucket (high-cardinality join key) together — they're complementary, not alternatives.
3. Preconditions for a shuffle-free bucketed join: same bucket count (or coalescable ratio), catalog-table reads, and bucketing enabled.
4. It's a one-time write cost that pays off over many reads — size bucket count for future growth since it can't change without a rewrite.
5. Bucketing doesn't fix value-level skew; combine with salting for known hot keys.
6. Iceberg's bucket partition transform is a different mechanism from Spark's `bucketBy` — verify, don't assume, join-shuffle elimination on Iceberg tables.

## 🔗 Next Steps
- **Day 27**: Dynamic Partition Pruning

## 📚 Additional Resources
- Spark bucketing / `bucketBy` and `sortBy` docs
- `spark.sql.sources.bucketing.coalesceBucketsInJoin.*` configuration reference
- Apache Iceberg partition transforms documentation (`bucket`, `truncate`, `identity`)

---

**Progress**: Day 26/40 ✅
