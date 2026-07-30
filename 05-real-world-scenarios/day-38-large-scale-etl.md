# Day 38: Large-Scale ETL & CDC Patterns

## 🎯 Learning Objectives
- Design robust, idempotent, incremental ETL pipelines
- Implement Change Data Capture (CDC) with MERGE
- Build in data-quality gates and schema handling
- Apply the medallion (bronze/silver/gold) layering
- Handle late-arriving data, schema drift, and SCD2 history correctly
- Recognize when to quarantine bad data instead of failing the whole pipeline

## 📚 Core Concepts

### 1. Incremental, not full-reload

Reprocessing the whole history nightly doesn't scale. Process **only new/changed data** per run, keyed by a watermark column (ingest time, `_loaded_at`, or CDC offset):
```python
last = spark.sql("SELECT COALESCE(MAX(loaded_at), TIMESTAMP '1970-01-01') m FROM silver.txns").first().m
new_rows = bronze.where(F.col("loaded_at") > F.lit(last))
```

**Key Points:**
- The watermark column must be **monotonic and reliable** — ingestion time (`_loaded_at`) is usually safer than a business event time (`event_ts`), because upstream systems can emit late or out-of-order business timestamps but your own ingestion pipeline controls `_loaded_at`.
- Incremental processing turns an O(all history) nightly job into an O(new data since last run) job — the same principle that makes partition pruning (Day 5) and dynamic partition overwrite (Day 21) effective, applied at the pipeline level instead of the query level.
- Track the watermark explicitly (e.g. `MAX(loaded_at)` from the target table, or a separate control table) rather than trusting "yesterday's date" — a paused pipeline that misses two days needs to catch up on exactly what it missed, not just "yesterday."

### 2. CDC with MERGE

Source systems emit inserts/updates/deletes. Land them, then upsert into the target atomically (Iceberg, Day 34):
```sql
MERGE INTO silver.customers t
USING bronze.customer_changes s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET *
WHEN NOT MATCHED AND s.op != 'D' THEN INSERT *;
```
For history, use **SCD Type 2** (close the old row, insert a new versioned row) instead of overwriting.

**Key Points:**
- CDC feeds typically arrive as a stream of row-level operations (`I`/`U`/`D`) from a source database's transaction log, a message queue, or a batch export with an `op` column — the MERGE pattern above works whether that stream lands via streaming (Day 30-31) or micro-batch.
- MERGE is **atomic** at the Iceberg snapshot level: a MERGE either commits its full set of inserts/updates/deletes or none of them — no reader ever sees a half-applied upsert (Day 34).
- Ordering matters within a MERGE batch: if the same key has both an update and a later delete in the same micro-batch, dedupe to the *latest* operation per key before the MERGE, or the `WHEN MATCHED` clauses can apply out of order.

**Example: SCD Type 2**
```sql
-- Close out the current row for any changed customer, then insert the new version.
MERGE INTO silver.dim_customer t
USING bronze.customer_changes s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND s.op = 'U' THEN
  UPDATE SET t.is_current = false, t.valid_to = s.change_ts
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, valid_from, valid_to, is_current)
  VALUES (s.customer_id, s.name, s.email, s.change_ts, NULL, true);
-- A second pass (or a UNION in the same statement) inserts the new current row
-- for every customer_id that was just closed out above.
```

### 3. Medallion layering

| Layer | Content | Transform |
|-------|---------|-----------|
| **Bronze** | raw, append-only, as-ingested | schema-on-read, minimal |
| **Silver** | cleaned, deduped, conformed, CDC-applied | quality + typing + MERGE |
| **Gold** | business marts / aggregates | joins + aggregations for BI |

Each layer is reproducible from the one below — bronze is the immutable source of truth.

**Key Points:**
- Bronze should never be mutated or deleted — if a bug corrupts silver or gold, you rebuild from bronze; this is why bronze is append-only and typically partitioned by *ingestion* date, not business date.
- Silver is where CDC/MERGE, deduplication, type casting, and column renames happen — the layer where "raw junk" becomes "trustworthy rows," but still at roughly the source's grain.
- Gold is where joins across silver tables and business-level aggregation happen — this is what feeds dbt marts (Day 36) and Superset dashboards (Day 37); gold tables are the ones BI should read, never bronze or raw silver.

### 4. Delivery semantics: at-least-once vs exactly-once

**Key Points:**
- Most upstream sources (message queues, retried batch exports, CDC connectors) provide **at-least-once** delivery — a row can arrive twice after a network blip or a consumer restart. The pipeline, not the source, is what makes the end result exactly-once.
- Two independent mechanisms combine to get **effectively-once** results: a **replayable source** (offsets/CDC log positions you can re-read) and an **idempotent sink** (MERGE keyed by a natural/business key, or partition overwrite) — the same two conditions Structured Streaming needs for end-to-end exactly-once (Day 30-31).
- Deduplication by natural key at the silver layer (`ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY _loaded_at DESC)` keeping rank 1) is the standard defense when a source can't guarantee delivery semantics at all.

**Example:**
```python
from pyspark.sql import Window
w = Window.partitionBy("txn_id").orderBy(F.col("_loaded_at").desc())
deduped = (new_rows
    .withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .drop("rn"))
# Now safe to MERGE: even if the source delivered txn_id=123 twice, only the latest survives.
```

## 🔍 Deep Dive: A resilient pipeline

### Step-by-Step Process

1. **Ingest → bronze** (append, partition by load date; never mutate).
2. **Quality gate**: validate row counts, null rates, referential integrity, schema. **Fail fast** before polluting silver.
3. **Clean/dedup → silver** via MERGE/upsert (idempotent).
4. **Aggregate → gold** marts for BI (Day 37).
5. **Maintain** (compaction/expiry, Day 34) and **catalog stats** (`ANALYZE`, Day 28).

### Data-quality gate example
```python
issues = []
if new_rows.where("customer_id IS NULL").count() > 0: issues.append("null customer_id")
if new_rows.count() == 0: issues.append("empty batch (upstream problem?)")
dupe = new_rows.groupBy("txn_id").count().where("count > 1").count()
if dupe: issues.append(f"{dupe} duplicate txn_id")
assert not issues, f"DQ failed: {issues}"     # fails the Airflow task, blocks publish
```

### Example: Handling late-arriving data and schema drift

```python
# Late-arriving data: a "yesterday" partition can still receive rows today.
# Reprocess a trailing window, not just the newest partition, and let MERGE
# make the reprocessing idempotent instead of relying on append-only writes.
watermark = spark.sql("SELECT MAX(loaded_at) m FROM silver.txns").first().m
late_window = bronze.where(F.col("event_date") >= F.date_sub(F.lit(watermark), 3))  # 3-day grace window

# Schema drift: absorb new upstream columns without a full rewrite (Day 33 schema evolution).
spark.sql("ALTER TABLE silver.txns ADD COLUMNS (promo_code STRING)")
cleaned = late_window.withColumn(
    "promo_code",
    F.col("promo_code") if "promo_code" in late_window.columns else F.lit(None).cast("string")
)
```

**Analysis:**
- Reprocessing a trailing window (not just "today") is how CDC/MERGE pipelines correctly absorb late-arriving facts without a full historical reload — the MERGE's `unique_key` match makes reprocessing the same rows twice a no-op, so widening the window costs a little extra compute but never corrupts the target.
- Handling schema drift explicitly (add the column, backfill `NULL` for old rows reading it) avoids the two bad outcomes: a hard failure on an unexpected column, or silently dropping new data because the write path has a stale, hardcoded column list.

### Example: The full bronze -> silver -> gold flow, end to end

```python
# 1. Bronze: append-only, partitioned by ingestion date, never mutated.
bronze_batch.write.mode("append").partitionBy("_ingest_date").saveAsTable("bronze.transactions")

# 2. Quality gate BEFORE touching silver.
issues = []
if bronze_batch.where("customer_id IS NULL").count() > 0: issues.append("null customer_id")
dupe = bronze_batch.groupBy("txn_id").count().where("count > 1").count()
if dupe: issues.append(f"{dupe} duplicate txn_id in this batch")

good = bronze_batch
if issues:
    # quarantine instead of hard-failing when only a fraction of rows are bad
    bad_ids = bronze_batch.groupBy("txn_id").count().where("count > 1").select("txn_id")
    good = bronze_batch.join(bad_ids, "txn_id", "left_anti")
    bronze_batch.join(bad_ids, "txn_id").write.mode("append").saveAsTable("bronze._rejects")
    print(f"quarantined rows due to: {issues}")

# 3. Silver: dedupe by key, then idempotent MERGE (trailing grace window absorbs late data).
w = Window.partitionBy("txn_id").orderBy(F.col("_loaded_at").desc())
deduped = good.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
deduped.createOrReplaceTempView("silver_changes")
spark.sql("""
    MERGE INTO silver.transactions t USING silver_changes s
    ON t.txn_id = s.txn_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# 4. Gold: aggregate silver into BI-ready marts (Day 37), then maintain (Day 34).
spark.sql("""
    MERGE INTO gold.fct_daily_sales t USING (
      SELECT txn_date, category, SUM(amount) AS total FROM silver.transactions
      GROUP BY txn_date, category
    ) s ON t.txn_date = s.txn_date AND t.category = s.category
    WHEN MATCHED THEN UPDATE SET t.total = s.total
    WHEN NOT MATCHED THEN INSERT *
""")
```
This is the same five-step process from the Deep Dive above, written as one script — in production each numbered step is its own Airflow task (Day 35) so a failure at any step is isolated and retryable independently.

## 💡 Key Insights for On-Premise

### 1. Idempotency is the master rule
Every stage must be safely re-runnable (Days 21, 35): partition + dynamic overwrite, or MERGE. Re-runs/backfills are then trivial and safe — the foundation of reliable on-prem ETL.

### 2. Schema evolution is inevitable
Upstream will add columns. Iceberg schema evolution (Day 33) + `mergeSchema` handling lets you absorb new columns without a full rewrite. Decide a policy for unexpected columns (accept/quarantine/alert) and write it down — "we silently accept new columns into bronze, but silver requires an explicit schema change" is a reasonable default.

### 3. Quarantine bad data, don't fail everything
Route rows that fail validation to a `_rejects` table and continue with the good rows when SLA matters — then alert. Blocking the whole pipeline on one bad row can be worse than delivering good data + a rejects report. Make the `_rejects` table queryable (same catalog, same access controls) so an on-call engineer can triage it without spelunking through logs.

### 4. Late data needs a grace window, not just "today's partition"
A pipeline that only ever processes "today's" partition will silently drop records that arrive a day or two late (common with distributed source systems and retried CDC deliveries). Reprocess a trailing window and rely on idempotent MERGE/overwrite to make the extra work safe rather than risky.

### 5. Bronze is cheap; keep it, even after silver is clean
Storage on-prem is shared but usually cheaper than re-extracting from a source system that has since rotated out old data. Retain bronze for a defined window (tied to compliance/audit needs) so a silver-layer bug is a re-run, not a data-loss incident.

## 🎯 Practical Exercises

### Exercise 1: Incremental + MERGE + DQ
```python
# See exercises/production/exercise-38-etl-cdc.py
# Build bronze->silver with an incremental watermark, a DQ gate, and an upsert.
# (MERGE path needs ENABLE_ICEBERG=1; a parquet dynamic-overwrite fallback is included.)
```

### Exercise 2: SCD2 sketch
```python
# Implement close-old-row + insert-new-version logic for a slowly-changing dimension.
```

### Exercise 3: Late data + quarantine
```python
# Extend exercise 38:
#   1. Widen the incremental window to re-include a 2-day grace period and confirm
#      the MERGE/overwrite still produces correct (not duplicated) results.
#   2. Instead of asserting on DQ failure, split failing rows into a `_rejects`
#      DataFrame, write both `_rejects` and the good rows, and print a summary
#      of how many rows were quarantined and why.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Rows in/out per stage; reject counts** — a sudden jump in rejects is often an upstream schema or data-quality regression, not a bug in this pipeline.
2. **Freshness/SLA of gold tables** — how stale is the newest row an analyst can see, versus the freshness SLA promised to BI consumers (Day 37)?
3. **DQ failure rate over time** (upstream degradation) — trend this, don't just alert on the current run (ties to the leading-indicator philosophy in Day 40).
4. **Watermark lag** — the gap between "now" and the latest watermark processed; a growing lag means the pipeline is falling behind its source.
5. **Late-arrival rate** — what fraction of rows in a given run belong to a partition older than the grace window assumes? Rising late-arrival rates mean the grace window may need widening.

### Spark UI Analysis
- Check the **SQL tab** for the MERGE statement's plan: confirm it's using a broadcast join against the change set if the change set is small (Day 25), and not an expensive shuffle join against the entire target table.
- Watch the **Stages tab** for the quality-gate job — DQ checks that do multiple full-table `count()`s can be as expensive as the transform itself; consider combining checks into a single aggregation pass instead of several separate actions.

## 🚨 Common Issues & Solutions

### Issue 1: Reprocessing corrupts the target
**Symptom**: duplicates/loss on re-run.
**Root Cause**: the silver/gold write uses blind `append` instead of an idempotent overwrite/merge, so any retry or backfill adds a second copy (or, with a naive overwrite, wipes data outside the intended window).
**Solution**: MERGE or dynamic partition overwrite; never blind-append.

### Issue 2: One bad batch blocks the pipeline nightly
**Symptom**: recurring hard failures at the DQ gate.
**Root Cause**: the DQ gate hard-fails the entire batch on any violation, even when only a small fraction of rows are actually bad.
**Solution**: quarantine + alert instead of hard-fail; fix the upstream cause. Reserve hard-fail for violations severe enough that publishing *any* of the batch would be worse than publishing none (e.g. a broken join key, an empty extract).

### Issue 3: Late-arriving data silently goes missing
**Symptom**: a customer's transaction from two days ago never appears, even though the source confirms it exists.
**Root Cause**: the incremental watermark logic only looks at "new since last run" with too narrow a window, so a delayed upstream write lands in a partition the pipeline already considers "done."
**Solution**: process a trailing grace window (e.g. last 3 days) on every run and rely on idempotent MERGE to absorb the overlap safely.

### Issue 4: Schema drift breaks the nightly job
**Symptom**: a `spark-submit` fails with a column-mismatch or `AnalysisException` right after an upstream release.
**Root Cause**: the source added/renamed/removed a column and the pipeline's write path (or a hardcoded `SELECT` column list) doesn't know how to handle it.
**Solution**: use Iceberg schema evolution (`ALTER TABLE ... ADD COLUMNS`, Day 33) and write the transform to tolerate new columns (default to `NULL` when absent) instead of hardcoding a brittle schema; decide and document a policy for unexpected columns.

### Issue 5: SCD2 dimension grows unbounded / joins get slower over time
**Symptom**: a "current row" dimension join gets progressively slower as history accumulates.
**Root Cause**: every historical version of every row lives in one table, and downstream joins forget to filter `is_current = true`, so they scan the whole history instead of just the current snapshot.
**Solution**: partition/cluster the SCD2 table so `is_current = true` prunes efficiently (or maintain a separate "current" view/table alongside the full history), and enforce the `is_current` filter in shared views so downstream consumers can't forget it.

### Issue 6: Duplicate rows survive even with a MERGE
**Symptom**: the target table has more than one row per business key despite using `MERGE INTO ... ON t.txn_id = s.txn_id`.
**Root Cause**: the *source side* of the MERGE (the incoming batch) itself contains duplicate `txn_id`s — e.g. an at-least-once delivery redelivered the same event within one micro-batch — and `MERGE` only guarantees one match per **existing target** row, not that the source side is pre-deduplicated.
**Solution**: dedupe the incoming batch by key (see the `ROW_NUMBER()` pattern above) *before* the MERGE — Spark's MERGE will raise an error or produce undefined behavior if the source has multiple matches for the same target row in some engines, so this is also a correctness requirement, not just a cleanliness one.

## 📝 Key Takeaways
1. Process incrementally by a watermark, not full reloads.
2. Apply CDC with atomic MERGE; use SCD2 for history.
3. Layer bronze/silver/gold; bronze is immutable truth.
4. Gate on data quality before publishing; quarantine bad rows instead of hard-failing everything.
5. Idempotency + schema evolution make pipelines resilient to retries and upstream change.
6. Reprocess a trailing grace window to correctly absorb late-arriving data.
7. Keep bronze around — it's what makes a silver/gold bug a re-run instead of a data-loss incident.
8. At-least-once sources + idempotent sinks = effectively-once results; dedupe by key before merging, not just after.

## 🔗 Next Steps
- **Day 39**: Architecture, Multi-Tenancy, HA & Security

## 📚 Additional Resources
- Medallion architecture (bronze/silver/gold layering)
- Iceberg MERGE / schema evolution docs (Day 33-34)
- CDC patterns (log-based CDC, SCD Type 2 dimensional modeling)

---

**Progress**: Day 38/40 ✅
