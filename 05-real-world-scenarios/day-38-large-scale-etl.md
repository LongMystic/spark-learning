# Day 38: Large-Scale ETL & CDC Patterns

## 🎯 Learning Objectives
- Design robust, idempotent, incremental ETL pipelines
- Implement Change Data Capture (CDC) with MERGE
- Build in data-quality gates and schema handling
- Apply the medallion (bronze/silver/gold) layering

## 📚 Core Concepts

### 1. Incremental, not full-reload
Reprocessing the whole history nightly doesn't scale. Process **only new/changed data** per run, keyed by a watermark column (ingest time, `_loaded_at`, or CDC offset):
```python
last = spark.sql("SELECT COALESCE(MAX(loaded_at), TIMESTAMP '1970-01-01') m FROM silver.txns").first().m
new_rows = bronze.where(F.col("loaded_at") > F.lit(last))
```

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

### 3. Medallion layering
| Layer | Content | Transform |
|-------|---------|-----------|
| **Bronze** | raw, append-only, as-ingested | schema-on-read, minimal |
| **Silver** | cleaned, deduped, conformed, CDC-applied | quality + typing + MERGE |
| **Gold** | business marts / aggregates | joins + aggregations for BI |
Each layer is reproducible from the one below — bronze is the immutable source of truth.

## 🔍 Deep Dive: A resilient pipeline

### Step-by-Step
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

## 💡 Key Insights for On-Premise
### 1. Idempotency is the master rule
Every stage must be safely re-runnable (Days 21, 35): partition + dynamic overwrite, or MERGE. Re-runs/backfills are then trivial and safe — the foundation of reliable on-prem ETL.

### 2. Schema evolution is inevitable
Upstream will add columns. Iceberg schema evolution (Day 33) + `mergeSchema` handling lets you absorb new columns without a full rewrite. Decide a policy for unexpected columns (accept/quarantine/alert).

### 3. Quarantine bad data, don't fail everything
Route rows that fail validation to a `_rejects` table and continue with the good rows when SLA matters — then alert. Blocking the whole pipeline on one bad row can be worse than delivering good data + a rejects report.

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

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Rows in/out per stage; reject counts.
2. Freshness/SLA of gold tables.
3. DQ failure rate over time (upstream degradation).

## 🚨 Common Issues & Solutions

### Issue 1: Reprocessing corrupts the target
**Symptom**: duplicates/loss on re-run.
**Solution**: MERGE or dynamic partition overwrite; never blind-append.

### Issue 2: One bad batch blocks the pipeline nightly
**Symptom**: recurring hard failures.
**Solution**: quarantine + alert instead of hard-fail; fix the upstream cause.

## 📝 Key Takeaways
1. Process incrementally by a watermark, not full reloads.
2. Apply CDC with atomic MERGE; use SCD2 for history.
3. Layer bronze/silver/gold; bronze is immutable truth.
4. Gate on data quality before publishing; quarantine bad rows.
5. Idempotency + schema evolution make pipelines resilient.

## 🔗 Next Steps
- **Day 39**: Architecture, Multi-Tenancy, HA & Security

## 📚 Additional Resources
- Medallion architecture; Iceberg MERGE / schema evolution; CDC patterns

---

**Progress**: Day 38/40 ✅
