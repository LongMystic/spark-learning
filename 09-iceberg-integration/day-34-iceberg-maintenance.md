# Day 34: Iceberg Maintenance — Compaction, Snapshots, Time Travel, MERGE

## 🎯 Learning Objectives
- Keep Iceberg tables healthy: compaction, snapshot expiry, orphan cleanup
- Use time travel and rollback for recovery and audits
- Implement upserts/CDC with `MERGE INTO`
- Schedule maintenance so tables don't degrade over time

## 📚 Core Concepts

### 1. Why maintenance is required
Streaming/CDC and frequent writes create **many small files** and **many snapshots**. Left alone, planning slows and storage bloats. Iceberg ships maintenance **procedures** you run periodically.

### 2. Compaction (rewrite small files)
```sql
CALL local.system.rewrite_data_files(
  table => 'db.transactions',
  options => map('target-file-size-bytes','134217728'));   -- ~128MB targets
```
Combines many small files into fewer right-sized ones — the direct fix for the small-file problem.

### 3. Expire snapshots & remove orphans
```sql
-- keep last 7 days of snapshots; drop older metadata + unreferenced data files
CALL local.system.expire_snapshots('db.transactions', TIMESTAMP '2026-07-15 00:00:00');
-- delete files no snapshot references (e.g. from failed writes)
CALL local.system.remove_orphan_files(table => 'db.transactions');
-- compact metadata manifests
CALL local.system.rewrite_manifests('db.transactions');
```

### 4. Time travel & rollback
```sql
SELECT * FROM db.transactions VERSION AS OF 3821550127947089009;   -- by snapshot id
SELECT * FROM db.transactions TIMESTAMP AS OF '2026-07-20 09:00:00';
CALL local.system.rollback_to_snapshot('db.transactions', 3821550127947089009);  -- undo a bad write
```
Rollback makes "we corrupted the table with a bad job" recoverable in seconds — a production superpower.

### 5. MERGE (upsert / CDC)
```sql
MERGE INTO db.transactions t
USING staging_changes s
ON t.txn_id = s.txn_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
Atomic upserts enable CDC pipelines (Day 38) and idempotent streaming sinks (Day 31) — no more read-modify-overwrite races.

## 🔍 Deep Dive: A maintenance schedule
| Task | Frequency | Procedure |
|------|-----------|-----------|
| Compact data files | daily (or after heavy writes) | `rewrite_data_files` |
| Expire snapshots | daily | `expire_snapshots` (retain N days) |
| Remove orphans | weekly | `remove_orphan_files` |
| Rewrite manifests | weekly | `rewrite_manifests` |
Run these from a scheduled Spark job (Airflow, Day 35). Keep enough snapshot history for your time-travel/audit needs, but not so much that metadata bloats.

## 💡 Key Insights for On-Premise
### 1. Compaction competes for cluster resources
Schedule it in a low-traffic window and/or a dedicated namespace (low-priority quota) — it's a full read+rewrite. Copy-on-write vs merge-on-read modes trade write cost vs read cost; pick per table access pattern.

### 2. Expiry is what actually frees storage
`expire_snapshots` (not just `DELETE`) is what lets old data files be removed. Without it, time travel keeps every version forever and the object store / bucket fills up.

## 🎯 Practical Exercises

### Exercise 1: Small files → compaction
```python
# See exercises/production/exercise-34-iceberg-maintenance.py  (ENABLE_ICEBERG=1)
# Write many small files, count them, run rewrite_data_files, count again.
```

### Exercise 2: MERGE upsert + time travel
```python
# Upsert a batch of changes with MERGE; then query a previous snapshot and rollback.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Data files per partition before/after compaction.
2. Snapshot count and metadata size over time.
3. Storage footprint (does expiry actually reclaim it?).

### Spark UI Analysis
- Files read at scan drops sharply after compaction (fewer, larger files).

## 🚨 Common Issues & Solutions

### Issue 1: Storage keeps growing despite deletes
**Symptom**: the object store / bucket fills up.
**Solution**: old snapshots pin old files — run `expire_snapshots` + `remove_orphan_files`.

### Issue 2: MERGE is slow
**Symptom**: expensive upserts.
**Solution**: partition/sort the target on the merge key, right-size files (compaction), consider merge-on-read for write-heavy tables.

## 📝 Key Takeaways
1. Compaction fixes small files; schedule it regularly.
2. `expire_snapshots` + `remove_orphan_files` are what reclaim storage.
3. Time travel + rollback make bad writes recoverable instantly.
4. `MERGE INTO` gives atomic upserts for CDC and idempotent sinks.
5. Automate maintenance (Airflow) in a low-traffic window/queue.

## 🔗 Next Steps
- **Day 35**: Airflow Orchestration Integration

## 📚 Additional Resources
- Iceberg maintenance procedures; MERGE INTO; time travel docs

---

**Progress**: Day 34/40 ✅
