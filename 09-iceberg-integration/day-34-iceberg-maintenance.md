# Day 34: Iceberg Maintenance — Compaction, Snapshots, Time Travel, MERGE

## 🎯 Learning Objectives
- Keep Iceberg tables healthy: compaction, snapshot expiry, orphan cleanup, manifest rewriting
- Use time travel and rollback for recovery and audits
- Implement upserts/CDC with `MERGE INTO`
- Understand copy-on-write vs merge-on-read trade-offs
- Schedule maintenance so tables don't degrade over time

## 📚 Core Concepts

### 1. Why maintenance is required

**Key Points:**
- Streaming/CDC and frequent writes create **many small files** and **many snapshots** — each `foreachBatch` MERGE (Day 31) or micro-batch append is its own commit.
- Left alone, planning slows (more manifests to read) and storage bloats (old snapshots keep old data files alive, even ones logically "deleted").
- Iceberg ships maintenance as **stored procedures** (`CALL local.system.<procedure>(...)`) you run periodically — there is no automatic background compaction process; it's on you (or your scheduler) to invoke them.
- These procedures are themselves regular Spark jobs — they consume executor resources like any other job and should be planned for accordingly.

**Example:**
```sql
-- Procedures live under the catalog's `system` namespace
CALL local.system.rewrite_data_files(table => 'db.transactions');
```

### 2. Compaction (rewrite small files)

**Key Points:**
- `rewrite_data_files` combines many small files into fewer right-sized ones — the direct fix for the small-file problem, and the Iceberg-native equivalent of Day 13's file-size tuning for plain Parquet tables.
- `target-file-size-bytes` controls the target output size (commonly ~128MB-1GB depending on workload) — too small and you haven't fixed the problem, too large and single-file scans become coarse-grained.
- Compaction can operate on the whole table or be scoped with a `where` filter to just the recently-written partitions, which is usually cheaper than recompacting historical data that's already well-compacted.
- Compaction itself creates a **new snapshot** — it doesn't retroactively rewrite history, it adds a commit that replaces the old (small) files with new (larger) ones going forward.

**Example:**
```sql
CALL local.system.rewrite_data_files(
  table => 'db.transactions',
  options => map('target-file-size-bytes','134217728'));   -- ~128MB targets

-- Scope compaction to recent data only (cheaper, common in production)
CALL local.system.rewrite_data_files(
  table => 'db.transactions',
  where => 'txn_ts >= current_date() - interval 2 days',
  options => map('target-file-size-bytes','134217728'));
```

### 3. Expire snapshots & remove orphans

**Key Points:**
- `expire_snapshots` removes snapshot metadata **and** the data files that were only referenced by expired snapshots — this is the procedure that actually frees storage, not `DELETE`/`overwrite` alone (those just create new snapshots; old files stay until expired).
- `remove_orphan_files` cleans up files in the table's data directory that **no snapshot references at all** — typically left behind by failed/aborted writes that wrote files but never committed a snapshot pointing at them.
- `rewrite_manifests` compacts the manifest files themselves (as opposed to data files) — useful when many small commits have produced many small manifests, which slows planning independent of data-file count.
- Order matters operationally: expire snapshots first (frees the "logical" references), then remove orphans (cleans up anything left physically un-referenced), then rewrite manifests (tidies what remains).

**Example:**
```sql
-- keep last 7 days of snapshots; drop older metadata + unreferenced data files
CALL local.system.expire_snapshots('db.transactions', TIMESTAMP '2026-07-15 00:00:00');
-- delete files no snapshot references (e.g. from failed writes)
CALL local.system.remove_orphan_files(table => 'db.transactions');
-- compact metadata manifests
CALL local.system.rewrite_manifests('db.transactions');
```

### 4. Time travel & rollback

**Key Points:**
- `VERSION AS OF <snapshot_id>` and `TIMESTAMP AS OF '<timestamp>'` query the table **as it existed** at that snapshot/time — a read-only historical view, useful for audits, debugging, and reproducing a report.
- `rollback_to_snapshot` actually **changes the table's current pointer** back to an earlier snapshot — a write operation (it creates a new entry in `.history`), not just a read.
- Rollback is only safe if you haven't already expired the snapshot you want to roll back to — this is a direct reason to be conservative with `expire_snapshots` retention on tables you might need to recover.
- Time travel works because old snapshots' manifests still point at valid data files until those files are expired — it's a natural consequence of Iceberg's immutable, append-only metadata model, not a separate backup system.

**Example:**
```sql
SELECT * FROM db.transactions VERSION AS OF 3821550127947089009;   -- by snapshot id
SELECT * FROM db.transactions TIMESTAMP AS OF '2026-07-20 09:00:00';
CALL local.system.rollback_to_snapshot('db.transactions', 3821550127947089009);  -- undo a bad write
```
Rollback makes "we corrupted the table with a bad job" recoverable in
seconds — a production superpower, provided the target snapshot hasn't
already been expired.

### 5. MERGE (upsert / CDC)

**Key Points:**
- `MERGE INTO` gives atomic, conditional upserts against the target table — matched rows can be updated or deleted, unmatched rows inserted, all in a single commit.
- This directly replaces the classic "read the whole table, apply changes in memory, overwrite the whole table" pattern, which is neither atomic nor safe under concurrent writers.
- `MERGE` is the sink-side building block behind the exactly-once streaming pipeline from Day 31 (`foreachBatch` + `MERGE`) and the CDC pipelines coming in Day 38.
- Table write mode (copy-on-write vs merge-on-read, see Deep Dive) changes how expensive a `MERGE` is, but not its correctness semantics.

**Example:**
```sql
MERGE INTO db.transactions t
USING staging_changes s
ON t.txn_id = s.txn_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
Atomic upserts enable CDC pipelines (Day 38) and idempotent streaming sinks
(Day 31) — no more read-modify-overwrite races.

## 🔍 Deep Dive: A maintenance schedule and copy-on-write vs merge-on-read

### Step-by-Step Process

1. **Decide the table's write mode.** Iceberg tables can be configured for **copy-on-write** (a `MERGE`/`DELETE`/`UPDATE` rewrites the affected data files immediately — more expensive writes, cheapest possible reads) or **merge-on-read** (changes are recorded as delete files applied at read time — cheap writes, readers pay a small extra cost reconciling deletes). This is set per-table via properties like `write.delete.mode`, `write.update.mode`, `write.merge.mode`.
2. **Pick copy-on-write for read-heavy, write-light tables** (e.g. a dimension table updated a few times a day, queried constantly by dashboards) — readers never pay a reconciliation cost.
3. **Pick merge-on-read for write-heavy tables** (e.g. a table receiving a `MERGE` every 10 seconds from a streaming pipeline) — rewriting data files on every micro-batch MERGE would be far too expensive; let compaction batch that cost up later instead.
4. **Schedule compaction to run after merge-on-read writes accumulate delete files**, since compaction is what folds those deletes back into base data files and keeps read-side reconciliation cost bounded.
5. **Schedule snapshot expiry and orphan removal** on a cadence that matches your time-travel/audit retention needs — daily expiry with a 7-day retention window is a reasonable on-prem default unless compliance requires longer.
6. **Run all of this from a scheduled Spark job** (Airflow, Day 35), not manually — maintenance that depends on someone remembering to run it by hand will eventually get skipped exactly when it matters most.

### Example: a maintenance schedule

| Task | Frequency | Procedure |
|------|-----------|-----------|
| Compact data files | daily (or after heavy writes) | `rewrite_data_files` |
| Expire snapshots | daily | `expire_snapshots` (retain N days) |
| Remove orphans | weekly | `remove_orphan_files` |
| Rewrite manifests | weekly | `rewrite_manifests` |

```sql
-- A write-heavy, streaming-fed table: merge-on-read to keep MERGE cheap per batch
ALTER TABLE local.db.txn_agg SET TBLPROPERTIES (
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
);
```

**Analysis:**
- Run these from a scheduled Spark job (Airflow, Day 35). Keep enough snapshot history for your time-travel/audit needs, but not so much that metadata bloats.
- Merge-on-read defers cost from the streaming write path (which needs to stay fast and predictable, Day 31) to the batch compaction path (which can run in a low-traffic window) — it's a deliberate shift of cost, not a free lunch.
- A table left in merge-on-read mode **without** regular compaction accumulates delete files indefinitely, and every read pays a growing reconciliation cost — the maintenance schedule isn't optional once you've chosen merge-on-read, it's the other half of that trade-off.

## 💡 Key Insights for On-Premise

### 1. Compaction competes for cluster resources
Schedule it in a low-traffic window and/or a dedicated namespace
(low-priority quota) — it's a full read+rewrite. Copy-on-write vs
merge-on-read modes trade write cost vs read cost; pick per table access
pattern, and revisit the choice if a table's access pattern changes (e.g. a
table that used to be batch-loaded daily starts being fed by a streaming
`foreachBatch` MERGE).

### 2. Expiry is what actually frees storage
`expire_snapshots` (not just `DELETE`) is what lets old data files be
removed. Without it, time travel keeps every version forever and the
object store / bucket (MinIO, on-prem, with a fixed disk footprint unlike
elastic cloud storage) fills up — this is a harder failure mode on-premise
than in the cloud, since there's no auto-scaling storage tier to quietly
absorb the bloat.

### 3. Retention is a business decision, not just a technical one
How many days of snapshots to retain before `expire_snapshots` should be
driven by real requirements — audit/compliance windows, how far back
rollback needs to reach, how much storage headroom MinIO actually has — not
a default copy-pasted from documentation.

### 4. Maintenance jobs need their own resource plan
Treat compaction/expiry/manifest-rewrite jobs as first-class scheduled Spark
applications (Day 35's Airflow integration), not an afterthought — give
them their own `SparkApplication` resource requests, their own namespace or
FAIR pool, and their own monitoring, exactly like any production ETL job.

## 🎯 Practical Exercises

### Exercise 1: Small files → compaction
```python
# See exercises/production/exercise-34-iceberg-maintenance.py  (ENABLE_ICEBERG=1)
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("""CREATE TABLE local.db.txn_m (customer_id BIGINT, total DOUBLE) USING iceberg""")

# 1. Create many small files (write in small batches)
for i in range(6):
    (txns.where(F.col("txn_id") % 6 == i)
         .groupBy("customer_id").agg(F.sum("amount").alias("total"))
         .writeTo("local.db.txn_m").append())
files_before = spark.sql("SELECT * FROM local.db.txn_m.files").count()
print("files before compaction:", files_before)

# 2. Compaction (rewrite_data_files)
spark.sql("CALL local.system.rewrite_data_files(table => 'db.txn_m', "
          "options => map('target-file-size-bytes','134217728'))").show(truncate=False)
files_after = spark.sql("SELECT * FROM local.db.txn_m.files").count()
print("files after compaction:", files_after)
```

### Exercise 2: MERGE upsert + time travel
```python
# 3. MERGE upsert
changes = spark.createDataFrame([(0, 999.0), (10 ** 9, 5.0)], "customer_id long, total double")
changes.createOrReplaceTempView("changes")
spark.sql("""
    MERGE INTO local.db.txn_m t USING changes s ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET t.total = t.total + s.total
    WHEN NOT MATCHED THEN INSERT *""")
print("merged. rows:", spark.table("local.db.txn_m").count())

# 4. Time travel + snapshots
spark.sql("SELECT snapshot_id, operation FROM local.db.txn_m.snapshots").show(truncate=False)
# Pick a snapshot_id from above and query it directly, then try rollback_to_snapshot.
```

### Exercise 3: Expire snapshots and confirm storage reclaim
```python
# After Exercise 1-2 have created several snapshots, expire all but the most
# recent, then confirm via .snapshots that old ones are gone and (in a real
# MinIO-backed run) that the underlying object count/size dropped too.
spark.sql("""
    CALL local.system.expire_snapshots(
        table => 'db.txn_m',
        older_than => TIMESTAMP_MILLIS(CAST((current_timestamp() - interval 1 second) AS LONG) * 1000)
    )
""")
spark.sql("SELECT snapshot_id, operation FROM local.db.txn_m.snapshots").show(truncate=False)
```

### Exercise 4: Copy-on-write vs merge-on-read comparison
```python
# Create two tables with the same data and MERGE workload, one copy-on-write
# (the default) and one merge-on-read; compare MERGE duration and file/delete
# counts between the two after several MERGE runs.
spark.sql("""
    CREATE TABLE local.db.txn_cow (customer_id BIGINT, total DOUBLE) USING iceberg
    TBLPROPERTIES ('write.merge.mode'='copy-on-write')
""")
spark.sql("""
    CREATE TABLE local.db.txn_mor (customer_id BIGINT, total DOUBLE) USING iceberg
    TBLPROPERTIES ('write.merge.mode'='merge-on-read')
""")
# Run the same MERGE against both tables several times and time each with `time.time()`.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Data files per partition before/after compaction** — the direct signal that `rewrite_data_files` is working.
2. **Snapshot count and metadata size over time** — should stay roughly flat once expiry is running on schedule, not grow indefinitely.
3. **Storage footprint** — does expiry actually reclaim space in MinIO, or are orphan files/still-referenced snapshots keeping it pinned?
4. **Delete file count** (for merge-on-read tables) — a growing count between compactions is expected; one that never drops after compaction runs means compaction isn't actually folding deletes back in.
5. **Query planning and scan time trends** — the end-to-end signal that maintenance is keeping the table healthy; a slow creep upward despite "maintenance running" warrants checking the other metrics above.

### Spark UI Analysis
- Files read at scan drops sharply after compaction (fewer, larger files) — compare the scan node's file count in `explain()`/SQL tab before and after running `rewrite_data_files`.
- Maintenance procedures (`CALL local.system....`) show up as regular Spark jobs in the Jobs/Stages tabs — a compaction job that's unexpectedly slow can be diagnosed the same way as any other job (check for skew, shuffle spill, etc.).
- For merge-on-read tables, the SQL tab's scan plan for a normal query shows the delete-file reconciliation step — growing time spent there over many commits since the last compaction is the read-side cost of deferred maintenance becoming visible.

## 🚨 Common Issues & Solutions

### Issue 1: Storage keeps growing despite deletes
**Symptom**: The object store / MinIO bucket fills up even though old data is regularly "deleted" or overwritten.
**Root Cause**: Old snapshots pin old files — a `DELETE`/`overwrite`/`MERGE` only creates a new snapshot; the previous snapshot (and the files it references) stays fully intact until explicitly expired.
**Solution**: Run `expire_snapshots` + `remove_orphan_files` on a schedule, with a retention window sized to real time-travel/audit needs.

### Issue 2: MERGE is slow
**Symptom**: Expensive upserts; `MERGE INTO` duration grows over time even though the batch of changes is roughly the same size each run.
**Root Cause**: Target table has too many small files (each `MERGE` must plan against all of them), is in copy-on-write mode with a workload that's actually write-heavy, or lacks sort/partition alignment on the merge key.
**Solution**: Partition/sort the target on the merge key, right-size files (compaction), and consider merge-on-read for write-heavy tables so `MERGE` doesn't pay a full data-file rewrite every run.

### Issue 3: Rollback fails or misses expected data
**Symptom**: `rollback_to_snapshot` errors, or the "restored" table is missing data you expected to see.
**Root Cause**: The target snapshot was already removed by a prior `expire_snapshots` run — you can't roll back to a snapshot that no longer exists.
**Solution**: Size snapshot retention around the realistic window in which you might need to recover from a bad write, and treat `expire_snapshots` retention as a deliberate recovery-window decision, not just a storage-cleanup knob.

### Issue 4: Compaction job competes with production traffic
**Symptom**: Dashboard/streaming query latency spikes while a `rewrite_data_files` job is running.
**Root Cause**: Compaction is a full read-and-rewrite job competing for the same executor pool (and possibly the same table) as regular traffic.
**Solution**: Schedule compaction in a low-traffic window, scope it with a `where` filter to only recently-written partitions, and/or run it in a separate namespace/scheduler pool with a resource quota (Day 29's FAIR scheduler pools apply here too).

### Issue 5: Delete files accumulate and never shrink
**Symptom**: On a merge-on-read table, read latency keeps climbing and `.files`/delete-file counts keep growing even though compaction "runs regularly."
**Root Cause**: The compaction job's `rewrite_data_files` call isn't actually folding delete files back into base data files — often because it's scoped too narrowly (e.g. only touching brand-new partitions) or running with options that don't target delete-file compaction.
**Solution**: Confirm the compaction job's scope covers the partitions actually receiving deletes/updates, and check post-compaction delete-file counts explicitly rather than assuming "a job ran" means "it worked."

## 📝 Key Takeaways
1. Compaction (`rewrite_data_files`) fixes small files; schedule it regularly, scoped to recently-written data where possible.
2. `expire_snapshots` + `remove_orphan_files` are what reclaim storage — deletes/overwrites alone do not.
3. Time travel + rollback make bad writes recoverable instantly, but only within your snapshot retention window.
4. `MERGE INTO` gives atomic upserts for CDC and idempotent streaming sinks — no more read-modify-overwrite races.
5. Copy-on-write vs merge-on-read is a real trade-off: pick based on read-heavy vs write-heavy access patterns, and pair merge-on-read with a real compaction schedule.
6. Automate maintenance (Airflow) in a low-traffic window/queue — it's a scheduled operational job, not a one-time setup step.

## 🔗 Next Steps
- **Day 35**: Airflow Orchestration Integration

## 📚 Additional Resources
- Iceberg maintenance procedures reference (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests`, `rollback_to_snapshot`)
- Iceberg `MERGE INTO` and row-level operations documentation
- Iceberg table properties: copy-on-write vs merge-on-read (`write.delete.mode`, `write.update.mode`, `write.merge.mode`)
- Iceberg time travel and snapshot documentation

---

**Progress**: Day 34/40 ✅
