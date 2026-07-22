# Capstone 2: Iceberg CDC Pipeline

**Goal**: Build a reliable bronze→silver→gold pipeline on Iceberg with idempotent
upserts, compaction, and time travel — the on-prem lakehouse pattern.

## Prerequisites
Iceberg runtime jar + `ENABLE_ICEBERG=1` (see `exercises/production/exercise-33`).

## Steps
1. **Bronze**: land `transactions` into an Iceberg table partitioned by `days(txn_ts)`
   (hidden partitioning). Append in a few batches to create multiple snapshots/files.
2. **Silver (CDC upsert)**: simulate a change feed (some updates + a few deletes,
   with an `op` column) and apply it with `MERGE INTO` (update/delete/insert).
   Prove it's idempotent by running the same MERGE twice.
3. **Gold**: build a `daily_category_sales` mart aggregated from silver.
4. **Maintenance**: run `rewrite_data_files` (compaction), then `expire_snapshots`;
   show file count dropping and storage reclaimed.
5. **Recovery**: use time travel to read a previous snapshot and `rollback_to_snapshot`
   to undo a deliberately-bad write.

## Rubric
- [ ] Bronze uses hidden partitioning; queries filter `txn_ts` directly.
- [ ] MERGE handles insert/update/delete and is provably idempotent.
- [ ] Compaction reduces file count; expiry reclaims storage.
- [ ] Time travel + rollback demonstrated.
- [ ] Write-up explains why MERGE beats read-modify-overwrite for CDC.

## Stretch
- Drive the silver upsert from a Structured Streaming query via `foreachBatch` (Day 31)
  and show exactly-once across a simulated restart.
