# Solutions — Production (Days 29, 32-40)

## exercise-29 (Thrift Server)
1. All query planning + result collection happen in the single shared STS driver.
2. `incrementalCollect=true` streams big results back instead of collecting all to the
   driver → avoids driver OOM on `SELECT *`.
3. Fair pools give each pool a weighted share, so a heavy analytics query can't starve
   dashboard queries.

## exercise-32 (PySpark best practices)
1. `rdd.map` bypasses Catalyst/Tungsten — no codegen, no pushdown, per-row Python round-trips.
2. `active_net_by_category()` is a pure function of a DataFrame (no I/O) → test with a tiny
   in-memory DataFrame, no cluster/files.
3. Ship a matching venv/conda archive via `--archives` and set `spark.pyspark.python`.

## exercise-33 (Iceberg fundamentals)
1. Hidden partitioning derives the partition from `txn_ts` — queries filter the real
   column, no error-prone `dt=` column, and pruning is automatic.
2. One append = one snapshot.
3. Manifest/file-level stats let Iceberg prune files without listing directories.
   (Needs the Iceberg jar + `ENABLE_ICEBERG=1`; otherwise the graceful fallback prints.)

## exercise-34 (Iceberg maintenance)
1. `rewrite_data_files` reduces file count (many small → few ~128MB files).
2. `expire_snapshots` (+ `remove_orphan_files`) actually reclaims storage; compaction alone
   doesn't free old files while snapshots reference them.
3. MERGE is atomic and concurrent-safe; read-modify-overwrite races and can corrupt data.

## exercise-35 (Airflow job)
1. `partitionBy` + dynamic overwrite rewrites only the run-date's partition → retries and
   backfills can't duplicate/corrupt other dates.
2. A `SparkSubmitOperator(application=..., application_args=["--run-date","{{ ds }}"])`.
3. Building DataFrames in the worker competes with the scheduler and doesn't use the cluster.

## exercise-36 (dbt model)
1. Mirrors an `incremental` materialization (staging = a view/model feeding the fact).
2. `merge`/`insert_overwrite` are idempotent; bare `append` duplicates on re-run.
3. dbt `threads` open concurrent connections to the shared STS driver — too many overwhelm it.

## exercise-37 (Superset mart)
1. The mart is orders of magnitude smaller → dashboard queries are fast and cheap, and
   don't scan the raw fact on the shared STS.
2. Partitioning prunes by date; compaction avoids small files → fast reads.
3. Set the Superset cache TTL to match the mart's refresh cadence (e.g. daily).

## exercise-38 (ETL & CDC)
1. Gating before writing prevents polluting silver/gold with bad data (cheaper than
   cleaning up downstream).
2. The incremental watermark + dynamic partition overwrite make it safe to re-run/backfill.
3. Quarantine bad rows (to a `_rejects` table) + alert when the SLA matters more than
   blocking on a few bad rows.

## exercise-39 (architecture)
1. Streaming needs steady, predictable slots — a fixed share avoids starving it (and it
   can't hog the cluster either).
2. Capacity-scheduler queue caps (analytics max < total) plus fair pools.
3. Kerberos delegation tokens expire; long-running apps need keytab-based renewal
   (`--principal`/`--keytab`).
4. Driver → idempotent retryable job; executor → task retry + external shuffle service;
   RM → RM HA; STS → supervised + load-balanced; streaming → checkpoint; Kafka → replication.

## exercise-40 (cost & observability)
1. On-prem cost = shared resources held (executor-core-seconds + memory) × time.
2. Usually: eliminate a full scan (pruning/DPP), fix skew, or right-size executors/partitions.
3. Alert on leading indicators — data-volume growth, rising GC/spill, stage-time trend —
   before the SLA breaks.
