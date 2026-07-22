# Interview Questions — Production & Ecosystem (Phase 5)

<details><summary>1. What is the Spark Thrift Server and when do you use it?</summary>
A long-running Spark app exposing a HiveServer2 JDBC/ODBC endpoint with one shared
SparkContext. Used for interactive multi-user BI (Superset), SQL clients, and dbt — great
for low-latency small/medium queries; risky for isolation (shared driver).</details>

<details><summary>2. How does Structured Streaming guarantee exactly-once?</summary>
Replayable source (Kafka offsets / file listing) recorded in the checkpoint + idempotent
or transactional sink (files+checkpoint, MERGE, Iceberg/Delta). Console/memory sinks are
not exactly-once.</details>

<details><summary>3. What do watermarks do?</summary>
Define how long to wait for late data: they let Spark finalize/emit windows and evict old
state. Without a watermark, stateful queries grow state unbounded → OOM. Events later than
the watermark are dropped.</details>

<details><summary>4. What does Iceberg give you over Hive tables?</summary>
ACID commits, snapshot isolation + time travel, hidden partitioning, schema/partition
evolution, file-level stats, and safe concurrent writes — plus a real fix for small-file
and directory-listing pain.</details>

<details><summary>5. How do you keep an Iceberg table healthy?</summary>
`rewrite_data_files` (compaction) for small files, `expire_snapshots` + `remove_orphan_files`
to reclaim storage, `rewrite_manifests` for metadata; schedule these off-peak.</details>

<details><summary>6. How do you orchestrate Spark from Airflow robustly?</summary>
Airflow schedules; Spark computes on YARN (SparkSubmit/Livy operators). Parameterize by
logical date, write only that partition idempotently (dynamic overwrite / MERGE), set
retries, add a DQ gate before publish. Never build DataFrames in the Airflow worker.</details>

<details><summary>7. How does dbt run on Spark, and which materialization for a big fact?</summary>
`dbt-spark` compiles SQL models and runs them through the Thrift Server. For big facts use
`incremental` with `merge` (Iceberg) or `insert_overwrite` — not full `table` rebuilds.</details>

<details><summary>8. How do you make Superset dashboards fast without hurting the cluster?</summary>
Read small pre-aggregated marts (not raw facts), partition + compact them, broadcast small
dims, and protect the shared STS with row limits, timeouts, fair pools/separate instances,
and caching aligned to the ETL schedule.</details>

<details><summary>9. Describe a CDC pipeline design on-prem.</summary>
Bronze (raw append) → DQ gate → Silver (dedup/clean + MERGE upsert, SCD2 for history) →
Gold (marts). Everything idempotent; Iceberg for atomic MERGE; compaction + stats as
maintenance.</details>

<details><summary>10. How do you share one cluster across teams fairly and securely?</summary>
Capacity/fair scheduler queues with guaranteed+max caps, fair pools (e.g. in the STS),
dynamic allocation + external shuffle service; Kerberos auth (with token renewal for long
jobs), Ranger/ACL authorization, TLS + at-rest encryption.</details>
