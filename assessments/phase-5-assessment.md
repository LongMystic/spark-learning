# Phase 5 Assessment — Production & Ecosystem (Days 29-40)

Pass mark: 8/10 quiz + one capstone started.

## Part A — Conceptual

1. What is the Spark Thrift Server, and why is its driver the shared bottleneck?
2. What does `spark.sql.thriftServer.incrementalCollect=true` protect against?
3. In Structured Streaming, what does the checkpoint store, and why does it enable recovery?
4. State the two conditions for end-to-end exactly-once.
5. What two jobs does a watermark do? What happens to state without one?
6. Rank RDD, DataFrame, and SQL for PySpark, and give two things that avoid Python UDFs.
7. What does Iceberg add over Hive/Parquet tables (name four)?
8. Which Iceberg procedures actually reclaim storage, and which fixes small files?
9. How do you make an Airflow-triggered Spark job safe to retry/backfill?
10. Why should Superset read pre-aggregated marts, and how do you isolate BI from ETL?

## Part B — Hands-on (pick two)
1. `exercise-30`: run a file-source stream with `trigger(availableNow)`; inspect the checkpoint.
2. `exercise-33/34` (with the Iceberg jar): create a table, write small files, compact, MERGE-upsert, time-travel.
3. `exercise-38`: build bronze→silver with a DQ gate and an idempotent incremental write.

## Part C — Capstone
- Start one project in [`capstones/`](capstones/) and write its plan (goal, data, steps, success metric).

---

<details><summary>Answer key</summary>

1. A long-running Spark app exposing a HiveServer2 JDBC endpoint with one shared SparkContext; all planning + result collection happen in its single driver.
2. Streams large result sets back incrementally instead of collecting them all to the driver → prevents driver OOM on big `SELECT`s.
3. Offsets + operator state; on restart Spark resumes from the last committed offset/state → recovery (and exactly-once with the right source/sink).
4. Replayable source (e.g. Kafka offsets) **and** idempotent/transactional sink (files+checkpoint, MERGE, Iceberg/Delta).
5. Finalizes/emits windows once passed, and bounds/evicts old state. Without one, state grows unbounded → OOM.
6. SQL ≈ DataFrame (both optimized) > RDD (no Catalyst). Avoid UDFs via built-ins and Pandas UDFs.
7. ACID commits, snapshots/time travel, hidden partitioning, schema/partition evolution, file-level stats (any four).
8. `expire_snapshots` (+ `remove_orphan_files`) reclaim storage; `rewrite_data_files` (compaction) fixes small files.
9. Parameterize by logical date and write only that partition via dynamic overwrite / MERGE (idempotent), with retries configured.
10. Marts are tiny/pre-aggregated → fast and cheap on the shared STS; isolate BI with separate STS/fair pools, row limits, timeouts, caching.
</details>
