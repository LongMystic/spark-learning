# Spark Mastery Checklist

Tick each item you can do **without notes**, on a real job. This is the definition
of "done" for the 40-day path — deeper than finishing the reading.

## Phase 1 — Fundamentals
- [ ] Explain jobs → stages → tasks and predict stage boundaries from code
- [ ] Read a DAG / `explain()` and identify narrow vs wide dependencies
- [ ] Describe the executor memory model (reserved / user / execution / storage)
- [ ] Explain how a shuffle works end-to-end (map write → fetch → reduce)
- [ ] Choose a partitioning strategy and column for a table
- [ ] Predict the join strategy Spark will pick and why
- [ ] Decide when caching helps and pick a storage level

## Phase 2 — Performance Tuning
- [ ] Calculate executor memory/cores/instances for a given cluster
- [ ] Configure static vs dynamic allocation correctly
- [ ] Detect data skew and fix it (salting / AQE / isolate hot key)
- [ ] Right-size `spark.sql.shuffle.partitions` (~100–200MB/partition)
- [ ] Turn a SortMergeJoin into a broadcast join safely
- [ ] Diagnose and reduce spill; tune memory fractions
- [ ] Reduce shuffle/network via pruning, filtering, compression

## Phase 3 — Troubleshooting
- [ ] Classify any failure into memory / shuffle / serialization / data / resource
- [ ] Read a stack trace to the true `Caused by:` and find the responsible task/executor
- [ ] Tell driver OOM from executor OOM and apply the right fix
- [ ] Recognize a pod **OOMKilled** (exit 137) = overhead/off-heap, not heap
- [ ] Distinguish transient vs deterministic task failures
- [ ] Trace a `FetchFailed` to its real (upstream executor-loss) cause
- [ ] Fix a "Task not serializable" and replace a slow UDF
- [ ] Run a production incident playbook (stabilize → root-cause → prevent)

## Phase 4 — Advanced
- [ ] Read an optimized plan and name the rules that fired
- [ ] Use window functions and multi-level aggregations efficiently
- [ ] Choose built-in vs Pandas UDF vs Python UDF and justify it
- [ ] Explain AQE (coalesce, skew join, dynamic switch) and see it in a plan
- [ ] Decide when to bucket a table and verify no-shuffle joins
- [ ] Recognize when Dynamic Partition Pruning fires (and why it didn't)
- [ ] Collect stats and use CBO for join reordering

## Phase 5 — Production & Ecosystem
- [ ] Configure & tune a Thrift Server for multi-user BI
- [ ] Build a Structured Streaming pipeline with checkpoints
- [ ] Handle late data with watermarks and bound streaming state
- [ ] Achieve exactly-once (replayable source + idempotent MERGE)
- [ ] Write idiomatic, testable PySpark; manage the executor Python env
- [ ] Create/maintain Iceberg tables (compaction, expiry, time travel, MERGE)
- [ ] Orchestrate idempotent jobs from Airflow
- [ ] Model transformations in dbt and accelerate Superset with marts
- [ ] Design a resilient, multi-tenant, secure on-prem architecture
- [ ] Run a cost/observability review and an optimization loop

## Applied mastery (the real test)
- [ ] Optimized ≥5 production jobs with measured before/after
- [ ] Fixed ≥3 production incidents and prevented recurrence
- [ ] Completed ≥1 [capstone](capstones/)
- [ ] Taught a teammate one thing from this path
