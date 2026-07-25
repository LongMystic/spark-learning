# Solutions — Troubleshooting (Days 15-21)

## exercise-15 (error triage)
1. Plan-time: the missing-column `AnalysisException` (fails before any job runs).
2. Root causes: missing column (data/plan), divide-by-zero (`ArithmeticException`, data/runtime),
   pickling error (serialization — captured lambda), path-not-found (data/io).
3. Retry helps none of these — all are deterministic (data/code), not transient infra.

## exercise-16 (OOM debugging)
1. A driver OOM is about the **driver JVM** collecting data; executors don't help.
2. Pod OOMKilled (exit 137) with the heap not full → raise `spark.executor.memoryOverhead` (which raises the pod memory limit).
3. PySpark runs Python workers **off-heap**, so overhead (not heap) carries that memory.

## exercise-17 (task failures)
1. Filter by `txn_id % 500 == 0` to find the offending rows.
2. No — the failure is deterministic; more retries just fail 16× instead of 4×.
3. Speculation is wrong for skew (duplicate is equally slow) and non-idempotent writes.

## exercise-18 (FetchFailed)
1. It's a symptom: the reducer can't fetch blocks because the **map-side executor** is gone.
2. Dynamic allocation removes executors; on K8S there's no external shuffle service, so
   without shuffle tracking (or decommissioning with block migration) their shuffle files
   vanish → reducers get FetchFailed.
3. Target ~100–200MB per shuffle partition.

## exercise-19 (serialization)
1. The closure captured a non-serializable object; Spark can't ship it to executors.
2. Catalyst can't see inside Python UDFs → no pushdown through them.
3. Built-in (codegen + pushdown) > Pandas UDF (vectorized/Arrow) > Python UDF (row-by-row).

## exercise-20 (performance debugging)
1. Section A: the skewed stage's max/median task-time ratio is large.
2. Section B: the partition-filtered read scans far fewer files/bytes than the full scan.
3. Section C: enabling broadcast removes the `Exchange` on the fact side (SMJ → BHJ).

## exercise-21 (incident response)
1. Dynamic partition overwrite replaces only the partitions present in the written data,
   so re-running one date doesn't wipe/duplicate the rest.
2. Queue starvation → higher-priority queue/more executors; sudden skew → AQE skew join /
   salt; bad upstream data → quarantine the bad partition + escalate.
3. The job's writes must be **idempotent** before "re-run" is a safe first move.
