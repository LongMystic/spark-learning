# Phase 3 Assessment — Troubleshooting (Days 15-21)

Pass mark: 8/10 quiz + both hands-on tasks.

## Part A — Conceptual

1. Name the five error families and one signature exception for each.
2. In a nested trace, where is the real root cause, and how do you find the responsible executor/task?
3. You see `Container killed by YARN … 6.2 GB of 6 GB physical memory used`. Heap or overhead? What do you change?
4. Same task index fails 4 times with the same exception — transient or deterministic? Next step?
5. What does `FetchFailedException` actually indicate, and what's the usual upstream cause?
6. Why must dynamic allocation be paired with the external shuffle service?
7. What causes "Task not serializable", and how do you fix a captured DB connection?
8. Rank Python UDF, Pandas UDF, and built-in expression by performance and explain why.
9. Give the 6-minute performance-triage steps using the SQL/Stages tabs.
10. List the incident-response playbook steps in order.

## Part B — Hands-on
1. Run `exercise-15`; for each triggered error, name its family and (uncommenting the traceback) its true `Caused by:`.
2. Run `exercise-21`; convert a full-overwrite write to dynamic partition overwrite and prove re-running doesn't duplicate rows.

## Part C — Reflection
- Pull `yarn logs` (or history) for one real failed job at work; classify it and write the one-line root cause.

---

<details><summary>Answer key</summary>

1. Memory (`OutOfMemoryError`), Shuffle (`FetchFailedException`), Serialization (`NotSerializableException`), Data (`AnalysisException`), Resource (AM failed / containers pending).
2. The last `Caused by:` on the failed **executor** log (driver shows the symptom). UI → failed stage → task → executor stderr.
3. **Overhead** (off-heap) — raise `spark.executor.memoryOverhead`; more heap won't help.
4. Deterministic — a data/UDF bug. Reproduce on that partition/row; retries won't help.
5. A reducer couldn't fetch shuffle blocks; usually a **map-side executor died** (OOM/kill/GC) — fix that, not the reduce stage.
6. So losing/removing an executor doesn't lose its shuffle files (the NodeManager serves them independently).
7. A closure captured a non-serializable object. Fix: create the connection **inside** the executor via `mapPartitions`/`foreachPartition`, or localize/`@transient` the field.
8. Built-in (codegen, pushdown) > Pandas UDF (vectorized/Arrow) > Python UDF (row-by-row, optimizer-opaque).
9. SQL tab slowest op → Stages max/median task time (skew) → Spill → scan files/bytes (I/O) → join strategy → change one thing, re-measure.
10. Acknowledge → Assess → Stabilize → Diagnose → Fix → Postmortem.
</details>
