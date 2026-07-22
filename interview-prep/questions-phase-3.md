# Interview Questions — Troubleshooting (Phase 3)

<details><summary>1. A job fails with "Job aborted due to stage failure." How do you debug?</summary>
The driver shows the symptom. Go to the failed stage/task in the UI, open the failing
task's **executor** stderr, read to the last `Caused by:`, classify into memory/shuffle/
serialization/data/resource, form one hypothesis, change one thing, re-run.</details>

<details><summary>2. How do you tell driver OOM from executor OOM?</summary>
Driver OOM: log in driver/AM, triggered by collect/toPandas/large broadcast/huge plan.
Executor OOM: executor log, from skew/large shuffle/wide rows. Opposite fixes — adding
executors won't help a driver OOM.</details>

<details><summary>3. What is a FetchFailedException really telling you?</summary>
A reducer couldn't fetch shuffle blocks — almost always because a **map-side executor
died** (OOM/kill/GC) or disk/network failed. Fix the executor loss; the fetch error is
downstream. Pair dynamic allocation with the external shuffle service.</details>

<details><summary>4. Task fails 4× with the same error — what does that mean?</summary>
Deterministic bug (data/UDF), not infrastructure. Retries won't help; reproduce on that
partition/row and fix. Transient failures fail different tasks/hosts with varied errors.</details>

<details><summary>5. How do you fix "Task not serializable"?</summary>
A closure captured a non-serializable object (connection, session, `this`). Localize the
needed value, create resources inside the executor via `mapPartitions`, broadcast large
read-only lookups, or mark fields `@transient`.</details>

<details><summary>6. A UDF-heavy job is slow — what do you do?</summary>
Replace the Python UDF with built-ins (pushdown + codegen), or vectorize with a Pandas
UDF (Arrow). Python UDFs cross the JVM↔Python boundary per row and block pushdown.</details>

<details><summary>7. Speculation — when does it help and when does it hurt?</summary>
Helps when a slow node makes a few tasks lag. Hurts with skew (the duplicate is equally
slow) and non-idempotent writes (duplicates).</details>

<details><summary>8. How do you get logs on a YARN cluster?</summary>
`yarn logs -applicationId <id>` (aggregated after the app ends), the RM UI per-container
logs while running, and the Spark History Server (:18080) to replay the UI.</details>

<details><summary>9. Walk me through your production incident playbook.</summary>
Acknowledge (impact/since-when) → Assess (failing/hung/slow, which stage) → Stabilize
(safe fast mitigation, e.g. idempotent re-run) → Diagnose (root cause) → Fix (permanent) →
Postmortem (blameless, prevention).</details>

<details><summary>10. Give a safe way to re-run a failed job.</summary>
Make writes idempotent: partition by run-date + dynamic partition overwrite, or MERGE
into an Iceberg table. Then re-running or backfilling can't duplicate/corrupt data.</details>
