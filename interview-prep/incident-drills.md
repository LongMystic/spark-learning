# Production Incident Drills

Timed scenario practice for Phase 3. For each: read the **symptom**, decide your
**first move**, then investigate. Expand only after you've committed to an answer.
Do these out loud or on paper, ~5 minutes each.

---

### Drill 1 — The nightly ETL is failing at 3am
**Symptom**: Airflow shows the daily job failing; the driver log ends with
`FetchFailedException: Failed to connect to worker-12:7337`. It's failed 3 retries.

<details><summary>Approach</summary>
FetchFailed is a symptom. First **stabilize**: is it a one-off node blip? Check the
Executors timeline around the failure — did worker-12 die? Why (OOM/kill/GC)? If it's a
transient node loss and the write is idempotent, **re-run** to restore the SLA. Then
root-cause the executor loss (Day 16/18): if OOM, right-size partitions/overhead; ensure
the external shuffle service is on for dynamic allocation. Prevent: alert on data-volume
growth that pushed it over the edge.</details>

---

### Drill 2 — A dashboard is timing out during business hours
**Symptom**: Superset tiles spin and time out; other teams complain their Spark jobs are
slow too. The Thrift Server driver memory is near 100%.

<details><summary>Approach</summary>
Stabilize the shared engine: identify the heavy query in the STS SQL tab (likely a
`SELECT *` / raw-fact scan pulling huge results to the driver). Kill it / apply a row
limit and query timeout. Enable `incrementalCollect`. Structural fix: move that dashboard
onto a pre-aggregated mart (Day 37), isolate BI in a fair-scheduler pool or a separate
STS, and cap result sizes. Prevent: BI reads marts, not facts.</details>

---

### Drill 3 — "It worked in dev, OOMs in prod"
**Symptom**: A PySpark job with a Pandas UDF succeeds on sample data but on the full
dataset executors die with `Container killed by YARN … 9.1 GB of 9 GB physical memory used`.

<details><summary>Approach</summary>
"Container killed" = **overhead/off-heap**, not heap — and Pandas UDFs run Python
off-heap. Raise `spark.executor.memoryOverhead` (15–20%+). Also check skew (one giant
`applyInPandas` group?) and reduce columns fed to the UDF. Verify pyarrow/pandas versions
match across executors. Prevent: overhead sizing baked into the job's submit config.</details>

---

### Drill 4 — Re-running a fix doubled the data
**Symptom**: After a failed run you re-ran the job; now the gold table has duplicate rows.

<details><summary>Approach</summary>
The write wasn't idempotent (blind append / full overwrite gone wrong). Stabilize:
identify affected partitions, restore from the previous snapshot (Iceberg time
travel/rollback) or recompute those partitions. Fix: switch to dynamic partition
overwrite or MERGE so re-runs replace, not append. Prevent: idempotency is a hard
requirement for every writing job (Days 21, 34, 38).</details>

---

### Drill 5 — One stage runs for an hour, the rest in seconds
**Symptom**: A join stage has 1 task at 58 min while 199 finished in <30s.

<details><summary>Approach</summary>
Classic value-level skew (Day 10). Confirm via key distribution + the straggler's
shuffle-read size. Fix: AQE skew join; if that's not enough, salt the hot key or isolate
it. If the other side is small, broadcast it to avoid the shuffle entirely. Note:
speculation won't help — the duplicate task is equally overloaded.</details>

---

### Drill 6 — Streaming job's memory grows until it dies daily
**Symptom**: A Structured Streaming aggregation runs fine for hours, then executors OOM;
restarting resets the clock.

<details><summary>Approach</summary>
Unbounded state (Day 31): the aggregation/dedup has no watermark, or the key space only
grows, so the state store expands forever. Add `withWatermark` to bound and evict old
state; consider the RocksDB state store for large state; verify the key cardinality is
bounded. Prevent: monitor state rows/size in the streaming tab.</details>
