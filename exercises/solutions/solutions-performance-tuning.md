# Solutions — Performance Tuning (Days 8-14)

## exercise-08 (configuration tuning)
1. Optimal executor size from the calculator: ~5 cores, split RAM per 2–3 executors/node, +overhead.
2. Bigger executors = fewer JVMs but longer GC; ~5 cores balances HDFS throughput.
3. Parallelism ≈ 2–3× total cores keeps all slots busy without tiny tasks.
4. ETL (cache-heavy) → higher storageFraction; analytics (compute-heavy) → higher execution.
5. AQE coalesces/splits partitions and can switch joins → less manual tuning.
6. Executor confs are submit-time (YARN); locally they're printed, not applied — expected.

## exercise-09 (resource allocation)
1. Dynamic allocation adds executors on backlog, removes idle ones after the timeout.
2. Backlog (pending tasks) → scale up; idle beyond `executorIdleTimeout` → scale down.
3. Executors holding cached data aren't removed (`cachedExecutorIdleTimeout=infinity`).
4. Dynamic uses fewer resources when idle but must pair with the external shuffle service.
5. Conservative = shared-cluster friendly; aggressive = dedicated/perf-critical.
6. YARN queue caps a job's share; submit with `--queue`.
   (Allocation confs are submit-time; locally printed via `show()` — expected.)

## exercise-10 (data skew)
**Expected**: on `transactions_skewed`, one task's shuffle-read/duration ≫ others.
1. Skew → straggler tasks; the slow one holds up the whole stage.
2. Perf impact scales with the hot-key share (here ~80%).
3. AQE detects oversized partitions and splits them (`skewedPartitionFactor`).
4. Salting spreads the hot key but adds a shuffle/replication and post-processing cost.
5. Two-phase aggregation when a single hot key dominates a groupBy.
6. With handling, task times converge and total time drops (visible at scale).

## exercise-11 (shuffle optimization)
1. Optimal partitions ≈ shuffle bytes / ~128MB (fewer → bigger/spill; more → tiny tasks).
2. Column pruning shrinks per-row shuffle bytes.
3. Compression cuts network/disk at some CPU cost; lz4 default.
4. AQE coalesces small post-shuffle partitions.
5. Larger buffers = less I/O syscalls, more memory used.
6. Compare Shuffle Read/Write and task count across settings in the Stages tab.

## exercise-12 (join optimization)
1. Broadcast large⨝small; watch driver/executor memory if the "small" side isn't.
2/3. Filter + prune before join → smaller shuffle.
4. CBO reorders multi-way joins with stats.
5. Bucket join avoids shuffle when both sides share bucket key + count (see the mismatch demo).
6. AQE skew join balances a skewed join at runtime.
   (Bucket section needs a catalog; skipped locally with a note — expected.)

## exercise-13 (memory optimization)
1. Workload-dependent: execution-heavy → lower storageFraction; cache-heavy → higher.
2. G1GC with a pause target reduces long pauses; watch GC time %.
3. `MEMORY_AND_DISK` for large reused data.
4. Reduce memory in code: appropriate types, project early, avoid wide rows.
5. Raise overhead for off-heap/Python/native memory (PySpark).
6/7. Spills (Stages tab) indicate pressure and slow stages.
   (executor.memory/overhead confs are submit-time; printed locally — expected.)

## exercise-14 (network optimization)
1. Shuffle read + remote bytes read quantify network I/O.
2. Compression reduces bytes transferred.
3. Broadcast avoids shuffling the fact table's join side over the network.
4. `maxSizeInFlight` controls how much is fetched concurrently.
5. Higher `network.timeout` tolerates slow networks/GC pauses.
6. Combined (prune + filter + broadcast) minimizes network — verify via shuffle metrics.
