# Interview Questions — Performance Tuning (Phase 2)

<details><summary>1. How do you size executors for a cluster?</summary>
Reserve ~1 core + RAM for OS/NodeManager per node, target ~2–3 executors/node, ~5 cores
each (beyond ~5, HDFS throughput drops), split remaining RAM per executor, add 10–20%
memoryOverhead. It's a method, not a magic number.</details>

<details><summary>2. Static vs dynamic allocation?</summary>
Static: fixed executors — predictable, dedicated jobs. Dynamic: scales executors with
demand — good for bursty/shared clusters; **requires the external shuffle service** so
scaled-down executors don't lose shuffle files.</details>

<details><summary>3. How do you detect and fix data skew?</summary>
Detect: max ≫ median task time, one huge shuffle-read task. Fix: AQE skew join, salting
the hot key, isolating hot keys, or broadcasting the small side. AQE covers joins, not
windows/aggs.</details>

<details><summary>4. How do you choose spark.sql.shuffle.partitions?</summary>
Target ~100–200MB per partition → shuffle bytes / ~128MB. With AQE, set generous and let
coalescing right-size it.</details>

<details><summary>5. What is spill and how do you address it?</summary>
Data written to disk when execution memory is exhausted (Spill Memory/Disk in the UI).
Fix: more partitions, more/execution memory, reduce per-task data, fix skew.</details>

<details><summary>6. Heap OOM vs "container killed by YARN"?</summary>
Heap OOM → JVM heap too small (more partitions / fix skew / more heap).
Container killed → off-heap/overhead exceeded (raise `memoryOverhead`; common in PySpark).</details>

<details><summary>7. Why filter and project before joins?</summary>
Less data enters the shuffle → smaller map output, less network, faster reduce. Catalyst
pushes many of these down, but explicit early filtering/projection guarantees it.</details>

<details><summary>8. How does AQE improve tuning?</summary>
At runtime it coalesces small post-shuffle partitions, splits skewed partitions, and can
switch a planned SMJ to a broadcast join — using actual stats, reducing manual tuning.</details>

<details><summary>9. Which knobs reduce shuffle/network cost?</summary>
Column pruning, early filtering, broadcast small tables, shuffle compression (lz4/zstd),
right-sized partitions, and better data locality.</details>

<details><summary>10. How do you tune a job you've never seen?</summary>
Measure first (SQL/Stages tabs): find the biggest stage, check skew/spill/scan bytes/join
strategy, change ONE thing, re-measure. Don't guess-tune five configs at once.</details>
