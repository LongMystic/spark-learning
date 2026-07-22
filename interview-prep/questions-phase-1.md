# Interview Questions — Fundamentals (Phase 1)

Conceptual Q&A. Try to answer aloud before expanding.

<details><summary>1. Explain how Spark executes a DataFrame query end-to-end.</summary>
User code builds a logical plan lazily → an action triggers Catalyst (analyze →
optimize → physical plans → selected plan) → the DAG scheduler splits it into stages
at shuffle boundaries → the task scheduler runs one task per partition on executors →
results return / are written.</details>

<details><summary>2. Narrow vs wide transformations — why does it matter?</summary>
Narrow (map/filter): each output partition depends on one input partition, no shuffle,
pipelined within a stage. Wide (groupBy/join): output depends on many input partitions,
requires a shuffle → a new stage → network + disk cost. Stage count = shuffle count + 1.</details>

<details><summary>3. Walk through Spark's unified memory model.</summary>
Executor heap = reserved (300MB) + user memory + unified memory (`spark.memory.fraction`).
Unified splits into execution (shuffle/join/agg) and storage (cache); they borrow from
each other. Under pressure cache is evicted first; execution can spill to disk.</details>

<details><summary>4. What is a shuffle and why is it expensive?</summary>
Redistribution of data across partitions by key: map tasks write partitioned shuffle
files to local disk, reducers fetch relevant blocks over the network, then combine/sort.
It's expensive due to disk I/O, network transfer, and serialization — the usual bottleneck.</details>

<details><summary>5. When is a broadcast join chosen and what are its limits?</summary>
When one side is below `autoBroadcastJoinThreshold` (or hinted). The small side is
collected to the driver and shipped to all executors, avoiding the big shuffle. Limit:
if the "small" side isn't small it causes driver/executor OOM and hits `maxResultSize`.</details>

<details><summary>6. cache() vs persist() vs checkpoint()?</summary>
`cache()` = `persist(MEMORY_AND_DISK)` (DataFrames). `persist(level)` picks the storage
level. Both keep lineage. `checkpoint()` truncates lineage by writing to reliable storage
— useful for very long lineages / iterative jobs.</details>

<details><summary>7. Why is lazy evaluation important?</summary>
It lets Catalyst optimize the whole plan (pushdown, pruning, join selection, fusing
narrow ops) before executing, rather than eagerly materializing each step.</details>

<details><summary>8. repartition() vs coalesce()?</summary>
`repartition(n)` does a full shuffle (can increase or balance partitions).
`coalesce(n)` merges partitions without a full shuffle (decrease only) — cheaper but can
create uneven partitions. Use coalesce to reduce output files, repartition to rebalance.</details>

<details><summary>9. What causes "too many small tasks" and how do you fix it?</summary>
Too many partitions (tiny files or a high shuffle-partition count) → scheduling overhead
dominates. Fix: coalesce, compaction, lower `shuffle.partitions`, or let AQE coalesce.</details>

<details><summary>10. How do you inspect a query plan and what do you look for?</summary>
`df.explain("formatted")`/SQL tab. Look for: Exchanges (shuffles), join strategy,
pushed filters + read schema at scans (pruning), and AQE nodes.</details>
