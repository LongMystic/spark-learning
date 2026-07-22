# Phase 2 Assessment — Performance Tuning (Days 8-14)

Pass mark: 8/10 quiz + both hands-on tasks.

## Part A — Conceptual

1. A node has 64GB RAM, 16 cores. You reserve ~1 core + some RAM for the OS/NM and want ~3 executors/node. Roughly size `--executor-memory`, `--executor-cores`, and `memoryOverhead`.
2. Static vs dynamic allocation — when do you pick each, and what must accompany dynamic allocation?
3. Give three symptoms of data skew visible in the Spark UI.
4. Explain salting for a skewed join. What's the cost/tradeoff?
5. How do you pick `spark.sql.shuffle.partitions`? What size per partition do you target?
6. What is spill, how do you see it, and name two fixes.
7. Why does filtering/projecting before a join reduce shuffle?
8. When would you raise `memoryOverhead` rather than `--executor-memory`?
9. Which shuffle compression codec is a good default and why?
10. How does AQE reduce the need to hand-tune shuffle partitions?

## Part B — Hands-on
1. **Skew**: run `exercise-10` on `transactions_skewed`; show the max/median task-time gap, then reduce it with AQE skew join and/or salting.
2. **Partition sizing**: run a heavy groupBy at `shuffle.partitions` = 8 vs 200 (AQE off); compare per-partition size and task count.

## Part C — Reflection
- Pick a slow job at work. Which single Phase-2 lever would you try first, and how will you measure it?

---

<details><summary>Answer key</summary>

1. ~ (64-≈4 reserved)/3 ≈ 18–20GB per executor JVM; cores ≈ (16-1)/3 = 5; overhead ≈ 10–20% (~2–4GB). Exact numbers vary — the method matters.
2. Static: predictable, dedicated workloads. Dynamic: bursty/shared clusters. Dynamic **requires the external shuffle service** so scaled-down executors don't lose shuffle files.
3. One task's duration ≫ median; one task's shuffle-read/input ≫ others; a single straggler holding up a stage.
4. Append a random suffix to the hot key to spread it across partitions, replicate the other side per salt value, aggregate, then strip salt. Cost: extra shuffle/replication and more complex code.
5. Aim ~100–200MB per partition → partitions ≈ shuffle bytes / 128MB. With AQE, set generous and let coalescing trim it.
6. Spill = data written to disk mid-stage under memory pressure. Seen as Spill(Memory/Disk) in Stages. Fixes: more partitions, more/execution memory, fix skew.
7. Less data enters the shuffle → smaller map output, less network, faster reduce.
8. When the log says "container killed by YARN … exceeds memory" (off-heap/Python) — raise overhead; heap wasn't the issue.
9. lz4 (or zstd for better ratio): fast, low CPU; good default for shuffle.
10. AQE coalesces tiny post-shuffle partitions and splits skewed ones at runtime using actual stats, so a single generous setting works across queries.
</details>
