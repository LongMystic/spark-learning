# Solutions — Fundamentals (Days 1-7)

## exercise-01 (DAG analysis)
**Expected**: filter/select → 1 stage (no Exchange). Join and groupBy each add an
Exchange → new stages. The complex query creates several stages.
1. Filter+select ≈ 1 stage; join = +1; groupBy = +1; a 3-shuffle query ≈ 4 stages.
2. Wide operations (join, groupBy, orderBy, repartition) cause stage boundaries.
3. Narrow: output partition depends on one input partition (no shuffle). Wide: depends on many (shuffle).
4. Task count per stage = number of partitions; check distribution in Stages tab.
5. Locality: `NODE_LOCAL` best → `RACK_LOCAL` → `ANY` (data fetched over network).

## exercise-02 (Catalyst optimizer)
1. Constant folding (`amount+0`→`amount`, `1=1`→true), predicate pushdown, column pruning.
2. Pushdown moves the filter to the Parquet scan (PushedFilters) → fewer rows read.
3. Only `customer_id, amount, status` appear in ReadSchema (pruned).
4. Disabling optimizations reads more data / bigger plans → slower (visible at scale).
5. With stats + CBO, join order is chosen to minimize intermediate sizes.

## exercise-03 (memory management)
1. Storage tab shows cached size; Executors tab shows used/total memory + GC.
2. Higher `memory.fraction` → more room for exec+storage → fewer spills.
3. G1GC with a pause target reduces long stop-the-world pauses under memory pressure.
4. `MEMORY_AND_DISK` for large reused data; `MEMORY_ONLY` only if it fits.
5. Spills (Memory/Disk in Stages) slow a stage; they signal execution-memory pressure.

## exercise-04 (shuffle mechanics)
1. groupBy and join show `Exchange`; filter does not (narrow).
2. Compare Shuffle Write vs input size in Stages — often much smaller after aggregation.
3. Optimal ≈ shuffle bytes / ~128MB; too few = spill, too many = tiny tasks.
4. Compression trades CPU for less network/disk; lz4 is a fast default.
5. AQE coalesces post-shuffle partitions → fewer, better-sized tasks than configured.

## exercise-05 (partitioning)
1. Partition pruning skips whole directories → far less data read (check PartitionFilters).
2. Granularity: enough to prune common filters, not so fine it makes tiny files.
3. Dynamic overwrite rewrites only present partitions (safe re-run); static wipes all.
4. Small files → coalesce/repartition before write, or compact (Iceberg Day 34).
5. Good partition column: moderate cardinality, frequently filtered, balanced. (The
   managed-table section may be skipped locally without Hive — that's expected.)

## exercise-06 (join algorithms)
1. Broadcast when one side < threshold (~10MB) — avoids the big shuffle.
2. Filtering before join shrinks the shuffled data.
3. Column pruning shuffles fewer bytes.
4. CBO reorders joins to keep intermediates small (needs stats).
5. AQE skew join splits oversized partitions at runtime.

## exercise-07 (caching)
1. Beneficial when the DF is reused by multiple actions / iterations.
2. `MEMORY_AND_DISK` for large reused data; `MEMORY_ONLY_SER` to save space at CPU cost.
3. Storage tab shows fraction cached and spill to disk.
4. Unpersist when done to free memory for other work.
5. Trade-offs: memory vs CPU (serialization) vs disk; don't cache single-use data.
