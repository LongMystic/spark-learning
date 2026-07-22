# Phase 1 Assessment — Deep Fundamentals (Days 1-7)

Pass mark: 8/10 on the quiz **and** both hands-on tasks completed. Answers at the bottom.

## Part A — Conceptual (10 questions)

1. What triggers a Spark **job**? Name three actions and three transformations.
2. Why is lazy evaluation useful to the optimizer?
3. What separates one **stage** from the next?
4. Give two narrow and two wide transformations.
5. In the executor memory model, what lives in *execution* vs *storage* memory, and what happens under pressure?
6. Walk through the three phases of a shuffle.
7. When does partition pruning happen, and what must be true of the query/table?
8. Contrast SortMergeJoin and BroadcastHashJoin — when is each chosen?
9. Name three good candidates for caching and two cases where caching hurts.
10. Which storage level would you pick for a large DataFrame reused many times that doesn't fit in memory, and why?

## Part B — Hands-on
1. **Stage counting**: write a query with exactly 3 stages; confirm in the Spark UI DAG. (Use `exercise-01`.)
2. **Plan reading**: run `explain("formatted")` on a filter+join+groupBy; identify each Exchange and the join type chosen.

## Part C — Reflection
- Which fundamental most changed how you'll write Spark? Note one job at work you'd rewrite.

---

<details><summary>Answer key</summary>

1. An **action** (collect, count, write, show, take). Transformations: map/select/filter/join/groupBy.
2. It lets Catalyst see the whole plan and optimize globally (pushdown, pruning, join selection) before any work runs.
3. A **shuffle** (wide dependency) boundary.
4. Narrow: map, filter, select, union. Wide: groupBy, join, repartition, distinct.
5. Execution = shuffle/join/agg buffers; storage = cache. Unified memory lets them borrow from each other; under pressure cache is evicted before execution, and execution can spill to disk.
6. Map writes shuffle files (partitioned by key) → reducers fetch relevant blocks over the network → reduce-side combines/sorts.
7. At scan (compile-time for static filters on partition columns); the table must be partitioned on the filtered column and the filter must be pushable (not wrapped in an opaque function/UDF).
8. SMJ: both sides large, shuffles+sorts both; BHJ: one side small enough to broadcast (< threshold), avoids the big shuffle.
9. Cache: iterative reuse, multiple actions on the same DF, small dimensions. Hurts: used once, larger than memory (churn), simple/cheap recomputation.
10. `MEMORY_AND_DISK` (optionally `_SER`): keeps hot data in memory and spills the rest to disk instead of recomputing.
</details>
