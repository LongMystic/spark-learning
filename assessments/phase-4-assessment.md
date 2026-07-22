# Phase 4 Assessment — Advanced Topics (Days 22-28)

Pass mark: 8/10 quiz + both hands-on tasks.

## Part A — Conceptual

1. Name the Catalyst pipeline stages and one extension point you can inject into each relevant one.
2. Before writing a custom rule, which five config-level levers should you try?
3. How does a window function execute? What drives its skew risk?
4. When do you use `ROLLUP`/`CUBE`/`GROUPING SETS` instead of multiple `groupBy`s?
5. Why is a Python UDF opaque to Catalyst, and what does that cost you?
6. Give the three Pandas-UDF flavors and when each applies.
7. What three things does AQE do at runtime?
8. State every precondition for a bucketed join to avoid shuffle.
9. What is Dynamic Partition Pruning and what must be true for it to fire?
10. What does CBO need to work, and what's its headline benefit?

## Part B — Hands-on
1. Run `exercise-25`; toggle broadcast and show which Exchange nodes disappear; then show AQE coalescing 200 partitions.
2. Run `exercise-24`; compare the plans (BatchEvalPython vs ArrowEvalPython vs neither) for the same transform three ways.

## Part C — Reflection
- Which advanced feature (bucketing / DPP / CBO / AQE) best fits a real table you own, and why?

---

<details><summary>Answer key</summary>

1. Parser → Analyzer (`injectResolutionRule`) → Optimizer (`injectOptimizerRule`) → Planner strategies (`injectPlannerStrategy`) → physical. Parser via `injectParser`.
2. CBO+ANALYZE, AQE, bucketing, DPP, and rewriting the query.
3. Shuffle by partition key → sort by order key → compute frame. Skew comes from a hot `partitionBy` key (and AQE does **not** fix window skew).
4. When you need several aggregation levels from one scan/shuffle instead of N separate queries.
5. Catalyst can't see inside Python code → no pushdown/codegen through it, and per-row JVM↔Python serialization.
6. Scalar (Series→Series, element-wise); grouped aggregate (Series→scalar, in `agg`); grouped map (`applyInPandas`, whole group as a DataFrame — beware group size).
7. Coalesce small post-shuffle partitions; split skewed partitions (skew join); switch a planned SMJ to broadcast when a side is small at runtime.
8. Both tables bucketed by the same column(s), same bucket count, join key = bucket key, read as tables (metastore), bucketing enabled.
9. A runtime filter from the (filtered) dimension prunes the fact's partitions. Needs: fact partitioned on the join key + filtered/broadcastable dimension + DPP enabled.
10. Needs collected stats (`ANALYZE … FOR ALL COLUMNS`) on catalog tables. Headline benefit: cost-based multi-way join reordering.
</details>
