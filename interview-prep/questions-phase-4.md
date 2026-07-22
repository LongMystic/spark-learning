# Interview Questions — Advanced Topics (Phase 4)

<details><summary>1. How would you extend Catalyst, and when should you?</summary>
Via `SparkSessionExtensions` (inject analyzer/optimizer rules, planner strategies,
parser). Only for genuine framework-level rewrites — first exhaust CBO, AQE, bucketing,
DPP, and query rewrites. Rules must preserve semantics for all inputs.</details>

<details><summary>2. How do window functions execute, and what's the skew risk?</summary>
Shuffle by `partitionBy` key → sort by `orderBy` → compute the frame. A hot partition key
skews the window; AQE skew join does NOT help windows — salt manually.</details>

<details><summary>3. ROLLUP/CUBE/GROUPING SETS vs multiple groupBys?</summary>
They compute several aggregation levels in one scan+shuffle instead of N queries — far
cheaper for multi-dimensional summaries.</details>

<details><summary>4. Rank Python UDF, Pandas UDF, built-in — why?</summary>
Built-in (codegen + pushdown, JVM) > Pandas UDF (vectorized, Arrow batches) > Python UDF
(row-by-row pickling, optimizer-opaque).</details>

<details><summary>5. What does AQE do at runtime?</summary>
Coalesces small post-shuffle partitions, splits skewed partitions (skew join), and
switches a planned SMJ to broadcast when a side is small — all from actual shuffle stats.</details>

<details><summary>6. When does bucketing avoid a shuffle, exactly?</summary>
Both tables bucketed by the same column(s), same bucket count, join key = bucket key,
read as catalog tables, bucketing enabled. Miss any → shuffle returns.</details>

<details><summary>7. Partitioning vs bucketing?</summary>
Partitioning = directories by a low-cardinality filter column (pruning). Bucketing =
fixed hashed files by a high-cardinality join key (shuffle-free joins/aggs). Use both.</details>

<details><summary>8. What is Dynamic Partition Pruning?</summary>
In a star join filtered on the dimension, Spark computes the dimension's keys first and
injects them as a runtime filter on the fact's partition column → reads only relevant
partitions. Needs a partitioned fact + filtered/broadcastable dimension.</details>

<details><summary>9. What does CBO need and what's its big win?</summary>
Collected stats (`ANALYZE … FOR ALL COLUMNS`) on catalog tables. Big win: cost-based
multi-way join reordering to keep intermediates small.</details>

<details><summary>10. CBO vs AQE?</summary>
CBO = compile-time, uses stored stats. AQE = runtime, uses actual shuffle stats. They're
complementary — enable both.</details>
