# Day 23: Advanced SQL & Window Functions

## 🎯 Learning Objectives
- Master window functions and how they execute (partition → sort → frame)
- Know the full catalog of ranking, aggregate, and offset window functions and when each applies
- Use grouping sets / rollup / cube for multi-level aggregation in one pass
- Apply higher-order functions on arrays/maps without exploding
- Write SQL that the optimizer can actually make fast, and diagnose window-specific skew

## 📚 Core Concepts

### 1. Window functions: the three-phase execution model

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy(F.col("txn_ts"))
df = (txns
   .withColumn("running_total", F.sum("amount").over(w))
   .withColumn("rn", F.row_number().over(w))
   .withColumn("prev_amount", F.lag("amount").over(w)))
```

**Key Points:**
- Execution has three phases: **shuffle** by the partition key (`Exchange hashpartitioning`), **sort** by the order key within each partition, then the **window** operator computes the frame for each row.
- Every row in a partition must be co-located on one task before the sort — this makes `Window.partitionBy()` behave exactly like a `groupBy` for shuffle purposes, including skew (Day 10).
- Unlike joins, `WindowExec` has **no AQE skew-join optimization** — AQE's skew handling only rewrites `SortMergeJoinExec` inputs, not window shuffles. A hot partition key here needs manual salting/restructuring.

**Example:**
```sql
SELECT customer_id, txn_ts, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY txn_ts) AS running_total,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY txn_ts) AS rn
FROM transactions
```

### 2. The window function catalog

**Key Points — three families, three behaviors:**

| Family | Functions | Behavior |
|---|---|---|
| Ranking | `row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile(n)`, `cume_dist` | Position-based; `rank`/`dense_rank` handle ties differently (`rank` leaves gaps, `dense_rank` doesn't) |
| Aggregate-over-window | `sum`, `avg`, `count`, `min`, `max`, `collect_list`, `collect_set` | Same aggregate functions as `groupBy`, evaluated per-frame instead of per-group |
| Offset | `lag(col, n)`, `lead(col, n)`, `first_value`, `last_value`, `nth_value` | Reach to a specific row relative to the current one within the frame |

**Example:**
```python
w = Window.partitionBy("category").orderBy(F.desc("amount"))
(txns
  .withColumn("rnk", F.rank().over(w))              # ties share rank, next rank skips
  .withColumn("dense_rnk", F.dense_rank().over(w))   # ties share rank, no gap
  .withColumn("pct_rank", F.percent_rank().over(w))  # 0.0-1.0 relative position
  .withColumn("decile", F.ntile(10).over(w)))        # bucket into 10 equal-ish groups
```

### 3. Frames: rows vs range, and the default trap

```python
# Rolling 3-row window (current row + 2 preceding)
w3 = Window.partitionBy("customer_id").orderBy("txn_ts").rowsBetween(-2, 0)

# Range frame: includes all rows with the same ORDER BY value, by value not row count
wr = Window.partitionBy("customer_id").orderBy("txn_ts").rangeBetween(Window.unboundedPreceding, 0)
```

**Key Points:**
- If you specify `ORDER BY` without an explicit frame, Spark defaults aggregate functions to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — a **growing** window, not the whole partition. This surprises people expecting `SUM(...) OVER (PARTITION BY x)` (no `ORDER BY`) to behave like `SUM() OVER (... ORDER BY y)` — the two are different frames.
- With **no** `ORDER BY` at all, the whole partition is one frame (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`) — useful for "total per group as a column" without collapsing rows via `groupBy`.
- `rows` counts physical rows; `range` counts by **value** of the order column, so ties in the order key are grouped together in the frame — pick deliberately, especially with duplicate timestamps.

### 4. Multi-dimensional aggregation in one pass

```python
txns.cube("category", "status").agg(F.sum("amount"))       # all combinations (2^n subtotals)
txns.rollup("txn_date", "category").agg(F.sum("amount"))    # hierarchical subtotals
```

```sql
-- Equivalent, explicit SQL: GROUPING SETS gives full control over which combinations
SELECT category, status, SUM(amount) AS total,
       GROUPING(category) AS is_cat_agg, GROUPING(status) AS is_status_agg
FROM transactions
GROUP BY GROUPING SETS ((category, status), (category), (status), ())
```

**Key Points:**
- `ROLLUP`/`CUBE`/`GROUPING SETS` compute several aggregation levels in a **single scan + shuffle**, instead of N separate `groupBy` queries unioned together.
- `ROLLUP(a, b)` produces hierarchical subtotals: `(a,b)`, `(a)`, `()` — good for date hierarchies (year → month → day).
- `CUBE(a, b)` produces **every** combination: `(a,b)`, `(a)`, `(b)`, `()` — grows as 2^n with the number of grouping columns, so use it only for a handful of low-cardinality dimensions.
- `GROUPING(col)` / `GROUPING_ID()` tell you which subtotal row you're looking at (1 = this column was rolled up to "all").

### 5. Higher-order functions (avoid explode when you can)

```python
df.select(F.transform("items", lambda x: x * 2).alias("doubled"))
df.select(F.filter("items", lambda x: x > 10).alias("big"))
df.select(F.aggregate("items", F.lit(0), lambda acc, x: acc + x).alias("sum"))
df.select(F.exists("items", lambda x: x < 0).alias("has_negative"))
df.select(F.zip_with("items", "prices", lambda x, p: x * p).alias("line_totals"))
```

**Key Points:**
- These operate on arrays **in place**, inside a single row — cheaper than `explode` → aggregate → `collect_list` back, because there's no shuffle and no row multiplication.
- `transform`/`filter`/`aggregate`/`exists`/`zip_with` compile to Catalyst's `LambdaFunction`/`HigherOrderFunction` expressions and participate in whole-stage codegen, unlike a UDF (Day 24).

### 6. PIVOT — reshaping long to wide

```sql
SELECT * FROM (
  SELECT category, status, amount FROM transactions
)
PIVOT (
  SUM(amount) FOR status IN ('active' AS active, 'refunded' AS refunded, 'pending' AS pending)
)
-- one row per category, one column per status value, cell = SUM(amount)
```

**Key Points:**
- `PIVOT` needs the list of output column values named **explicitly** (`IN (...)`) — Spark must know the target schema at plan time, so it can't pivot on an arbitrary runtime-discovered set of values.
- Under the hood, `PIVOT` compiles to a `GROUP BY` with conditional aggregates (`SUM(CASE WHEN status = 'active' THEN amount END)`) per target column — logically equivalent to writing that out by hand, but far more concise, and exactly what the earlier `groupBy(...).agg(F.sum(F.when(...)))` pattern from Core Concept 3 does manually.
- For a **wide-to-long** reshape (the inverse), use `stack()` or `explode` over an array of structs — Spark has no single `UNPIVOT` keyword prior to Spark 3.4's `UNPIVOT` clause.

## 🔍 Deep Dive: Top-N per group (a very common pattern)

### Step-by-Step Process
1. Build a window ordered by the ranking criterion, partitioned by the group.
2. Apply `row_number()` (strict 1..N, no ties) or `rank()` (ties share a position) depending on whether duplicates should count as one rank or many.
3. Filter `WHERE rn <= N` — this filter runs **after** the window operator, so the full partition is still shuffled and sorted first.
4. If the table is huge and only a few groups matter, pre-filter or pre-aggregate to shrink what gets shuffled into the window.

### Example: Practical Example
```python
w = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = txns.withColumn("rn", F.row_number().over(w)).where("rn <= 3")
top3.explain()
```

**Analysis:**
- This shuffles the **whole table** by `category` before the window can rank anything — the `WHERE rn <= 3` filter cannot be pushed below the `Window` node because row numbers don't exist until the window runs.
- If `category` is low-cardinality and the table is huge, consider: pre-aggregating to a smaller intermediate, or broadcasting a much smaller candidate set (e.g. pre-filter to the last 30 days) before ranking.
- Watch task-time balance in the Spark UI stage view: one category with far more rows than others produces one straggler task — this is window skew, not join skew, and AQE will not fix it.

### Alternative: pre-filter before ranking
```python
# If you only care about recent, high-value transactions, shrink the input
# BEFORE the window shuffles/sorts it — the ranking itself is unavoidable,
# but the amount of data it operates on doesn't have to be the whole table.
recent_high_value = txns.where("txn_date >= date_sub(current_date(), 30) AND amount > 100")
w = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = recent_high_value.withColumn("rn", F.row_number().over(w)).where("rn <= 3")
top3.explain()   # compare the shuffle size against ranking the full table
```
This doesn't change the window's fundamental cost model, but shrinking the partition-and-sort input directly shrinks the shuffle bytes and the sort's memory pressure — often the single biggest lever available before reaching for salting.

## 💡 Key Insights for On-Premise

### 1. One window spec, many columns
Define a `Window` object once and reuse it for several `withColumn` calls — Spark's optimizer collapses them into a **single** `Window` physical operator (one shuffle, one sort) rather than repeating the shuffle/sort per column. Confirm with `explain()`: you should see one `Window` node even with three window columns.

### 2. Skewed window partitions need manual handling
`Window.partitionBy(hot_key)` skews exactly like a `groupBy`/join on that key (Day 10), but **AQE's skew-join handling does not apply to windows** — only `SortMergeJoinExec` gets automatic sub-partition splitting. For a known hot key, restructure: split the hot key out, rank it separately, and union results, or salt-and-recombine similarly to a skewed join.

### 3. Grouping sets beat multiple queries on shared on-prem clusters
On a resource-constrained Kubernetes Spark cluster, three separate `groupBy` queries each pay their own shuffle and their own executor allocation/wait time. A single `ROLLUP`/`CUBE`/`GROUPING SETS` query pays one shuffle for all subtotal levels — meaningfully cheaper when executors are scarce and queueing behind other Spark Operator jobs.

## 🎯 Practical Exercises

### Exercise 1: Windows & top-N (see `exercises/advanced/exercise-23-advanced-sql.py`)
```python
w = Window.partitionBy("customer_id").orderBy("txn_ts")
enriched = (txns
    .withColumn("running_total", F.sum("amount").over(w))
    .withColumn("txn_seq", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("amount").over(w)))
enriched.explain()   # confirm ONE Window operator handles all three columns

wc = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = txns.withColumn("rn", F.row_number().over(wc)).where("rn <= 3")
top3.select("category", "amount", "rn").orderBy("category", "rn").show(12)
```

### Exercise 2: rollup vs N queries
```python
# One-pass rollup:
(txns.rollup("category", "status")
     .agg(F.sum("amount").alias("total"))
     .orderBy("category", "status")
     .explain())   # one shuffle for all subtotal levels

# Compare against the equivalent 3 separate groupBys unioned together —
# count Exchange nodes in each plan and compare stage counts in the SQL tab.
```

### Exercise 3: Ranking families and PIVOT
```python
w = Window.partitionBy("category").orderBy(F.desc("amount"))
ranked = (txns
    .withColumn("rnk", F.rank().over(w))
    .withColumn("dense_rnk", F.dense_rank().over(w))
    .withColumn("pct", F.percent_rank().over(w)))
ranked.where("category = (SELECT category FROM transactions LIMIT 1)") \
      .select("category", "amount", "rnk", "dense_rnk", "pct").show(10)
# 1. Find two rows with tied `amount` in the same category — compare rnk vs dense_rnk.

txns.createOrReplaceTempView("txns_v")
spark.sql("""
    SELECT * FROM (SELECT category, status, amount FROM txns_v)
    PIVOT (SUM(amount) FOR status IN ('active' AS active, 'refunded' AS refunded))
""").show()
# 2. Compare this PIVOT's plan to an equivalent groupBy + conditional SUM you write by hand.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Number of `Window`/`Exchange` operators** in the plan — fewer, reused operators beat one-per-column duplication.
2. **Task-time skew** on the window's partition key (max task time vs median in the stage view).
3. **Spill** during the sort phase of a window — a partition too large to sort in memory spills to disk and slows the stage.
4. **Shuffle read/write size** for `ROLLUP`/`CUBE` vs the equivalent separate `groupBy` queries.

### Spark UI Analysis
- **SQL tab**: a single `Window` node feeding multiple reused columns confirms the shuffle/sort isn't repeated; multiple `Window` nodes for what should be one spec indicates the code created separate `Window` objects instead of reusing one.
- **Stage tab**: check the task duration distribution for the window's shuffle stage — a long tail of one or two tasks is partition-key skew.

## 🚨 Common Issues & Solutions

### Issue 1: Window query is very slow
**Symptom**: One big shuffle + sort stage dominates the job's wall-clock time.
**Root Cause**: Unnecessary columns carried through the shuffle, a separate `Window` spec per output column, or skew on the partition key.
**Solution**: Select only needed columns before the window, define and reuse one `Window` object, and address partition-key skew (split hot key + union, or restructure the partitioning).

### Issue 2: Wrong running total at ties
**Symptom**: Unexpected/non-deterministic values when the `ORDER BY` column has duplicate values.
**Root Cause**: With a `RANGE` frame (the default when only `ORDER BY` is given), all rows sharing the same order-key value are included in each other's frame — a running total "jumps" all tied rows together instead of one at a time.
**Solution**: Add a tiebreaker column to `orderBy` (e.g. a unique id) to make ordering deterministic, and choose `rowsBetween` vs `rangeBetween` deliberately based on whether you want row-count-based or value-based framing.

### Issue 3: `CUBE` explodes into an unusably large result
**Symptom**: A `CUBE` over several columns returns far more rows than expected, some queries OOM on the driver during `collect()`.
**Root Cause**: `CUBE(a, b, c, ...)` produces 2^n grouping combinations — adding one more column doubles the output row count.
**Solution**: Use `ROLLUP` (hierarchical, n+1 levels) instead of `CUBE` (2^n levels) when only a natural hierarchy is needed, or use explicit `GROUPING SETS` to list only the combinations you actually query.

### Issue 4: Filter after a window doesn't reduce shuffle cost
**Symptom**: `WHERE rn <= 3` after `row_number()` still shuffles/sorts the entire table.
**Root Cause**: Row numbers don't exist until the `Window` operator runs, so the filter can't be pushed below it — Catalyst cannot know which rows would survive before ranking them.
**Solution**: Reduce the input first with a coarser filter or pre-aggregation that's valid *before* ranking (e.g. restrict to the last N days), then rank the smaller set.

### Issue 5: Window shuffle straggler not fixed by AQE
**Symptom**: `spark.sql.adaptive.skewJoin.enabled=true` is set, but one window-partition task is still far slower than the rest.
**Root Cause**: AQE's skew handling rewrites `SortMergeJoinExec` inputs only; `WindowExec`'s upstream shuffle isn't covered.
**Solution**: Manually split the known hot partition key into its own branch, rank it separately (possibly with extra parallelism via salting), and union the results back together.

## 📝 Key Takeaways
1. Windows = shuffle(partition) + sort(order) + frame computation; partition-key skew applies just like joins/groupBys.
2. Ranking (`row_number`/`rank`/`dense_rank`), aggregate-over-window, and offset (`lag`/`lead`) functions behave differently at ties and frame edges — pick deliberately.
3. Reuse one `Window` spec across columns to get a single shuffle/sort instead of one per column.
4. `ROLLUP`/`CUBE`/`GROUPING SETS` do multi-level aggregation in one pass; prefer `ROLLUP` when `CUBE`'s 2^n growth isn't needed.
5. Higher-order functions (`transform`/`filter`/`aggregate`/`zip_with`) beat `explode` for in-row array work.
6. AQE skew handling doesn't cover windows — salt/restructure manually for a hot partition key.

## 🔗 Next Steps
- **Day 24**: UDF/UDAF & Pandas/Arrow UDF Performance

## 📚 Additional Resources
- Spark SQL window functions reference (ranking, analytic, and aggregate window functions)
- Spark SQL higher-order functions (`transform`, `filter`, `aggregate`, `zip_with`, `exists`)
- `GROUPING SETS` / `ROLLUP` / `CUBE` SQL reference

---

**Progress**: Day 23/40 ✅
