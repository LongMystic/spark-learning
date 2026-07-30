# Day 28: Cost-Based Optimization (CBO)

## 🎯 Learning Objectives
- Understand how CBO uses table/column statistics — including histograms — to choose plans
- Collect and inspect statistics with `ANALYZE TABLE` and know what each stat actually feeds
- See CBO's biggest win: multi-way join reordering, and its dynamic-programming limits
- Know CBO's blind spots and how it interacts with AQE and DPP

## 📚 Core Concepts

### 1. Rule-based vs cost-based optimization

Without stats, Catalyst uses **rules and rough size estimates** (e.g. raw file size on disk) to pick a plan — good enough for simple pushdown/pruning decisions, but blind to actual data distribution. CBO adds **real statistics** — row counts, column min/max/distinct/null counts, and optionally histograms — so the optimizer can estimate the *cost* of alternative plans (mainly join order and join strategy) and pick the cheapest.

```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
```

**Key Points:**
- CBO is off by default (`spark.sql.cbo.enabled=false`) — the cost model has real planning-time overhead, so Spark only pays it when explicitly enabled.
- CBO's estimates feed into the **same** cost decisions AQE later re-verifies at runtime (Day 25) — CBO is the compile-time guess, AQE is the runtime correction.

### 2. Statistics are the fuel

CBO does nothing useful without stats:
```sql
ANALYZE TABLE transactions COMPUTE STATISTICS;                       -- table-level (row count, total size)
ANALYZE TABLE transactions COMPUTE STATISTICS FOR ALL COLUMNS;       -- column-level (needed for join reorder/selectivity)
ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS customer_id, amount;  -- targeted subset
```
Inspect them:
```sql
DESCRIBE EXTENDED transactions;                 -- see the Statistics line (row count, size in bytes)
DESCRIBE EXTENDED transactions customer_id;     -- column stats: min, max, distinct count, null count, avg/max length
```

**Key Points:**
- Column-level stats capture, per column: **min**, **max**, **distinct count (NDV)**, **null count**, and average/max length for variable-width types — this is what lets CBO estimate a filter's *selectivity* (what fraction of rows a `WHERE` clause will keep) instead of guessing.
- For **skewed** columns, a single min/max/NDV summary can badly misestimate selectivity (e.g. `WHERE status = 'active'` when 95% of rows are `'active'`). Equi-height **histograms** fix this:
```python
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS status")
```
Enabling histograms makes `ANALYZE` slower and more expensive to compute (it must bucket the actual value distribution), so it's worth it selectively — on columns you know are skewed and are commonly filtered/joined on — rather than blanket-enabling it for every column.

### 3. The headline benefit: join reordering

For `A ⨝ B ⨝ C ⨝ D`, join **order** hugely affects intermediate result sizes — joining the two smallest tables first, then joining the result to the next, keeps every intermediate result small; joining in a bad order can produce a massive intermediate before it's finally filtered down. With column stats, CBO estimates each candidate intermediate's cardinality and reorders to minimize total estimated cost.

**Key Points:**
- Join reordering uses **dynamic programming** over the set of join inputs, but that search space grows combinatorially — `spark.sql.cbo.joinReorder.dp.threshold` (default 12) caps how many joined tables CBO will exhaustively search before falling back to a simpler heuristic ordering.
- `spark.sql.cbo.joinReorder.card.weight` (default 0.7) balances the cost formula between estimated **cardinality** (row count) and estimated **size** (bytes) when comparing candidate plans — cardinality-weighted by default, since row count often better predicts downstream operator cost.
- A separate, lighter-weight heuristic — `spark.sql.cbo.starSchemaDetection` — can recognize a classic star-schema shape (one large fact table joined to several small dimensions) and order joins fact-last / dimensions-first without needing the full DP search, useful when full stats collection on every table isn't practical.

### 4. How selectivity estimation actually works

**Key Points:**
- For an equality filter (`WHERE status = 'active'`), CBO's default estimate (without a histogram) is roughly `1 / distinct_count` of the column — i.e. it assumes values are **uniformly distributed**. If `status` actually has 2 values but one covers 95% of rows, this estimate is off by nearly 20x.
- For a range filter (`WHERE amount > 100`), the estimate uses the column's `min`/`max` to interpolate what fraction of the value range is selected — again assuming uniform distribution between min and max.
- For an `IN (...)` list, CBO sums the per-value equality estimates (capped at 1.0).
- A **histogram** replaces this uniform-distribution assumption with actual equi-height buckets built from the real data, giving CBO a much more accurate selectivity estimate for skewed columns — at the cost of a more expensive `ANALYZE` pass to build it.

```sql
-- Without a histogram: CBO assumes status's values are evenly spread
-- With a histogram: CBO knows 'active' actually covers 95% of rows
ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS status
-- (with spark.sql.statistics.histogram.enabled=true set beforehand)
```

## 🔍 Deep Dive: Seeing CBO work

### Step-by-Step Process
1. Register the tables involved as catalog tables (`saveAsTable`), not raw paths.
2. Run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` on each.
3. Enable `spark.sql.cbo.enabled` and `spark.sql.cbo.joinReorder.enabled`.
4. Call `.explain("cost")` on the multi-way join and read the **estimated** row counts/sizes annotated on each plan node.
5. Repeat with `cbo.enabled=false` and compare the chosen join **order** and the estimates shown.

### Example: Practical Example
```python
# With stats collected and CBO on:
plan = a.join(b, "k1").join(c, "k2").join(d, "k3")
plan.explain("cost")     # shows size/row estimates per node, and the join order chosen
```

**Analysis:**
- `explain("cost")` prints the optimizer's size/row estimates alongside the plan, so you can judge whether they're *realistic* — wildly wrong estimates (e.g. estimating 10 rows where there are actually 10 million) are the leading cause of a "CBO made it worse" complaint, and almost always trace back to stale or missing stats, not a CBO bug.
- Compare the join **order** between the CBO-on and CBO-off runs on the same query — a genuine improvement should show smaller intermediate result estimates at each step of the CBO-on plan.

### Example: A skewed filter, with and without a histogram
```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.sql("ANALYZE TABLE transactions_cbo COMPUTE STATISTICS FOR COLUMNS status")  # no histogram yet
spark.sql("SELECT * FROM transactions_cbo WHERE status = 'active'").explain("cost")
# Estimated row count likely assumes a uniform split across status's distinct values

spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE transactions_cbo COMPUTE STATISTICS FOR COLUMNS status")  # rebuild with histogram
spark.sql("SELECT * FROM transactions_cbo WHERE status = 'active'").explain("cost")
# Estimated row count should now track the ACTUAL skew toward 'active'
```
**Analysis:** compare the estimated row count in both `explain("cost")` outputs against the query's actual row count (`.count()`) — the histogram version should land much closer to reality on a genuinely skewed column, directly improving any downstream broadcast/join-order decision that depends on this estimate.

## 💡 Key Insights for On-Premise

### 1. Stats go stale
`ANALYZE` is a **snapshot**, not a live view. After large loads (a daily ETL batch, a backfill), the stats on disk no longer reflect the table's actual current distribution, and Spark's estimates silently drift from reality — CBO and auto-broadcast decisions degrade together. Bake `ANALYZE TABLE` into your ETL/DBT pipeline for hot, frequently-joined tables (e.g. as a post-load step in the Airflow DAG), or rely on catalog features that maintain stats incrementally where available, rather than treating `ANALYZE` as a one-time setup task.

### 2. CBO needs managed/catalog tables
Column stats live in the **metastore**. Raw Parquet paths read ad-hoc via `spark.read.parquet(path)` don't carry any stats — register tables (Hive or Iceberg) to benefit from CBO at all. **Iceberg** keeps richer file-level statistics (per-column min/max, null counts at the manifest level) that Spark can use for scan-time pruning even *without* running `ANALYZE`, though the full CBO join-reordering cost model still benefits from explicit `ANALYZE`-collected column stats (NDV, histograms) that Iceberg's manifest stats alone don't provide.

### 3. CBO vs AQE vs DPP — three complementary layers
- **CBO** = *compile-time*, uses **stored** stats to pick join order/strategy before anything runs.
- **AQE** (Day 25) = *runtime*, uses **actual** shuffle statistics to correct the plan mid-query.
- **DPP** (Day 27) = *runtime*, uses a filtered dimension's actual keys to prune fact-table partitions at scan time.
They stack: CBO picks a good initial plan and reasonable broadcast decisions; DPP then prunes scan I/O for star joins; AQE corrects whatever both got wrong once real data arrives. Enable all three together rather than treating them as alternatives.

## 🎯 Practical Exercises

### Exercise 1: Stats-driven plans (see `exercises/advanced/exercise-28-cbo.py`)
```python
for name in ["transactions", "customers", "products"]:
    read_table(spark, name).write.mode("overwrite").saveAsTable(f"{name}_cbo")
    spark.sql(f"ANALYZE TABLE {name}_cbo COMPUTE STATISTICS FOR ALL COLUMNS")

spark.sql("DESCRIBE EXTENDED transactions_cbo").where("col_name = 'Statistics'").show(truncate=False)

query = """
    SELECT c.segment, p.category, SUM(t.amount) AS total
    FROM transactions_cbo t
    JOIN customers_cbo c ON t.customer_id = c.customer_id
    JOIN products_cbo  p ON t.product_id  = p.product_id
    GROUP BY c.segment, p.category
"""
for flag in ["true", "false"]:
    spark.conf.set("spark.sql.cbo.enabled", flag)
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", flag)
    spark.sql(query).explain("cost")
```

### Exercise 2: Estimate accuracy
```python
# Use explain("cost") to compare estimated vs actual row counts:
plan = spark.sql(query)
plan.explain("cost")            # estimated rows per node
actual = plan.count()           # actual final row count
# Cross-check intermediate estimates against the SQL tab's actual metrics
# for each node after running the query, and note where estimates are furthest off.
```

### Exercise 3: Histogram vs no-histogram selectivity
```python
# 1. ANALYZE a known-skewed column WITHOUT a histogram, note the estimated
#    row count for an equality filter on its majority value via explain("cost").
# 2. Enable spark.sql.statistics.histogram.enabled, re-ANALYZE the same column.
# 3. Re-run explain("cost") and compare both estimates against the actual
#    .count() for that filter. Which estimate was closer, and by how much?
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Estimated vs actual rows per operator** — compare `explain("cost")` output against the SQL tab's post-execution actual row counts for each node.
2. **Join order chosen** with CBO on vs off, and whether it matches your intuition about which tables are smallest.
3. **`ANALYZE TABLE` staleness** — track when stats were last collected per table relative to the last load, ideally as an ETL pipeline metric.
4. **Planning time** for very wide multi-way joins — watch for the `joinReorder.dp.threshold` fallback kicking in on queries with more than ~12 joined tables.

### Spark UI Analysis
- **SQL tab**: verify the chosen join order keeps intermediate row counts small at each step; a node with a much larger "actual rows" than its estimate is a sign stats are stale or a histogram is needed.
- **SQL tab (cost plan)**: `explain("cost")` output isn't shown directly in the UI, but you can capture it in driver logs/notebook output alongside the UI's actual metrics for the same query run.

## 🚨 Common Issues & Solutions

### Issue 1: CBO on, but plans unchanged
**Symptom**: No reorder happens even with `cbo.enabled=true`.
**Root Cause**: No column stats were ever collected (only table-level `ANALYZE TABLE` without `FOR ALL COLUMNS`), or the tables were read via path rather than through the catalog.
**Solution**: Run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` on every table in the join, on registered catalog tables (Hive/Iceberg), then re-check `explain("cost")`.

### Issue 2: Bad plan from wrong estimates
**Symptom**: The optimizer picks a join order that performs worse than the naive one, or badly underestimates a huge intermediate result.
**Root Cause**: Stats are stale relative to the table's current data (a large load happened since the last `ANALYZE`), or a skewed column has no histogram so its selectivity is misestimated.
**Solution**: Re-run `ANALYZE TABLE` after significant loads; enable `spark.sql.statistics.histogram.enabled` and re-analyze known-skewed filter/join columns; as an immediate workaround, use an explicit broadcast/join-order hint to override the optimizer's bad estimate.

### Issue 3: `ANALYZE TABLE ... COMPUTE STATISTICS` itself is slow/expensive
**Symptom**: The `ANALYZE` step in the ETL pipeline takes longer than expected on very large tables, especially with histograms enabled.
**Root Cause**: Column-level and histogram statistics require scanning (a sample of, or the full) data to compute distinct counts and bucket boundaries — this is a real, non-trivial job, not metadata-only.
**Solution**: Scope `FOR COLUMNS` to just the columns actually used in filters/joins rather than `FOR ALL COLUMNS` on very wide tables, and enable histograms selectively only on columns known to be skewed and commonly queried.

### Issue 4: Join reorder ignored beyond a certain number of tables
**Symptom**: A wide multi-way join (12+ tables) doesn't get the same aggressive reordering seen on smaller joins.
**Root Cause**: `spark.sql.cbo.joinReorder.dp.threshold` (default 12) caps the dynamic-programming search; beyond it, Spark falls back to a cheaper heuristic ordering rather than an exhaustive search.
**Solution**: For very wide joins, either accept the heuristic fallback, manually restructure the query to pre-join the smallest/most selective tables first, or (cautiously) raise the threshold and monitor planning time, since the DP search cost grows quickly with table count.

### Issue 5: CBO conflicts with a manual broadcast/join hint
**Symptom**: An explicit `/*+ BROADCAST(t) */` or join-order hint produces a different (sometimes worse) plan than CBO alone would have chosen.
**Root Cause**: Explicit hints always **override** the cost-based decision for that join — they're a deliberate escape hatch, not additional input to the cost model.
**Solution**: Only add manual hints where you have information the optimizer doesn't (e.g. you know a table will shrink drastically after an upstream filter that stats can't reflect); otherwise let CBO decide once stats are current, and remove stale hints left over from a time when stats were unavailable.

## 📝 Key Takeaways
1. CBO uses stored column statistics — min/max/NDV/nulls, optionally histograms — to estimate cost and reorder joins.
2. Collect stats with `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`; without them CBO has nothing to work with.
3. Histograms (`spark.sql.statistics.histogram.enabled`) fix selectivity estimation on skewed columns, at extra `ANALYZE` cost.
4. Join reorder uses a dynamic-programming search capped by `joinReorder.dp.threshold` (default 12 tables), falling back to a heuristic beyond that.
5. Stats go stale after loads — refresh them as part of ETL, ideally in the same pipeline that writes the data.
6. CBO (compile-time) + DPP (scan-time pruning) + AQE (runtime correction) are complementary layers — enable all three.
7. CBO needs catalog tables (Hive/Iceberg), not raw paths; Iceberg's own file-level stats help scan pruning independent of `ANALYZE`.

## 🔗 Next Steps
- **Phase 4 complete** → [assessments/phase-4-assessment.md](../assessments/phase-4-assessment.md)
- **Day 29**: Spark Thrift Server Architecture & Tuning (Phase 5)

## 📚 Additional Resources
- Spark Cost-Based Optimizer design docs; `ANALYZE TABLE` SQL reference
- `spark.sql.cbo.*` and `spark.sql.statistics.*` configuration reference
- Apache Iceberg table statistics and manifest metadata documentation

---

**Progress**: Day 28/40 ✅
