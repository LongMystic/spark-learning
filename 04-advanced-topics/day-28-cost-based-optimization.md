# Day 28: Cost-Based Optimization (CBO)

## 🎯 Learning Objectives
- Understand how CBO uses table/column statistics to choose plans
- Collect and inspect statistics with `ANALYZE TABLE`
- See CBO's biggest win: multi-way join reordering
- Know CBO's limits and how it interacts with AQE

## 📚 Core Concepts

### 1. Rule-based vs cost-based
Without stats, Catalyst uses **rules and rough size estimates**. CBO adds **actual statistics** — row counts, column min/max/distinct/null counts — so the optimizer can estimate the *cost* of alternative plans and pick the cheapest.

```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
```

### 2. Statistics are the fuel
CBO does nothing useful without stats:
```sql
ANALYZE TABLE transactions COMPUTE STATISTICS;                       -- table-level (row count, size)
ANALYZE TABLE transactions COMPUTE STATISTICS FOR ALL COLUMNS;       -- column-level (needed for join reorder/selectivity)
ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS customer_id, amount;
```
Inspect them:
```sql
DESCRIBE EXTENDED transactions;                 -- see Statistics line
DESCRIBE EXTENDED transactions customer_id;     -- column stats
```

### 3. The headline benefit: join reordering
For `A ⨝ B ⨝ C ⨝ D`, join order hugely affects intermediate sizes. With column stats, CBO estimates each intermediate result and reorders to keep intermediates small — turning an accidental cross-blowup into an efficient plan.

## 🔍 Deep Dive: Seeing CBO work
```python
# With stats collected and CBO on:
plan = a.join(b, "k1").join(c, "k2").join(d, "k3")
plan.explain("cost")     # shows size/row estimates per node
# Compare join order and estimated sizes with cbo.enabled=false.
```
`explain("cost")` prints the optimizer's size/row estimates so you can see whether they're realistic (garbage estimates → bad plans).

## 💡 Key Insights for On-Premise
### 1. Stats go stale
`ANALYZE` is a snapshot. After large loads, re-run it (or Spark's estimates drift and CBO/broadcast decisions degrade). Bake `ANALYZE TABLE` into your ETL for hot tables, or use auto-stats where available.

### 2. CBO needs managed/catalog tables
Column stats live in the metastore. Raw parquet paths read ad-hoc don't carry them — register tables (Hive/Iceberg) to benefit. Iceberg keeps richer file-level stats that Spark can use for pruning even without `ANALYZE`.

### 3. CBO vs AQE
- **CBO** = *compile-time*, uses **stored** stats.
- **AQE** = *runtime*, uses **actual** shuffle stats.
They're complementary: CBO picks a good initial plan; AQE corrects it with real numbers. Enable both.

## 🎯 Practical Exercises

### Exercise 1: Stats-driven plans
```python
# See exercises/advanced/exercise-28-cbo.py
# Register tables, ANALYZE, and compare join plans with CBO on vs off.
# (Requires catalog support; falls back with a note locally.)
```

### Exercise 2: Estimate accuracy
```python
# Use explain("cost") to compare estimated vs actual row counts; note where estimates are off.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Estimated vs actual rows per operator (`explain("cost")` vs SQL-tab metrics).
2. Join order chosen with/without CBO.

### Spark UI Analysis
- SQL tab: verify the chosen join order keeps intermediate row counts small.

## 🚨 Common Issues & Solutions

### Issue 1: CBO on, but plans unchanged
**Symptom**: no reorder.
**Solution**: no column stats collected, or tables read via path not catalog — run `ANALYZE ... FOR ALL COLUMNS` on registered tables.

### Issue 2: Bad plan from wrong estimates
**Symptom**: optimizer underestimates a huge intermediate.
**Solution**: stats are stale — re-`ANALYZE`; consider a broadcast/join hint to override.

## 📝 Key Takeaways
1. CBO uses stored stats to estimate cost and reorder joins.
2. Collect stats with `ANALYZE TABLE ... FOR ALL COLUMNS`; they're required.
3. Stats go stale — refresh after big loads (ideally in ETL).
4. CBO needs catalog tables (Hive/Iceberg), not raw paths.
5. CBO (compile-time) + AQE (runtime) together give the best plans.

## 🔗 Next Steps
- **Phase 4 complete** → [assessments/phase-4-assessment.md](../assessments/phase-4-assessment.md)
- **Day 29**: Spark Thrift Server Architecture & Tuning (Phase 5)

## 📚 Additional Resources
- Spark Cost-Based Optimizer docs; `ANALYZE TABLE` reference

---

**Progress**: Day 28/40 ✅
