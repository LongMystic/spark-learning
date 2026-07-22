# Day 32: PySpark Best Practices & Zeppelin

## 🎯 Learning Objectives
- Write PySpark that is fast, testable, and optimizer-friendly
- Manage the Python environment across the cluster
- Use Zeppelin/notebooks productively without the usual traps
- Structure real PySpark projects for maintenance

## 📚 Core Concepts

### 1. The golden rules
- **Prefer DataFrame/SQL over RDDs** — RDDs bypass Catalyst/Tungsten (no codegen, no pushdown).
- **Avoid Python UDFs** — use built-ins or Pandas UDFs (Day 24).
- **Project & filter early** — read only the columns/partitions you need.
- **Don't `collect()` big data** to the driver (Day 16).
- **Cache only reused, expensive results** — and `unpersist()` when done.

### 2. Idiomatic patterns
```python
from pyspark.sql import functions as F

# GOOD: expression-based, chainable, pushdown-friendly
result = (spark.read.parquet("data/transactions")
    .where(F.col("status") == "active")            # filter early
    .select("customer_id", "amount", "category")   # project early
    .groupBy("category").agg(F.sum("amount").alias("total")))

# AVOID: rdd.map with a python lambda for column math
```

### 3. Environment management (the on-prem pain point)
Every executor runs Python. Driver and executor environments **must match**:
- Package a venv/conda env and ship it: `--archives env.tar.gz#env` with `spark.pyspark.python=./env/bin/python`.
- Pin versions of `pyspark`, `pyarrow`, `pandas` across the cluster.
- Mismatched versions → cryptic executor-only failures (esp. with Pandas UDFs).

## 🔍 Deep Dive: Testable PySpark structure
```python
# transforms.py — pure functions, no I/O, easy to unit test
def active_totals(df):
    return (df.where("status = 'active'")
              .groupBy("category").sum("amount"))

# job.py — wires I/O around the pure transform
def main(spark):
    df = spark.read.parquet("data/transactions")
    active_totals(df).write.mode("overwrite").parquet("out/")

# test_transforms.py — local SparkSession + tiny in-memory DataFrame
def test_active_totals(spark):
    df = spark.createDataFrame([(1, "active", "a", 10.0)], "id long, status string, category string, amount double")
    assert active_totals(df).count() == 1
```
Separating **pure transforms** from **I/O** makes jobs unit-testable with a local session (exactly what `common/spark_session.py` gives you).

## 💡 Key Insights for On-Premise / Zeppelin
### 1. Notebooks leak state
Zeppelin/Jupyter keep a long-lived session; stale cached DataFrames and redefined UDFs accumulate. Periodically restart the interpreter, and don't cache exploratory DataFrames you won't reuse.
### 2. One interpreter = shared resources
In Zeppelin, a shared Spark interpreter means notebooks compete for the same executors. Use per-user/per-note interpreter scoping on multi-tenant setups, and scheduler pools for fairness.
### 3. Arrow for `toPandas()`
Enable `spark.sql.execution.arrow.pyspark.enabled=true` so bounded `toPandas()` conversions are fast — but still only on **limited** result sets.

## 🎯 Practical Exercises

### Exercise 1: Refactor RDD/UDF code to DataFrame idioms
```python
# See exercises/production/exercise-32-pyspark-best-practices.py
# Take an anti-pattern (rdd.map + python udf) and rewrite it; compare plans.
```

### Exercise 2: Pure transform + local test
```python
# Extract a pure transform and test it with a tiny in-memory DataFrame.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Presence of `BatchEvalPython` (avoidable UDFs).
2. Driver memory during `toPandas()`/`collect()`.

### Spark UI Analysis
- SQL tab: confirm pushdown/pruning survive your transformations.

## 🚨 Common Issues & Solutions

### Issue 1: Works locally, fails on executors
**Symptom**: import/version error only when distributed.
**Solution**: cluster Python env mismatch — ship a matching env with `--archives`.

### Issue 2: Notebook slows down over time
**Symptom**: memory creeps up.
**Solution**: unpersist stale caches; restart the interpreter periodically.

## 📝 Key Takeaways
1. DataFrame/SQL over RDDs; built-ins over UDFs.
2. Filter and project early; don't collect big data.
3. Match the Python env across driver and all executors.
4. Separate pure transforms from I/O for testability.
5. Manage notebook/interpreter state and scoping on shared clusters.

## 🔗 Next Steps
- **Day 33**: Iceberg Fundamentals & Read/Write

## 📚 Additional Resources
- PySpark usage guide; Arrow-in-PySpark; Zeppelin Spark interpreter docs

---

**Progress**: Day 32/40 ✅
