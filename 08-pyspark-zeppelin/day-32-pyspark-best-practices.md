# Day 32: PySpark Best Practices & Zeppelin

## 🎯 Learning Objectives
- Write PySpark that is fast, testable, and optimizer-friendly
- Understand exactly why RDDs and Python UDFs bypass Catalyst/Tungsten
- Manage the Python environment across the cluster
- Use Arrow to speed up Pandas UDFs and `toPandas()`/`createDataFrame()`
- Use Zeppelin/notebooks productively without the usual traps
- Structure real PySpark projects for maintenance and unit testing

## 📚 Core Concepts

### 1. The golden rules

**Key Points:**
- **Prefer DataFrame/SQL over RDDs** — RDDs bypass Catalyst/Tungsten (no codegen, no pushdown, no whole-stage optimization).
- **Avoid Python UDFs** — use built-ins or Pandas/Arrow UDFs (Day 24) when a built-in genuinely can't express the logic.
- **Project & filter early** — read only the columns/partitions you need, so pushdown/pruning can do their job (Day 16).
- **Don't `collect()` big data** to the driver — it defeats distributed execution and risks driver OOM (Day 16).
- **Cache only reused, expensive results** — and `unpersist()` when done (Day 7).

**Example:**
```python
from pyspark.sql import functions as F

# GOOD: expression-based, chainable, pushdown-friendly
result = (spark.read.parquet("data/transactions")
    .where(F.col("status") == "active")            # filter early
    .select("customer_id", "amount", "category")   # project early
    .groupBy("category").agg(F.sum("amount").alias("total")))

# AVOID: rdd.map with a python lambda for column math
```

### 2. Why RDDs and Python UDFs are expensive

**Key Points:**
- A DataFrame operation compiles to a Catalyst logical/physical plan, which Spark's whole-stage codegen turns into JVM bytecode — data never leaves the JVM, and the optimizer can reorder/push down/prune around it.
- `df.rdd.map(...)` forces Spark to materialize `Row` objects and hand them to a Python lambda — this exits the Catalyst plan entirely, so nothing downstream of the RDD conversion can be optimized, and every row pays Python object overhead.
- A plain Python UDF (`@udf`) runs **row-at-a-time**: each executor JVM serializes rows out to a Python worker process, the worker runs your function, and results are serialized back — this round trip is the dominant cost, not the Python code itself.
- Pandas UDFs (`@pandas_udf`) instead ship **whole columns as Arrow batches** to the Python worker, so the worker operates on vectorized Pandas Series instead of one row at a time — dramatically fewer serialization round trips for the same data volume.

**Example:**
```python
# AVOID: row-at-a-time Python UDF
@F.udf("double")
def net_amount(amount):
    return amount * 0.9

# BETTER: built-in expression (fully pushed down, no python round trip)
df.withColumn("net", F.col("amount") * F.lit(0.9))

# WHEN you truly need custom Python logic: vectorized Pandas UDF
import pandas as pd
@F.pandas_udf("double")
def net_amount_vec(amount: pd.Series) -> pd.Series:
    return amount * 0.9   # runs on a whole Arrow batch, not row-by-row
```

### 3. Environment management (the on-prem pain point)

Every executor runs Python. Driver and executor environments **must match**:

**Key Points:**
- Package a venv/conda env and ship it: `--archives env.tar.gz#env` with `spark.pyspark.python=./env/bin/python`.
- Pin versions of `pyspark`, `pyarrow`, and `pandas` across the cluster — Arrow's wire format has changed across versions historically, and mismatches surface as opaque executor-only failures.
- On Kubernetes, the cleanest fix is often to **bake the Python environment into the executor image** (Day 17's custom Spark image) rather than shipping archives per job, since every executor pod starts from the same image anyway.
- Mismatched versions → cryptic executor-only failures (esp. with Pandas UDFs, since Arrow serialization is version-sensitive).

**Example:**
```bash
# Ship a matching venv as an archive (works with any deploy mode)
spark-submit \
  --archives env.tar.gz#env \
  --conf spark.pyspark.python=./env/bin/python \
  --conf spark.pyspark.driver.python=./env/bin/python \
  job.py
```
```dockerfile
# Or: bake it into the Kubernetes executor image (preferred on-prem approach)
FROM apache/spark:3.5.1
COPY requirements.txt .
RUN pip install -r requirements.txt   # pyspark, pyarrow, pandas pinned to matching versions
```

### 4. Arrow-accelerated Pandas interop

**Key Points:**
- `spark.sql.execution.arrow.pyspark.enabled=true` makes `toPandas()` and `createDataFrame()` (from a Pandas DataFrame) use Arrow's columnar format to transfer data, instead of row-by-row serialization — often an order of magnitude faster for medium-sized transfers.
- This setting also controls whether Pandas UDFs use Arrow batching under the hood — it's on by default in modern Spark, but worth confirming explicitly in cluster configs.
- Arrow acceleration does **not** change the fundamental rule: `toPandas()` still collects the full result to the **driver**. Only use it on already-aggregated or filtered, bounded result sets — never on a raw fact table.
- `spark.sql.execution.arrow.maxRecordsPerBatch` controls the Arrow batch size for Pandas UDFs — tune it if a UDF's per-batch memory footprint is a concern.

**Example:**
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Safe: bounded, already-aggregated result
summary_pdf = df.groupBy("category").agg(F.sum("amount").alias("total")).toPandas()

# UNSAFE: raw fact table straight to the driver, Arrow or not
# huge_pdf = df.toPandas()   # DON'T
```

### 5. Project structure for real PySpark jobs

**Key Points:**
- A typical maintainable layout separates **entrypoints** (`jobs/*.py`, thin, I/O-only) from **transforms** (`transforms/*.py`, pure functions) from **shared utilities** (`common/spark_session.py` in this course's repo, session creation and table reads).
- Keep configuration (paths, table names, thresholds) out of transform functions — pass them in as arguments so the same transform is reusable across environments (local test, staging, production) without editing code.
- Package shared code as a proper installable module (or at least a consistent `sys.path` convention, as the exercises in this repo do) so `spark-submit` jobs and unit tests import the same code, not copy-pasted duplicates that drift apart.
- Log meaningfully at the entrypoint level (row counts read/written, key config values) — pure transforms shouldn't log, since that's an I/O side effect that complicates testing.

**Example:**
```
repo/
  common/spark_session.py       # get_spark(), read_table() — shared, I/O-aware
  transforms/billing.py         # pure functions: active_totals(df), net_amount(df)
  jobs/daily_billing_job.py     # main(spark): read -> transforms.billing.* -> write
  tests/test_billing.py         # local SparkSession + tiny DataFrames
```

## 🔍 Deep Dive: Testable PySpark structure

### Step-by-Step Process

1. **Separate pure transforms from I/O.** A pure transform takes DataFrames in, returns DataFrames out, and touches no files/tables/network — this is what makes it unit-testable without a cluster.
2. **Keep a thin `main()`/job entrypoint** that only does I/O (`spark.read`, `.write`) and wires transforms together — this is the part that's hard to unit test and should stay minimal.
3. **Write unit tests against a local `SparkSession`** with tiny in-memory DataFrames (a handful of rows) — fast enough to run on every commit, no cluster or files required.
4. **Use `explain()` in development** to confirm a refactor didn't lose pushdown/pruning — compare the physical plan before and after.
5. **Keep the local test suite fast** by reusing one `SparkSession` fixture across tests instead of creating a new one per test (JVM startup dominates test time otherwise).

### Example: Pure transform + local test

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

**Analysis:**
- Separating **pure transforms** from **I/O** makes jobs unit-testable with a local session (exactly what `common/spark_session.py` gives you) — `active_totals` needs no files, no cluster, and no mocks.
- Because `active_totals` is pure DataFrame logic, `.explain()` on it shows the same physical plan shape whether it runs on 3 rows locally or 1M rows on the cluster — what you verify in a fast unit test generalizes to production behavior.
- This structure also makes it trivial to compare an RDD/UDF anti-pattern against its DataFrame rewrite side by side (Exercise 1) — both take the same input DataFrame and can be asserted equal on output.

## 💡 Key Insights for On-Premise / Zeppelin

### 1. Notebooks leak state
Zeppelin/Jupyter keep a long-lived session; stale cached DataFrames and
redefined UDFs accumulate across cells and across days if the interpreter
isn't restarted. Periodically restart the interpreter, and don't cache
exploratory DataFrames you won't reuse.

### 2. One interpreter = shared resources
In Zeppelin, a shared Spark interpreter means notebooks compete for the
same executors. Use per-user/per-note interpreter scoping on multi-tenant
setups, and scheduler pools (Day 29) for fairness between concurrent
notebook users.

### 3. Arrow for `toPandas()`
Enable `spark.sql.execution.arrow.pyspark.enabled=true` so bounded
`toPandas()` conversions are fast — but still only on **limited** result
sets; Arrow makes the transfer faster, it doesn't make collecting a huge
table to the driver safe.

### 4. Ship one blessed environment, not per-notebook pip installs
On a shared on-prem cluster, letting every notebook `pip install` whatever
it wants leads to exactly the driver/executor mismatch described above, but
worse — different notebooks on the same cluster can want different
versions. Standardize on one image/venv per Spark version and treat
library upgrades as a coordinated change, not an ad-hoc notebook action.

## 🎯 Practical Exercises

### Exercise 1: Refactor RDD/UDF code to DataFrame idioms
```python
# See exercises/production/exercise-32-pyspark-best-practices.py

# 1. Anti-pattern: rdd.map + python logic (bypasses Catalyst/Tungsten)
rdd_way = (txns.rdd
    .filter(lambda r: r["status"] == "active")
    .map(lambda r: (r["category"], r["amount"] * 0.9)))

# 2. Idiomatic DataFrame version (pushdown + codegen)
def active_net_by_category(df):
    """Pure transform: no I/O -> easy to unit test with a tiny in-memory DataFrame."""
    return (df.where(F.col("status") == "active")
              .withColumn("net", F.col("amount") * F.lit(0.9))
              .groupBy("category").agg(F.sum("net").alias("net_total")))

result = active_net_by_category(txns)
result.explain()   # compare this plan against the RDD version's absence of one
result.show(5)
```

### Exercise 2: Pure transform + local test
```python
# Extract a pure transform and test it with a tiny in-memory DataFrame.
tiny = spark.createDataFrame(
    [(1, "active", "a", 100.0), (2, "cancelled", "a", 50.0), (3, "active", "b", 200.0)],
    "customer_id long, status string, category string, amount double",
)
active_net_by_category(tiny).show()
# -> pure transforms make unit tests trivial (no cluster, no files).
```

### Exercise 3: Arrow-enabled Pandas round trip
```python
# Enable Arrow, then compare toPandas() timing on a small aggregated result
# with and without spark.sql.execution.arrow.pyspark.enabled.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
agg = active_net_by_category(txns)
pdf = agg.toPandas()   # bounded (one row per category) -> safe to collect
print(pdf)
```

### Exercise 4: Vectorized Pandas UDF vs plain UDF plan comparison
```python
import pandas as pd

@F.udf("double")
def net_amount_row(amount):
    return amount * 0.9

@F.pandas_udf("double")
def net_amount_vec(amount: pd.Series) -> pd.Series:
    return amount * 0.9

txns.withColumn("net", net_amount_row(F.col("amount"))).explain()   # look for BatchEvalPython
txns.withColumn("net", net_amount_vec(F.col("amount"))).explain()   # look for ArrowEvalPython
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Presence of `BatchEvalPython`/`ArrowEvalPython` in physical plans** — the former signals row-at-a-time Python UDFs (avoidable), the latter signals vectorized Pandas UDFs (much cheaper, but still not free).
2. **Driver memory during `toPandas()`/`collect()`** — a spike here on a job that "just does some Pandas conversion" is the classic sign of a missing filter before collecting.
3. **Python worker CPU/memory on executors** (via `kubectl top pods` or executor logs) — high Python worker time relative to JVM task time points at UDF-heavy stages.
4. **Task count and skew for RDD-based stages** — RDD code often loses the partition-pruning/pushdown that keeps DataFrame stages balanced.

### Spark UI Analysis
- The **SQL tab** is the primary tool here: open a query's physical plan and look for `BatchEvalPython` (plain UDF, row-at-a-time) vs `ArrowEvalPython` (Pandas UDF, vectorized) vs neither (fully native, ideal).
- Confirm pushdown/pruning **survive** your transformations — a `ParquetScan` node should show `PushedFilters` and a reduced set of read columns; if a UDF or RDD conversion appears upstream of a scan in the plan, check it hasn't blocked pruning.
- The **Executors tab** shows per-executor task time; stages with heavy Python UDF usage often show longer task durations without a corresponding increase in shuffle read/write, which is a tell that time is going into Python serialization, not I/O or compute.
- The **Stages tab**'s task metrics can be compared before/after a UDF-to-builtin refactor to quantify the improvement.

## 🚨 Common Issues & Solutions

### Issue 1: Works locally, fails on executors
**Symptom**: Import error or version mismatch that only appears when the job runs distributed, never when running a script directly against a local `SparkSession`.
**Root Cause**: Cluster Python env mismatch — the driver's local environment has a package/version the executor pods don't, or vice versa.
**Solution**: Ship a matching env with `--archives`, or (preferred on Kubernetes) bake the exact same Python environment into the executor image so driver and executors are guaranteed identical.

### Issue 2: Notebook slows down over time
**Symptom**: Memory creeps up and cell execution gets slower over a multi-hour Zeppelin/Jupyter session, even though the underlying data hasn't grown.
**Root Cause**: Accumulated cached DataFrames and redefined UDFs/closures held alive by the long-running interpreter session.
**Solution**: Unpersist stale caches explicitly (`df.unpersist()`), and restart the interpreter periodically (start of day, or after heavy exploratory work) rather than letting it run for days.

### Issue 3: Pandas UDF fails with an Arrow-related error
**Symptom**: `pyarrow.lib.ArrowInvalid` or similar serialization errors specific to Pandas UDFs, absent from plain-Python-UDF equivalents.
**Root Cause**: Mismatched `pyarrow` versions between driver and executors, or a Pandas UDF return type that doesn't match the declared Spark schema.
**Solution**: Pin `pyarrow` (and `pandas`) to the same version everywhere the environment is built, and double-check the UDF's declared return type matches what the Pandas Series/DataFrame actually produces.

### Issue 4: `toPandas()` OOMs the driver
**Symptom**: Driver OOM or a hang, previously fine on a smaller dataset.
**Root Cause**: The DataFrame being collected grew (more rows, a filter was loosened, or an upstream table grew) beyond what the driver's memory can hold as a Pandas DataFrame.
**Solution**: Aggregate/filter down to a bounded result before calling `toPandas()`, or use `.limit(n)` deliberately for exploratory work, treating `toPandas()` as a reporting/summary step, never a full-data-extraction step.

### Issue 5: Same job, wildly different performance in a notebook vs `spark-submit`
**Symptom**: A transform runs fine in isolation via `spark-submit` but is slow inside a shared Zeppelin notebook.
**Root Cause**: The shared interpreter's SparkContext is contending with other notebooks/users for the same executor pool, or a scheduler pool isn't configured for fairness (Day 29).
**Solution**: Check `kubectl top pods`/the Executors tab for contention from other notebooks, and apply FAIR scheduler pools or per-note interpreter scoping so one notebook's heavy exploration doesn't starve others.

## 📝 Key Takeaways
1. DataFrame/SQL over RDDs; built-ins over UDFs — and Pandas/Arrow UDFs when custom Python logic is unavoidable.
2. RDDs and plain UDFs cost you Catalyst optimization and pay per-row Python serialization; Pandas UDFs vectorize that cost via Arrow.
3. Filter and project early; don't collect big data — Arrow speeds up `toPandas()` but doesn't make collecting unbounded data safe.
4. Match the Python env across driver and all executors — baking it into the image is the most reliable on-prem approach.
5. Separate pure transforms from I/O for testability; test locally with tiny in-memory DataFrames.
6. Manage notebook/interpreter state and scoping on shared clusters — restart periodically, unpersist stale caches, use scheduler pools.

## 🔗 Next Steps
- **Day 33**: Iceberg Fundamentals & Read/Write

## 📚 Additional Resources
- PySpark usage guide (RDD vs DataFrame APIs)
- Apache Arrow in PySpark documentation (`toPandas()`, Pandas UDFs, config knobs)
- Zeppelin Spark interpreter documentation (interpreter scoping, per-note isolation)

---

**Progress**: Day 32/40 ✅
