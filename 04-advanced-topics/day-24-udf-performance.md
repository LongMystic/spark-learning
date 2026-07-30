# Day 24: UDF/UDAF & Pandas/Arrow UDF Performance

## 🎯 Learning Objectives
- Quantify the cost of Python UDFs and explain precisely why they hurt
- Use Pandas UDFs (vectorized, Arrow) and the pandas function APIs (`mapInPandas`, `applyInPandas`, `cogroup`)
- Choose the right UDF type: scalar, scalar-iterator, grouped map, grouped aggregate
- Tune Arrow batching and know when a UDF is unavoidable and how to make it as cheap as possible

## 📚 Core Concepts

### 1. The cost model

| Type | Transport | Granularity | Catalyst sees it? | Speed |
|------|-----------|-------------|-------------------|-------|
| Built-in expression | none (JVM) | codegen | yes | fastest |
| Pandas UDF (Arrow) | Arrow batches | vectorized | no (opaque) | fast |
| Python UDF | pickle, row-by-row | per row | no | slow |
| Scala UDF | none (JVM) | per row | partial | fast-ish |

**Key Points:**
- A Python UDF serializes each row to the Python worker process and back through a socket, row by row. At millions of rows, the serialization/deserialization and process-boundary crossing dominates the runtime — often 10-100x slower than an equivalent built-in.
- Catalyst cannot see inside *any* Python or Scala UDF's logic — it treats the whole call as an opaque black box, so no constant folding, no predicate reordering across it, no codegen fusion through it.
- A Python UDF shows as `BatchEvalPython` in the physical plan (still batched at the RPC layer, but conceptually row-at-a-time from Python's point of view); a Pandas UDF shows as `ArrowEvalPython`.

**Example:**
```python
df.withColumn("net", F.col("amount") * F.lit(0.9)).explain()      # Project, no eval node — pure codegen
df.withColumn("net", python_udf("amount")).explain()               # BatchEvalPython
df.withColumn("net", pandas_udf_fn("amount")).explain()             # ArrowEvalPython
```

### 2. Pandas UDF flavors

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Series -> Series (element-wise, vectorized) — most common case
@pandas_udf("double")
def net(amount: pd.Series) -> pd.Series:
    return amount * 0.9

# Iterator of Series -> Iterator of Series — same shape, but the function is
# called ONCE per worker for the whole iterator of batches, letting you
# amortize expensive setup (loading a model, opening a connection) once.
from typing import Iterator
@pandas_udf("double")
def net_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_expensive_model()   # loaded once per worker process, not per batch
    for s in batches:
        yield model.transform(s)

# Series -> scalar (used in groupBy().agg()) — grouped aggregate Pandas UDF
@pandas_udf("double")
def gmean(v: pd.Series) -> float:
    return float(v.prod() ** (1 / len(v)))
```

**Key Points:**
- **Scalar** Pandas UDFs receive and return `pd.Series` of the *same length* — they map cleanly onto a `SELECT`/`withColumn`.
- **Iterator of Series** is the same contract but amortizes per-worker setup cost across many batches — use it whenever the UDF has expensive initialization (ML model load, regex compilation, external client setup).
- **Grouped aggregate** Pandas UDFs (`Series -> scalar`) plug directly into `groupBy(...).agg(...)`, replacing what would otherwise require a Scala `Aggregator`-based UDAF.

### 3. Grouped map (`applyInPandas`) — whole group as a DataFrame

```python
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["z"] = (pdf["amount"] - pdf["amount"].mean()) / pdf["amount"].std()
    return pdf

schema = "customer_id long, amount double, z double"
txns.groupBy("customer_id").applyInPandas(normalize, schema)
```

**Key Points:**
- Powerful — arbitrary per-group Python/pandas logic — but the **entire group must fit in one executor's memory** as a single `pandas.DataFrame`. A skewed group (Day 10) becomes a per-task OOM risk, not just a slow task.
- `mapInPandas` is the non-grouped sibling: it maps whole **partitions** (not groups) through a pandas transform, useful for row-count-changing operations (filtering, exploding) without a `groupBy` at all.
- `cogroup().applyInPandas` joins two grouped DataFrames' pandas groups together in user code — an escape hatch for join logic pandas can express more easily than SQL, at the cost of both groups being fully materialized together.

### 4. Registering UDFs for SQL and choosing Arrow for scalar UDFs

```python
# Register a Python or Pandas UDF for use from spark.sql(...)
spark.udf.register("net_sql", net)          # usable as: SELECT net_sql(amount) FROM txns

# Spark 3.4+: ask a plain scalar Python UDF to use Arrow transport without
# rewriting it as a formal pandas_udf — same wire format, less code.
@F.udf("double", useArrow=True)
def net_arrow(amount):
    return amount * 0.9
```

**Key Points:**
- `useArrow=True` (or the session-wide `spark.sql.execution.pythonUDF.arrow.enabled`) gives a plain scalar `udf` Arrow-batched transport without requiring the pandas-Series function signature — a lighter migration path for existing UDFs.
- Registering via `spark.udf.register` makes the UDF callable from raw SQL strings and from BI tools hitting the Thrift Server (Day 29), not just the DataFrame API.

### 5. UDAFs: Scala `Aggregator` vs Python grouped-aggregate Pandas UDF

**Key Points:**
- Spark's typed Scala `Aggregator[IN, BUF, OUT]` API defines a true custom aggregate function (with a mutable buffer, merge, and finish step) that participates in partial aggregation before the shuffle — the same way built-in `sum`/`avg` do. It's JVM code, callable from SQL after registration, including from PySpark's `spark.sql(...)`.
- Python has no equivalent "typed aggregator with pre-shuffle partial aggregation" API — a Python grouped-aggregate `pandas_udf` (Core Concept 2) runs **after** the shuffle, on the fully grouped data per key, not as a partial-aggregation step. For most business logic this difference is invisible; for very high-cardinality grouping keys with expensive per-group computation, a Scala `Aggregator` can meaningfully reduce shuffle volume where a Python grouped-agg UDF cannot.
- Practical rule of thumb: if the aggregate logic is simple math (mean, weighted average, geometric mean), a Pandas grouped-aggregate UDF is simplest. If it needs partial pre-shuffle combining for performance at scale, and a team has Scala/JVM skills, a custom `Aggregator` registered as a SQL function is the better tool — and it's directly usable from `spark.sql()` in a PySpark job once registered.

```python
# Once a Scala Aggregator is packaged as a jar and registered as a SQL function
# (e.g. via spark.udf.registerJavaUDAF in older APIs, or a SparkSessionExtensions
# function injection, Day 22), PySpark uses it exactly like a built-in:
spark.sql("SELECT category, my_scala_udaf(amount) FROM transactions GROUP BY category")
```

## 🔍 Deep Dive: Replace, vectorize, or isolate

### Step-by-Step decision process
1. **Can a built-in do it?** (`when`, `regexp_extract`, `transform`, date/math functions, higher-order functions from Day 23) → use it. This is the only option Catalyst can optimize through.
2. **Row-wise Python math/logic on columns?** → **scalar Pandas UDF**. If setup cost is significant, use the **iterator** variant.
3. **Per-group custom logic that genuinely needs the whole group at once** (e.g. group-wise normalization, per-entity time series ops)? → `applyInPandas`, but budget for group-size skew.
4. **Only then**, a plain Python UDF — and make it do the absolute minimum: no per-row object construction, no repeated regex compilation, no network calls per row.

### Example: amortizing expensive setup with the iterator API
```python
from typing import Iterator
import re

@pandas_udf("string")
def extract_domain(urls: Iterator[pd.Series]) -> Iterator[pd.Series]:
    pattern = re.compile(r"https?://([^/]+)/?")   # compiled ONCE per worker, not per row/batch
    for s in urls:
        yield s.str.extract(pattern, expand=False)
```

**Analysis:**
- Compiling the regex inside a scalar (non-iterator) Pandas UDF would still only happen once *per batch* rather than once per row — already much better than a plain Python UDF — but the iterator form amortizes it once per **worker process lifetime**, the cheapest option available.

### Arrow settings
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")       # required for pandas() toPandas()/createDataFrame() fast path
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")   # rows per Arrow batch handed to the UDF
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")  # fall back to row-at-a-time on Arrow errors (mask failures — use for migration only)
```

## 💡 Key Insights for On-Premise

### 1. Pandas UDFs need pyarrow on every executor
Every executor's Python environment must have compatible `pyarrow`/`pandas` versions — mismatched versions between driver and executors, or between executors themselves, are a classic on-prem failure mode. On Kubernetes you **bake `pyspark`/`pyarrow`/`pandas` into the container image** and point both driver and executors at the *same* image (`spark.kubernetes.container.image` in the `SparkApplication` spec), so every pod is byte-for-byte identical. This actually SOLVES the driver/executor version-skew problem more cleanly than YARN-style per-node Python installs did, because there is no per-node Python to drift out of sync. (Shipping a conda/venv archive with `--archives` still works if you must override the image's baked-in environment for a specific job.)

### 2. UDFs inflate memoryOverhead
Python worker processes live **off-heap** from the JVM executor's perspective — they're separate OS processes communicating over a socket. Heavy Pandas UDF workloads (large batches, big model objects held in worker memory) need a correspondingly larger `spark.executor.memoryOverhead` (Day 16), or the executor pod gets OOM-killed by Kubernetes even though the *JVM heap* looks fine.

### 3. Batch size is a memory/throughput tradeoff
`spark.sql.execution.arrow.maxRecordsPerBatch` trades off per-batch overhead against per-batch memory: larger batches amortize the Python call overhead further but hold more rows in memory as a pandas Series/DataFrame at once inside the worker. On memory-constrained on-prem executor pods, tune this down before scaling executor memory up.

## 🎯 Practical Exercises

### Exercise 1: Three ways, one transform (see `exercises/advanced/exercise-24-udf-performance.py`)
```python
# Built-in
txns.withColumn("net", F.col("amount") * F.lit(0.9)).explain()

# Python UDF -> BatchEvalPython
@F.udf("double")
def net_udf(amount):
    return amount * 0.9
txns.withColumn("net", net_udf("amount")).explain()

# Pandas UDF -> ArrowEvalPython
@pandas_udf("double")
def net_pandas(s: pd.Series) -> pd.Series:
    return s * 0.9
txns.withColumn("net", net_pandas("amount")).explain()

# At --scale medium/large, time .count() for each and compare
```

### Exercise 2: Grouped aggregate UDF
```python
# Geometric mean of amount per category via a grouped-aggregate Pandas UDF
@pandas_udf("double")
def gmean(v: pd.Series) -> float:
    import numpy as np
    return float(np.exp(np.log(v.clip(lower=1e-9)).mean()))

txns.groupBy("category").agg(gmean("amount").alias("geo_mean_amount")).show()
```

### Exercise 3: Amortize setup with the iterator API
```python
from typing import Iterator
import time

# Simulate an expensive one-time setup cost (e.g. loading a model/lookup table)
@pandas_udf("double")
def slow_scalar(batches: pd.Series) -> pd.Series:
    time.sleep(0.5)   # pretend this is model-loading, paid EVERY batch
    return batches * 1.1

@pandas_udf("double")
def fast_iterator(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    time.sleep(0.5)   # paid ONCE per worker process, not per batch
    for s in batches:
        yield s * 1.1

# 1. Run both over a DataFrame with many partitions/batches and time each with .count()
# 2. Explain why the iterator variant's relative advantage grows with batch count
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **SQL-tab node type**: `BatchEvalPython` (slow, row-by-row) vs `ArrowEvalPython` (vectorized) — confirms which transport a given UDF actually uses.
2. **Off-heap/overhead memory** during UDF stages — check executor pod memory (RSS) vs configured `memoryOverhead` in the Kubernetes dashboard or Spark UI executors tab.
3. **Python worker startup count** — repeated worker restarts (visible in executor stderr logs) signal crashes or idle-timeout churn, both adding latency.
4. **Batch count vs row count** for Pandas UDFs — very small `maxRecordsPerBatch` relative to partition size means excess per-batch overhead.

### Spark UI Analysis
- **SQL tab**: confirm a filter placed logically "above" a UDF isn't silently evaluated per-row alongside it — Catalyst cannot push a filter *through* a UDF boundary, so check the plan and manually reorder (`filter().withColumn(udf(...))`) so cheap filters run first and shrink what reaches the UDF.
- **Executors tab**: compare "Peak JVM Memory" against actual pod memory usage — a gap that keeps growing under Pandas UDF load points at off-heap Python worker memory, not the JVM heap.

## 🚨 Common Issues & Solutions

### Issue 1: Pandas UDF errors on executors only
**Symptom**: Works fine on the driver/locally, fails distributed with an Arrow/serialization error.
**Root Cause**: `pyarrow`/`pandas` missing or version-mismatched on executors relative to the driver.
**Solution**: Rebuild the container image with pinned, matching `pyarrow`/`pandas` versions and use that **same image** for both driver and executors (`spark.kubernetes.container.image`) so their Python environments are identical byte-for-byte.

### Issue 2: `applyInPandas` OOMs
**Symptom**: One task fails with an out-of-memory error while others succeed.
**Root Cause**: The grouping key is skewed — one group is far larger than the rest and must be materialized whole as a single pandas DataFrame in executor memory.
**Solution**: Pre-aggregate before the grouped-map call where possible, split and separately process the known hot group, or avoid whole-group materialization by re-expressing the logic as a scalar/grouped-aggregate Pandas UDF instead.

### Issue 3: Pandas UDF return schema mismatch
**Symptom**: `PythonException`/schema errors like "Number of columns of the returned pandas.DataFrame doesn't match" or silently wrong column order.
**Root Cause**: The function's returned pandas object doesn't exactly match the declared `pandas_udf`/`applyInPandas` schema in column count, order, or dtype.
**Solution**: Explicitly construct and name every output column to match the declared schema string exactly, and add an assertion in development (`assert list(pdf.columns) == expected_cols`) before shipping.

### Issue 4: UDF closure captures a huge object
**Symptom**: Task serialization is slow, or executors fail with a large-object serialization/broadcast warning.
**Root Cause**: The UDF's Python closure accidentally captures a large object from the driver's scope (a full DataFrame collected to a list, a big dict) which then gets pickled and shipped with every task.
**Solution**: Move large lookup data into a `broadcast()` variable (Day 25) and reference `bc.value` inside the UDF instead of closing over the raw object directly.

### Issue 5: Filter placed after a UDF still costs the UDF's price
**Symptom**: A `.filter()` written after `.withColumn("x", py_udf(...))` doesn't reduce UDF invocation count.
**Root Cause**: Catalyst cannot see inside the UDF to know the filter could logically move earlier — pushdown stops at the UDF boundary.
**Solution**: Manually reorder so cheap, built-in filters run **before** the UDF call in the code, shrinking the row count the UDF actually has to process.

## 📝 Key Takeaways
1. Python UDFs are row-by-row and optimizer-opaque — avoid them whenever a built-in or higher-order function can do the job.
2. Vectorize with Pandas UDFs (Arrow, `ArrowEvalPython`) when Python logic is genuinely required; use the iterator variant to amortize expensive setup once per worker.
3. `applyInPandas`/`mapInPandas`/`cogroup` unlock arbitrary pandas logic but require each group/partition to fit in memory — beware skew.
4. Pandas UDFs require matching `pyarrow`/`pandas` on every executor — a single baked container image fixes this cleanly on Kubernetes.
5. Manually filter before UDFs (Catalyst can't push through them); watch `memoryOverhead` for off-heap Python worker memory.
6. Broadcast large lookup data instead of closing over it, to avoid re-shipping it with every task.

## 🔗 Next Steps
- **Day 25**: Broadcast Strategies & AQE Deep Dive

## 📚 Additional Resources
- Spark "pandas function APIs" docs (`applyInPandas`, `mapInPandas`, `cogroup`)
- PySpark `pandas_udf` API reference and Arrow integration guide
- `spark.sql.execution.arrow.*` configuration reference

---

**Progress**: Day 24/40 ✅
