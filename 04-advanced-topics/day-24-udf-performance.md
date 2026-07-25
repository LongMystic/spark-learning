# Day 24: UDF/UDAF & Pandas/Arrow UDF Performance

## 🎯 Learning Objectives
- Quantify the cost of Python UDFs and why they hurt
- Use Pandas UDFs (vectorized, Arrow) and the pandas function APIs
- Choose the right UDF type: scalar, grouped map, grouped agg
- Know when a UDF is unavoidable and how to make it as cheap as possible

## 📚 Core Concepts

### 1. The cost model
| Type | Transport | Granularity | Catalyst sees it? | Speed |
|------|-----------|-------------|-------------------|-------|
| Built-in expression | none (JVM) | codegen | yes | fastest |
| Pandas UDF (Arrow) | Arrow batches | vectorized | no (opaque) | fast |
| Python UDF | pickle, row-by-row | per row | no | slow |
| Scala UDF | none (JVM) | per row | partial | fast-ish |

A Python UDF serializes each row to the Python worker and back. At millions of rows the boundary crossing dominates.

### 2. Pandas UDF flavors
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Scalar: Series -> Series (element-wise, vectorized)
@pandas_udf("double")
def net(amount: pd.Series) -> pd.Series:
    return amount * 0.9

# Grouped aggregate: Series -> scalar (used in groupBy().agg())
@pandas_udf("double")
def gmean(v: pd.Series) -> float:
    return float(v.prod() ** (1 / len(v)))
```

### 3. Grouped map (applyInPandas) — whole group as a DataFrame
```python
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["z"] = (pdf["amount"] - pdf["amount"].mean()) / pdf["amount"].std()
    return pdf

schema = "customer_id long, amount double, z double"
txns.groupBy("customer_id").applyInPandas(normalize, schema)
```
Powerful, but the **whole group must fit in one executor's memory** — dangerous with skew.

## 🔍 Deep Dive: Replace, vectorize, or isolate

### Step-by-Step decision
1. **Can a built-in do it?** (`when`, `regexp_extract`, `transform`, date/math functions) → use it.
2. **Row-wise Python math on columns?** → **scalar Pandas UDF**.
3. **Per-group custom logic?** → `applyInPandas` (watch group size).
4. **Only then** a plain Python UDF, and make it do minimal work.

### Arrow settings
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

## 💡 Key Insights for On-Premise
### 1. Pandas UDFs need pyarrow on every executor
Every executor's Python environment must have compatible `pyarrow`/`pandas`. Version mismatches between driver and executors are a classic on-prem failure. On Kubernetes you **bake `pyspark`/`pyarrow`/`pandas` into the container image** and point driver and executors at the *same* image (`spark.kubernetes.container.image`), so every pod is byte-for-byte identical — this actually SOLVES the driver/executor version-skew problem more cleanly than YARN did, because there is no per-node Python to drift. (Shipping a conda/venv archive with `--archives` still works if you must override the image's env.)

### 2. UDFs inflate memoryOverhead
Python workers live off-heap. Heavy Pandas UDFs → raise `spark.executor.memoryOverhead` (Day 16).

## 🎯 Practical Exercises

### Exercise 1: Three ways, one transform
```python
# See exercises/advanced/exercise-24-udf-performance.py
# Built-in vs Python UDF vs Pandas UDF: compare plans and (at scale) timing.
```

### Exercise 2: Grouped aggregate UDF
```python
# Geometric mean of amount per category via a grouped-agg Pandas UDF.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. SQL-tab node: `BatchEvalPython` (slow) vs `ArrowEvalPython` (vectorized).
2. Off-heap/overhead memory during UDF stages.

### Spark UI Analysis
- A filter above a UDF isn't pushed below it — confirm in the plan and reorder so filters run first.

## 🚨 Common Issues & Solutions

### Issue 1: Pandas UDF errors on executors only
**Symptom**: works on driver, fails distributed.
**Solution**: pyarrow/pandas missing or mismatched on executors — rebuild the container image with the right versions and use that **same image** for driver and executors so their Python envs are identical.

### Issue 2: applyInPandas OOMs
**Symptom**: one group too big for memory.
**Solution**: the group is skewed — pre-aggregate, split the hot group, or avoid whole-group materialization.

## 📝 Key Takeaways
1. Python UDFs are row-by-row and optimizer-opaque — avoid when possible.
2. Vectorize with Pandas UDFs (Arrow) when Python is truly needed.
3. `applyInPandas` needs each group to fit in memory — beware skew.
4. Pandas UDFs require matching pyarrow/pandas on all executors.
5. Filter before UDFs; watch memoryOverhead.

## 🔗 Next Steps
- **Day 25**: Broadcast Strategies & AQE Deep Dive

## 📚 Additional Resources
- Spark "pandas function APIs" and Arrow integration docs

---

**Progress**: Day 24/40 ✅
