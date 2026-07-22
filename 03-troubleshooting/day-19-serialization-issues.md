# Day 19: Serialization & UDF Issues

## 🎯 Learning Objectives
- Understand why "Task not serializable" happens and how closures capture state
- Fix the classic PySpark and Scala serialization traps
- Diagnose slow/failing UDFs and prefer built-ins / Pandas UDFs
- Use Kryo and broadcast correctly

## 📚 Core Concepts

### 1. Why serialization exists
Spark ships your **closures** (the functions inside `map`, `filter`, UDFs) and any variables they capture from the driver to the executors. Everything captured must be **serializable**. If a closure drags along a database connection, a `SparkSession`, or a whole enclosing object, serialization fails.

```
org.apache.spark.SparkException: Task not serializable
Caused by: java.io.NotSerializableException: com.acme.MyService
```

### 2. The classic traps

**Scala/Java** — capturing `this`:
```scala
class Job(val svc: NonSerializableService) {
  def run(df: DataFrame) = df.map(r => svc.enrich(r))   // captures `this` -> svc -> NotSerializable
}
// Fix: pull the needed value into a local val, or make the field @transient lazy.
```

**PySpark** — capturing a connection or the session in a UDF:
```python
conn = make_db_connection()          # not picklable
@udf("string")
def enrich(x):
    return conn.lookup(x)            # BAD: `conn` captured into the UDF -> pickling error / reused across tasks
```
Fix: create the resource **inside** the executor, once per partition:
```python
def enrich_partition(rows):
    conn = make_db_connection()      # created on the executor
    for r in rows:
        yield conn.lookup(r)
df.rdd.mapPartitions(enrich_partition)
```

### 3. UDFs are a performance cliff
- Python UDFs cross the JVM↔Python boundary **per row** and are **opaque to Catalyst** (no predicate pushdown, no codegen).
- Order of preference: **built-in SQL functions** → **Pandas/Arrow UDFs** (vectorized) → plain Python UDF (last resort).

## 🔍 Deep Dive

### Replace a UDF with built-ins
```python
from pyspark.sql import functions as F
# BAD: python udf
@F.udf("double")
def to_net(amount): return amount * 0.9
# GOOD: expression — pushdown + codegen
df.withColumn("net", F.col("amount") * F.lit(0.9))
```

### Pandas UDF when you truly need Python
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def zscore(v: pd.Series) -> pd.Series:      # vectorized: whole batches, Arrow transport
    return (v - v.mean()) / v.std()
df.withColumn("z", zscore("amount"))
```

### Kryo (Scala/RDD-heavy jobs)
```bash
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.kryo.registrationRequired=false
```
Kryo is faster/smaller than Java serialization for RDD data; the DataFrame API already uses efficient internal encoders, so Kryo matters most for RDD/custom-object workloads.

## 💡 Key Insights for On-Premise
### 1. Broadcast big read-only lookups
Don't capture a large dict in every task closure — **broadcast** it once:
```python
lookup = spark.sparkContext.broadcast(big_dict)
@F.udf("string")
def f(x): return lookup.value.get(x)
```
### 2. One connection per partition, not per row
Opening a connection per row (inside a UDF) is both a serialization smell and a throughput killer. Use `mapPartitions` / `foreachPartition`.

## 🎯 Practical Exercises

### Exercise 1: Break and fix serialization
```python
# See exercises/troubleshooting/exercise-19-serialization.py
# Trigger "Task not serializable", then fix by localizing the captured value.
```

### Exercise 2: UDF vs built-in vs Pandas UDF
```python
# Compare plans (explain) and timing for the same transform three ways.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Task Deserialization Time** (Stages tab) — high = heavy closures.
2. **UDF time** vs total task time — Python UDF overhead shows as high compute with no shuffle.

### Spark UI Analysis
- SQL tab: a `BatchEvalPython` / `ArrowEvalPython` node marks a Python/Pandas UDF boundary.
- No pushdown below a UDF filter = Catalyst can't see through it.

## 🚨 Common Issues & Solutions

### Issue 1: "Task not serializable" referencing your class
**Symptom**: closure captured `this`.
**Solution**: copy needed fields into local vals, or mark heavy fields `@transient lazy`.

### Issue 2: UDF job is correct but 10× too slow
**Symptom**: high CPU, boundary crossing per row.
**Solution**: rewrite with built-ins, or vectorize with a Pandas UDF.

## 📝 Key Takeaways
1. Closures ship captured state — it must be serializable.
2. Create connections **on the executor**, per partition — never capture them.
3. Built-ins > Pandas UDF > Python UDF (performance and optimizer visibility).
4. Broadcast large read-only lookups.
5. Kryo helps RDD/custom-object jobs; DataFrames already use encoders.

## 🔗 Next Steps
- **Day 20**: Performance Debugging (Spark UI & SQL tab)
- Practice: find one Python UDF at work and replace it with built-ins.

## 📚 Additional Resources
- Spark UDF & Pandas UDF (pandas function API) docs
- Kryo serialization tuning

---

**Progress**: Day 19/40 ✅
