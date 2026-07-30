# Day 19: Serialization & UDF Issues

## 🎯 Learning Objectives
- Understand why "Task not serializable" happens and how closures capture state
- Fix the classic PySpark and Scala serialization traps
- Diagnose slow/failing UDFs and prefer built-ins / Pandas UDFs
- Use Kryo and broadcast correctly
- Read a query plan to spot where Catalyst loses visibility across a UDF boundary

## 📚 Core Concepts

### 1. Why serialization exists

Spark ships your **closures** (the functions inside `map`, `filter`, UDFs) and any variables they capture from the driver to the executors. Everything captured must be **serializable**. If a closure drags along a database connection, a `SparkSession`, or a whole enclosing object, serialization fails.

```
org.apache.spark.SparkException: Task not serializable
Caused by: java.io.NotSerializableException: com.acme.MyService
```

**Key Points:**
- This happens at **submission time on the driver**, before any task is scheduled — Spark serializes the closure to ship it, and if that serialization step fails, the job never even reaches an executor.
- The exception names the *first* non-serializable class it hits while walking the object graph — but the real culprit might be several fields deep (e.g. your class holds a config object, which holds a connection pool, which is what's actually unserializable).
- In PySpark the underlying mechanism is `cloudpickle`, not Java serialization — the concept ("everything captured must survive being shipped to another process") is identical, but the failure mode is a pickling error rather than `NotSerializableException`.
- Serialization failures are a **plan-time-adjacent** problem in the sense that they surface before real cluster work begins, similar to how `AnalysisException` surfaces at plan time (Day 15) — but they're triggered by task-closure construction, not query analysis.

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

**Key Points:**
- The Scala trap is almost always about capturing `this` implicitly — calling any instance method or referencing any instance field inside a lambda captures the *entire enclosing object*, not just the field you meant to use.
- The PySpark trap is almost always a live resource (DB connection, file handle, network client) created on the driver and referenced inside a UDF — even if it happens to pickle "successfully," reusing one connection object across many executor processes/threads is a correctness and stability risk, not just a serialization one.
- `@transient lazy val` in Scala tells the JVM "don't serialize this field; recreate it lazily on first access on the executor" — the standard fix for a field that's expensive/impossible to serialize but cheap to reconstruct per-executor.
- `mapPartitions`/`foreachPartition` is the PySpark equivalent: create the expensive resource once per partition (once per executor task), not once per row and not once on the driver.

### 3. UDFs are a performance cliff

**Key Points:**
- Python UDFs cross the JVM↔Python boundary **per row** and are **opaque to Catalyst** (no predicate pushdown, no codegen).
- Order of preference: **built-in SQL functions** → **Pandas/Arrow UDFs** (vectorized) → plain Python UDF (last resort).
- Every row that crosses the JVM↔Python boundary is serialized on the JVM side, sent over a socket/pipe, deserialized in the Python worker, processed, then serialized back — this round-trip, multiplied by billions of rows, is where the "10x slower" complaints usually come from.
- Because Catalyst treats a Python UDF as an opaque black box, it can't push filters through it, can't reorder it relative to other operations, and can't apply whole-stage code generation across it — the UDF becomes a hard boundary in the optimized plan.

### 4. Kryo vs Java serialization vs DataFrame encoders

Spark actually has three distinct serialization paths, and confusing them leads to tuning the wrong one.

| Path | Used for | Configured by |
|---|---|---|
| Java serialization | RDD closures and RDD data by default | `spark.serializer` (default) |
| Kryo | RDD data, opt-in, faster/smaller | `spark.serializer=org.apache.spark.serializer.KryoSerializer` |
| Encoders (Tungsten) | DataFrame/Dataset rows internally | Automatic — not user-configured |

**Key Points:**
- Kryo only affects **RDD-level** serialization (closures and RDD object data); it has no effect on DataFrame/Dataset row encoding, which already uses Spark's own binary format (Tungsten encoders) regardless of `spark.serializer`.
- If your job is 100% DataFrame/SQL API with no `.rdd`/`.map`/custom classes, enabling Kryo typically changes nothing measurable — it's not a general-purpose performance knob.
- Kryo matters most when you have custom Scala/Java classes flowing through RDD transformations, or large closures being shipped repeatedly (e.g. inside a loop calling `.collect()`/`.foreach()` many times).

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

### Step-by-Step Process: diagnosing a "Task not serializable" error
1. **Read the exception** — the last `Caused by: java.io.NotSerializableException: <ClassName>` names the offending class.
2. **Find where that class enters your closure** — search your code for references to it inside any `map`/`filter`/`udf`/lambda.
3. **Ask: did I mean to capture the whole object, or just one field?** — usually you meant one field; the whole object got captured because you called a method or referenced a field on `this` implicitly.
4. **Fix by localizing**: copy the needed value into a local variable *before* the closure, or restructure so the closure only touches serializable data.
5. **Re-run and confirm** the same exception is gone; watch for a *new* `NotSerializableException` deeper in the object graph, which means there's another field with the same problem.

### Example: tracing a real "Task not serializable" trace

```scala
class ReportJob(spark: SparkSession, config: JobConfig) {
  val threshold = config.threshold

  def run(df: DataFrame): DataFrame = {
    // BAD: references `threshold` via `this.threshold`, capturing the WHOLE
    // ReportJob instance -- including `spark`, which is not serializable.
    df.filter(r => r.getAs[Double]("amount") > threshold)
  }
}

// Trace:
// org.apache.spark.SparkException: Task not serializable
// Caused by: java.io.NotSerializableException: org.apache.spark.sql.SparkSession

// FIX: copy the primitive into a local val BEFORE the closure so only that
// value (a Double) is captured, not `this`.
class ReportJobFixed(spark: SparkSession, config: JobConfig) {
  def run(df: DataFrame): DataFrame = {
    val localThreshold = config.threshold   // local val, not a `this` field
    df.filter(r => r.getAs[Double]("amount") > localThreshold)
  }
}
```

**Analysis:**
- The exception names `SparkSession`, not `JobConfig` — because the closure captured the *entire enclosing `ReportJob` instance* (to resolve `this.threshold`), and `SparkSession` happened to be the first non-serializable field Java's serializer hit while walking that object graph.
- The actual line that "looks fine" (`r.getAs[Double]("amount") > threshold`) is the culprit precisely because `threshold` is a field access on `this`, not a local variable.
- The fix changes nothing about behavior — only what gets captured — which is why this class of bug is easy to introduce accidentally and easy to fix once correctly diagnosed.

### Example: a PySpark closure that "works" but is silently expensive

```python
# Looks fine, doesn't error, but is a serialization/throughput smell:
config = {"api_key": "abc123", "timeout": 30, "big_static_table": {i: i * 2 for i in range(500_000)}}

@F.udf("int")
def lookup(x):
    return config["big_static_table"].get(x, -1)   # `config` captured whole, every task

txns.withColumn("v", lookup(F.col("customer_id"))).count()
```
```python
# Fixed: broadcast once, and only capture the small pieces each task truly needs.
big_table_bc = spark.sparkContext.broadcast(config["big_static_table"])

@F.udf("int")
def lookup_fixed(x):
    return big_table_bc.value.get(x, -1)

txns.withColumn("v", lookup_fixed(F.col("customer_id"))).count()
```

**Analysis:**
- The first version never throws `NotSerializableException` — `config` is a plain dict, which pickles fine — so this class of problem is easy to miss entirely without deliberately checking task closure size.
- The cost shows up as elevated **Task Deserialization Time** in the Stages tab, and as extra network/serialization overhead multiplied by thousands of tasks, not as a hard failure.
- The fixed version ships the 500k-entry dict exactly once (to each executor, not each task) via the broadcast mechanism, and every task references it through `.value` instead of re-capturing it.

## 💡 Key Insights for On-Premise
### 1. Broadcast big read-only lookups
Don't capture a large dict in every task closure — **broadcast** it once:
```python
lookup = spark.sparkContext.broadcast(big_dict)
@F.udf("string")
def f(x): return lookup.value.get(x)
```
Without broadcasting, the same dictionary is serialized and shipped **with every task** — for a large lookup and thousands of tasks, this can dwarf your actual data volume in network/serialization overhead, and on a shared on-premise cluster that's wasted bandwidth other jobs could have used.

### 2. One connection per partition, not per row
Opening a connection per row (inside a UDF) is both a serialization smell and a throughput killer. Use `mapPartitions` / `foreachPartition`. On an on-premise cluster this also matters for connection-limited downstream systems (a database, an internal API) — one connection per *partition* is bounded by your parallelism; one connection per *row* can overwhelm a shared service with thousands of simultaneous connections.

### 3. Kryo registration on a shared cluster
`spark.kryo.registrationRequired=false` (the default) allows unregistered classes, which is convenient but slightly slower and slightly less safe (a typo'd class name fails at runtime, not job-submit time). For RDD-heavy, performance-sensitive jobs running routinely on shared infrastructure, consider registering your custom classes explicitly and setting `registrationRequired=true` so serialization mistakes fail fast and loud in testing rather than quietly in production.

### 4. UDF opacity compounds with partition pruning
On this cluster's Hive/Iceberg tables, a filter expressed as a built-in (`F.col("amount") > 100`) can be pushed down to the scan and, for partitioned tables, prune entire files/partitions before they're even read from `s3a://`. The same filter expressed inside a Python UDF cannot be pushed down at all — every partition/file gets scanned regardless. On a large on-premise table this difference alone can be the entire performance gap.

## 🎯 Practical Exercises

### Exercise 1: Break and fix serialization
```python
# See exercises/troubleshooting/exercise-19-serialization.py
# Exercise 1 in that file demonstrates capturing a large dict directly in a
# UDF (ships the dict with every task) vs broadcasting it once. Also
# practice triggering a genuine "Task not serializable" by capturing a
# SparkSession or a live connection inside a UDF, then fix by localizing
# the captured value or moving resource creation into mapPartitions.
```

### Exercise 2: UDF vs built-in vs Pandas UDF
```python
# See exercise-19-serialization.py sections 2-3: compare .explain() output
# for the same transform written as a Python UDF (BatchEvalPython node,
# opaque to Catalyst) vs a built-in expression (pushdown + codegen) vs a
# Pandas UDF (ArrowEvalPython, vectorized). Time all three on the same data.
```

### Exercise 3: One connection per partition
```python
def enrich_partition(rows):
    conn = make_db_connection()      # created once per partition/task
    try:
        for r in rows:
            yield r, conn.lookup(r["customer_id"])
    finally:
        conn.close()

result = df.rdd.mapPartitions(enrich_partition)
# Compare against a (broken) per-row UDF version that opens a connection
# inside the UDF body itself; observe connection count at the downstream
# service under each approach.
```

### Exercise 4: Measure task deserialization time before/after broadcasting
```python
# Compare Stages tab "Task Deserialization Time" for the same lookup UDF
# with and without broadcasting the underlying dict:
big_lookup = {i: f"seg_{i % 3}" for i in range(200_000)}

@F.udf("string")
def unbroadcast(cid): return big_lookup.get(cid, "unknown")

bc = spark.sparkContext.broadcast(big_lookup)
@F.udf("string")
def broadcasted(cid): return bc.value.get(cid, "unknown")

txns.withColumn("seg", unbroadcast("customer_id")).count()   # check Stages tab
txns.withColumn("seg", broadcasted("customer_id")).count()   # check Stages tab again
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Task Deserialization Time** (Stages tab) — high = heavy closures.
2. **UDF time** vs total task time — Python UDF overhead shows as high compute with no shuffle.
3. **Task startup/scheduler delay** — unusually large closures increase the time to ship a task to an executor before it even starts running.
4. **Executor Python worker count/memory** (relevant for Pandas UDFs) — visible indirectly via `memoryOverhead` pressure (see Day 16).

### Spark UI Analysis
- SQL tab: a `BatchEvalPython` / `ArrowEvalPython` node marks a Python/Pandas UDF boundary.
- No pushdown below a UDF filter = Catalyst can't see through it.
- Compare the "duration" and "rows" annotations on either side of a `BatchEvalPython` node in the SQL tab's graph — a large per-row overhead is visible as unexpectedly high time relative to row count.
- The Stages tab's per-task "Task Deserialization Time" column, if consistently high across all tasks in a stage, points to a large or expensive-to-deserialize closure — worth checking for accidentally captured large objects even when the job doesn't outright fail.

## 🚨 Common Issues & Solutions

### Issue 1: "Task not serializable" referencing your class
**Symptom**: closure captured `this`.
**Root Cause**: referencing any instance field or method inside a closure implicitly captures the entire enclosing object, including any non-serializable fields it holds.
**Solution**: copy needed fields into local vals, or mark heavy fields `@transient lazy`.

### Issue 2: UDF job is correct but 10× too slow
**Symptom**: high CPU, boundary crossing per row.
**Root Cause**: every row pays a JVM↔Python serialize/deserialize round trip, and Catalyst can't optimize across the UDF boundary.
**Solution**: rewrite with built-ins, or vectorize with a Pandas UDF.

### Issue 3: PySpark UDF works locally but fails/pickles oddly on the cluster
**Symptom**: `PicklingError` or similar only shows up when actually distributed, not in a local quick test.
**Root Cause**: the UDF closure captured a live object (open file handle, DB connection, non-picklable third-party client) that happened to still be usable in a single local process but cannot survive being shipped to a separate executor process.
**Solution**: move creation of any live resource inside `mapPartitions`/`foreachPartition` (or lazily inside the UDF body itself, memoized per worker process) so it's constructed fresh on each executor rather than captured from the driver.

### Issue 4: The exception names a class you don't recognize
**Symptom**: `NotSerializableException` points at some internal or third-party class you never directly referenced.
**Root Cause**: the actual non-serializable object is several fields deep inside something you *did* capture (e.g. your service object holds a connection pool, which holds a socket) — the exception reports the first unserializable node it hit while walking the graph, not necessarily the field you touched directly.
**Solution**: trace backward from the named class through your own object's fields to find which one holds it, then apply the local-val/`@transient` fix at that level.

### Issue 5: Broadcast variable seems to have no effect on performance
**Symptom**: wrapped a lookup in `sparkContext.broadcast()`, but task closure size and network usage look unchanged.
**Root Cause**: usually the UDF/closure still references the raw dict directly (`big_lookup.get(...)`) instead of `bc.value.get(...)` — the broadcast object was created but never actually used, so the raw object is still captured and shipped per task.
**Solution**: verify every reference in the closure goes through `.value` on the broadcast handle, not the original variable.

## 📝 Key Takeaways
1. Closures ship captured state — it must be serializable.
2. Create connections **on the executor**, per partition — never capture them.
3. Built-ins > Pandas UDF > Python UDF (performance and optimizer visibility).
4. Broadcast large read-only lookups.
5. Kryo helps RDD/custom-object jobs; DataFrames already use encoders.
6. Referencing `this.field` implicitly captures the whole enclosing object — localize what you actually need.
7. A UDF boundary blocks predicate pushdown and partition pruning, not just codegen — this can dominate cost on partitioned on-premise tables.

## 🔗 Next Steps
- **Day 20**: Performance Debugging (Spark UI & SQL tab)
- Practice: find one Python UDF at work and replace it with built-ins.
- Experiment: compare `.explain()` output and wall-clock time for the same logic as a Python UDF, a Pandas UDF, and a built-in expression.

## 📚 Additional Resources
- Spark UDF & Pandas UDF (pandas function API) docs
- Kryo serialization tuning
- `cloudpickle` and PySpark closure serialization internals

---

**Progress**: Day 19/40 ✅
