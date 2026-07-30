# Day 30: Structured Streaming Fundamentals

## 🎯 Learning Objectives
- Understand the micro-batch model and the "unbounded table" abstraction
- Build a basic streaming read → transform → write pipeline
- Use triggers, output modes, and checkpoints correctly
- Reason about the delivery guarantees Structured Streaming provides
- Know which streaming sources and sinks are available and when to use each

## 📚 Core Concepts

### 1. The unbounded table model

Structured Streaming treats a stream as a table that **grows over time**. You
write (mostly) the **same DataFrame code** as batch; Spark incrementally
executes it as new data arrives, re-running an incremental version of your
query plan against only the new rows on each micro-batch.

**Key Points:**
- The programming model is the *same* DataFrame/Dataset API and Catalyst optimizer as batch — there is no separate "streaming DSL" to learn.
- Internally, each trigger runs a small batch job against the new data since the last offset, and the Spark UI's Jobs/Stages/SQL tabs show these as regular (small) jobs.
- Sources supported out of the box include `kafka`, `file` (Parquet/JSON/CSV/text with schema), `socket` (dev only), and `rate` (synthetic data generator for testing, used in Day 31's exercise).
- Sinks include `console` (debug), `memory` (debug, queryable temp table), `file` (Parquet/JSON/etc.), `kafka`, and `foreachBatch` (arbitrary batch sink logic, the main integration point for Iceberg/JDBC).

**Example:**
```python
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .load())

from pyspark.sql import functions as F
parsed = (stream
    .select(F.from_json(F.col("value").cast("string"), "txn_id long, customer_id long, amount double, event_time timestamp").alias("j"))
    .select("j.*"))

query = (parsed.groupBy("customer_id").agg(F.sum("amount").alias("total"))
    .writeStream
    .outputMode("update")
    .format("console")
    .option("checkpointLocation", "checkpoints/day30")
    .trigger(processingTime="5 seconds")
    .start())
query.awaitTermination()
```

### 2. Output modes

| Mode | Emits | Use with |
|------|-------|----------|
| `append` | only new rows | non-aggregated, or aggregations with a watermark |
| `update` | changed rows | aggregations (most common) |
| `complete` | the whole result table each trigger | small aggregations only |

**Key Points:**
- `append` is required for sinks that can't handle updates (e.g. plain `file`/`parquet` sinks) — which is why unbounded aggregations without a watermark can't use it (Spark would need to guarantee a row will never change again, and without a watermark it can't).
- `update` only emits rows that changed since the last trigger — efficient for dashboards that want the latest state of each key.
- `complete` re-emits the **entire** result table every trigger — only viable when the result is small (e.g. a handful of aggregate groups), because the whole table is rewritten to the sink each time.
- Picking the wrong mode is one of the most common first-week Structured Streaming errors — Spark will refuse to start the query with a clear `AnalysisException` naming the incompatible combination.

**Example:**
```python
# complete mode: fine for a small, bounded set of groups (e.g. per-category totals)
(agg.writeStream.outputMode("complete").format("console").start())

# append mode: requires a watermark on a windowed aggregation (Day 31)
(windowed.writeStream.outputMode("append").format("parquet")
    .option("path", "out/").option("checkpointLocation", "chk/").start())
```

### 3. Triggers

**Key Points:**
- `processingTime="5 seconds"` — fixed micro-batch interval; if a batch takes longer than the interval, the next one starts immediately after (no overlap, but no idle wait either).
- `once` (legacy) / `availableNow` (preferred, Spark 3.3+) — process all currently available data across possibly multiple micro-batches, then stop. This turns a "24/7 streaming job" into a **scheduled batch job** you can run from Airflow (Day 35) every N minutes, which is often the more operable pattern on-premise than an always-on driver pod.
- Default trigger (none specified) — runs micro-batches back-to-back as fast as possible; rarely what you want in production because it maximizes resource churn.
- `continuous` mode (experimental, low-latency) exists in Spark but has narrow operator support — the micro-batch engine described here is what virtually all production Structured Streaming pipelines use.

**Example:**
```python
# Fixed-interval micro-batch
query = stream.writeStream.trigger(processingTime="5 seconds")...start()

# Scheduled drain-and-stop, ideal for Airflow-triggered "streaming-as-batch"
query = stream.writeStream.trigger(availableNow=True)...start()
query.awaitTermination()   # returns once all available data is processed
```

### 4. Checkpoints = the source of truth

The **checkpoint location** stores offsets and state. It is what makes the
query recoverable and (with replayable sources + idempotent sinks)
**exactly-once**. Never share one checkpoint between two queries; never
delete it if you want to resume.

**Key Points:**
- The checkpoint directory holds `offsets/` (what's been read from the source per batch), `commits/` (which batches finished successfully), and — for stateful queries — `state/` (Day 31).
- On restart, Spark reads the last committed offset and resumes from exactly there — this is what "recovers" the query, not magic re-detection of "new" data.
- Checkpoints are **tied to the query's logical plan**. Changing the transformation in incompatible ways (e.g. adding an aggregation where there was none) after a restart can break checkpoint compatibility — test schema/logic changes carefully.
- On-premise, checkpoints should live on durable storage the driver can always reach — object storage (`s3a://.../checkpoints/...` on MinIO) or a reliable PVC, never local disk on a pod that could be rescheduled elsewhere.

### 5. Choosing a source: Kafka vs files vs rate

**Key Points:**
- **Kafka** is the production choice for real event streams — it's replayable by offset, supports many concurrent consumers, and is what Day 31's exactly-once pipeline relies on.
- The **file source** turns a directory of files into a stream by treating each new file as new data — it needs an explicit `.schema(...)` and is a good fit for "streaming-as-batch" pipelines where an upstream system drops files (e.g. exports, sensor dumps) rather than pushing to a message bus.
- The **rate source** generates synthetic rows at a configurable rate purely for testing/learning — never used in production, but ideal for exercises that need a stream without standing up Kafka (as Day 31's exercise does).
- The **socket source** reads newline-delimited text from a TCP socket — explicitly documented as for testing only (no fault tolerance, no offset tracking), never for production pipelines.

**Example:**
```python
# Same downstream logic, three different sources for development vs production
kafka_stream = spark.readStream.format("kafka").option("subscribe", "transactions")...load()
file_stream  = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).parquet(src)
rate_stream  = spark.readStream.format("rate").option("rowsPerSecond", 20).load()
```

## 🔍 Deep Dive: Delivery guarantees

### Step-by-Step Process

1. **Spark reads a batch of new offsets from the source** (e.g. new Kafka offsets since the last checkpoint, or new files since the last listing).
2. **The batch is recorded in `offsets/`** before processing starts — this is the durable record of "what we're about to process."
3. **The micro-batch job executes** your transformation against just that new data.
4. **The sink receives the batch's output.** For exactly-once, the sink must either be transactional (write succeeds fully or not at all) or idempotent (writing the same batch twice has no extra effect, e.g. an upsert keyed on a natural key).
5. **The batch is recorded in `commits/`** only after the sink has durably received the output — this is what tells Spark "batch N is done" on the next restart.
6. **On failure between steps 3 and 5**, Spark simply reprocesses the same batch from the same offsets on restart — which is safe *only if* the sink is idempotent/transactional, otherwise it's a duplicate.

### Example: Why console/memory sinks are "at-least-once at best"

```python
# Console sink: prints each batch's output. If the driver crashes after printing
# but before the commit log is written, the SAME rows print again on restart.
# Fine for demos (Exercise 1); never use in production pipelines.
query = agg.writeStream.format("console").option("checkpointLocation", "chk/").start()
```

**Analysis:**
- Structured Streaming gives **exactly-once** when the **source is replayable** (Kafka offsets, file listing — Spark can always re-read from a specific point) **and** the **sink is idempotent/transactional** (files with checkpoint, `foreachBatch` with upsert, Iceberg `MERGE`, Delta).
- The guarantee is a property of the **combination**, not of Structured Streaming alone — a replayable source with a non-idempotent sink (e.g. blind `INSERT`) is only **at-least-once** (duplicates possible on restart).
- Console/`memory` sinks are **not** exactly-once — they're for development only, since they have no transactional write path and no dedup keying.
- This is exactly why Day 31 builds a Kafka → Iceberg `MERGE` pipeline: replayable Kafka offsets (source) + idempotent `MERGE INTO` (sink) together close the loop.

## 💡 Key Insights for On-Premise

### 1. `foreachBatch` bridges streaming to batch sinks
Use it to write each micro-batch with normal batch APIs (e.g. MERGE into
Iceberg, JDBC upsert) — the pragmatic way to get exactly-once into on-prem
stores:
```python
def upsert(batch_df, batch_id):
    batch_df.write.mode("append").parquet("out/")   # or MERGE INTO iceberg table
parsed.writeStream.foreachBatch(upsert).option("checkpointLocation", "checkpoints/fb").start()
```
`foreachBatch` gives you the micro-batch as a regular `DataFrame` plus a
monotonically increasing `batch_id` you can use for idempotency bookkeeping
(e.g. skip if `batch_id` was already applied) on sinks that aren't naturally
upsert-friendly.

### 2. Trigger `availableNow` for scheduled micro-batch
On-prem you often don't want an always-on job occupying a driver pod and a
pool of executors 24/7 for a source that only produces data every few
minutes. `trigger(availableNow=True)` processes the backlog and exits —
schedule it every N minutes via Airflow (Day 35) and keep exactly-once via
the checkpoint, which persists between runs even though the query itself
stops.

### 3. Checkpoint placement matters on Kubernetes
Because driver pods are ephemeral, the checkpoint must live somewhere
outside the pod's local filesystem — point `checkpointLocation` at
`s3a://warehouse/checkpoints/...` (MinIO) so a rescheduled driver pod picks
up exactly where the last one left off.

## 🎯 Practical Exercises

### Exercise 1: File-source streaming (no Kafka needed)
```python
# See exercises/streaming/exercise-30-streaming-basics.py
# Splits the batch `transactions` table into files, then:
stream = (spark.readStream
    .schema(schema)                 # streaming file source requires an explicit schema
    .option("maxFilesPerTrigger", 1)  # process one file per micro-batch
    .parquet(src))

agg = stream.groupBy("category").agg(F.sum("amount").alias("total"))

query = (agg.writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("cat_totals")
    .option("checkpointLocation", chk)
    .trigger(availableNow=True)      # process all currently-available files, then stop
    .start())
query.awaitTermination()

spark.sql("SELECT * FROM cat_totals ORDER BY category").show()
# Then inspect the checkpoint directory's offsets/ and commits/ subfolders.
```

### Exercise 2: Kafka source (with the streaming profile)
```bash
# Start Kafka + producer (environment/README.md), then consume 'transactions'.
kubectl apply -f environment/k8s/06-kafka.yaml
kubectl -n spark-jobs port-forward svc/kafka 9092:9092 &
kubectl run kafka-producer --image=python:3.9 --restart=Never -- \
  bash -c 'pip install kafka-python && python environment/produce_stream.py --rate 20 --topic transactions'
```
```python
stream = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load())
# Parse the JSON value, aggregate, and write to console with a processingTime trigger.
# Compare inputRowsPerSecond in the Structured Streaming UI against the producer's --rate.
```

### Exercise 3: `foreachBatch` to a file sink with a batch counter
```python
# Write each micro-batch's output plus its batch_id to a Parquet sink, so you
# can see exactly which rows belonged to which batch and reason about what
# "replaying batch N" would mean for idempotency.
def write_with_batch_id(batch_df, batch_id):
    (batch_df.withColumn("batch_id", F.lit(batch_id))
        .write.mode("append").parquet("out/day30_foreachbatch/"))

query = (agg.writeStream
    .foreachBatch(write_with_batch_id)
    .option("checkpointLocation", "checkpoints/day30_fb")
    .trigger(availableNow=True)
    .start())
query.awaitTermination()
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **`inputRowsPerSecond` vs `processedRowsPerSecond`** — if input consistently exceeds processed, the query is falling behind and batches will pile up.
2. **Batch duration vs trigger interval** — a `processingTime` batch that regularly takes longer than the interval means Spark is always "catching up," never idle.
3. **Checkpoint commit success** — failed or stalled commits mean the query isn't making durable progress even if it looks "running."
4. **Number of input rows per batch** — a sudden spike (e.g. after downtime) shows the backlog being drained; watch it doesn't overwhelm the sink.
5. **Source-specific lag** — for Kafka, consumer lag (latest offset minus processed offset) is the most direct "are we keeping up" signal.

### Spark UI Analysis
- The **Structured Streaming tab** lists every active/completed streaming query with a graph of input rate, processing rate, and batch duration over time — the first place to check "is this query healthy."
- Clicking into a query shows **per-batch details**: how many rows were read, how long planning vs execution took, and the durations of each internal stage (`getBatch`, `addBatch`, etc.).
- The regular **Jobs/Stages/SQL tabs** still apply — each micro-batch shows up as its own small job, so you can drill into a slow batch exactly like a slow batch job.
- For `foreachBatch` sinks, the SQL tab shows the physical plan Spark ran for that specific batch's sink write (e.g. the `MERGE` plan), which is useful for diagnosing a slow upsert.

## 🚨 Common Issues & Solutions

### Issue 1: "Complete mode not supported"
**Symptom**: `AnalysisException` on start, e.g. "Data source ... does not support Complete output mode" or "... does not support Append output mode."
**Root Cause**: Output mode doesn't match the query shape — `complete` requires an aggregation with no watermark bound, `append` on an aggregation requires a watermark so Spark knows a row is truly final.
**Solution**: Use `append` (with a watermark, Day 31) or `update` for aggregations; reserve `complete` for small, fully-materializable result sets.

### Issue 2: Duplicates after a restart
**Symptom**: Reprocessed rows appear in the sink after the query is restarted (manually or after a crash).
**Root Cause**: Non-idempotent sink (e.g. blind append/insert) combined with a replayed batch, or the checkpoint was deleted/moved so Spark can't resume from the last committed offset.
**Solution**: Use `foreachBatch` with an upsert/MERGE keyed on a natural key (idempotent), or a transactional sink; and never delete or relocate the checkpoint if you intend to resume the same query.

### Issue 3: Query falls behind and never catches up
**Symptom**: `inputRowsPerSecond` stays above `processedRowsPerSecond` for many batches in a row; batch durations keep growing.
**Root Cause**: Under-provisioned executors for the input rate, or an expensive per-batch operation (large shuffle, a UDF) that doesn't scale with trigger interval.
**Solution**: Increase parallelism/executors, shorten the trigger interval only if the batch itself is fast, or add `maxOffsetsPerTrigger`/`maxFilesPerTrigger` to deliberately cap batch size and stabilize latency (Day 31 covers this for Kafka).

### Issue 4: Streaming file source doesn't pick up new files
**Symptom**: Files land in the source directory but the query never sees them.
**Root Cause**: The streaming file source lists a directory to discover new files, but requires an **explicit schema** — without `.schema(...)`, Spark either fails at start or (with schema inference on) re-infers per listing, which is slow and can silently mis-map columns.
**Solution**: Always pass `.schema(...)` explicitly for file sources, as in Exercise 1, and confirm files are written atomically (e.g. write-then-rename) so partially-written files aren't picked up mid-write.

### Issue 5: Job never terminates / hangs on `awaitTermination()`
**Symptom**: With `trigger(availableNow=True)`, the driver script hangs indefinitely instead of returning once the backlog drains.
**Root Cause**: `awaitTermination()` with no arguments blocks until the query is explicitly stopped or fails — with `availableNow`, the query *does* stop itself once the backlog is drained, but a bug in the sink (e.g. a `foreachBatch` function raising silently-swallowed exceptions) can leave the query looping.
**Solution**: Pass a timeout to `awaitTermination(timeout_ms)` in scheduled contexts and check `query.exception()` after, or rely on `query.awaitTermination()` returning cleanly for `availableNow` and add explicit logging inside `foreachBatch` to catch silent failures.

## 📝 Key Takeaways
1. A stream is an unbounded table; batch DataFrame code mostly just works against it.
2. Output mode — append/update/complete — must match your query's aggregation shape.
3. Triggers control cadence: fixed interval, default (fastest), or `availableNow` for scheduled drain-and-stop.
4. Checkpoints store offsets + commits (+ state) and are what enables recovery — never delete or share them.
5. Exactly-once needs a replayable source **and** an idempotent/transactional sink — it's a property of the pair, not either alone.
6. `foreachBatch` + `availableNow` fit on-prem scheduled pipelines better than always-on streaming jobs in many cases.

## 🔗 Next Steps
- **Day 31**: Stateful Streaming, Watermarks, Kafka & Exactly-Once

## 📚 Additional Resources
- Structured Streaming Programming Guide (output modes, triggers, checkpointing)
- Structured Streaming + Kafka Integration Guide
- `foreachBatch` and `foreach` sink documentation

---

**Progress**: Day 30/40 ✅
