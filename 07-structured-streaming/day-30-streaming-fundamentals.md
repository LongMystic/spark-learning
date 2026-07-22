# Day 30: Structured Streaming Fundamentals

## 🎯 Learning Objectives
- Understand the micro-batch model and the "unbounded table" abstraction
- Build a basic streaming read → transform → write pipeline
- Use triggers, output modes, and checkpoints correctly
- Reason about the delivery guarantees Structured Streaming provides

## 📚 Core Concepts

### 1. The unbounded table model
Structured Streaming treats a stream as a table that **grows over time**. You write (mostly) the **same DataFrame code** as batch; Spark incrementally executes it as new data arrives.

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

### 3. Triggers
- `processingTime="5 seconds"` — micro-batch every 5s.
- `once` / `availableNow` — process all currently available data then stop (great for scheduled "streaming-as-batch" jobs run by Airflow).
- default — as fast as possible.

### 4. Checkpoints = the source of truth
The **checkpoint location** stores offsets and state. It is what makes the query recoverable and (with replayable sources + idempotent sinks) **exactly-once**. Never share one checkpoint between two queries; never delete it if you want to resume.

## 🔍 Deep Dive: Delivery guarantees
Structured Streaming gives **exactly-once** when:
- the **source is replayable** (Kafka offsets, file listing) — Spark re-reads from the last committed offset after failure, and
- the **sink is idempotent/transactional** (files with checkpoint, `foreachBatch` with upsert, Iceberg, Delta).
Console/`memory` sinks are **not** exactly-once — they're for development only.

## 💡 Key Insights for On-Premise
### 1. `foreachBatch` bridges streaming to batch sinks
Use it to write each micro-batch with normal batch APIs (e.g. MERGE into Iceberg, JDBC upsert) — the pragmatic way to get exactly-once into on-prem stores:
```python
def upsert(batch_df, batch_id):
    batch_df.write.mode("append").parquet("out/")   # or MERGE INTO iceberg table
parsed.writeStream.foreachBatch(upsert).option("checkpointLocation", "checkpoints/fb").start()
```
### 2. Trigger `availableNow` for scheduled micro-batch
On-prem you often don't want an always-on job. `trigger(availableNow=True)` processes the backlog and exits — schedule it every N minutes via Airflow and keep exactly-once via the checkpoint.

## 🎯 Practical Exercises

### Exercise 1: File-source streaming (no Kafka needed)
```python
# See exercises/streaming/exercise-30-streaming-basics.py
# Stream from a directory of files, aggregate, write to console; inspect the checkpoint.
```

### Exercise 2: Kafka source (with the streaming profile)
```python
# Start Kafka + producer (environment/README.md), then consume 'transactions'.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. `inputRowsPerSecond` / `processedRowsPerSecond` (keeping up?).
2. Batch duration vs trigger interval (falling behind?).
3. Checkpoint commit success.

### Spark UI Analysis
- The Structured Streaming tab shows per-batch input/processing rates and durations.

## 🚨 Common Issues & Solutions

### Issue 1: "Complete mode not supported"
**Symptom**: error on a non-aggregation query.
**Solution**: use `append` (with watermark) or `update`; `complete` is only for aggregations.

### Issue 2: Duplicates after a restart
**Symptom**: reprocessed rows.
**Solution**: non-idempotent sink or deleted checkpoint — use `foreachBatch` upsert / transactional sink and keep the checkpoint.

## 📝 Key Takeaways
1. A stream is an unbounded table; batch code mostly just works.
2. Output mode: append/update/complete — match it to your query.
3. Checkpoints store offsets+state and enable recovery.
4. Exactly-once needs replayable source + idempotent sink.
5. `foreachBatch` + `availableNow` fit on-prem scheduled pipelines.

## 🔗 Next Steps
- **Day 31**: Stateful Streaming, Watermarks, Kafka & Exactly-Once

## 📚 Additional Resources
- Structured Streaming Programming Guide

---

**Progress**: Day 30/40 ✅
