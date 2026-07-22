# Day 31: Stateful Streaming, Watermarks, Kafka & Exactly-Once

## 🎯 Learning Objectives
- Handle late data with **watermarks** and windowed aggregations
- Manage streaming **state** and keep it bounded
- Do stream–stream joins and deduplication
- Wire up Kafka with real exactly-once end-to-end

## 📚 Core Concepts

### 1. Event time vs processing time
Aggregations should use **event time** (when it happened), not processing time (when Spark saw it). Late/out-of-order events are the norm; watermarks tell Spark how long to wait.

```python
from pyspark.sql import functions as F
windowed = (parsed
    .withWatermark("event_time", "10 minutes")     # tolerate 10 min lateness
    .groupBy(F.window("event_time", "5 minutes"), "customer_id")
    .agg(F.sum("amount").alias("total")))
```

### 2. What the watermark does
- Lets Spark **emit and finalize** a window once the watermark passes its end.
- Lets Spark **drop state** for old windows so state doesn't grow forever.
- Events later than the watermark are **discarded** (a tradeoff you choose).

### 3. State stores
Aggregations, dedup, and stream-stream joins keep **state** in a state store, checkpointed for recovery. Unbounded state (no watermark, or a key space that only grows) is the #1 streaming failure — always bound state with watermarks and reasonable keys.

### 4. Deduplication
```python
deduped = parsed.withWatermark("event_time", "1 hour").dropDuplicates(["txn_id", "event_time"])
```

## 🔍 Deep Dive: Exactly-once with Kafka → Iceberg

```python
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load())

events = raw.selectExpr("CAST(value AS STRING) AS v") \
    .select(F.from_json("v", "txn_id long, customer_id long, amount double, event_time timestamp").alias("j")) \
    .select("j.*")

def upsert_to_table(batch_df, batch_id):
    batch_df.createOrReplaceTempView("b")
    batch_df.sparkSession.sql("""
        MERGE INTO local.db.txn_agg t
        USING (SELECT customer_id, SUM(amount) amt FROM b GROUP BY customer_id) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET t.amt = t.amt + s.amt
        WHEN NOT MATCHED THEN INSERT *""")

(events.writeStream
    .foreachBatch(upsert_to_table)
    .option("checkpointLocation", "checkpoints/kafka-iceberg")
    .trigger(processingTime="10 seconds")
    .start())
```
The combination of **Kafka offsets in the checkpoint** (replayable source) + **idempotent MERGE** (transactional sink) gives end-to-end exactly-once.

## 💡 Key Insights for On-Premise
### 1. Watermark = latency vs completeness knob
A longer watermark catches more late data but delays results and grows state. Pick it from your data's real lateness distribution, not a guess.

### 2. State store sizing & RocksDB
For large state, use the **RocksDB state store** (`spark.sql.streaming.stateStore.providerClass`) to keep state off-heap and avoid executor OOM. Monitor state rows/size in the streaming tab.

### 3. Kafka consumer config on-prem
Set `maxOffsetsPerTrigger` to cap batch size (backpressure), `failOnDataLoss` deliberately, and secure with SASL/SSL to match your cluster.

## 🎯 Practical Exercises

### Exercise 1: Windowed aggregation with watermark
```python
# See exercises/streaming/exercise-31-stateful.py
# Windowed event-time aggregation over a file/rate source; observe state and late-data drop.
```

### Exercise 2: Dedup + rate source
```python
# Use dropDuplicates with a watermark; verify state stays bounded.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. State rows/size per batch (bounded?).
2. Watermark timestamp progression.
3. Rows dropped as "too late".

### Spark UI Analysis
- Streaming tab: state operator metrics (numRowsTotal, memoryUsedBytes) and watermark gap.

## 🚨 Common Issues & Solutions

### Issue 1: State grows forever → OOM
**Symptom**: batches slow down, executors OOM over hours.
**Solution**: add a watermark; bound the key space; use RocksDB state store.

### Issue 2: Late data silently missing
**Symptom**: totals lower than expected.
**Solution**: watermark too tight — widen it (accepting more latency) per real lateness.

## 📝 Key Takeaways
1. Aggregate on **event time**; use watermarks for late data.
2. Watermarks both finalize windows and bound state.
3. State without a watermark grows unbounded → OOM.
4. Exactly-once = replayable Kafka offsets + idempotent MERGE via `foreachBatch`.
5. Use RocksDB state store and `maxOffsetsPerTrigger` for large/steady on-prem streams.

## 🔗 Next Steps
- **Day 32**: PySpark Best Practices & Zeppelin

## 📚 Additional Resources
- Structured Streaming: watermarking, state store, Kafka integration guide

---

**Progress**: Day 31/40 ✅
