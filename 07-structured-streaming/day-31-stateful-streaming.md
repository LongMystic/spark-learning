# Day 31: Stateful Streaming, Watermarks, Kafka & Exactly-Once

## 🎯 Learning Objectives
- Handle late data with **watermarks** and windowed aggregations
- Manage streaming **state** and keep it bounded
- Do stream–stream joins and deduplication
- Wire up Kafka with real exactly-once end-to-end
- Configure Kafka source options and state store providers for production stability

## 📚 Core Concepts

### 1. Event time vs processing time

Aggregations should use **event time** (when it happened), not processing
time (when Spark saw it). Late/out-of-order events are the norm — a mobile
client can be offline for minutes, a Kafka producer can retry — and
watermarks tell Spark how long to wait before treating a window as final.

**Key Points:**
- Event time is a column in your data (e.g. `event_time` parsed from the payload); processing time is simply "wall clock when Spark received the row."
- Grouping by processing time gives meaningless results whenever data arrives out of order relative to when it happened — event-time windows are what make results correct regardless of arrival order.
- Watermarks are Spark's mechanism for bounding "how out of order can data be" so it can eventually finalize a window instead of waiting forever.

**Example:**
```python
from pyspark.sql import functions as F
windowed = (parsed
    .withWatermark("event_time", "10 minutes")     # tolerate 10 min lateness
    .groupBy(F.window("event_time", "5 minutes"), "customer_id")
    .agg(F.sum("amount").alias("total")))
```

### 2. What the watermark does

**Key Points:**
- Lets Spark **emit and finalize** a window once the watermark (max event time seen so far, minus the lateness threshold) passes its end.
- Lets Spark **drop state** for old windows once they're finalized, so state doesn't grow forever — this is the mechanism that bounds memory in a long-running aggregation query.
- Events later than the watermark are **discarded** (in `append`/`update` mode) — a deliberate tradeoff between completeness and bounded resource usage that you tune via the lateness threshold.
- The watermark only advances based on data actually seen — an idle source (no new events) means the watermark doesn't advance either, so windows won't finalize until new data arrives.

**Example:**
```python
# 10-minute watermark: a window ending at 10:05 finalizes once Spark has seen
# an event with event_time >= 10:15 (10:05 + 10 min lateness tolerance).
windowed = (parsed
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "customer_id")
    .agg(F.sum("amount").alias("total")))
```

### 3. State stores

**Key Points:**
- Aggregations, dedup, and stream-stream joins keep **state** (partial aggregates, seen keys, buffered rows) in a **state store**, checkpointed alongside offsets for recovery.
- Unbounded state (no watermark, or a key space that only grows — e.g. deduplicating on a globally unique ID forever) is the **#1 streaming failure mode** — always bound state with watermarks and sane key cardinality.
- The default state store keeps state in executor memory (backed by the checkpoint on write); for large state, the **RocksDB state store provider** keeps state off-heap on local disk, avoiding executor JVM heap pressure.
- State is partitioned the same way the aggregation/join key is partitioned — skewed keys create skewed state just like skewed joins create skewed shuffles (Day 12).

**Example:**
```python
# Switch to RocksDB-backed state store for large/steady-state aggregations
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

### 4. Deduplication

**Key Points:**
- `dropDuplicates` on a streaming DataFrame keeps a record of seen keys in the state store — without a watermark, this record grows forever.
- Adding a watermark on the event-time column lets Spark expire old "seen key" state once it's safely past the lateness window, bounding memory the same way aggregation state is bounded.
- Choose a dedup key that reflects true uniqueness (e.g. `(txn_id, event_time)` or a message UUID) — deduplicating on a non-unique key silently drops legitimate distinct events.

**Example:**
```python
deduped = parsed.withWatermark("event_time", "1 hour").dropDuplicates(["txn_id", "event_time"])
```

### 5. Stream-stream joins

**Key Points:**
- Joining two streams (e.g. `orders` and `payments`) requires watermarks on **both** sides plus a join condition that constrains how far apart their event times can be — otherwise Spark would need to buffer every row from both streams forever waiting for a possible match.
- The join condition's time bound (e.g. "a payment arrives within 1 hour of its order") lets Spark discard buffered rows from each side once the watermark passes that bound, keeping join-state bounded the same way windowed aggregation state is bounded.
- Inner stream-stream joins are the simplest case; outer joins (`left_outer`, `right_outer`) are also supported but only in `append` output mode, and only emit an unmatched row once Spark is sure (via the watermark) that a late match can no longer arrive.
- Without the time-bounded join condition, Spark raises an analysis error refusing to run the query — this is a deliberate guardrail against accidentally building an unbounded-state join.

**Example:**
```python
orders = spark.readStream.format("kafka").option("subscribe", "orders").load() \
    .select(F.from_json(F.col("value").cast("string"), "order_id long, customer_id long, order_time timestamp").alias("j")).select("j.*") \
    .withWatermark("order_time", "1 hour")

payments = spark.readStream.format("kafka").option("subscribe", "payments").load() \
    .select(F.from_json(F.col("value").cast("string"), "order_id long, amount double, pay_time timestamp").alias("j")).select("j.*") \
    .withWatermark("pay_time", "1 hour")

joined = orders.join(
    payments,
    F.expr("""
        orders.order_id = payments.order_id AND
        pay_time BETWEEN order_time AND order_time + interval 1 hour
    """),
    "leftOuter")
```

## 🔍 Deep Dive: Exactly-once with Kafka → Iceberg

### Step-by-Step Process

1. **Read from Kafka with an explicit `startingOffsets` policy.** `latest` for a fresh query, `earliest` to reprocess history — the checkpoint takes over after the first successful batch regardless of this setting.
2. **Parse the Kafka `value` (and optionally `key`) bytes.** Kafka rows arrive as raw `key`/`value`/`topic`/`partition`/`offset`/`timestamp` columns — decode with `from_json` (or Avro/Protobuf deserializers) against your event schema.
3. **Aggregate or transform using event time**, with a watermark if the transform is stateful (window aggregation, dedup, stream-stream join).
4. **Write with `foreachBatch`, using an idempotent `MERGE INTO`** against the Iceberg target — this is the sink half of exactly-once, since Iceberg's atomic commits make the merge all-or-nothing per batch.
5. **Rely on the checkpoint to store committed Kafka offsets.** On restart, Spark resumes from the last committed offset and reprocesses only what's needed — because the sink is a `MERGE` keyed on the business key, replaying a batch produces the same end state, not duplicates.
6. **Tune batch size for stability**, not just throughput — `maxOffsetsPerTrigger` caps how many Kafka records a single micro-batch consumes, keeping batch latency predictable under bursty load.

### Example: Kafka → Iceberg with `foreachBatch` MERGE

```python
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "true")
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

**Analysis:**
- The combination of **Kafka offsets in the checkpoint** (replayable source) + **idempotent MERGE** (transactional sink) gives end-to-end exactly-once: if batch N is retried after a failure, the same rows are read again (replayable) and the same `MERGE` produces the same final table state (idempotent), rather than double-counting.
- Note this particular `upsert_to_table` is idempotent for a **crash-and-retry of the same batch**, but it is *additive* (`t.amt = t.amt + s.amt`) across genuinely different batches — that's correct because each batch contains a disjoint slice of new events, not because the MERGE itself deduplicates across batches. If exactly-once across reprocessing a wider offset range is required, key the merge on a batch/offset marker or use a non-additive `UPDATE SET` with a idempotency check.
- `maxOffsetsPerTrigger` bounds batch size so a burst of backlog (e.g. after downtime) doesn't produce one enormous, slow batch — it trades a bit of latency for predictable batch durations.
- `failOnDataLoss=true` is a deliberate choice: it fails the query loudly if Kafka has deleted offsets Spark expected to read (e.g. retention expired) rather than silently skipping data — the right default for financial/transaction data.

## 💡 Key Insights for On-Premise

### 1. Watermark = latency vs completeness knob
A longer watermark catches more late data but delays results and grows
state. Pick it from your data's real lateness distribution (measure it),
not a guess — a mobile-app pipeline with offline clients needs a much wider
watermark than a pipeline reading directly from a well-connected service.

### 2. State store sizing & RocksDB
For large state (many keys, wide windows, or big dedup key spaces), use the
**RocksDB state store** (`spark.sql.streaming.stateStore.providerClass`) to
keep state off-heap and avoid executor OOM. Monitor state rows/size in the
Structured Streaming UI tab; a steadily growing `numRowsTotal` despite a
watermark being set usually means the watermark or key cardinality needs
attention.

### 3. Kafka consumer config on-prem
Set `maxOffsetsPerTrigger` to cap batch size (backpressure), decide
`failOnDataLoss` deliberately (fail loud vs. skip silently), and secure the
connection with SASL/SSL options matching your cluster's Kafka ACLs — the
course's local `environment/k8s/06-kafka.yaml` runs a single-node,
plaintext, KRaft-mode broker for learning, but a real on-prem deployment
would add `kafka.security.protocol`/`kafka.sasl.mechanism` options here.

### 4. Kafka topic/partition layout drives Spark parallelism
Structured Streaming's Kafka source maps Kafka partitions to Spark tasks —
the number of Kafka partitions on the `transactions` topic is a hard upper
bound on read parallelism for that micro-batch, independent of how many
executors you have. If a topic is under-partitioned, adding executors won't
speed up the read stage.

## 🎯 Practical Exercises

### Exercise 1: Windowed aggregation with watermark
```python
# See exercises/streaming/exercise-31-stateful.py
# Uses the built-in 'rate' source (rows/sec with a timestamp) — no Kafka needed.
rate = (spark.readStream.format("rate").option("rowsPerSecond", 20).load())
events = rate.withColumn("customer_id", (F.col("value") % 10))

windowed = (events
    .withWatermark("timestamp", "10 seconds")
    .groupBy(F.window("timestamp", "5 seconds"), "customer_id")
    .agg(F.count("*").alias("events"), F.sum("value").alias("sum_value")))

query = (windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", chk)
    .trigger(processingTime="5 seconds")
    .start())
query.awaitTermination(20)   # run ~20s then stop cleanly
query.stop()
# Watch console output: windows finalize roughly 10s after their end time.
```

### Exercise 2: Dedup + rate source
```python
# Use dropDuplicates with a watermark; verify state stays bounded.
deduped = (rate
    .withWatermark("timestamp", "30 seconds")
    .dropDuplicates(["value"]))   # 'value' repeats slowly enough to see dedup happen
# Compare row counts with and without the watermark-bounded dedup, and check
# the Structured Streaming UI's state-store "numRowsTotal" stays flat over time
# instead of climbing.
```

### Exercise 3: Kafka end-to-end (with the streaming profile)
```bash
kubectl apply -f environment/k8s/06-kafka.yaml
kubectl -n spark-jobs port-forward svc/kafka 9092:9092 &
kubectl run kafka-producer --image=python:3.9 --restart=Never -- \
  bash -c 'pip install kafka-python && python environment/produce_stream.py --rate 20 --topic transactions'
```
```python
# Run the Kafka -> Iceberg MERGE pattern from the Deep Dive against the
# 'transactions' topic; kill the query mid-run and restart it, then confirm
# via local.db.txn_agg.snapshots that the restart didn't double-count.
```

### Exercise 4: Stream-stream join with a time bound (conceptual)
```python
# Using two 'rate' sources as stand-ins for orders/payments, join them with a
# watermark + time-bounded condition on each side, and confirm the query
# starts (removing the time bound should make Spark refuse to run it).
left = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        .withColumnRenamed("timestamp", "order_time").withColumnRenamed("value", "order_id")
        .withWatermark("order_time", "30 seconds"))
right = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
         .withColumnRenamed("timestamp", "pay_time").withColumnRenamed("value", "order_id")
         .withWatermark("pay_time", "30 seconds"))

joined = left.join(right, F.expr(
    "left.order_id = right.order_id AND pay_time BETWEEN order_time AND order_time + interval 30 seconds"))
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **State rows/size per batch** (`numRowsTotal`, `memoryUsedBytes`) — should plateau, not grow unbounded, once the watermark reaches steady state.
2. **Watermark timestamp progression** — should track close behind the max event time seen; a stalled watermark means an idle or stuck source.
3. **Rows dropped as "too late"** (`numRowsDroppedByWatermark` in the streaming query's state operator metrics) — a nonzero, growing count means real data is being discarded.
4. **Kafka consumer lag** — latest topic offset minus the offset Spark has processed; the direct "are we keeping up" signal for a Kafka source.
5. **`MERGE` duration per batch** (from the SQL tab for that batch) — a slow merge against a poorly-compacted Iceberg table will eventually make batches exceed the trigger interval.

### Spark UI Analysis
- The **Structured Streaming tab** shows, for each batch, dedicated **state operator metrics**: `numRowsTotal`, `numRowsUpdated`, `memoryUsedBytes`, and the watermark value at that point in time — this is the primary tool for diagnosing unbounded state before it OOMs an executor.
- The **watermark gap** (current batch time minus watermark) shown per batch tells you how much lateness tolerance is actually being "spent" — if it never approaches your configured threshold, you may be able to tighten it and reduce state/latency.
- For Kafka sources specifically, the streaming tab also surfaces per-partition offset progress, useful for spotting one skewed/stuck partition.
- The regular **SQL tab** shows the `MERGE INTO` plan executed by `foreachBatch` for each batch — check it uses a sensible join strategy against the target table rather than a full table scan every batch.

## 🚨 Common Issues & Solutions

### Issue 1: State grows forever → OOM
**Symptom**: Batches slow down progressively; executors OOM after hours of otherwise-stable running.
**Root Cause**: No watermark on a stateful aggregation/dedup, or a watermark that's technically present but the key space itself is unbounded (e.g. deduplicating on a always-unique surrogate key with no time bound).
**Solution**: Add a watermark sized to real lateness; bound the key space (dedup on a meaningful business key, not a random ID); switch to the RocksDB state store for headroom while you fix the root cause.

### Issue 2: Late data silently missing
**Symptom**: Aggregated totals are consistently lower than an independent batch recount of the same data.
**Root Cause**: Watermark threshold is tighter than the data's actual lateness distribution, so legitimately late events are discarded (`numRowsDroppedByWatermark` climbing).
**Solution**: Widen the watermark based on measured lateness (accepting more result latency and more state), and monitor `numRowsDroppedByWatermark` going forward to catch drift.

### Issue 3: Duplicate/double-counted aggregates after a restart
**Symptom**: `local.db.txn_agg` totals are higher than expected after the query was killed and restarted mid-batch.
**Root Cause**: The `foreachBatch` MERGE logic is additive across batches (`t.amt = t.amt + s.amt`) but not idempotent for a *replayed* batch — if the same batch's offsets are reprocessed after a crash, the additive update double-applies them.
**Solution**: Make the merge idempotent per batch — e.g. track a processed-batch marker in the target table, or key the source aggregation so re-merging the same offset range is a true upsert (`UPDATE SET t.amt = s.amt`, computed from a full recompute of that key) rather than an in-place addition.

### Issue 4: One Kafka partition lags the rest
**Symptom**: Overall consumer lag climbs even though most partitions are caught up.
**Root Cause**: Uneven key distribution across Kafka partitions (a hot key concentrated on one partition) or an under-partitioned topic limiting parallelism for that key range.
**Solution**: Repartition the Kafka topic with a better key/partitioner upstream, or accept the skew and size `maxOffsetsPerTrigger` so no single partition's backlog dominates a batch.

### Issue 5: Query fails to restart after a code change
**Symptom**: `StreamingQueryException` mentioning the checkpoint's stored plan is incompatible on restart.
**Root Cause**: The transformation changed in a way that's incompatible with the checkpointed state (e.g. changing the group-by keys of a stateful aggregation, or the watermark column).
**Solution**: For incompatible logic changes, start a **new checkpoint location** (accepting a one-time state reset / possible reprocessing), and treat checkpoint-compatible changes (e.g. adding a stateless filter) as the safer default for in-place upgrades.

### Issue 6: Stream-stream join query fails to start
**Symptom**: `AnalysisException` at query start, complaining the join has no watermark or time-range condition.
**Root Cause**: One or both sides of the join are missing `withWatermark`, or the join condition doesn't bound the event-time difference between the two streams — Spark refuses to run a join whose state it can't ever bound.
**Solution**: Add a watermark to both streams and a join condition with an explicit time range (as in the Core Concepts #5 example) so Spark can prove state will eventually be discarded.

## 📝 Key Takeaways
1. Aggregate on **event time**; use watermarks to define how much lateness you tolerate.
2. Watermarks both finalize windows and bound state — they are the mechanism, not just a tuning knob.
3. State without a watermark, or with an unbounded key space, grows without limit → OOM.
4. Exactly-once = replayable Kafka offsets (source) + idempotent MERGE (sink) via `foreachBatch` — and idempotency must hold for *replayed* batches, not just be additive across distinct ones.
5. Use the RocksDB state store for large state and `maxOffsetsPerTrigger` for stable, bounded batch sizes on steady on-prem streams.
6. Kafka partition count is a hard ceiling on read parallelism — plan topic layout accordingly.
7. Stream-stream joins need watermarks on both sides plus a time-bounded join condition, or Spark refuses to run the query at all.

## 🔗 Next Steps
- **Day 32**: PySpark Best Practices & Zeppelin

## 📚 Additional Resources
- Structured Streaming Programming Guide: watermarking and window operations
- Structured Streaming state store documentation (default and RocksDB providers)
- Structured Streaming + Kafka Integration Guide (source/sink options, offsets, security)
- `environment/k8s/06-kafka.yaml` and `environment/README.md` for this course's local Kafka setup

---

**Progress**: Day 31/40 ✅
