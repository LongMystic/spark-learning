# Solutions — Streaming (Days 30-31)

## exercise-30 (streaming basics)
**Expected**: with `maxFilesPerTrigger=1`, the query processes one file per micro-batch
until all are drained, then stops (because of `trigger(availableNow)`). The final
`cat_totals` table holds per-category totals; the checkpoint dir contains `offsets/`,
`commits/`, and `metadata`.
1. `availableNow` processes all currently-available data then stops (batch-like);
   `processingTime` runs continuously every interval.
2. The checkpoint stores source offsets + operator state + query metadata → on restart
   the query resumes from the last committed offset (recovery / exactly-once).
3. Aggregation output must be `complete` or `update`; `complete` re-emits the full result
   table each trigger (fine for a small aggregation).

## exercise-31 (stateful streaming)
**Expected**: 5-second event-time windows per `customer_id`, emitted in `update` mode
every 5s; the query runs ~20s then stops. In the Structured Streaming UI tab, state-store
metrics (`numRowsTotal`, memory used) stay **bounded** because the 10s watermark evicts
old windows.
1. The watermark lets Spark finalize/emit a window once passed and **evict its state**.
2. An event later than the watermark is **dropped** (not counted) — the completeness vs
   latency tradeoff you chose.
3. State-store metrics should be small/stable; without a watermark they'd grow every batch.
