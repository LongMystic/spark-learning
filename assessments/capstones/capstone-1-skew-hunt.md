# Capstone 1: Skew Hunt & Fix

**Goal**: Take a deliberately slow, skewed join/aggregation and make it fast, proving
the improvement with Spark UI metrics.

## Data
`transactions_skewed` (≈80% of rows on 5 hot customers) joined/aggregated against
`customers`. Generate at `--scale medium` for visible effects.

## Steps
1. **Establish the baseline.** Run a `groupBy("customer_id")` aggregation and a
   `transactions_skewed ⨝ customers` join with AQE **off** and `shuffle.partitions=200`.
   Record from the Spark UI: total time, slowest stage, and the **max/median task
   duration ratio**.
2. **Diagnose.** Show the skew: top keys by count, and the straggler task in the UI.
   Confirm it's value-level skew, not just too few partitions.
3. **Fix — try in order and measure each:**
   - Enable AQE + skew join (`spark.sql.adaptive.skewJoin.enabled=true`).
   - Salt the hot key for the aggregation/join.
   - Isolate the hot keys (process separately) and union results.
4. **Re-measure** against the baseline. Pick the best approach and explain the tradeoff.

## Rubric
- [ ] Baseline captured with concrete numbers (not "it felt slow").
- [ ] Skew demonstrated (key distribution + straggler task screenshot/notes).
- [ ] At least two mitigations tried and measured.
- [ ] Max/median task ratio meaningfully reduced; total time improved.
- [ ] Write-up explains **why** the winning fix worked and its cost.

## Stretch
- Show that AQE skew join does **not** help a skewed **window** function, and fix that with salting.
