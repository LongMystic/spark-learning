# Day 40: Cost/Observability + Capstone Kickoff

## 🎯 Learning Objectives
- Measure and reduce the real cost of Spark jobs on-premise
- Build observability: metrics, history, alerting, and SLAs
- Establish a continuous optimization loop
- Attribute shared-cluster cost back to teams/namespaces
- Kick off a capstone that exercises the whole path

## 📚 Core Concepts

### 1. "Cost" on-premise = shared finite resources

There's no cloud bill, but cluster capacity is fixed and shared. A wasteful job **steals capacity** from everyone. Cost ≈ **executor-core-seconds** + **memory reserved** + **I/O**. The goal: the same result with fewer resources held for less time.

**Key Points:**
- Because there's no per-job invoice, cost has to be **derived** from Spark's own metrics (executor-core-seconds, memory-GB-seconds) rather than read off a bill — this is why the History Server and metrics pipeline (below) matter as much as any dashboard.
- Two jobs that produce the same output can have wildly different cost: one that holds 50 executors idle between stages for 20 minutes "costs" the same core-seconds as running a genuinely useful job on those executors — the resource was reserved either way.
- Cost attribution on a shared on-prem cluster is a **namespace-level** exercise (Day 39): tag every `SparkApplication` with its owning team/namespace so utilization and cost roll up per tenant, not just per cluster.

**Example:**
```python
# Rough executor-core-seconds for one run, computable from the History Server's event log
executor_core_seconds = sum(
    executor.total_cores * executor.active_duration_seconds
    for executor in application.executors
)
# Two jobs with the same output can cost very differently:
#   Job A: 10 executors x 5 cores x 600s  = 30,000 core-seconds
#   Job B: 50 executors x 5 cores x 600s  = 150,000 core-seconds (5x the "cost" for the same result)
```

### 2. The optimization loop

```
Measure  ->  Find the biggest waste  ->  Fix one thing  ->  Re-measure  ->  repeat
```

Biggest wins usually come from: eliminating full scans (pruning/DPP), fixing skew, right-sizing partitions/executors, replacing UDFs, and pre-aggregating for BI — everything from Phases 1-4 applied deliberately.

**Key Points:**
- Fix **one thing at a time** and re-measure against the same baseline — stacking three changes at once makes it impossible to know which one actually helped (or which one made something else worse).
- Order matters: fixing a full scan or a skewed join usually dwarfs the gains from micro-tuning executor sizing, so start with the biggest structural issue in the Spark UI (Day 20), not the smallest config knob.
- The loop never truly ends — a job that was optimal for last month's data volume can silently become the next quarter's biggest offender as data grows (see the leading-indicator alerting below).

### 3. Observability layers

| Layer | Tool | Watch |
|-------|------|-------|
| Per-app | Spark UI / History Server | stages, skew, spill, SQL metrics |
| Cluster | kubectl top / Grafana (Prometheus) | namespace quota utilization, pending pods |
| Metrics | Spark metrics sink → Prometheus/Graphite | executor mem, GC, task rates |
| Pipeline | Airflow | SLAs, retries, DAG duration |
| Data | dbt tests / DQ gates | freshness, quality |

```bash
# Ship Spark metrics to Prometheus (via a metrics.properties sink) for dashboards + alerts.
--conf spark.metrics.conf=/etc/spark/metrics.properties
--conf spark.eventLog.enabled=true    # so the History Server can replay every run
```

**Key Points:**
- Spark's built-in **metrics system** (`spark.metrics.conf`) can emit executor/driver metrics (heap usage, GC time, task counts, shuffle read/write) to a `PrometheusServlet`/`GraphiteSink`/`JmxSink` — Prometheus scraping the driver/executor pods' metrics endpoint is the standard on-prem pattern, feeding Grafana dashboards.
- The **History Server** replays completed applications from event logs — it's the source of truth for "what actually happened" after a job finishes, independent of whether you were watching the live Spark UI at the time.
- These five layers answer different questions: is *this run* healthy (per-app), is the *cluster* healthy (cluster/metrics), is the *pipeline* on schedule (pipeline), and is the *data* trustworthy (data) — an incident review usually needs to check more than one layer to find the real root cause.

**Example (`metrics.properties` sketch):**
```properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
driver.sink.prometheusServlet.path=/metrics/driver/prometheus
executor.sink.prometheusServlet.path=/metrics/executor/prometheus
```

### 4. Designing alerts and SLAs that actually get acted on

**Key Points:**
- An **SLA** is a promise to consumers ("gold tables are fresh by 7am"); an **alert threshold** should fire well before that promise breaks, giving someone time to react — if the alert and the SLA breach happen at the same moment, the alert is too late to be useful.
- Distinguish **leading indicators** (data volume trending up, GC time creeping, stage duration drifting) from **lagging indicators** (the SLA was missed, the job failed) — leading indicators are what let you fix a problem before a consumer ever notices, matching the recurrence-prevention mindset from Day 21.
- Alert fatigue is a real cost: a threshold set too tight pages people for noise and trains them to ignore alerts; a threshold set too loose misses real degradation. Tune thresholds against the History Server's own historical distribution (e.g. "alert if duration exceeds the 95th percentile of the last 30 runs"), not an arbitrary fixed number.

**Example:**
```python
# A simple trend-based alert using the last N runs from the History Server / a metrics store,
# instead of a fixed threshold that ignores normal variance.
recent_durations = [42, 40, 45, 41, 39, 44, 43]  # minutes, last 7 runs
import statistics
mean, stdev = statistics.mean(recent_durations), statistics.stdev(recent_durations)
today = 58
if today > mean + 2 * stdev:
    alert(f"nightly_sales_rollup duration {today}m is {today - mean:.0f}m above its 7-day trend")
```

## 🔍 Deep Dive: A cost/observability review of one job

### Step-by-Step Process

1. **History Server**: total task time, biggest stage, skew, spill.
2. **Resources held**: executors × cores × duration — are executors idle (over-allocated) or queued (under)?
3. **I/O**: bytes read vs result size — full scan? small files?
4. **Trend**: is duration/data creeping up week over week? Set an alert before it breaches SLA.
5. **Act**: apply the single highest-impact fix; re-measure against the History Server baseline.

### Example: Worked review

```
Job: nightly_sales_rollup   Duration: 42 min   Executors: 40 x 5 cores (held whole run)

History Server findings:
  - Stage 3 (the groupBy) = 31 of 42 min of total task time -> the clear bottleneck
  - Max task duration 340s vs median 12s in Stage 3           -> classic skew (Day 10)
  - Spill (Disk) present in Stage 3: 8 GB per task on the skewed key -> confirms skew, not just slow I/O
  - Bytes read in Stage 1 (the scan): 900 GB read, 4 GB written as final output
    -> 225x read:write ratio suggests a missed partition-pruning opportunity (Day 5)

Resources held: 40 executors x 5 cores x 42 min = 8,400 core-minutes for a job whose
  real bottleneck (one skewed key) affects a single stage -> most of that capacity was
  reserved, not used, for the other 39 minutes of mostly-idle executors during Stage 3.

Act: apply salting to the skewed join key (Day 10) AND add a date filter that was
  missing from the source read (fixes the 225x scan ratio).
Re-measure: duration drops to 11 min; executors right-sized to 15 x 5 cores based on
  the new, unskewed task distribution -> ~85% fewer core-minutes for the same output.
```

**Analysis:**
- The single most expensive-looking number (42 minutes, 40 executors) turned out to have two independent root causes stacked together — skew *and* a missed scan filter — which is exactly why the loop insists on fixing one thing and re-measuring: fixing only the skew would have left most of the waste in place.
- "Executors held" is the right cost lens here, not just wall-clock duration: a 42-minute job holding 40 executors the whole time is far more expensive to the shared cluster than an 11-minute job holding 15, even before considering that the 11-minute job also finishes faster for its downstream consumers.
- The read:write ratio (900 GB read for 4 GB written) is a fast, cheap smell test for "are we scanning more than we need" that doesn't require deep profiling — check it before diving into stage-level skew analysis.

### Example: A namespace-level cost rollup query

Once `SparkApplication`s are consistently labeled (team/namespace/pipeline) and their metrics land in Prometheus, a cost-attribution report is just a `GROUP BY` over the scraped metrics — the same mental model as the Superset marts from Day 37, applied to platform metrics instead of business data:

```sql
-- Against a metrics table fed by the Prometheus sink (e.g. via a periodic export job)
SELECT namespace, team,
       SUM(executor_cores * active_duration_seconds) AS core_seconds,
       SUM(executor_cores * active_duration_seconds) / 3600.0 AS core_hours
FROM spark_executor_metrics
WHERE run_date = current_date() - 1
GROUP BY namespace, team
ORDER BY core_hours DESC;
-- Feed this into a daily Grafana panel or a Superset mart of its own — "which namespace
-- consumed the most core-hours yesterday" is the platform-level analog of Day 37's
-- "which query is the heaviest recurring BI load."
```

## 💡 Key Insights for On-Premise

### 1. Idle executors are pure waste
Fixed executor pods sitting idle between stages hold cores others could use. Dynamic allocation + shuffle tracking (Day 18; no external shuffle service on K8S) returns them — one of the biggest shared-cluster efficiency wins.

### 2. Alert on leading indicators
Don't wait for the SLA breach. Alert on **data-volume growth**, **rising GC/spill**, and **stage-time trend** — the causes that *will* breach the SLA next month (ties to Day 21 recurrence prevention).

### 3. Cost attribution needs a tagging convention
Label every `SparkApplication` (and its pods) with `team`/`namespace`/`pipeline` labels so Prometheus/Grafana can roll up executor-core-seconds per tenant (Day 39). Without a consistent labeling convention up front, retroactive cost attribution across hundreds of historical runs is nearly impossible.

### 4. Cheap dashboards beat perfect ones
A Grafana panel showing "executor-core-seconds per namespace per day" that's actually watched weekly is more valuable than a perfect, unused cost model. Start with the metrics you already have from `spark.eventLog` and the metrics sink before building anything bespoke.

### 5. The optimization loop and the assessment/capstone below are the same skill
Everything in Phases 1-4 (skew, partitioning, joins, caching, UDFs) exists to be applied through exactly this loop — Day 40 isn't new material so much as the discipline of applying all of it, deliberately and repeatedly, against a real job.

## 🎯 Practical Exercises

### Exercise 1: Cost/observability review
```python
# See exercises/production/exercise-40-cost-observability.py
# Run a job, then read its History Server entry and fill a resource/waste scorecard.
```

### Exercise 2: Capstone kickoff
See [assessments/capstones/](../assessments/capstones/). Pick one and scope it:
- **Skew hunt & fix** — find and remediate a slow skewed join end-to-end.
- **Iceberg CDC pipeline** — bronze→silver MERGE + compaction + time travel.
- **BI acceleration** — pre-aggregate a mart and cut a "dashboard" query's cost.

### Exercise 3: Build a namespace cost rollup
```python
# Extend exercise 40: tag the sample job's SparkApplication config with team/namespace
# labels (as comments/a dict), then sketch (in code comments or a small script) how you'd
# aggregate executor-core-seconds per namespace per day from event-log/metrics data,
# and what threshold would trigger a "this namespace is over its expected budget" alert.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Executor-core-seconds per job** (efficiency trend) — the primary on-prem cost proxy; track it per job and roll it up per namespace.
2. **Idle vs busy executor time** — a high idle fraction is the clearest sign of over-allocation or a job whose stages don't parallelize well.
3. **SLA adherence and leading indicators** (volume, GC, spill) — alert on the leading indicators, not just the SLA breach itself.
4. **Namespace quota utilization** (Day 39) alongside cost — a namespace near its quota ceiling *and* trending up in core-seconds needs a capacity conversation before it starts blocking other tenants.
5. **Read:write byte ratio** per job — a fast smell test for missed pruning/full-scan issues (Day 5, Day 27).

### Spark UI Analysis
- **History Server → SQL tab**: bytes read vs rows/bytes output for the final result — the fast way to spot a full scan or a missing partition filter.
- **Stages tab**: max/median task-duration ratio flags skew (Day 10); the Spill (Memory/Disk) columns confirm whether skew is also forcing disk spill.
- **Executors tab**: compare each executor's active task time against total wall-clock duration — a low ratio across the board means executors were reserved far longer than they were actually busy, the core "idle executor" waste pattern.
- **Timeline view**: visually confirms whether dynamic allocation actually scaled executors down between stages, or whether a fixed executor count sat idle the whole run.

## 🚨 Common Issues & Solutions

### Issue 1: Job "works" but hogs the cluster
**Symptom**: complaints from other teams; the job's namespace sits near its `ResourceQuota` ceiling even though the job "succeeds."
**Root Cause**: fixed (non-dynamic) executor allocation sized for the worst-case stage, held for the entire job, plus an unaddressed full scan or skew inflating the worst-case stage in the first place.
**Solution**: dynamic allocation, right-size executors, fix full scans/skew — reduce resources held.

### Issue 2: Silent degradation until an outage
**Symptom**: a job that slowly got slower finally breaches SLA, seemingly out of nowhere.
**Root Cause**: alerting only fires on the SLA breach itself, so a multi-week trend (growing data volume, creeping GC time) was invisible until it crossed the threshold.
**Solution**: trend-based alerting on data volume and stage time — catch the trend weeks before it becomes an incident.

### Issue 3: Cost can't be attributed to a team
**Symptom**: "the cluster is expensive/full" but nobody can say which team or pipeline is responsible.
**Root Cause**: `SparkApplication`s aren't consistently labeled with owning team/namespace, so metrics can't be rolled up per tenant after the fact.
**Solution**: enforce a labeling convention (team, namespace, pipeline) at `SparkApplication` creation time — ideally via an admission policy (Day 39) — so every historical run is attributable going forward.

### Issue 4: Metrics exist but nobody looks at them
**Symptom**: Prometheus/Grafana is wired up, but the same waste patterns (idle executors, full scans) recur for months.
**Root Cause**: dashboards exist but aren't part of any routine review; metrics collection without a review cadence doesn't change behavior.
**Solution**: put a lightweight weekly review of the top-N most expensive jobs (by executor-core-seconds) on the team's calendar — the loop only works if someone actually runs it.

### Issue 5: Event logs pile up and slow the History Server
**Symptom**: the History Server becomes slow to list or open applications after months of accumulated event logs.
**Root Cause**: `spark.eventLog.enabled=true` (correctly) keeps every run's logs, but nothing ever cleans up old ones.
**Solution**: configure the History Server's log-cleaner settings (retention window) so old event logs are pruned on a schedule, the same way Iceberg snapshots need `expire_snapshots` (Day 34) to keep metadata from bloating.

### Issue 6: Alerts fire constantly and get ignored
**Symptom**: an on-call channel is full of Spark job alerts, and the team has started muting the channel.
**Root Cause**: thresholds were set as fixed numbers (e.g. "alert if duration > 30 min") without accounting for normal run-to-run variance, so routine fluctuation trips the alert as often as real problems do.
**Solution**: move to trend-based thresholds derived from each job's own recent history (e.g. mean + N standard deviations, or a percentile of the last 30 runs) so alerts fire on genuine deviation, not normal noise — and review/tune thresholds periodically as the job's baseline shifts.

## 📝 Key Takeaways
1. On-prem cost = shared resources held × time; minimize both.
2. Optimize in a measured loop against a History Server baseline — one change at a time.
3. Layer observability: app, cluster, metrics, pipeline, data.
4. Idle executors are waste — use dynamic allocation.
5. Alert on leading indicators, not just SLA breaches.
6. Attribute cost to teams/namespaces with a consistent labeling convention, enforced at creation time.
7. Metrics only change behavior if someone reviews them on a cadence — build that habit, not just the dashboard.
8. Set alert thresholds from each job's own historical trend, not a fixed number — otherwise alerts either fire on noise or miss real degradation.

## 🔗 Next Steps
- 🎉 **You've reached Day 40.** Take [assessments/phase-5-assessment.md](../assessments/phase-5-assessment.md),
  complete the [mastery checklist](../assessments/mastery-checklist.md), and finish a
  [capstone](../assessments/capstones/). Then apply it all at work — that's true mastery.

## 📚 Additional Resources
- Spark metrics system (`spark.metrics.conf`, Prometheus/Graphite sinks)
- History Server configuration and log retention docs
- Prometheus/Grafana for Spark-on-Kubernetes dashboards

---

**Progress**: Day 40/40 ✅ 🎓
