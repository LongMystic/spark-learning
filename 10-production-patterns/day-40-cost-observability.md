# Day 40: Cost/Observability + Capstone Kickoff

## 🎯 Learning Objectives
- Measure and reduce the real cost of Spark jobs on-premise
- Build observability: metrics, history, alerting, and SLAs
- Establish a continuous optimization loop
- Kick off a capstone that exercises the whole path

## 📚 Core Concepts

### 1. "Cost" on-premise = shared finite resources
There's no cloud bill, but cluster capacity is fixed and shared. A wasteful job **steals capacity** from everyone. Cost ≈ **executor-core-seconds** + **memory reserved** + **I/O**. The goal: the same result with fewer resources held for less time.

### 2. The optimization loop
```
Measure  ->  Find the biggest waste  ->  Fix one thing  ->  Re-measure  ->  repeat
```
Biggest wins usually come from: eliminating full scans (pruning/DPP), fixing skew, right-sizing partitions/executors, replacing UDFs, and pre-aggregating for BI — everything from Phases 1–4 applied deliberately.

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

## 🔍 Deep Dive: A cost/observability review of one job
1. **History Server**: total task time, biggest stage, skew, spill.
2. **Resources held**: executors × cores × duration — are executors idle (over-allocated) or queued (under)?
3. **I/O**: bytes read vs result size — full scan? small files?
4. **Trend**: is duration/data creeping up week over week? Set an alert before it breaches SLA.
5. **Act**: apply the single highest-impact fix; re-measure against the History Server baseline.

## 💡 Key Insights for On-Premise
### 1. Idle executors are pure waste
Fixed executor pods sitting idle between stages hold cores others could use. Dynamic allocation + shuffle tracking (Day 18; no external shuffle service on K8S) returns them — one of the biggest shared-cluster efficiency wins.
### 2. Alert on leading indicators
Don't wait for the SLA breach. Alert on **data-volume growth**, **rising GC/spill**, and **stage-time trend** — the causes that *will* breach the SLA next month (ties to Day 21 recurrence prevention).

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

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Executor-core-seconds per job (efficiency trend).
2. Idle vs busy executor time.
3. SLA adherence and leading indicators (volume, GC, spill).

## 🚨 Common Issues & Solutions

### Issue 1: Job "works" but hogs the cluster
**Symptom**: complaints from other teams.
**Solution**: dynamic allocation, right-size executors, fix full scans/skew — reduce resources held.

### Issue 2: Silent degradation until an outage
**Symptom**: a job that slowly got slower finally breaches SLA.
**Solution**: trend-based alerting on data volume and stage time.

## 📝 Key Takeaways
1. On-prem cost = shared resources held × time; minimize both.
2. Optimize in a measured loop against a History Server baseline.
3. Layer observability: app, cluster, metrics, pipeline, data.
4. Idle executors are waste — use dynamic allocation.
5. Alert on leading indicators, not just SLA breaches.

## 🔗 Next Steps
- 🎉 **You've reached Day 40.** Take [assessments/phase-5-assessment.md](../assessments/phase-5-assessment.md),
  complete the [mastery checklist](../assessments/mastery-checklist.md), and finish a
  [capstone](../assessments/capstones/). Then apply it all at work — that's true mastery.

## 📚 Additional Resources
- Spark metrics system; History Server; Prometheus/Grafana for Spark

---

**Progress**: Day 40/40 ✅ 🎓
