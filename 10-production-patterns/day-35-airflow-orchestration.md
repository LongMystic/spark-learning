# Day 35: Airflow Orchestration Integration

## 🎯 Learning Objectives
- Orchestrate Spark jobs from Airflow the robust way
- Choose between operators (SparkSubmit, SSH, Livy, Kubernetes)
- Make DAGs idempotent, retryable, and observable
- Pass config/dates cleanly from Airflow to Spark

## 📚 Core Concepts

### 1. Airflow's job: schedule & dependencies, not compute
Airflow triggers and tracks tasks; **Spark does the heavy lifting on YARN**. Keep transformation logic in versioned Spark jobs; keep Airflow as thin orchestration.

### 2. Ways to launch Spark from Airflow (on-prem)
| Operator | How | Notes |
|----------|-----|-------|
| `SparkSubmitOperator` | runs `spark-submit` from the Airflow worker | needs Spark client + configs on the worker |
| `SSHOperator` / `BashOperator` | ssh to an edge node, `spark-submit` | simple, common on-prem |
| `LivyOperator` | REST to Apache Livy | no Spark client on workers; good isolation |
| Kubernetes operators | pods | if you run Spark-on-k8s |

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_etl = SparkSubmitOperator(
    task_id="daily_etl",
    application="/opt/jobs/daily_etl.py",
    conn_id="spark_yarn",
    conf={"spark.sql.adaptive.enabled": "true",
          "spark.dynamicAllocation.enabled": "true"},
    application_args=["--run-date", "{{ ds }}"],   # Airflow templating passes the logical date
)
```

### 3. Idempotency + retries (the whole point)
- Pass the **logical date** (`{{ ds }}`) and have the job write only that partition (dynamic partition overwrite / MERGE — Days 21, 34) so **retries and backfills are safe**.
- Set `retries` + `retry_delay`; a transient YARN/network failure should self-heal.
- Make tasks **atomic**: one Spark job = one clear output, so a partial failure never leaves half-written state.

## 🔍 Deep Dive: A resilient daily pipeline
```
extract  >>  transform  >>  data_quality_check  >>  publish  >>  iceberg_maintenance
```
- Each task is a separate `spark-submit`, parameterized by `{{ ds }}`.
- `data_quality_check` fails the DAG **before** publishing bad data (Day 38).
- `iceberg_maintenance` runs compaction/expiry (Day 34) after publish.
- Use `depends_on_past`/`wait_for_downstream` carefully — they help correctness but can stall backfills.

## 💡 Key Insights for On-Premise
### 1. Don't run Spark inside the Airflow worker process
Never build a big DataFrame **in the DAG file / a PythonOperator on the Airflow worker** — it competes with the scheduler and doesn't use the cluster. Always submit to YARN.

### 2. Connection & config management
Store the YARN/queue/keytab details in an Airflow **Connection**, not hardcoded. Kerberized clusters need the keytab/principal available to the submitting worker.

### 3. Observability
Surface the Spark app URL (RM/history) in task logs so an operator can jump from a failed Airflow task straight to the Spark UI.

## 🎯 Practical Exercises

### Exercise 1: Parameterized, idempotent job
```python
# See exercises/production/exercise-35-airflow-job.py
# A --run-date-driven Spark job that writes only that date's partition (safe to re-run).
```

### Exercise 2: Sketch the DAG
```python
# Write the operator definitions (no live Airflow needed) for extract->transform->DQ->publish.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Task success rate & retry counts (flapping tasks).
2. SLA misses / DAG duration trend.
3. Per-job Spark app metrics via the linked UI.

## 🚨 Common Issues & Solutions

### Issue 1: Backfill produces duplicates
**Symptom**: re-running a date double-counts.
**Solution**: partition by run-date and use dynamic overwrite / MERGE — make the write idempotent.

### Issue 2: Airflow worker OOM
**Symptom**: scheduler/worker memory spikes.
**Solution**: you're computing in Airflow — move it into `spark-submit` on the cluster.

## 📝 Key Takeaways
1. Airflow orchestrates; Spark computes on YARN.
2. Prefer SparkSubmit/Livy operators; keep logic in versioned jobs.
3. Parameterize by logical date and write idempotently → safe retries/backfills.
4. Add a data-quality gate before publish; run Iceberg maintenance after.
5. Never build DataFrames in the Airflow worker.

## 🔗 Next Steps
- **Day 36**: DBT-on-Spark Integration

## 📚 Additional Resources
- Airflow Spark provider; Apache Livy docs

---

**Progress**: Day 35/40 ✅
