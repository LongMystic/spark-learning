# Day 35: Airflow Orchestration Integration

## 🎯 Learning Objectives
- Orchestrate Spark jobs from Airflow the robust way
- Choose between operators (SparkKubernetes CRD, SparkSubmit, Livy)
- Make DAGs idempotent, retryable, and observable
- Pass config/dates cleanly from Airflow to Spark

## 📚 Core Concepts

### 1. Airflow's job: schedule & dependencies, not compute
Airflow triggers and tracks tasks; **Spark runs on Kubernetes** — the driver and executor pods do the heavy lifting. Keep transformation logic in versioned Spark jobs; keep Airflow as thin orchestration.

### 2. Ways to launch Spark from Airflow (on-prem Kubernetes)
| Operator | How | Notes |
|----------|-----|-------|
| `SparkKubernetesOperator` + `SparkKubernetesSensor` | applies a `SparkApplication` CRD, then waits for it | **preferred**: declarative, the Spark Operator runs the job; the sensor tracks pod state |
| `SparkSubmitOperator` | runs `spark-submit --master k8s://` from the Airflow worker | needs Spark client + kubeconfig on the worker |
| `LivyOperator` | REST to Apache Livy | no Spark client on workers; good isolation |
| `KubernetesPodOperator` | a bare pod that runs `spark-submit` | lowest-level fallback |

```python
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# The operator applies a SparkApplication CRD; the Spark Operator reconciles it into
# a driver pod (which requests executor pods). The sensor waits for it to finish.
run_etl = SparkKubernetesOperator(
    task_id="daily_etl",
    namespace="spark-jobs",
    application_file="daily_etl_sparkapplication.yaml",  # rendered with {{ ds }}
    kubernetes_conn_id="kubernetes_default",             # a Kubernetes connection, NOT spark_yarn
    do_xcom_push=True,
)

watch_etl = SparkKubernetesSensor(
    task_id="daily_etl_monitor",
    namespace="spark-jobs",
    application_name="daily-etl-{{ ds_nodash }}",
    kubernetes_conn_id="kubernetes_default",
)
run_etl >> watch_etl
```

The `SparkApplication` CRD it applies carries the config and the templated run date:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata: { name: daily-etl-{{ ds_nodash }}, namespace: spark-jobs }
spec:
  type: Python
  mode: cluster
  image: <registry>/spark:3.5.1
  mainApplicationFile: local:///opt/spark-apps/jobs/daily_etl.py
  arguments: ["--run-date", "{{ ds }}"]   # Airflow templating passes the logical date
  sparkVersion: "3.5.1"
  restartPolicy: { type: Never }
  driver:   { cores: 1, memory: 4g, serviceAccount: spark }
  executor: { cores: 5, instances: 30, memory: 14g }
  sparkConf:
    "spark.sql.adaptive.enabled": "true"
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.shuffleTracking.enabled": "true"   # required on K8S (no external shuffle service)
```

### 3. Idempotency + retries (the whole point)
- Pass the **logical date** (`{{ ds }}`) and have the job write only that partition (dynamic partition overwrite / MERGE — Days 21, 34) so **retries and backfills are safe**.
- Set `retries` + `retry_delay`; a transient pod/network failure should self-heal.
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
Never build a big DataFrame **in the DAG file / a PythonOperator on the Airflow worker** — it competes with the scheduler and doesn't use the cluster. Always submit to the cluster (apply a `SparkApplication`/`spark-submit`); don't build big DataFrames in the DAG or a PythonOperator.

### 2. Connection & config management
Store the **Kubernetes context / namespace / image** in an Airflow **Connection** (a `kubernetes` conn such as `kubernetes_default`), not hardcoded. Instead of a Kerberos keytab, the driver/executor pods authenticate to the API server with an **RBAC ServiceAccount**, and read MinIO/S3 credentials from a **Secret** referenced by the SparkApplication (`envFrom: secretRef`) — no keytab to distribute to the worker.

### 3. Observability
Surface the driver-pod name and the History Server URL in task logs (the `SparkKubernetesSensor` tracks pod status) so an operator can jump from a failed Airflow task straight to `kubectl logs <driver-pod>` and the Spark UI.

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
1. Airflow orchestrates; Spark computes on Kubernetes (driver + executor pods).
2. Prefer the SparkKubernetes CRD operator (or SparkSubmit/Livy); keep logic in versioned jobs.
3. Parameterize by logical date and write idempotently → safe retries/backfills.
4. Add a data-quality gate before publish; run Iceberg maintenance after.
5. Never build DataFrames in the Airflow worker.

## 🔗 Next Steps
- **Day 36**: DBT-on-Spark Integration

## 📚 Additional Resources
- Airflow Spark provider; Apache Livy docs

---

**Progress**: Day 35/40 ✅
