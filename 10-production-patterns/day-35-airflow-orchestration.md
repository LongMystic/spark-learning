# Day 35: Airflow Orchestration Integration

## 🎯 Learning Objectives
- Orchestrate Spark jobs from Airflow the robust way
- Choose between operators (SparkKubernetes CRD, SparkSubmit, Livy, KubernetesPod)
- Make DAGs idempotent, retryable, and observable
- Pass config/dates cleanly from Airflow to Spark using Jinja templating
- Design a multi-task pipeline with quality gates, SLAs, and failure callbacks
- Diagnose the operational failure modes unique to Airflow-on-Kubernetes

## 📚 Core Concepts

### 1. Airflow's job: schedule & dependencies, not compute

Airflow triggers and tracks tasks; **Spark runs on Kubernetes** — the driver and executor pods do the heavy lifting. Keep transformation logic in versioned Spark jobs; keep Airflow as thin orchestration.

**Key Points:**
- Airflow's scheduler decides *when* and *in what order* tasks run; it never should decide *how much data to process* by materializing it itself.
- A DAG is a contract about dependencies and timing (`extract >> transform >> publish`), not a place to write business logic.
- Every task in the DAG should map to a single Spark application (or a lightweight API call), so failure and retry semantics are clean at the task boundary.

**Example:**
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="daily_sales_etl",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,          # don't silently backfill every historical day on deploy
    max_active_runs=1,      # one run of THIS dag at a time (protects idempotent writes)
    default_args=default_args,
    tags=["etl", "spark", "production"],
) as dag:
    ...
```

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
    poke_interval=30,
    timeout=60 * 60 * 2,   # fail the sensor (not just retry forever) after 2h
    mode="reschedule",     # frees the worker slot between pokes — cheap long waits
)
run_etl >> watch_etl
```

**Alternative: `SparkSubmitOperator`** (useful when the Airflow worker image already ships a Spark client and you don't run the Spark Operator):
```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_etl_submit = SparkSubmitOperator(
    task_id="daily_etl_submit",
    conn_id="spark_k8s",                       # Airflow Connection holding master=k8s://... + namespace
    application="local:///opt/spark-apps/jobs/daily_etl.py",
    application_args=["--run-date", "{{ ds }}"],
    conf={
        "spark.kubernetes.container.image": "<registry>/spark:3.5.1",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    },
    executor_cores=5,
    num_executors=30,
    driver_memory="4g",
    executor_memory="14g",
    verbose=False,
)
```

**Alternative: `KubernetesPodOperator`** (lowest-level fallback — no Spark Operator, no Spark client on the Airflow worker, just a pod that runs `spark-submit` inside its own container):
```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

run_etl_pod = KubernetesPodOperator(
    task_id="daily_etl_pod",
    namespace="spark-jobs",
    name="daily-etl-{{ ds_nodash }}",
    image="<registry>/spark:3.5.1",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=["--master", "k8s://https://kubernetes.default.svc",
               "--deploy-mode", "cluster",
               "local:///opt/spark-apps/jobs/daily_etl.py",
               "--run-date", "{{ ds }}"],
    service_account_name="spark",
    get_logs=True,
    is_delete_operator_pod=True,   # clean up the launcher pod after completion
)
```

### 3. The `SparkApplication` CRD it applies

The CRD carries the config and the templated run date; the Spark Operator (a controller running in the cluster) watches for `SparkApplication` objects and turns each one into a driver pod, which in turn requests executor pods:

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

**Key fields to know:**
- `restartPolicy.type: Never` — let Airflow own retries (via `default_args["retries"]`), not the Spark Operator, so retry counts and alerting stay in one place.
- `driver.serviceAccount` — the RBAC identity the driver pod uses to create/watch executor pods (Day 39).
- `spec.arguments` — this is where Airflow's Jinja-rendered `{{ ds }}` actually lands inside the Spark job's `argparse`.

### 4. Templating: how Airflow variables reach Spark

**Key Points:**
- Airflow renders Jinja templates in specific operator fields (`application_args`, `application_file`, `bash_command`, etc.) at task-execution time, substituting **macros**.
- The most-used macros: `{{ ds }}` (logical date, `YYYY-MM-DD`), `{{ ds_nodash }}` (`YYYYMMDD` — safe for k8s object names, which can't contain `/`), `{{ data_interval_start }}` / `{{ data_interval_end }}` (Airflow 2.2+, the actual processing window — prefer these over `{{ ds }}` for interval-based pipelines), `{{ params }}` (DAG-level parameters), and `{{ ti.xcom_pull(task_ids=...) }}` (pull a value pushed by an upstream task).
- `application_file`/CRD YAML fields must be marked as **templated fields** in the operator (`SparkKubernetesOperator` already does this for `application_file`) or Jinja won't be substituted.

**Example:**
```python
# Passing multiple templated values into a SparkApplication invocation
run_etl = SparkKubernetesOperator(
    task_id="daily_etl",
    namespace="spark-jobs",
    application_file="daily_etl_sparkapplication.yaml",
    kubernetes_conn_id="kubernetes_default",
    params={"source_table": "raw.transactions", "target_table": "silver.transactions"},
)
# Inside daily_etl_sparkapplication.yaml:
#   arguments: ["--run-date", "{{ ds }}", "--source", "{{ params.source_table }}"]
```

## 🔍 Deep Dive: A resilient daily pipeline

### Step-by-Step Process

1. **`extract`**: land raw source data into bronze (append-only), parameterized by `{{ ds }}`.
2. **`transform`**: build silver from bronze using the day's watermark (Day 38).
3. **`data_quality_check`**: validate row counts / null rates / duplicates; **fails the DAG before publishing bad data**.
4. **`publish`**: promote validated silver/gold data — the only task allowed to make data visible to BI/downstream consumers.
5. **`iceberg_maintenance`**: run compaction/snapshot-expiry (Day 34) *after* publish, so maintenance never blocks the SLA-critical path.

```python
extract  >>  transform  >>  data_quality_check  >>  publish  >>  iceberg_maintenance
```

### Example: The full DAG wiring

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import timedelta


def alert_slack_on_failure(context):
    ti = context["task_instance"]
    # post ti.task_id, ti.log_url, context["exception"] to a Slack webhook / PagerDuty
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed on {context['ds']}")


default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_slack_on_failure,
}

with DAG(
    "daily_sales_etl",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=alert_slack_on_failure,
) as dag:

    def spark_task(name, app_file, sla=None):
        run = SparkKubernetesOperator(
            task_id=name, namespace="spark-jobs",
            application_file=app_file, kubernetes_conn_id="kubernetes_default",
            sla=sla,
        )
        watch = SparkKubernetesSensor(
            task_id=f"{name}_monitor", namespace="spark-jobs",
            application_name=f"{name}-{{{{ ds_nodash }}}}",
            kubernetes_conn_id="kubernetes_default", mode="reschedule",
        )
        run >> watch
        return watch

    extract  = spark_task("extract", "extract_app.yaml", sla=timedelta(minutes=30))
    transform = spark_task("transform", "transform_app.yaml")
    dq_check  = spark_task("data_quality_check", "dq_app.yaml")
    publish   = spark_task("publish", "publish_app.yaml", sla=timedelta(hours=1))
    maintenance = spark_task("iceberg_maintenance", "maintenance_app.yaml")
    # Maintenance should still run even if it's delayed by upstream retries,
    # but never block publish — put it last, and let it fail independently.
    maintenance.trigger_rule = TriggerRule.ALL_SUCCESS

    extract >> transform >> dq_check >> publish >> maintenance
```

**Analysis:**
- `on_failure_callback` + `sla_miss_callback` turn silent DAG failures into pages — critical in a shared on-prem cluster where "did today's ETL run?" needs a fast answer.
- `max_active_runs=1` combined with idempotent writes (below) means a slow run never overlaps a retry of itself and corrupts a partition mid-write.
- `mode="reschedule"` on the sensors frees the Airflow worker slot while waiting on a long Spark job — with `poke` mode (the default), long-running Spark jobs would hold a worker slot the entire time, starving other sensors.

### Idempotency + retries (the whole point)
- Pass the **logical date** (`{{ ds }}`) and have the job write only that partition (dynamic partition overwrite / MERGE — Days 21, 34) so **retries and backfills are safe**.
- Set `retries` + `retry_delay` (with exponential backoff); a transient pod/network failure should self-heal.
- Make tasks **atomic**: one Spark job = one clear output, so a partial failure never leaves half-written state.
- `depends_on_past=True` refuses to run today until yesterday succeeded — good for strictly sequential state (e.g. running totals), but it can stall a backfill; use deliberately, not by default.

## 💡 Key Insights for On-Premise

### 1. Don't run Spark inside the Airflow worker process
Never build a big DataFrame **in the DAG file / a PythonOperator on the Airflow worker** — it competes with the scheduler and doesn't use the cluster. Always submit to the cluster (apply a `SparkApplication`/`spark-submit`); don't build big DataFrames in the DAG or a PythonOperator. Even "just a quick pandas check" in a `PythonOperator` on a shared worker can OOM the scheduler host and take down every other DAG.

### 2. Connection & config management
Store the **Kubernetes context / namespace / image** in an Airflow **Connection** (a `kubernetes` conn such as `kubernetes_default`), not hardcoded. Instead of a Kerberos keytab, the driver/executor pods authenticate to the API server with an **RBAC ServiceAccount**, and read MinIO/S3 credentials from a **Secret** referenced by the SparkApplication (`envFrom: secretRef`) — no keytab to distribute to the worker. Keep environment-specific values (image tag, namespace, resource sizing) in Airflow **Variables** or DAG `params` so the same DAG file promotes cleanly from staging to production.

### 3. Observability
Surface the driver-pod name and the History Server URL in task logs (the `SparkKubernetesSensor` tracks pod status) so an operator can jump from a failed Airflow task straight to `kubectl logs <driver-pod>` and the Spark UI. Set `execution_timeout` on tasks so a hung driver pod fails the Airflow task (and pages someone) instead of silently occupying cluster capacity forever.

### 4. Backfills are a first-class operation, not an afterthought
Because each task writes idempotently by logical date, backfilling is just `airflow dags backfill -s 2026-06-01 -e 2026-06-30 daily_sales_etl`. Keep `catchup=False` on the DAG itself (so a redeploy doesn't silently trigger years of runs) and drive backfills explicitly and deliberately.

### 5. Pools and priority weight for shared clusters
Use Airflow **pools** (`spark_jobs_pool`) to cap how many Spark-submitting tasks run concurrently from Airflow's side, complementing the Kubernetes `ResourceQuota` on the cluster side (Day 39) — two independent guardrails against one DAG monopolizing capacity.

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

### Exercise 3: Add resilience primitives
```python
# Extend your Exercise 2 DAG with:
#   - retries + exponential backoff on every task
#   - an execution_timeout and an SLA on the publish task
#   - an on_failure_callback stub that prints what a real alert would contain
#   - max_active_runs=1 and catchup=False
# Then explain, in comments, which failure each primitive protects against.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Task success rate & retry counts** — flapping tasks indicate a transient infra issue (pod scheduling, network) vs. a real bug.
2. **SLA misses / DAG duration trend** — is the pipeline creeping toward its SLA boundary week over week?
3. **Per-job Spark app metrics via the linked UI** — stage time, skew, spill for the Spark application each task launched (Day 40).
4. **Sensor poke/reschedule counts** — a sensor stuck rescheduling for hours may indicate the underlying Spark job is hung, not just slow.
5. **Pool slot utilization** — are Spark-submitting tasks queued behind a full pool during peak hours?

### Spark UI Analysis
- Each Airflow task that runs a `SparkApplication` has a corresponding entry in the **History Server** (Day 40) — link the driver pod's application ID in the task log so operators go from a red Airflow box directly to the Spark UI's Stages/SQL tabs.
- Check the **Executors** tab for the linked application to confirm dynamic allocation actually released executors between DAG runs (idle executors between runs are wasted quota, Day 40).

## 🚨 Common Issues & Solutions

### Issue 1: Backfill produces duplicates
**Symptom**: re-running a date double-counts rows in the target table.
**Root Cause**: the Spark job appends blindly instead of overwriting/merging by the logical date, so a retry or backfill adds a second copy of the same day's data.
**Solution**: partition by run-date and use dynamic overwrite / MERGE — make the write idempotent (Day 21, 34).
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
```

### Issue 2: Airflow worker OOM
**Symptom**: scheduler/worker memory spikes and unrelated DAGs start failing.
**Root Cause**: a `PythonOperator` is computing over real data in the Airflow worker process instead of submitting to Spark.
**Solution**: move the computation into a `spark-submit`/`SparkApplication` on the cluster; the Airflow task should only trigger and poll, never transform.

### Issue 3: Sensor blocks the worker pool
**Symptom**: other DAGs stall even though the cluster has capacity; Airflow worker slots are all "busy" waiting.
**Root Cause**: sensors running in default `poke` mode hold a worker slot for the entire wait window.
**Solution**: set `mode="reschedule"` on long-running sensors (`SparkKubernetesSensor`, `ExternalTaskSensor`) so the slot is released between checks.

### Issue 4: DAG silently stops running after a deploy
**Symptom**: no runs appear after updating the DAG file, or years of backfill suddenly trigger.
**Root Cause**: `start_date` was changed to a value in the past with `catchup=True` (the default historically), or the DAG file has an import error masking the schedule.
**Solution**: set `catchup=False` explicitly, check `airflow dags list-import-errors`, and treat `start_date` as append-only (never move it earlier after the DAG is live).

### Issue 5: Retries mask a real, persistent bug
**Symptom**: a task "succeeds" only after 2-3 retries every single day.
**Root Cause**: retries are configured generously (good for transient pod scheduling delays) but nobody is watching the retry-count metric, so a real, deterministic bug (e.g. a race with an upstream job) hides in plain sight.
**Solution**: alert on **any** retry, not just final failure, and review recurring retries weekly — ties into the leading-indicator alerting philosophy from Day 40.

## 📝 Key Takeaways
1. Airflow orchestrates; Spark computes on Kubernetes (driver + executor pods).
2. Prefer the SparkKubernetes CRD operator (or SparkSubmit/Livy/KubernetesPod); keep logic in versioned jobs.
3. Parameterize by logical date via Jinja templating and write idempotently → safe retries/backfills.
4. Add a data-quality gate before publish; run Iceberg maintenance after, not before.
5. Never build DataFrames in the Airflow worker; use `reschedule` mode for long sensors.
6. Use `on_failure_callback`/SLAs to turn silent failures into pages, and pools to cap concurrent Spark-submitting tasks.
7. `catchup=False` + explicit backfills keep deploys predictable.

## 🔗 Next Steps
- **Day 36**: DBT-on-Spark Integration

## 📚 Additional Resources
- Airflow Spark provider (`apache-airflow-providers-apache-spark`); `apache-airflow-providers-cncf-kubernetes`
- Apache Livy docs
- Airflow Jinja templating reference (macros, `params`, `xcom_pull`)
- Kubernetes Operator for Apache Spark (`spark-operator`) CRD reference

---

**Progress**: Day 35/40 ✅
