# Local Environment 🧪

Run **every** exercise on your laptop — no access to the production cluster required.
The realistic path runs Spark on a real (single-node) **Kubernetes** cluster via
**minikube**, so you see actual **driver and executor pods**, and shuffle / skew /
data-locality lessons are observable.

Storage is **MinIO** (S3-compatible) reached over `s3a://` — there is no HDFS. The
cluster manager is **Kubernetes**, not YARN.

There are two ways to run. Pick one.

---

## Option A — Local PySpark (fastest to start)

Best for reading plans, small data, and most fundamentals/tuning exercises.

```bash
# 1. Install (a virtualenv is recommended)
pip install -r environment/requirements.txt

# 2. Generate sample data (~1M rows, laptop-friendly)
python environment/generate_data.py --scale small

# 3. Run any exercise (from the repo root)
python exercises/fundamentals/exercise-01-dag-analysis.py
```

Spark runs in-process as `local[*]`. The per-application UI is at **http://localhost:4040**
while a job is running. Finished runs are replayed by the History Server (Option B).

> **Windows note:** local PySpark needs Java 8/11/17 and (sometimes) `winutils.exe`/`HADOOP_HOME`.
> If that's fiddly, use Option B (minikube) — the container image bundles everything.

---

## Option B — minikube Kubernetes cluster (most realistic)

A single-node Kubernetes cluster running the **Spark Operator**, **MinIO** (S3 storage),
and the **History Server**. This is the recommended way to *see* how Spark schedules
driver/executor **pods** and spreads work across them.

```bash
# 0. Prereqs: minikube, kubectl, helm, Docker. Then, from the repo root:
bash environment/setup.sh          # starts minikube, installs the operator, MinIO, buckets
```

<details>
<summary>Windows PowerShell equivalent (if you don't use Git Bash)</summary>

```powershell
minikube start --cpus=4 --memory=8192 --driver=docker
kubectl apply -f environment/k8s/00-namespaces-quota.yaml          # namespaces must exist first
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm upgrade --install spark-operator spark-operator/spark-operator `
  --namespace spark-operator --create-namespace --set "spark.jobNamespaces={spark-jobs}"
docker pull apache/spark:3.5.1; minikube image load apache/spark:3.5.1
kubectl apply -f environment/k8s/01-spark-rbac.yaml
kubectl apply -f environment/k8s/02-minio.yaml
kubectl apply -f environment/k8s/03-spark-history.yaml
```
</details>

**Generate data into MinIO** (writes Parquet to `s3a://warehouse/...`):

```bash
kubectl -n spark-jobs apply -f - <<'YAML'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata: { name: gen-data, namespace: spark-jobs }
spec:
  type: Python
  mode: cluster
  image: spark:3.5.1
  mainApplicationFile: local:///opt/spark-apps/environment/generate_data.py
  arguments: ["--scale", "small", "--out", "s3a://warehouse/data"]
  sparkVersion: "3.5.1"
  driver:   { cores: 1, memory: 2g, serviceAccount: spark, envFrom: [{ secretRef: { name: minio-creds } }] }
  executor: { cores: 2, instances: 2, memory: 2g, envFrom: [{ secretRef: { name: minio-creds } }] }
YAML
```

**Submit an exercise two ways:**

```bash
# (1) Declarative — Spark Operator CRD (used in the production lessons)
kubectl -n spark-jobs apply -f environment/k8s/05-example-sparkapplication.yaml

# (2) Native spark-submit into the cluster (used in the fundamentals lessons)
spark-submit \
  --master k8s://$(minikube ip):8443 \
  --deploy-mode cluster \
  --name dag-analysis \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.container.image=spark:3.5.1 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  local:///opt/spark-apps/exercises/fundamentals/exercise-01-dag-analysis.py
```

| UI | How to reach it | Shows |
|----|-----------------|-------|
| Live driver UI | `kubectl -n spark-jobs port-forward <driver-pod> 4040:4040` → http://localhost:4040 | Jobs, Stages, SQL, Executors |
| History Server | `kubectl -n spark-jobs port-forward svc/spark-history 18080:18080` → http://localhost:18080 | Replay any finished app |
| MinIO console | `kubectl -n default port-forward svc/minio 9001:9001` → http://localhost:9001 | Browse `s3a://` buckets |
| Pods / cluster | `kubectl -n spark-jobs get pods`, `kubectl top pods` | Driver + executor pods, resource use |

Tear everything down with `minikube delete`.

---

## Pointing exercises at data

Exercises read tables through [`common/spark_session.py`](../common/spark_session.py):

- `get_spark("name")` — SparkSession with sensible, observable defaults.
- `read_table(spark, "transactions")` — reads a generated parquet table.

Both honour environment variables so the **same code** runs locally or on the cluster:

| Variable | Default | Use |
|----------|---------|-----|
| `SPARK_MASTER` | `local[*]` | set to `k8s://https://<api-server>:6443` for the cluster |
| `DATA_DIR` | `<repo>/data` | point at `s3a://warehouse/data` on the cluster |
| `ENABLE_ICEBERG` | `0` | `1` adds a local Iceberg catalog (Days 33-34) |
| `SPARK_EVENTLOG` | `1` | write event logs for the History Server |

**Run against the cluster instead of local data:**

```bash
export SPARK_MASTER="k8s://$(minikube ip):8443"
export DATA_DIR=s3a://warehouse/data           # MinIO bucket instead of local disk
python exercises/performance-tuning/exercise-10-data-skew-handling.py
```

---

## Generated datasets

`generate_data.py` writes these Parquet tables into `DATA_DIR`:

| Table | Rows (small) | Notes |
|-------|--------------|-------|
| `customers` | 50K | dimension, medium cardinality |
| `products` | 500 | small → **broadcast-join** candidate |
| `stores` | 50 | tiny → broadcast candidate |
| `transactions` | ~1M | fact, **evenly** distributed, partitioned by `txn_date` |
| `transactions_skewed` | ~1M | fact, ~80% of rows on 5 hot customers → **skew** practice |

Scales: `--scale small` (~1M) · `medium` (~10M) · `large` (~50M, cluster recommended).

---

## Streaming (Days 30-31 only)

```bash
kubectl apply -f environment/k8s/06-kafka.yaml
kubectl -n spark-jobs port-forward svc/kafka 9092:9092 &
python environment/produce_stream.py --rate 20 --topic transactions
```

Then run the Day 30-31 streaming exercises, which read from `localhost:9092`.

---

## What replaced what (coming from the old YARN/Hadoop setup)

| Was (YARN / Hadoop) | Now (Kubernetes) |
|---------------------|------------------|
| `docker compose up` standalone cluster | `minikube` + Spark Operator |
| YARN ResourceManager / NodeManager | kube-scheduler + kubelet |
| YARN container | Pod (driver pod + executor pods) |
| `--master yarn` | `--master k8s://https://<api-server>:6443` |
| HDFS (`hdfs:///`) | MinIO / S3 (`s3a://`) |
| YARN queue | Namespace + ResourceQuota |
| `yarn logs -applicationId` | `kubectl logs <pod>` |
