#!/usr/bin/env bash
# One-shot bring-up of the local Spark-on-Kubernetes practice cluster (minikube).
# Replaces the old `docker compose up`. Idempotent — safe to re-run.
#
# Prereqs: minikube, kubectl, helm, and Docker (minikube's driver) installed.
# On Windows run this from Git Bash, or follow environment/README.md for the
# equivalent PowerShell commands.
set -euo pipefail

SPARK_IMAGE="spark:3.5.1"
NS="spark-jobs"

echo "==> 1/9  Start minikube (4 CPU / 8g is comfortable for the exercises)"
if minikube status >/dev/null 2>&1 && [ "$(minikube status --format='{{.Host}}')" = "Running" ]; then
    echo "✅ Minikube is running."
else
  echo "⚠️ Minikube is not running. Starting it with command: minikube start"
  minikube start
fi

echo "==> 2/9  Create namespaces and quotas (must exist before Spark Operator install)"
kubectl apply -f environment/k8s/00-namespaces-quota.yaml

echo "==> 3/9  Install the Spark Operator (kubeflow) via Helm"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={${NS}}" \
  --set webhook.enable=true

echo "==> 4/9  Load a Spark image into the minikube node"
# Pull the base Spark image and load it into minikube.
# Note: apache/spark:3.5.1 does NOT include hadoop-aws jars; they are added
# via initContainers in the K8s manifests (03-spark-history.yaml, etc.).
docker build -t ${SPARK_IMAGE} .
minikube image load ${SPARK_IMAGE}

echo "==> 5/9  Apply RBAC, MinIO, History Server"
kubectl apply -f environment/k8s/01-spark-rbac.yaml
kubectl apply -f environment/k8s/02-minio.yaml
kubectl -n default rollout status deploy/minio --timeout=180s
kubectl apply -f environment/k8s/03-spark-history.yaml

echo "==> 6/9  Create the MinIO buckets (warehouse, spark-events) and logs/ directory"
export MSYS_NO_PATHCONV=1
kubectl -n default run mc --rm -i --restart=Never --image=minio/mc:latest \
  --command -- /bin/sh -c '
    mc alias set local http://minio.default.svc.cluster.local:9000 minioadmin minioadmin &&
    mc mb -p local/warehouse local/spark-events &&
    touch /tmp/.keep && mc cp /tmp/.keep local/spark-events/logs/.keep || true
  '

echo "==> 7/9  Create ConfigMaps for Python code (common + exercises)"
kubectl create configmap spark-code-common \
  --from-file=common/ -n "${NS}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap spark-code-dag-analysis \
  --from-file=exercises/fundamentals/ -n "${NS}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> 8/9  Deploy the example SparkApplication"
kubectl apply -f environment/k8s/05-example-sparkapplication.yaml

echo "==> 9/9  Done. Handy commands:"
cat <<'EOF'
  kubectl -n spark-jobs get pods
  kubectl -n spark-jobs port-forward svc/spark-history 18080:18080   # History UI
  kubectl -n default    port-forward svc/minio 9001:9001             # MinIO console
  # Submit an exercise (native spark-submit into the cluster):
  kubectl -n spark-jobs apply -f environment/k8s/05-example-sparkapplication.yaml
  # Tear everything down:
  minikube delete
EOF
