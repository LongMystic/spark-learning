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

echo "==> 1/6  Start minikube (4 CPU / 8g is comfortable for the exercises)"
minikube start --cpus=4 --memory=8192 --driver=docker

echo "==> 2/6  Install the Spark Operator (kubeflow) via Helm"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={${NS}}" \
  --set webhook.enable=true

echo "==> 3/6  Load a Spark image into the minikube node"
# Pull an image that bundles the S3A/hadoop-aws jars, then load it into minikube.
docker pull apache/spark:3.5.1
minikube image load apache/spark:3.5.1
minikube image tag apache/spark:3.5.1 "${SPARK_IMAGE}" 2>/dev/null || true

echo "==> 4/6  Apply namespaces, quotas, RBAC, MinIO, History Server"
kubectl apply -f environment/k8s/00-namespaces-quota.yaml
kubectl apply -f environment/k8s/01-spark-rbac.yaml
kubectl apply -f environment/k8s/02-minio.yaml
kubectl -n default rollout status deploy/minio --timeout=180s
kubectl apply -f environment/k8s/03-spark-history.yaml

echo "==> 5/6  Create the MinIO buckets (warehouse, spark-events)"
kubectl -n default run mc --rm -i --restart=Never --image=minio/mc:latest -- \
  sh -c '
    mc alias set local http://minio.default.svc.cluster.local:9000 minioadmin minioadmin &&
    mc mb -p local/warehouse local/spark-events || true
  '

echo "==> 6/6  Done. Handy commands:"
cat <<'EOF'
  kubectl -n spark-jobs get pods
  kubectl -n spark-jobs port-forward svc/spark-history 18080:18080   # History UI
  kubectl -n default    port-forward svc/minio 9001:9001             # MinIO console
  # Submit an exercise (native spark-submit into the cluster):
  kubectl -n spark-jobs apply -f environment/k8s/05-example-sparkapplication.yaml
  # Tear everything down:
  minikube delete
EOF
