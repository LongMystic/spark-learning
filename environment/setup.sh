#!/usr/bin/env bash
# One-shot bring-up of the local Spark-on-Kubernetes practice cluster.
# Replaces the old `docker compose up`. Idempotent — safe to re-run.
#
# Prereqs: kubectl, helm, and a running Kubernetes cluster (minikube, kind,
# EKS, GKE, AKS, etc.). Docker is needed only for the initial image build.
# On Windows run this from Git Bash, or follow environment/README.md for the
# equivalent PowerShell commands.
set -euo pipefail

SPARK_IMAGE="longmystic/spark:3.5.1"
NS="spark-jobs"

echo "==> 1/8  Verify cluster connectivity"
if kubectl cluster-info >/dev/null 2>&1; then
  echo "✅ Connected to Kubernetes cluster."
else
  echo "❌ Cannot reach a Kubernetes cluster. Starting minikube cluster (only in this course)."
  minikube start
fi

echo "==> 2/8  Create namespaces and quotas (must exist before Spark Operator install)"
kubectl apply -f environment/k8s/00-namespaces-quota.yaml

echo "==> 3/8  Install the Spark Operator (kubeflow) via Helm"
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={${NS}}" \
  --set webhook.enable=true

echo "==> 4/8  Build & load the Spark image"
# The image contains only Python dependencies (no application code).
# Code is synced separately via a PersistentVolume (see step 7).
docker build -t ${SPARK_IMAGE} .
minikube image load ${SPARK_IMAGE}

echo "==> 5/8  Apply RBAC, MinIO, History Server"
kubectl apply -f environment/k8s/01-spark-rbac.yaml
kubectl apply -f environment/k8s/02-minio.yaml
kubectl -n default rollout status deploy/minio --timeout=180s
kubectl apply -f environment/k8s/03-spark-history.yaml

echo "==> 6/8  Create the MinIO buckets (warehouse, spark-events) and logs/ directory"
export MSYS_NO_PATHCONV=1
kubectl -n default run mc --rm -i --restart=Never --image=minio/mc:latest \
  --command -- /bin/sh -c '
    mc alias set local http://minio.default.svc.cluster.local:9000 minioadmin minioadmin &&
    mc mb -p local/warehouse local/spark-events &&
    touch /tmp/.keep && mc cp /tmp/.keep local/spark-events/logs/.keep || true
  '

echo "==> 7/8  Create PVC and sync Python code into it"
# A PersistentVolumeClaim stores the application code.  Both the Spark
# driver and executor pods mount this PVC.  To update the code after a
# change, just re-run:  bash environment/sync-code.sh
kubectl apply -f environment/k8s/04-spark-code-pvc.yaml
bash environment/sync-code.sh

echo "==> 8/8  Done. Handy commands:"
cat <<'EOF'
  kubectl -n spark-jobs get pods
  kubectl -n spark-jobs port-forward svc/spark-history 18080:18080   # History UI
  kubectl -n default    port-forward svc/minio 9001:9001             # MinIO console
  # Submit an exercise:
  kubectl -n spark-jobs apply -f environment/k8s/05-example-sparkapplication.yaml
  # Re-sync code after editing Python files (no rebuild needed):
  bash environment/sync-code.sh
  # Tear everything down:
  minikube delete   # or: kind delete cluster
EOF
