#!/usr/bin/env bash
# Sync local Python code into the spark-code-pvc PersistentVolume.
#
# Usage:  bash environment/sync-code.sh          (from project root)
#
# How it works:
#   1. Spins up a tiny busybox pod that mounts the PVC.
#   2. Uses `kubectl cp` to copy common/ and exercises/ into the volume.
#   3. Tears down the helper pod.
#
# Run this whenever you modify Python code and want the cluster to pick it up.
# No Docker rebuild, no image push, no ConfigMap update needed.
set -euo pipefail

NS="spark-jobs"
SYNC_POD="code-sync"

export MSYS_NO_PATHCONV=1
echo "==> Starting helper pod to mount spark-code-pvc ..."
kubectl -n "${NS}" run "${SYNC_POD}" \
  --image=busybox:latest \
  --restart=Never \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "code-sync",
        "image": "busybox:latest",
        "command": ["sleep", "300"],
        "volumeMounts": [{
          "name": "code",
          "mountPath": "/opt/spark-apps"
        }]
      }],
      "volumes": [{
        "name": "code",
        "persistentVolumeClaim": {
          "claimName": "spark-code-pvc"
        }
      }]
    }
  }' 2>/dev/null || true

echo "==> Waiting for helper pod to be ready ..."
kubectl -n "${NS}" wait --for=condition=Ready pod/${SYNC_POD} --timeout=60s

echo "==> Cleaning old code in PVC ..."
kubectl -n "${NS}" exec ${SYNC_POD} -- sh -c "rm -rf /opt/spark-apps/common /opt/spark-apps/exercises"

echo "==> Copying common/ ..."
kubectl cp common/ "${NS}/${SYNC_POD}:/opt/spark-apps/common"

echo "==> Copying exercises/ ..."
kubectl cp exercises/ "${NS}/${SYNC_POD}:/opt/spark-apps/exercises"

echo "==> Verifying ..."
kubectl -n "${NS}" exec ${SYNC_POD} -- find /opt/spark-apps -type f -name "*.py" | head -20

echo "==> Cleaning up helper pod ..."
kubectl -n "${NS}" delete pod ${SYNC_POD} --grace-period=0 --force 2>/dev/null || true

echo "✅ Code synced to spark-code-pvc."
