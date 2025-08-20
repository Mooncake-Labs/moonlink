#!/usr/bin/env bash
set -euo pipefail

# ==== Config (override via env) ====
CLUSTER="${CLUSTER:-kind-moonlink-dev}"
NS="${NS:-moonlink}"
MANIFEST_DIR="${MANIFEST_DIR:-deploy/kind}"
DEPLOYMENT_CONFIG_FILE="${MANIFEST_DIR}/deployment/moonlink_deployment.yaml"
SERVICE_CONFIG_FILE="${MANIFEST_DIR}/service/moonlink_service.yaml"
NUKE_NAMESPACE="${NUKE_NAMESPACE:-false}"
NUKE_CLUSTER="${NUKE_CLUSTER:-false}"

# 0) Quick checks
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "Cluster '$CLUSTER' not found. Nothing to clean."
  exit 0
fi

# 1) Optionally delete the kind cluster first
if [[ "$NUKE_CLUSTER" == "true" ]]; then
  echo "Deleting kind cluster: $CLUSTER"
  kind delete cluster --name "$CLUSTER"
  echo "Cleanup completed."
  exit 0
fi

# 2) Check if namespace exists
if ! kubectl get ns "$NS" >/dev/null 2>&1; then
  echo "Namespace '$NS' not found. Nothing to clean."
  exit 0
fi

# 3) Optionally delete the whole namespace
if [[ "$NUKE_NAMESPACE" == "true" ]]; then
  echo "Deleting namespace: $NS"
  kubectl delete ns "$NS" --ignore-not-found
  echo "Cleanup completed."
  exit 0
fi

# 4) Delete specific resources defined by your manifests
if [[ -e "$DEPLOYMENT_CONFIG_FILE" ]]; then
  echo "Deleting resources from path '$DEPLOYMENT_CONFIG_FILE' in namespace '$NS'..."
  kubectl delete -n "$NS" -f "$DEPLOYMENT_CONFIG_FILE" --ignore-not-found --wait=true
else
  echo "Manifest path '$DEPLOYMENT_CONFIG_FILE' not found; skipping manifest deletion."
fi

if [[ -e "$SERVICE_CONFIG_FILE" ]]; then
  echo "Deleting resources from path '$SERVICE_CONFIG_FILE' in namespace '$NS'..."
  kubectl delete -n "$NS" -f "$SERVICE_CONFIG_FILE" --ignore-not-found --wait=true
else
  echo "Manifest path '$SERVICE_CONFIG_FILE' not found; skipping manifest deletion."
fi

# 4) Show what's left (if anything)
echo "Remaining resources in namespace '$NS' (if any):"
kubectl get all,cm,secret,pvc -n "$NS" || true

echo
echo "Cleanup completed."
