#!/usr/bin/env bash
set -euo pipefail

# === Configurable parameters ===
CLUSTER="${CLUSTER:-kind-moonlink-dev}"
NS="${NS:-moonlink}"
MANIFEST_DIR="${MANIFEST_DIR:-deploy/kind}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60s}"

DEPLOYMENT_CONFIG_FILE="${MANIFEST_DIR}/deployment/moonlink_deployment.yaml"
SERVICE_CONFIG_FILE="${MANIFEST_DIR}/service/moonlink_service.yaml"

echo "==> Checking if kind cluster exists: $CLUSTER"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "Cluster '$CLUSTER' does not exist. Creating..."
  kind create cluster --name "$CLUSTER"
else
  echo "Cluster '$CLUSTER' already exists."
fi

echo "==> Ensuring namespace: $NS"
kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS"

if docker image inspect moonlink:dev >/dev/null 2>&1; then
    echo "==> Local image 'moonlink:dev' already exists."
else
    echo "==> Building local image 'moonlink:dev'"
    docker build -t moonlink:dev -f Dockerfile.aarch64 .
fi

echo "==> Loading image into kind nodes"
kind load docker-image moonlink:dev --name kind-moonlink-dev

echo "==> Applying Kubernetes manifests from: $DEPLOYMENT_CONFIG_FILE and $SERVICE_CONFIG_FILE"
kubectl apply -f "$DEPLOYMENT_CONFIG_FILE" -f "$SERVICE_CONFIG_FILE" -n "$NS"

DEPLOY_NAME="$(yq '.metadata.name' "$DEPLOYMENT_CONFIG_FILE")"

echo "==> Waiting for deployment rollout: $DEPLOY_NAME"
kubectl rollout status -n "$NS" deploy/"$DEPLOY_NAME" --timeout="$WAIT_TIMEOUT"

echo "==> Current Pods and Services in namespace: $NS"
kubectl get pods,svc -n "$NS"

echo
echo "Setup completed successfully."
