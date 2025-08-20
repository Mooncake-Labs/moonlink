# Moonlink on kind cluster — Setup & Cleanup

A tiny, reproducible workflow to **deploy** Moonlink to a local [kind](https://kind.sigs.k8s.io/) cluster and **tear it down** cleanly. Two scripts only:

- `local_setup_script.sh` — creates/ensures a kind cluster, loads the `moonlink:dev` image, applies manifests, and waits for rollout.
- `cleanup.sh` — deletes resources from your manifests, and can optionally nuke the namespace and/or the entire kind cluster.

---

## Prerequisites

- Docker
- kind
- kubectl
- yq

> **Verify installation:**
> ```bash
> docker --version && kind --version && kubectl version --client=true --output=yaml && yq --version
> ```

---

## Quick Start

```bash
# 1) Create/ensure cluster & namespace, build & load image, deploy manifests, wait for rollout
./deploy/kind/local_setup_script.sh

# 2) Verify deployment status
kubectl get pods,svc -n moonlink

# 3) Clean up (resources only)
./deploy/kind/cleanup.sh
```

## Environment Variables - Setup & Cleanup

- **CLUSTER**  
  Name of the Kubernetes cluster.  
  *Default:* `kind-moonlink-dev`

- **NS**  
  Namespace in the cluster.  
  *Default:* `moonlink`

- **MANIFEST_DIR**  
  Directory containing Kubernetes manifests.  
  *Default:* `deploy/kind`

- **WAIT_TIMEOUT**  
  Maximum time to wait for deployment to be ready.  
  *Default:* `60s`

- **DEPLOYMENT_CONFIG_FILE**  
  Path to the deployment manifest file.  
  *Default:* `${MANIFEST_DIR}/deployment/moonlink_deployment.yaml`

- **SERVICE_CONFIG_FILE**  
  Path to the service manifest file.  
  *Default:* `${MANIFEST_DIR}/service/moonlink_service.yaml`

## Additional Settings

Use these flags with the cleanup script:

```bash
# Delete the entire kind cluster
NUKE_CLUSTER=true ./deploy/kind/cleanup.sh

# Delete the 'moonlink' namespace
NUKE_NAMESPACE=true ./deploy/kind/cleanup.sh
```

---

## Verify Deployment Status

Test connectivity to different service ports:

```bash
# Test health endpoint on port 3030
kubectl port-forward -n moonlink svc/moonlink-service 3030:3030
curl 127.0.0.1:3030/health

# Test connectivity on port 3031
kubectl port-forward -n moonlink svc/moonlink-service 3031:3031
nc -vz 127.0.0.1 3031
 
# Test web interface on port 8080
kubectl port-forward -n moonlink svc/moonlink-service 8080:8080
kubectl get pods -l app=moonlink-dev -n $NS -o name # obtain pod name
kubectl exec -n moonlink -it <pod name> -c nginx -- sh -lc 'echo "<h1>OK</h1>" > /usr/share/nginx/html/index.html'
curl 127.0.0.1:8080
```

## Manifest Structure

The deployment uses the following manifests:
- `/deploy/kind/deplyment/moonlink_deployment.yaml` - Main deployment with moonlink, nginx, and postgres containers
- `/deploy/kind/service/moonlink_service.yaml` - Service to expose the deployment ports