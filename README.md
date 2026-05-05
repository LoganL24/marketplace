# Distributed Marketplace (CSCI 5105)

A distributed marketplace built on gRPC with a three-tier architecture: a **Controller** (cluster coordinator), **Service Nodes** (stateless API layer), and **Storage Nodes** (replicated data store). The system can be run locally for development or deployed on Kubernetes for production.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Local Development Setup](#local-development-setup)
- [Running Locally](#running-locally)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Running Tests](#running-tests)
- [API Reference](#api-reference)

---

## Architecture Overview

```
Client
  │
  ▼
Service Nodes  (3 replicas, port 50051)
  │  stateless; routes reads & writes
  ▼
Controller     (1 pod, port 50050)
  │  tracks healthy nodes, elects primary
  ▼
Storage Nodes  (StatefulSet: storage-0, storage-1)
  │  primary handles writes & replicates to backups
  └─ storage-0 ⟺ storage-1  (active replication)
```

| Component | Role | Default Port |
|-----------|------|-------------|
| Controller | Cluster coordinator, leader election, health monitoring | `50050` |
| Service Node | Stateless gRPC API; routes client requests to storage | `50051` (local `50053`) |
| Storage Node | Persistent in-memory store with primary/backup replication | `50051` |

---

## Prerequisites

| Tool | Minimum Version | Purpose |
|------|----------------|---------|
| Python | 3.11+ | Runtime |
| Docker | 20+ | Build container image |
| kubectl | 1.26+ | Kubernetes cluster management |
| A local K8s cluster | — | e.g. [minikube](https://minikube.sigs.k8s.io/) or [kind](https://kind.sigs.k8s.io/) |

---

## Local Development Setup

### 1. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate gRPC code from the proto definition

```bash
chmod +x generate_proto.sh
./generate_proto.sh
```

This compiles `proto/src/marketplace.proto` and writes the generated Python stubs into the same directory. It also applies an import fix required on both macOS and Linux.

---

## Running Locally

Open **three separate terminals** (all with the virtual environment activated) and start each component in order.

### Terminal 1 — Controller

```bash
python -m src.controller
# Listening on port 50050
```

### Terminal 2 — Storage Node (primary)

```bash
python -m src.storage_node
# Connects to controller on localhost:50050
# Registers, is assigned the PRIMARY role
```

### Terminal 3 — Service Node

```bash
python -m src.service_node
# Connects to controller on localhost:50050
# Listens for client requests on port 50053
```

Once all three are running, point your client (or the test scripts) at `localhost:50053`.

### Environment variables (local overrides)

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTROLLER_HOST` | `localhost` | Hostname of the controller |
| `CONTROLLER_PORT` | `50050` | Port of the controller |
| `NODE_PORT` | `50051` | Port this storage node listens on |
| `SERVICE_PORT` | `50053` | Port the service node listens on |
| `POD_IP` | `localhost` | This node's own address (used for self-registration) |
| `PEER_ADDRESSES` | _(empty)_ | Comma-separated list of sibling storage node addresses for replication |

---

## Kubernetes Deployment

### 1. Build the Docker image

The image must be built so Kubernetes can pull it. If you are using **minikube**, point your Docker client at minikube's daemon first:

```bash
# minikube only — skip if using kind or a remote cluster
eval $(minikube docker-env)

docker build -t marketplace-app:latest .
```

> **Note:** The Kubernetes manifests use `imagePullPolicy: IfNotPresent`, so the image must be present in the cluster's local registry.

### 2. Deploy all components

```bash
chmod +x apply_all.sh
./apply_all.sh
```

This script:
1. Creates (or reuses) the `marketplace` namespace and switches to it.
2. Deploys the **Controller** (`k8s/controller.yaml`) and waits 5 seconds for it to become ready.
3. Deploys the **Storage StatefulSet** (`k8s/storage.yaml`) — two pods: `storage-0` and `storage-1`.
4. Deploys three **Service Node** replicas (`k8s/service.yaml`).

### 3. Check pod status

```bash
kubectl get pods
# NAME                            READY   STATUS    RESTARTS
# controller-<hash>               1/1     Running   0
# storage-0                       1/1     Running   0
# storage-1                       1/1     Running   0
# service-node-<hash>-{0,1,2}     1/1     Running   0
```

### 4. Access the service

The controller is exposed via a `LoadBalancer` service on port `50050`.

```bash
# minikube: get the external IP
minikube service controller-service --url -n marketplace

# Other clusters: wait for EXTERNAL-IP to be assigned
kubectl get svc controller-service -n marketplace
```

Service nodes are exposed as a `ClusterIP` service (`service-nodes:50051`) — they are reached internally by other pods or via `kubectl port-forward`:

```bash
kubectl port-forward svc/service-nodes 50053:50051 -n marketplace
# Then point your client at localhost:50053
```

### 5. View logs

```bash
kubectl logs -l app=controller -n marketplace --follow
kubectl logs -l app=service-node -n marketplace --follow
kubectl logs storage-0 -n marketplace --follow
kubectl logs storage-1 -n marketplace --follow
```

### 6. Tear down

```bash
chmod +x delete_all.sh
./delete_all.sh
```

---

## Running Tests

All tests are in the `tests/` directory and can be run with Python's built-in `unittest` runner (virtual environment must be active and gRPC stubs must be generated).

### Run all unit tests

```bash
python -m unittest discover -s tests -v
```

### Run a specific test file

```bash
python -m unittest tests.test_controller -v
python -m unittest tests.test_storage_node -v
python -m unittest tests.test_service_node -v
python -m unittest tests.test_replication_and_fault_tolerance -v
```

### Integration test scripts (requires a running system)

```bash
# Full create / update / optimistic-locking test against localhost:50053
python test_full_system.py

# Backend-only test (storage + controller, no service node)
python test_backend_only.py

# PUT stress test
python test_put.py
```

---

## API Reference

The full protobuf definition lives in `proto/src/marketplace.proto`. The three main client-facing RPCs exposed by the **Service Node** are:

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `CreateItem` | `CreateItemRequest` | `CreateItemResponse` | Add a new listing to the marketplace |
| `UpdateItem` | `UpdateItemRequest` | `UpdateItemResponse` | Update an existing listing (optimistic locking via `expected_version`) |
| `QueryItems` | `QueryRequest` | `QueryResponse` | Search listings by title, category, or description |

Writes are routed to the **primary** storage node; reads are load-balanced across all healthy replicas.
