# Step-by-Step Guide

This guide walks through deploying a two-node k3s cluster, preparing a service account, and deploying the Swarmchestrate monitoring stack with the `swchmonclient` Python library.

## Table of Contents

- [Prerequisites](#prerequisites)
- [1) Kubernetes](#1-kubernetes)
  - [1.1 Install k3s on the master and get the join token](#11-install-k3s-on-the-master-and-get-the-join-token)
  - [1.2 Join the worker node](#12-join-the-worker-node)
- [2) Create the service account and deploy a test pod](#2-create-the-service-account-and-deploy-a-test-pod)
  - [2.1 Clone the monitoring-client repository](#21-clone-the-monitoring-client-repository)
  - [2.2 Create the `mon-client` service account and RBAC](#22-create-the-mon-client-service-account-and-rbac)
  - [2.3 Verify RBAC](#23-verify-rbac)
  - [2.4 Deploy a test pod](#24-deploy-a-test-pod)
- [3) Install swchmonclient, deploy monitoring, and gather metrics](#3-install-swchmonclient-deploy-monitoring-and-gather-metrics)
  - [3.1 Install the library](#31-install-the-library)
  - [3.2 API overview](#32-api-overview)
  - [3.3 Deploy the monitoring stack](#33-deploy-the-monitoring-stack)
  - [3.4 Gather Composite metrics (EPM)](#34-gather-composite-metrics-epm)
  - [3.5 Gather SLO violations (EPM)](#35-gather-slo-violations-epm)
  - [3.6 Gather raw metrics directly from node IPs (EPA)](#36-gather-raw-metrics-directly-from-node-ips-epa)
  - [3.7 Undeploy the monitoring stack](#37-undeploy-the-monitoring-stack)
- [Appendix: Namespace-parametric RBAC](#appendix-namespace-parametric-rbac)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- **2 VMs** (1 master, 1 worker) with unrestricted network access
  - Linux OS (e.g., Ubuntu 22.04+), 2 vCPU / 2 GB RAM minimum each
  - Root or sudo access on both
  - Hostname should be different on both nodes.
  - VMs can reach each other (master port `6443/tcp` must be open to the worker. For full functionality, allow `8472/udp` for flannel VXLAN and `10250/tcp` for kubelet metrics)
  - Outbound internet access (k3s installer, container images, PyPI, release assets)

Throughout this guide:

| Placeholder | Meaning |
| --- | --- |
| `<MASTER_IP>` | Private/internal IP of the master VM |
| `<TOKEN>` | k3s join token from the master |

---

## 1) Kubernetes

### 1.1 Install k3s on the master and get the join token

SSH into the **master VM** and run:

```bash
curl -sfL https://get.k3s.io | sh -
```

Verify the node is up:

```bash
sudo kubectl get nodes
```

Expected output (status `Ready`):

```
NAME      STATUS   ROLES                  AGE   VERSION
master    Ready    control-plane,master   30s   v1.35.x+k3s1
```

#### Get the join token

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

Sample output:

```bash
K10976d249a335a4f8b91ad01a3bfc81e84ebec569e6cb68fc9e9ee67682fc5bd42::server:e1c1bd272551b1cfd827d61fbdd096d1
```

Save this value. It is the `<TOKEN>` the worker needs to join.

Also note the master's IP:

```bash
hostname -I | awk '{print $1}'
```

Sample output:

```bash
192.168.0.135
```

#### (Optional) Use kubectl without sudo

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
export KUBECONFIG=~/.kube/config
```

### 1.2 Join the worker node

SSH into the **worker VM** and run (substitute `<MASTER_IP>` and `<TOKEN>`):

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://<MASTER_IP>:6443 K3S_TOKEN=<TOKEN> sh -
```

Following the sample outputs:

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://192.168.0.135:6443 K3S_TOKEN=K10976d249a335a4f8b91ad01a3bfc81e84ebec569e6cb68fc9e9ee67682fc5bd42::server:e1c1bd272551b1cfd827d61fbdd096d1 sh -
```

Back on the **master**, verify the worker joined:

```bash
sudo kubectl get nodes -o wide
```

Expected output:

```
NAME      STATUS   ROLES                  AGE     VERSION
master    Ready    control-plane,master   5m      v1.35.x+k3s1
worker    Ready    <none>                 30s     v1.35.x+k3s1
```

Both nodes must show `Ready` before continuing.

---

## 2) Create the service account and deploy a test pod

All commands in this section run on the **master** (or anywhere with cluster kubeconfig access).

### 2.1 Clone the monitoring-client repository

```bash
git clone https://github.com/Swarmchestrate/monitoring-client.git
cd monitoring-client
```

### 2.2 Create the `mon-client` service account and RBAC

The service account needs two sets of permissions:

1. **Metric subscriptions** (`node="all"`, `node="local"`, or file-backed `node="cluster"`): read pods and/or nodes.
2. **`deploy_monitoring(...)` / `undeploy_monitoring(...)`**: create, patch, get, list, and delete the resources defined by the monitoring manifests (`emsconfig.yaml` and `ems+netdata-k3s_parametric.yaml`): ConfigMaps, ServiceAccounts, Services, DaemonSets, Deployments, and the netdata/EMS RBAC objects.

#### Easy way: apply the bundled manifest

The repository's `./manifests/mon-client-rbac.yaml` contains everything. Apply it:

```bash
sudo kubectl create namespace swarm-system
sudo kubectl apply -f ./manifests/mon-client-rbac.yaml
```

> The bundled manifest defaults to `swarm-system`. If you deploy elsewhere, update the namespace fields before applying.


If you need a namespace-parametric RBAC manifest instead of editing the bundled one, use the [appendix manifest](#appendix-namespace-parametric-rbac).

### 2.3 Verify RBAC

```bash
sudo kubectl auth can-i get pods --as=system:serviceaccount:swarm-system:mon-client -n swarm-system
sudo kubectl auth can-i get nodes --as=system:serviceaccount:swarm-system:mon-client
sudo kubectl auth can-i list nodes --as=system:serviceaccount:swarm-system:mon-client
# deployer permissions:
sudo kubectl auth can-i create configmaps --as=system:serviceaccount:swarm-system:mon-client -n swarm-system
sudo kubectl auth can-i create daemonsets --as=system:serviceaccount:swarm-system:mon-client -n swarm-system
sudo kubectl auth can-i create deployments --as=system:serviceaccount:swarm-system:mon-client -n swarm-system
sudo kubectl auth can-i delete clusterroles --as=system:serviceaccount:swarm-system:mon-client
```

All should return `yes`. If you installed into another namespace, replace `swarm-system` with your namespace in both the service-account identity and `-n` argument. If you later see `Failed to list Kubernetes nodes: Forbidden` or `Failed to determine current Kubernetes node IP: Forbidden`, the service account authenticates but lacks RBAC.

### 2.4 Deploy a test pod

#### Easy way: apply the bundled manifest

> The bundled test pod manifest uses `swarm-system`. If you installed `mon-client` elsewhere, update its `namespace` field before applying it.

```bash
sudo kubectl apply -f ./manifests/mon-client-test-pod.yaml
```

Wait for it to become ready, then open a shell:

```bash
sudo kubectl wait -n swarm-system --for=condition=Ready pod/python-shell --timeout=120s
sudo kubectl get pod -n swarm-system python-shell
sudo kubectl exec -n swarm-system -it python-shell -- bash
```

If you installed into another namespace, add `-n <NAMESPACE>` to these `kubectl` commands.

Inside the pod, verify the service account token and Kubernetes service env vars are present:

```bash
ls /var/run/secrets/kubernetes.io/serviceaccount
env | grep KUBERNETES_SERVICE
```

You should see `ca.crt`, `namespace`, `token` and the `KUBERNETES_SERVICE_HOST` / `KUBERNETES_SERVICE_PORT` variables.

---

## 3) Install swchmonclient, deploy monitoring, and gather metrics

These steps run **inside the test pod** (the in-cluster service account config is required by `deploy_monitoring`).

### 3.1 Install the library

```bash
apt update && apt install -y nano git && git clone https://github.com/Swarmchestrate/monitoring-client.git
cd monitoring-client
pip install .
```

> **Note:** Always check for the latest version on [PyPI](https://pypi.org/project/swchmonclient/) or the [GitHub releases page](https://github.com/Swarmchestrate/monitoring-client/releases) and install that instead.

### 3.2 API overview

This section summarizes the functions used in the deployment, subscription, raw subscription, SLO subscription, and undeploy steps below.

#### `deploy_monitoring(...)`

```python
deploy_monitoring(
    sat_file: str,
    optimusdb_url: str = "http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb",
    use_kb: bool = True,
    upload_kb: bool = False,
    logger: logging.Logger | None = None,
    namespace: str = "swarm-system",
) -> int
```

**Input parameters:**

| Parameter | Required | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `sat_file` | Yes | `str` | No default | Local path to the SAT YAML file. Its basename is turned into a unique timestamped filename for `SAT_FILE`, and its content is loaded into `ConfigMap/tosca-model-configmap` as `test-tosca-model.yaml`. |
| `optimusdb_url` | No | `str` | Swarmchestrate OptimusDB endpoint | OptimusDB / knowledgebase URL injected into the templated manifest. Used by default because `use_kb` defaults to `True`. |
| `use_kb` | No | `bool` | `True` | Controls the rendered `USE_KB` value in `emsconfig.yaml`. Defaults to `True`, so EMS resolves the SAT through the knowledgebase at `optimusdb_url`. Set to `False` to use the SAT content from the ConfigMap instead. |
| `upload_kb` | No | `bool` | `False` | If `True`, uploads the SAT file to the knowledgebase (under its generated unique filename) before the Kubernetes resources are deployed. Defaults to `False`. |
| `logger` | No | `logging.Logger \| None` | `None` | Custom logger. If omitted, stdout logging is configured automatically. |
| `namespace` | No | `str` | `swarm-system` | Namespace assigned to namespaced monitoring resources and ServiceAccount subjects in RBAC bindings. The namespace must already exist. |

Default `optimusdb_url`: `http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb`

**Output:** a process-style exit code. `0` on success, `1` if one or more deploy steps fail.

**SAT file input parameter:**

A **SAT (Swarm Application Template)** is the main application input to a Swarmchestrate Universe. Application owners author a SAT, which defines the microservices and resource requirements that make up an application.

The `sat_file` value passed to `deploy_monitoring(...)` is a **local file path**. At deploy time the library reads it locally, appends a UTC timestamp to its basename to produce a unique `SAT_FILE` name, injects that into the rendered manifest, and loads the file content into `ConfigMap/tosca-model-configmap` under the fixed key `test-tosca-model.yaml`.

How the SAT is resolved depends on `use_kb`:

- `use_kb=True` (default): EMS resolves the SAT through the knowledgebase at `optimusdb_url`. Combine with `upload_kb=True` to have `deploy_monitoring(...)` upload the SAT to the knowledgebase for you first. No additional upload step is needed.
- `use_kb=False`: EMS uses the SAT content straight from the deployed ConfigMap, so no knowledgebase is involved.

> **Note:** SAT uploading is now built into `deploy_monitoring(...)` via `upload_kb=True`

#### `subscribe_metric(...)`

```python
subscribe_metric(metric: str) -> str
query_metric_values(metric: str) -> list[dict[str, Any]]
unsubscribe_metric(metric: str) -> None
```

`subscribe_metric(...)` starts or reuses the shared EPM listener for a standard metric topic. Plain metric names are normalized to `/topic/<metric>`.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |

`query_metric_values(...)` returns and consumes the latest buffered sample for the metric as `[{"timestamp": ..., "value": ...}]`. `unsubscribe_metric(...)` stops the subscription.

#### `subscribe_slo(...)`

```python
subscribe_slo(
    slo_name: str,
    cache_size: int | None = None,
    on_violation: Callable[[float], None] | None = None,
) -> str
query_slo_violations(slo_name: str) -> list[float]
unsubscribe_slo(slo_name: str) -> None
```

EMS reports an SLO violation by publishing an event on the `<slo_name>_CONSTRAINT`
topic, e.g. `{"level":3,"metricValue":4.497942,"timestamp":1792795789558}`.
`subscribe_slo(...)` listens on that topic through the same shared EPM listener as
`subscribe_metric(...)`.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `slo_name` | Yes | `str` | SLO name as defined in the SAT `slo-constraints` (e.g. `high_cpu`). `high_cpu_CONSTRAINT` and `/topic/high_cpu_CONSTRAINT` are accepted too. |
| `cache_size` | No | `int \| None` | Maximum number of buffered violations for this SLO; older ones are dropped first. Defaults to `1000`. |
| `on_violation` | No | `Callable[[float], None] \| None` | Called with each violation's timestamp (Unix seconds) as it arrives. Runs on the listener thread, so keep it short; exceptions are logged and ignored. |

`query_slo_violations(...)` returns and consumes the timestamps (Unix seconds) of
every buffered violation, in arrival order. `unsubscribe_slo(...)` stops the subscription.

#### `subscribe_metric_raw(...)`

```python
subscribe_metric_raw(
    metric: str,
    node: list[str] | str | None = None,
    cache_size: int | None = None,
    *,
    source_file: str | Path | None = None,
) -> dict[str, str]
query_metric_values_raw(metric: str, seconds: int) -> dict[str, list[dict[str, Any]]]
unsubscribe_metric(metric: str, nodes: list[str] | None = None) -> None
```

`subscribe_metric_raw(...)` starts raw metric listeners that connect directly to
node IPs. Pass `source_file=` to replay values from a versioned JSON file without
connecting to the monitoring system.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |
| `node` | Live mode only | `list[str] \| str \| None` | Live mode requires explicit node/IP values, `"all"`, or `"local"`. In file mode, omit it for all matching file nodes, select explicit file IDs, or use `"cluster"`. |
| `cache_size` | No | `int \| None` | Per metric per node sample buffer size. Defaults to `1000`. |
| `source_file` | No | `str \| Path \| None` | JSON replay file. In this mode, omitted `node` or `"all"` selects every file node defining the metric; `"cluster"` maps unique file profiles onto Kubernetes VM private IPs; `"local"` is not supported. |

File mode does not require a `node` argument:

```python
subscribe_metric_raw(
    "cpu_util_prct",
    source_file="./examples/raw_metrics.json",
)
```

Use `node="cluster"` to discover the current Kubernetes VM private IPs and map
file profiles onto them. The mapping is one-to-one and deterministic: existing
valid assignments for the source file are preserved, exact profile/IP matches
are preferred when initially available, and a profile is never reused for two
nodes. Surplus profiles remain unused; surplus cluster nodes are logged and do
not receive replay values. Cluster discovery requires permission to list nodes.
The client refreshes the cluster every 30 seconds, maps new nodes without
changing valid assignments, and unsubscribes departed nodes. A failed refresh
keeps the last valid replay state active and logs the error.

For example, the bundled ten-profile fixture can replay CPU and RAM data for up
to ten cluster nodes while preserving the same assignment across both metrics:

```python
source_file = "./examples/raw_metrics_10_nodes.json"
subscribe_metric_raw("cpu_util_prct", "cluster", source_file=source_file)
subscribe_metric_raw("ram_util_prct", "cluster", source_file=source_file)
```

Query results use actual Kubernetes VM private IPs as keys, not the file profile
IDs.

`query_metric_values_raw(...)` returns and consumes buffered raw values from the requested time window. `unsubscribe_metric(...)` stops all raw node listeners for the metric, or only the nodes passed with `nodes=`.

The file schema is:

```json
{
  "version": 1,
  "nodes": [
    {
      "node_id": "simulated-node-a",
      "metrics": {
        "cpu_util_prct": {
          "interval_seconds": 5,
          "values": [42.0, 44.5],
          "repeat": true
        }
      }
    }
  ]
}
```

`interval_seconds` defines regular reporting frequency; no separate
`delta_time` is needed. For an irregular trace, use `samples` entries with
`offset_seconds` and, when repeating, a `cycle_duration_seconds`. Replay offsets
are anchored when the subscription starts, and emitted samples receive
wall-clock timestamps so normal raw time-window queries continue to work.

#### `undeploy_monitoring(...)`

```python
undeploy_monitoring(
    namespace: str | None = "swarm-system",
    logger: logging.Logger | None = None,
) -> int
```

`undeploy_monitoring(...)` removes the monitoring stack and its cleanup resources. Unlike deploy, it does **not** take `sat_file`, `optimusdb_url`, `use_kb`, or `upload_kb`. It deletes `ConfigMap/emsconfig` and `ConfigMap/tosca-model-configmap` by name and undeploys the remaining manifest-defined resources from the static manifest files.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `namespace` | No | `str \| None` | Namespace containing the resources to delete. Defaults to `swarm-system`. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** a process-style exit code. `0` on success, `1` if one or more undeploy steps fail.

### 3.3 Deploy the monitoring stack

> Before running the example, set `USE_KB`, `UPLOAD_KB`, and `NAMESPACE` in `./examples/deploy.py` for your deployment. `NAMESPACE` defaults to `swarm-system`, which must already exist. Use `USE_KB=True` and `UPLOAD_KB=True` when the script should upload the SAT and EMS should resolve it through the knowledgebase. Use `USE_KB=False` when the script should read the SAT from the local path and deploy it in the ConfigMap.

```
python ./examples/deploy.py
```

Expected output:

```log
2026-10-29 10:27:54,324 INFO swchmonclient.deploy_monitoring: Found local manifest ./manifests/emsconfig.yaml.
2026-10-29 10:27:55,245 INFO swchmonclient.deploy_monitoring: Local manifest ./manifests/emsconfig.yaml matches release asset.
2026-10-29 10:27:55,245 INFO swchmonclient.deploy_monitoring: Found local manifest ./manifests/ems+netdata-k3s_parametric.yaml.
2026-10-29 10:27:55,343 INFO swchmonclient.deploy_monitoring: Local manifest ./manifests/ems+netdata-k3s_parametric.yaml matches release asset.
2026-10-29 10:27:55,359 INFO swchmonclient.deploy_monitoring: Deploying ConfigMap/tosca-model-configmap from SAT file /monitoring-client/manifests/stressng.yaml ...
2026-10-29 10:27:55,388 INFO swchmonclient.deploy_monitoring:   Created or patched resources:
2026-10-29 10:27:55,388 INFO swchmonclient.deploy_monitoring:   - ConfigMap/tosca-model-configmap (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,457 INFO swchmonclient.deploy_monitoring: Deploying ./manifests/emsconfig.yaml with variables:
2026-10-29 10:27:55,457 INFO swchmonclient.deploy_monitoring:     • sat_file: stressng-20261029102755344197.yaml
2026-10-29 10:27:55,457 INFO swchmonclient.deploy_monitoring:     • optimusdb_url: http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb
2026-10-29 10:27:55,458 INFO swchmonclient.deploy_monitoring:     • use_kb: False
2026-10-29 10:27:55,458 INFO swchmonclient.deploy_monitoring:     • upload_kb: False
2026-10-29 10:27:55,478 INFO swchmonclient.deploy_monitoring:   Created or patched resources:
2026-10-29 10:27:55,478 INFO swchmonclient.deploy_monitoring:   - ConfigMap/emsconfig (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,478 INFO swchmonclient.deploy_monitoring: Deploying ./manifests/ems+netdata-k3s_parametric.yaml ...
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   Created or patched resources:
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   - ServiceAccount/netdata (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   - ConfigMap/netdata-conf-child (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   - ConfigMap/netdata-child-sd-config-map (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   - ClusterRole/netdata (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-10-29 10:27:55,706 INFO swchmonclient.deploy_monitoring:   - ClusterRoleBinding/netdata (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - DaemonSet/netdata-child (apiVersion=apps/v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - ServiceAccount/ems-server-service-account (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - ClusterRole/ems-server-cluster-role (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - ClusterRoleBinding/ems-server-cluster-role-binding (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - Role/ems-server-role (apiVersion=rbac.authorization.k8s.io/v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - RoleBinding/ems-server-role-binding (apiVersion=rbac.authorization.k8s.io/v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - Service/emsserver-ems-server (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - ConfigMap/tosca-script-config (apiVersion=v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring:   - Deployment/emsserver-ems-server (apiVersion=apps/v1, namespace=swarm-system)
2026-10-29 10:27:55,707 INFO swchmonclient.deploy_monitoring: All manifests deployed successfully.
Monitoring deployed successfully.
```

Notes:

- Uses in-cluster Kubernetes config from the pod's service account. It does not read a local kubeconfig.
- If `./manifests/emsconfig.yaml` or `./manifests/ems+netdata-k3s_parametric.yaml` is missing locally, the library downloads it from the `v0.1.0` release assets automatically. Existing local copies are compared with the release asset; a copy that differs is kept and a warning is logged.

Verify on the **master** that monitoring workloads are coming up:

```bash
sudo kubectl get pods -A
```

Sample output:

```
NAMESPACE      NAME                                      READY   STATUS      RESTARTS       AGE
swarm-system   ems-client-daemonset-92rmm                1/1     Running     0              15m
swarm-system   ems-client-daemonset-cq6vk                1/1     Running     0              15m
swarm-system   emsserver-ems-server-58c4c8df96-sghln     1/1     Running     1 (15m ago)    16m
swarm-system   netdata-child-7qjps                       1/1     Running     0              16m
swarm-system   netdata-child-q9cdm                       1/1     Running     0              16m
swarm-system   python-shell                              1/1     Running     0              18m
...
```

### 3.4 Gather Composite metrics (EPM)

Run the example:

```bash
python examples/subscribe_metric.py
```

Sample output:

```
2026-10-29 10:30:05,652 INFO root: Subscribing to metric: cpu_util_prct
2026-10-29 10:30:05,654 INFO swchmonclient.listener: Starting listener for [/topic/>] on emsserver-ems-server:61610
2026-10-29 10:30:05,655 INFO root: Latest buffered sample for cpu_util_prct: []
2026-10-29 10:30:05,658 INFO stomp.py: established connection to host emsserver-ems-server, port 61610
2026-10-29 10:30:05,765 INFO swchmonclient.listener: Subscription created successfully
2026-10-29 10:30:10,656 INFO root: Latest buffered sample for cpu_util_prct: []
...
2026-10-29 10:31:05,668 INFO root: Latest buffered sample for cpu_util_prct: []
2026-10-29 10:31:10,669 INFO root: Latest buffered sample for cpu_util_prct: [{'timestamp': 1793269869.422, 'value': 18.98381}]
2026-10-29 10:31:15,670 INFO root: Latest buffered sample for cpu_util_prct: []
^C
2026-10-29 10:31:19,765 INFO root: Stop requested, shutting down metric subscription...
2026-10-29 10:31:20,670 INFO root: Unsubscribing from metric cpu_util_prct
2026-10-29 10:31:20,671 WARNING swchmonclient.listener: Disconnected from ActiveMQ
2026-10-29 10:31:20,671 INFO swchmonclient.listener: Shutdown requested. Closing connection...
2026-10-29 10:31:20,671 INFO swchmonclient.listener: Connection cleanup completed
2026-10-29 10:31:20,672 INFO root: Metric subscription stopped
```

The example subscribes to `cpu_util_prct`, polls until you stop it with `Ctrl+C`, then unsubscribes cleanly.

Or use the API directly:

```python
from swchmonclient import subscribe_metric, query_metric_values, unsubscribe_metric

subscribe_metric("cpu_util_prct")  # plain names normalize to /topic/<metric>

# Wait some seconds while samples are buffered, then:
values = query_metric_values("cpu_util_prct")
# e.g. [{"timestamp": 1716712345.12, "value": 42.0}]. Returned samples are consumed from the buffer
print(values)
# Unsubscribe from the metric
unsubscribe_metric("cpu_util_prct")
```

### 3.5 Gather SLO violations (EPM)

SLO violations are events, not metric values: EMS publishes one on the
`<slo_name>_CONSTRAINT` topic each time the SLO is violated. Use the SLO name from
the SAT `slo-constraints` section, not the metric name.

Run the example:

```bash
python examples/subscribe_slo.py
```

The example polls until you stop it with `Ctrl+C`. A violation only appears while the
SLO is actually violated: with the bundled `stressng.yaml`, that is `cpu_util_prct > 80`
on the 5-minute global average, so both nodes have to be under sustained CPU load for a
few minutes. EMS then publishes a violation on every evaluation (every 30 seconds).

Sample output (both nodes under full CPU load from about 10:34):

```
2026-10-29 10:32:10,786 INFO root: Subscribing to SLO violations: cpu_utilization
2026-10-29 10:32:10,788 INFO swchmonclient.listener: Starting listener for [/topic/>] on emsserver-ems-server:61610
2026-10-29 10:32:10,788 INFO root: Violation timestamps for cpu_utilization: []
2026-10-29 10:32:10,794 INFO stomp.py: established connection to host emsserver-ems-server, port 61610
2026-10-29 10:32:10,807 INFO swchmonclient.listener: Subscription created successfully
...
2026-10-29 10:38:40,870 INFO root: Violation timestamps for cpu_utilization: []
2026-10-29 10:38:45,871 INFO root: Violation timestamps for cpu_utilization: [1793270319.563]
2026-10-29 10:38:50,874 INFO root: Violation timestamps for cpu_utilization: []
...
2026-10-29 10:39:15,882 INFO root: Violation timestamps for cpu_utilization: [1793270351.939]
...
2026-10-29 10:43:15,936 INFO root: Violation timestamps for cpu_utilization: [1793270589.456]
^C
2026-10-29 10:43:39,934 INFO root: Stop requested, shutting down SLO subscription...
2026-10-29 10:43:40,942 INFO root: Unsubscribing from SLO cpu_utilization
2026-10-29 10:43:40,943 INFO swchmonclient.listener: Shutdown requested. Closing connection...
2026-10-29 10:43:40,944 WARNING stomp.py: unable to issue SHUT_RDWR on socket because of error '[Errno 9] Bad file descriptor'
2026-10-29 10:43:40,945 WARNING swchmonclient.listener: Disconnected from ActiveMQ
2026-10-29 10:43:40,945 INFO swchmonclient.listener: Connection cleanup completed
2026-10-29 10:43:40,946 INFO root: SLO subscription stopped
```

Or use the API directly:

```python
from swchmonclient import query_slo_violations, subscribe_slo, unsubscribe_slo

subscribe_slo("cpu_utilization")  # listens on /topic/cpu_utilization_CONSTRAINT

# Later: timestamps of every violation since the last query, in arrival order
for timestamp in query_slo_violations("cpu_utilization"):
    print("violated at", timestamp)

unsubscribe_slo("cpu_utilization")
```

To react immediately instead of polling, pass a callback:

```python
subscribe_slo("cpu_utilization", on_violation=lambda ts: print("SLO violated at", ts))
```

### 3.6 Gather raw metrics directly from node IPs (EPA)

Live `subscribe_metric_raw(metric, node)` supports three selector modes:

| Selector | Behavior | RBAC needed |
| --- | --- | --- |
| `["10.0.0.1", "10.0.0.2"]` | One raw listener per explicit node/IP | None (no Kubernetes API access) |
| `"all"` | Resolves all Kubernetes VM private IPs | cluster-wide `get,list nodes` |
| `"local"` | Resolves the current node's InternalIP | `get pods` in the namespace + cluster-wide `get,list nodes` |

Run the example:

```bash
python examples/subscribe_metric_raw.py
```

The example subscribes to `host_load_instance` (the host's 1-minute load average) on all nodes, polls for up to 180 seconds (`RUN_SECONDS`) or until you stop it with `Ctrl+C`, then unsubscribes cleanly.

> Raw subscriptions read the per-node EMS brokers, which publish the SAT's **raw** metrics, each collected from the local host's Netdata (`host_load_instance`, `cpu_idle_instance`, `ram_free_instance`, ...). Composite metrics with `grouping: "global"`, such as `cpu_util_prct` in `stressng.yaml`, are only computed on the EMS server; read them with `subscribe_metric(...)` instead, otherwise the raw query stays empty.

Sample output:

```
2026-10-29 11:52:58,285 INFO swchmonclient.metrics: Subscribing to raw metric 'host_load_instance' using node selector 'all' resolved to nodes ['192.168.0.135', '192.168.0.60']
2026-10-29 11:52:58,286 INFO swchmonclient.listener: Starting listener for [/topic/>] on 192.168.0.135:61610
2026-10-29 11:52:58,287 INFO swchmonclient.listener: Starting listener for [/topic/>] on 192.168.0.60:61610
2026-10-29 11:52:58,288 INFO root: Started raw listener threads: {'192.168.0.135': 'metric-raw-listener:192.168.0.135', '192.168.0.60': 'metric-raw-listener:192.168.0.60'}
2026-10-29 11:52:58,288 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [], '192.168.0.135': []}
2026-10-29 11:52:58,288 INFO stomp.py: established connection to host 192.168.0.135, port 61610
2026-10-29 11:52:58,290 INFO stomp.py: established connection to host 192.168.0.60, port 61610
2026-10-29 11:52:58,313 INFO swchmonclient.listener: Subscription created successfully
2026-10-29 11:52:58,329 INFO swchmonclient.listener: Subscription created successfully
2026-10-29 11:53:03,289 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [], '192.168.0.135': []}
2026-10-29 11:53:08,289 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [{'timestamp': 1793274785.556, 'value': 1.26}], '192.168.0.135': [{'timestamp': 1793274787.164, 'value': 0.79}]}
2026-10-29 11:53:13,290 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [], '192.168.0.135': []}
...
2026-10-29 11:53:38,293 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [{'timestamp': 1793274815.556, 'value': 0.76}], '192.168.0.135': [{'timestamp': 1793274817.16, 'value': 0.55}, {'timestamp': 1793274817.16, 'value': 0.36}]}
2026-10-29 11:53:43,293 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [], '192.168.0.135': []}
...
2026-10-29 11:54:08,296 INFO root: Buffered raw values for host_load_instance from the last 30 seconds: {'192.168.0.60': [{'timestamp': 1793274845.556, 'value': 0.52}], '192.168.0.135': [{'timestamp': 1793274847.16, 'value': 0.27}]}
^C
2026-10-29 11:54:12,578 INFO root: Stop requested, shutting down raw metric subscription...
2026-10-29 11:54:13,296 INFO root: Unsubscribing from raw metric host_load_instance
2026-10-29 11:54:13,297 INFO swchmonclient.listener: Shutdown requested. Closing connection...
2026-10-29 11:54:13,297 WARNING swchmonclient.listener: Disconnected from ActiveMQ
2026-10-29 11:54:13,298 INFO swchmonclient.listener: Connection cleanup completed
2026-10-29 11:54:13,298 WARNING swchmonclient.listener: Disconnected from ActiveMQ
2026-10-29 11:54:13,298 INFO swchmonclient.listener: Shutdown requested. Closing connection...
2026-10-29 11:54:13,299 WARNING swchmonclient.listener: Disconnected from ActiveMQ
2026-10-29 11:54:13,299 INFO swchmonclient.listener: Connection cleanup completed
2026-10-29 11:54:13,300 INFO root: Raw metric subscription stopped
```

Or use the API directly:

```python
from swchmonclient import subscribe_metric_raw, query_metric_values_raw, unsubscribe_metric

METRIC_NAME = "host_load_instance"
NODES = ["10.0.0.1", "10.0.0.2"]

# Option 1: Subscribe to the raw metric for specific nodes.
# Use this when you want metric data only from the nodes listed in the NODES variable.
# threads = subscribe_metric_raw(METRIC_NAME, NODES)

# Option 2: Subscribe to the raw metric for the local node only.
# Use this when you want metric data only from the node running this code.
# threads = subscribe_metric_raw(METRIC_NAME, "local")

# Option 3: Subscribe to the raw metric for all nodes.
# Use this when you want metric data from every available node.
threads = subscribe_metric_raw(METRIC_NAME, "all")

print(threads)  # {"<node-ip>": "<listener-thread-name>", ...}

raw_values = query_metric_values_raw(METRIC_NAME, seconds=60)
print(raw_values)
# {
#     "10.0.0.1": [
#         {"timestamp": 1716712345.12, "value": 42.0},
#     ],
# }

unsubscribe_metric(METRIC_NAME)
# or stop a single node listener: unsubscribe_metric(METRIC_NAME, nodes=["10.0.0.1"])
```

Notes:

- Each raw metric on each node keeps up to 1000 cached samples (oldest dropped first). Override with `cache_size=`.
- Raw listeners connect directly to node IPs instead of `MON_CLIENT_STOMP_HOST`. One listener thread per node is reused for additional raw metrics.
- Current API behavior rejects standard and raw subscriptions for the same metric name at the same time, even though they connect to different sources.

### 3.7 Undeploy the monitoring stack

Run it from inside the pod, either from Python:

```python
from swchmonclient import undeploy_monitoring

exit_code = undeploy_monitoring(namespace="swarm-system")
```

or via the bundled example:

```bash
python examples/undeploy.py
```

Expected output:

```
2026-10-29 10:27:12,888 INFO swchmonclient.undeploy_monitoring: Found local manifest ./manifests/ems+netdata-k3s_parametric.yaml.
2026-10-29 10:27:13,806 INFO swchmonclient.undeploy_monitoring: Local manifest ./manifests/ems+netdata-k3s_parametric.yaml matches release asset.
2026-10-29 10:27:13,933 INFO swchmonclient.undeploy_monitoring: Undeploying ./manifests/ems+netdata-k3s_parametric.yaml ...
2026-10-29 10:27:14,312 INFO swchmonclient.undeploy_monitoring:   Done. 14 resource(s) deleted.
2026-10-29 10:27:14,312 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/emsconfig ...
2026-10-29 10:27:14,322 INFO swchmonclient.undeploy_monitoring:   Done. 1 configmap resource(s) deleted.
2026-10-29 10:27:14,322 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/tosca-model-configmap ...
2026-10-29 10:27:14,332 INFO swchmonclient.undeploy_monitoring:   Done. 1 configmap resource(s) deleted.
2026-10-29 10:27:14,332 INFO swchmonclient.undeploy_monitoring: Undeploying DaemonSet/ems-client-daemonset ...
2026-10-29 10:27:14,351 INFO swchmonclient.undeploy_monitoring:   Done. 1 daemonset resource(s) deleted.
2026-10-29 10:27:14,352 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/ems-client-configmap ...
2026-10-29 10:27:14,410 INFO swchmonclient.undeploy_monitoring:   Done. 1 configmap resource(s) deleted.
2026-10-29 10:27:14,410 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/monitoring-configmap ...
2026-10-29 10:27:14,461 INFO swchmonclient.undeploy_monitoring:   Done. 1 configmap resource(s) deleted.
2026-10-29 10:27:14,462 INFO swchmonclient.undeploy_monitoring: All manifests undeployed successfully.
Monitoring undeployed successfully.
```

Delete the test pod (from the master):

```bash
sudo kubectl delete pod -n <NAMESPACE> python-shell
```

## Appendix: Namespace-parametric RBAC

Use this version if you want to install the `mon-client` service account and RBAC into a configurable namespace without editing the bundled manifest:

```bash
NAMESPACE=${NAMESPACE:-swarm-system}

sudo kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | sudo kubectl apply -f -

cat <<EOF | sudo kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: mon-client
  namespace: ${NAMESPACE}
---
# --- Required for node="local": get pods in the SA namespace ---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: mon-client-pod-reader
  namespace: ${NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: mon-client-pod-reader
  namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: mon-client-pod-reader
subjects:
  - kind: ServiceAccount
    name: mon-client
    namespace: ${NAMESPACE}
---
# --- Required for node="all", node="local", and file node="cluster": get,list nodes cluster-wide ---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: mon-client-node-reader
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: mon-client-node-reader
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: mon-client-node-reader
subjects:
  - kind: ServiceAccount
    name: mon-client
    namespace: ${NAMESPACE}
---
# --- Required for deploy_monitoring(...) / undeploy_monitoring(...) ---
# Covers every resource kind defined in emsconfig.yaml and
# ems+netdata-k3s_parametric.yaml:
#   ConfigMaps:        emsconfig, netdata-conf-child, netdata-child-sd-config-map,
#                      tosca-script-config, tosca-model-configmap
#                      (+ EMS-created: monitoring-configmap, ems-client-configmap)
#   ServiceAccounts:   netdata, ems-server-service-account
#   Services:          emsserver-ems-server
#   DaemonSets:        netdata-child (+ EMS-created: ems-client-daemonset)
#   Deployments:       emsserver-ems-server
#   RBAC:              netdata ClusterRole/Binding,
#                      ems-server-cluster-role/-binding, ems-server-role/-binding
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: mon-client-deployer
rules:
  - apiGroups: [""]
    resources: ["configmaps", "serviceaccounts", "services"]
    verbs: ["create", "get", "list", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["daemonsets", "deployments"]
    verbs: ["create", "get", "list", "patch", "delete"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["roles", "rolebindings", "clusterroles", "clusterrolebindings"]
    verbs: ["create", "get", "list", "patch", "delete", "escalate", "bind"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: mon-client-deployer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: mon-client-deployer
subjects:
  - kind: ServiceAccount
    name: mon-client
    namespace: ${NAMESPACE}
EOF
```

For example, to install into `monitoring` instead of `swarm-system`:

```bash
NAMESPACE=monitoring
# then run the same block
```

> The monitoring manifests create the `netdata` and `ems-server` (Cluster)Roles, which contain permissions `mon-client` does not hold itself (e.g. `watch`, `secrets`, `nodes/proxy`). Kubernetes' RBAC escalation prevention would reject creating those roles/bindings without the `escalate` / `bind` verbs.

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Worker stuck `NotReady` or never appears | Check `K3S_URL` IP, port `6443` reachability, and token correctness. Run `journalctl -u k3s-agent -f` on the worker |
| `Failed to list Kubernetes nodes: Forbidden` | Service account lacks `get,list nodes` ClusterRole. Re-check section 2.2 |
| `Failed to determine current Kubernetes node IP: Forbidden` | Missing `get pods` Role and/or node read permissions for `node="local"` |
| `deploy_monitoring` returns `1` | Service account lacks create/patch RBAC on manifest resources, the local `sat_file` path is wrong/unreadable, or the OptimusDB URL is wrong. Check pod logs |
| No metric values returned | Allow time for samples to buffer. `query_*` consumes returned samples, so successive calls return only new data |
| `query_metric_values_raw` always returns empty lists | The metric is not published by the node brokers. Global composite metrics (e.g. `cpu_util_prct`) exist only on the EMS server; subscribe to a raw metric such as `host_load_instance`, or use `subscribe_metric(...)` |
| `query_slo_violations` always returns `[]` | No violation happened yet: EMS only publishes on `<slo_name>_CONSTRAINT` while the SLO condition holds. Check the SLO name matches the SAT `slo-constraints` name, not the metric name |
| No metrics at all after changing a composite's `grouping` to `per_host` / `per_zone`; EMS server log shows `NullPointerException: connections is marked non-null but is null` | Non-global groupings make EMS cluster its clients, which fails with the bundled `emsconfig.yaml`. Keep composite metrics on `grouping: "global"` and use raw metrics for per-host values |
| `emsserver-ems-server` shows 1 restart right after deploy | EMS can take ~35 s to open its port, and the liveness probe has no initial delay, so the first start may be killed. It comes up on the second start |
