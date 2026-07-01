# Step-by-Step Guide

This guide walks through deploying a two-node k3s cluster, preparing a service account, and deploying the Swarmchestrate monitoring stack with the `swchmonclient` Python library.

## Prerequisites

- **2 VMs** (1 master, 1 worker) with unrestricted network access
  - Linux OS (e.g., Ubuntu 22.04+), 2 vCPU / 2 GB RAM minimum each
  - Root or sudo access on both
  - Hostname should be different on both nodes.
  - VMs can reach each other (master port `6443/tcp` must be open to the worker; for full functionality allow `8472/udp` for flannel VXLAN and `10250/tcp` for kubelet metrics)
  - Outbound internet access (k3s installer, container images, PyPI, release assets)

Throughout this guide:

| Placeholder | Meaning |
| --- | --- |
| `<MASTER_IP>` | Private/internal IP of the master VM |
| `<TOKEN>` | k3s join token from the master |

---

## 1) Install k3s on the master and get the join token

SSH into the **master VM** and run:

```bash
curl -sfL https://get.k3s.io | sh -
```

Verify the node is up:

```bash
sudo k3s kubectl get nodes
```

Expected output (status `Ready`):

```
NAME      STATUS   ROLES                  AGE   VERSION
master    Ready    control-plane,master   30s   v1.35.x+k3s1
```

### Get the join token

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

Sample output:

```bash
K10976d249a335a4f8b91ad01a3bfc81e84ebec569e6cb68fc9e9ee67682fc5bd42::server:e1c1bd272551b1cfd827d61fbdd096d1
```

Save this value — it is the `<TOKEN>` the worker needs to join.

Also note the master's IP:

```bash
hostname -I | awk '{print $1}'
```

Sample output:

```bash
192.168.0.135
```

### (Optional) Use kubectl without sudo

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
export KUBECONFIG=~/.kube/config
```

---

## 2) Join the worker node

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
sudo k3s kubectl get nodes -o wide
```

Expected output:

```
NAME      STATUS   ROLES                  AGE     VERSION
master    Ready    control-plane,master   5m      v1.35.x+k3s1
worker    Ready    <none>                 30s     v1.35.x+k3s1
```

Both nodes must show `Ready` before continuing.

---

## 3) Create the service account and deploy a test pod

All commands in this section run on the **master** (or anywhere with cluster kubeconfig access).

### 3.1 Clone the monitoring-client repository

```bash
git clone https://github.com/Swarmchestrate/monitoring-client.git
cd monitoring-client
```

### 3.2 Create the `mon-client` service account and RBAC

The service account needs two sets of permissions:

1. **Metric subscriptions** (`node="all"` / `node="local"`): read pods and nodes.
2. **`deploy_monitoring(...)` / `undeploy_monitoring(...)`**: create, patch, get, list, and delete the resources defined by the monitoring manifests (`emsconfig.yaml` and `ems+netdata-k3s_parametric.yaml`): ConfigMaps, ServiceAccounts, Services, DaemonSets, Deployments, and the netdata/EMS RBAC objects.

#### Easy way: apply the bundled manifest

The repository's `./manifests/mon-client-rbac.yaml` contains everything needed — just apply it:

```bash
kubectl apply -f ./manifests/mon-client-rbac.yaml
```

> If you deploy outside the `default` namespace, update the namespace fields in the manifest before applying.

#### Advanced way: define the manifest yourself (works in any namespace)

This version is namespace-parametric: set `NAMESPACE` once and the heredoc fills in every namespace field (ServiceAccount, Role/RoleBinding, and the ClusterRoleBinding subjects), so it is valid in every namespace without editing the file:

```bash
NAMESPACE=${NAMESPACE:-default}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl apply -f -
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
# --- Required for node="all" and node="local": get,list nodes cluster-wide ---
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

For example, to install into `monitoring` instead of `default`:

```bash
NAMESPACE=monitoring
# then run the same block
```

> The monitoring manifests create the `netdata` and `ems-server` (Cluster)Roles, which contain permissions `mon-client` does not hold itself (e.g. `watch`, `secrets`, `nodes/proxy`). Kubernetes' RBAC escalation prevention would reject creating those roles/bindings without the `escalate` / `bind` verbs.

### 3.3 Verify RBAC

```bash
kubectl auth can-i get pods --as=system:serviceaccount:default:mon-client -n default
kubectl auth can-i get nodes --as=system:serviceaccount:default:mon-client
kubectl auth can-i list nodes --as=system:serviceaccount:default:mon-client
# deployer permissions:
kubectl auth can-i create configmaps --as=system:serviceaccount:default:mon-client -n default
kubectl auth can-i create daemonsets --as=system:serviceaccount:default:mon-client -n default
kubectl auth can-i create deployments --as=system:serviceaccount:default:mon-client -n default
kubectl auth can-i delete clusterroles --as=system:serviceaccount:default:mon-client
```

All should return `yes`. If you installed into another namespace, replace `default` with your namespace in both `--as=system:serviceaccount:default:mon-client` and `-n default`. If you later see `Failed to list Kubernetes nodes: Forbidden` or `Failed to determine current Kubernetes node IP: Forbidden`, the service account authenticates but lacks RBAC.

### 3.4 Deploy a test pod

#### Easy way: apply the bundled manifest

```bash
kubectl apply -f ./manifests/mon-client-test-pod.yaml
```

#### Advanced way: inline heredoc

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: python-shell
spec:
  serviceAccountName: mon-client
  restartPolicy: Never
  containers:
  - name: python-shell
    image: python:3.14-slim
    command: ["bash", "-lc", "sleep infinity"]
    stdin: true
    tty: true
EOF
```

Wait for it to run, then open a shell:

```bash
kubectl get pod python-shell
kubectl exec -it python-shell -- bash
```

Inside the pod, verify the service account token and Kubernetes service env vars are present:

```bash
ls /var/run/secrets/kubernetes.io/serviceaccount
env | grep KUBERNETES_SERVICE
```

You should see `ca.crt`, `namespace`, `token` and the `KUBERNETES_SERVICE_HOST` / `KUBERNETES_SERVICE_PORT` variables.

---

## 4) Install swchmonclient, deploy monitoring, and gather metrics

These steps run **inside the test pod** (the in-cluster service account config is required by `deploy_monitoring`).

### 4.1 Install the library

```bash
apt update && apt install -y nano git && git clone https://github.com/Swarmchestrate/monitoring-client.git
pip install swchmonclient==0.2.1
cd monitoring-client
```

> **Note:** Always check for the latest version on [PyPI](https://pypi.org/project/swchmonclient/) or the [GitHub releases page](https://github.com/Swarmchestrate/monitoring-client/releases) and install that instead.

### 3.2 The `deploy_monitoring` API

```python
deploy_monitoring(
    sat_file: str,
    optimusdb_url: str = "http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb",
    use_kb: bool = True,
    upload_kb: bool = False,
    logger: logging.Logger | None = None,
) -> int
```

**Input parameters:**

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `sat_file` | Yes | `str` | Local path to the SAT YAML file. Its basename is turned into a unique timestamped filename for `SAT_FILE`, and its content is loaded into `ConfigMap/tosca-model-configmap` as `test-tosca-model.yaml`. |
| `optimusdb_url` | Yes | `str` | OptimusDB / knowledgebase URL injected into the templated manifest. Used by default because `use_kb` defaults to `True`. |
| `use_kb` | No | `bool` | Controls the rendered `USE_KB` value in `emsconfig.yaml`. Defaults to `True`, so EMS resolves the SAT through the knowledgebase at `optimusdb_url`. Set to `False` to use the SAT content from the ConfigMap instead. |
| `upload_kb` | No | `bool` | If `True`, uploads the SAT file to the knowledgebase (under its generated unique filename) before the Kubernetes resources are deployed. Defaults to `False`. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** a process-style exit code — `0` on success, `1` if one or more deploy steps fail.

`undeploy_monitoring(namespace: str | None = None, logger: logging.Logger | None = None) -> int` does **not** take `sat_file` or `optimusdb_url` — it deletes `ConfigMap/emsconfig` and `ConfigMap/tosca-model-configmap` by name and undeploys the remaining resources from the static manifest files. It likewise returns `0` / `1`.

#### The SAT file input parameter

A **SAT (Swarm Application Template)** is the main application input to a Swarmchestrate Universe. Application owners author a SAT, which defines the microservices and resource requirements that make up an application.

The `sat_file` value passed to `deploy_monitoring(...)` is a **local file path**. At deploy time the library reads it locally, appends a UTC timestamp to its basename to produce a unique `SAT_FILE` name, injects that into the rendered manifest, and loads the file content into `ConfigMap/tosca-model-configmap` under the fixed key `test-tosca-model.yaml`.

How the SAT is resolved depends on `use_kb`:

- `use_kb=True` (default) — EMS resolves the SAT through the knowledgebase at `optimusdb_url`. Combine with `upload_kb=True` to have `deploy_monitoring(...)` upload the SAT to the knowledgebase for you first; no separate upload step is needed.
- `use_kb=False` — EMS uses the SAT content straight from the deployed ConfigMap, so no knowledgebase is involved.

> **Note:** SAT uploading is now built into `deploy_monitoring(...)` via `upload_kb=True` — there is no longer a separate upload script to run.

### 4.3 Deploy the monitoring stack

```
python ./examples/deploy.py
```

Expected output:

```python
2026-06-24 13:12:54,344 INFO swchmonclient.deploy_monitoring: Found local manifest ./manifests/emsconfig.yaml.
2026-06-24 13:12:54,570 WARNING swchmonclient.deploy_monitoring: Local manifest ./manifests/emsconfig.yaml differs from release asset https://github.com/Swarmchestrate/monitoring-client/releases/download/v0.1.0/emsconfig.yaml. Keeping local file.
2026-06-24 13:12:54,570 INFO swchmonclient.deploy_monitoring: Found local manifest ./manifests/ems+netdata-k3s_parametric.yaml.
2026-06-24 13:12:54,665 WARNING swchmonclient.deploy_monitoring: Local manifest ./manifests/ems+netdata-k3s_parametric.yaml differs from release asset https://github.com/Swarmchestrate/monitoring-client/releases/download/v0.1.0/ems+netdata-k3s_parametric.yaml. Keeping local file.
2026-06-24 13:12:54,714 INFO swchmonclient.deploy_monitoring: Deploying ./manifests/emsconfig.yaml with variables:
2026-06-24 13:12:54,714 INFO swchmonclient.deploy_monitoring:     • sat_file: stressng.yaml
2026-06-24 13:12:54,714 INFO swchmonclient.deploy_monitoring:     • optimusdb_url: http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb
2026-06-24 13:12:54,714 INFO swchmonclient.deploy_monitoring:     • use_kb: True
2026-06-24 13:12:54,737 INFO swchmonclient.deploy_monitoring:   Created or patched resources:
2026-06-24 13:12:54,737 INFO swchmonclient.deploy_monitoring:   - ConfigMap/emsconfig (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,737 INFO swchmonclient.deploy_monitoring: Deploying ./manifests/ems+netdata-k3s_parametric.yaml ...
2026-06-24 13:12:54,953 INFO swchmonclient.deploy_monitoring:   Created or patched resources:
2026-06-24 13:12:54,953 INFO swchmonclient.deploy_monitoring:   - ServiceAccount/netdata (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ConfigMap/netdata-conf-child (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ConfigMap/netdata-child-sd-config-map (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ClusterRole/netdata (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ClusterRoleBinding/netdata (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - DaemonSet/netdata-child (apiVersion=apps/v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ServiceAccount/ems-server-service-account (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ClusterRole/ems-server-cluster-role (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ClusterRoleBinding/ems-server-cluster-role-binding (apiVersion=rbac.authorization.k8s.io/v1, namespace=<cluster-scoped>)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - Role/ems-server-role (apiVersion=rbac.authorization.k8s.io/v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - RoleBinding/ems-server-role-binding (apiVersion=rbac.authorization.k8s.io/v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - Service/emsserver-ems-server (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - ConfigMap/tosca-script-config (apiVersion=v1, namespace=default)
2026-06-24 13:12:54,954 INFO swchmonclient.deploy_monitoring:   - Deployment/emsserver-ems-server (apiVersion=apps/v1, namespace=default)
2026-06-24 13:12:54,955 INFO swchmonclient.deploy_monitoring: All manifests deployed successfully.
Monitoring deployed successfully.
```

Notes:

- Uses in-cluster Kubernetes config from the pod's service account — it does not read a local kubeconfig.
- If `./manifests/emsconfig.yaml` or `./manifests/ems+netdata-k3s_parametric.yaml` is missing locally, the library downloads it from the `v0.1.0` release assets automatically; existing local copies are validated against the release and replaced if they differ.

Verify on the master that monitoring workloads are coming up:

```bash
kubectl get pods -A
```

Sample output:

```
NAMESPACE     NAME                                      READY   STATUS      RESTARTS   AGE
default       emsserver-ems-server-58c4c8df96-4qkk6     0/1     Init:0/1    0          3m30s
default       netdata-child-55lq2                       1/1     Running     0          3m30s
default       netdata-child-l4nw5                       1/1     Running     0          3m30s
...
```

### 4.4 Gather standard metrics (via EPM / STOMP)

```python
from swchmonclient import subscribe_metric, query_metric_values, unsubscribe_metric

thread_name = subscribe_metric("cpu_util_instance")  # plain names normalize to /topic/<metric>
print(thread_name)  # "metric-listener"

# Wait some seconds while samples are buffered, then:
values = query_metric_values("cpu_util_instance")
print(values)  # e.g. [42.0, 41.7] — returned samples are consumed from the buffer

unsubscribe_metric("cpu_util_instance")
```

### 4.5 Gather raw metrics directly from node IPs (EPA)

`subscribe_metric_raw(metric, node)` supports three selector modes:

| Selector | Behavior | RBAC needed |
| --- | --- | --- |
| `["10.0.0.1", "10.0.0.2"]` | One raw listener per explicit node/IP | None (no Kubernetes API access) |
| `"all"` | Resolves all Kubernetes VM private IPs | cluster-wide `get,list nodes` |
| `"local"` | Resolves the current node's InternalIP | `get pods` in the namespace + cluster-wide `get,list nodes` |

```python
from swchmonclient import subscribe_metric_raw, query_metric_values_raw, unsubscribe_metric

METRIC_NAME = "cpu_util_prct"
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

- Each raw metric on each node keeps up to 1000 cached samples (oldest dropped first); override with `cache_size=`.
- Raw listeners connect directly to node IPs instead of `MON_CLIENT_STOMP_HOST`; one listener thread per node is reused for additional raw metrics.
- Mixing `subscribe_metric(...)` and `subscribe_metric_raw(...)` for the same metric is rejected.

### 4.6 Undeploy the monitoring stack

`undeploy_monitoring(...)` removes the monitoring stack and its cleanup resources. Unlike deploy, it does **not** take `sat_file`, `optimusdb_url`, `use_kb`, or `upload_kb` — it deletes `ConfigMap/emsconfig` and `ConfigMap/tosca-model-configmap` by name and undeploys the remaining manifest-defined resources from the static manifest files.

```python
undeploy_monitoring(
    namespace: str | None = None,
    logger: logging.Logger | None = None,
) -> int
```

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `namespace` | No | `str \| None` | Namespace override for deleting namespaced resources. If omitted, manifest/default namespaces are used. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** a process-style exit code — `0` on success, `1` if one or more undeploy steps fail.

Run it from inside the pod, either from Python:

```python
from swchmonclient import undeploy_monitoring

exit_code = undeploy_monitoring(namespace="default")
```

or via the bundled example:

```bash
python examples/undeploy.py
```

Expected output:

```
2026-06-24 13:52:43,277 INFO swchmonclient.undeploy_monitoring: Found local manifest ./manifests/ems+netdata-k3s_parametric.yaml.
2026-06-24 13:52:43,762 WARNING swchmonclient.undeploy_monitoring: Local manifest ./manifests/ems+netdata-k3s_parametric.yaml differs from release asset https://github.com/Swarmchestrate/monitoring-client/releases/download/v0.1.0/ems+netdata-k3s_parametric.yaml. Keeping local file.
2026-06-24 13:52:43,817 INFO swchmonclient.undeploy_monitoring: Undeploying ./manifests/ems+netdata-k3s_parametric.yaml ...
2026-06-24 13:52:44,249 INFO swchmonclient.undeploy_monitoring:   Done. 14 resource(s) deleted.
2026-06-24 13:52:44,249 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/emsconfig ...
2026-06-24 13:52:44,270 INFO swchmonclient.undeploy_monitoring:   Done. 1 configmap resource(s) deleted.
2026-06-24 13:52:44,270 INFO swchmonclient.undeploy_monitoring: Undeploying DaemonSet/ems-client-daemonset ...
2026-06-24 13:52:44,289 INFO swchmonclient.undeploy_monitoring:   Done. 0 daemonset resource(s) deleted.
2026-06-24 13:52:44,289 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/ems-client-configmap ...
2026-06-24 13:52:44,300 INFO swchmonclient.undeploy_monitoring:   Done. 0 configmap resource(s) deleted.
2026-06-24 13:52:44,301 INFO swchmonclient.undeploy_monitoring: Undeploying ConfigMap/monitoring-configmap ...
2026-06-24 13:52:44,310 INFO swchmonclient.undeploy_monitoring:   Done. 0 configmap resource(s) deleted.
2026-06-24 13:52:44,311 INFO swchmonclient.undeploy_monitoring: All manifests undeployed successfully.
Monitoring undeployed successfully.
```

> The `0 resource(s) deleted` lines for the EMS-client resources are expected — those are created lazily by the EMS server, so they may not exist at undeploy time.

### 4.7 Cleanup (optional)

Delete the test pod (from the master):

```bash
kubectl delete pod python-shell
```

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Worker stuck `NotReady` or never appears | Check `K3S_URL` IP, port `6443` reachability, token correctness; `journalctl -u k3s-agent -f` on the worker |
| `Failed to list Kubernetes nodes: Forbidden` | Service account lacks `get,list nodes` ClusterRole — re-check section 3.2 |
| `Failed to determine current Kubernetes node IP: Forbidden` | Missing `get pods` Role and/or node read permissions for `node="local"` |
| `deploy_monitoring` returns `1` | Service account lacks create/patch RBAC on manifest resources, the local `sat_file` path is wrong/unreadable, or the OptimusDB URL is wrong — check pod logs |
| No metric values returned | Allow time for samples to buffer; `query_*` consumes returned samples, so successive calls return only new data |
