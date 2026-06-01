# swchmonclient

A Python library for deploying the monitoring stack and consuming metric events over STOMP in the Swarmchestrate project.

## Install

```bash
pip install swchmonclient
uv add swchmonclient
```

## Overview

- Metrics defined in the SAT are monitored by EMS.
- Composite metrics and SLOs are consumed from the central EPM.
- Raw metrics can be consumed from the local EPA, an explicit list of nodes, or all nodes discovered from Kubernetes.
- The library buffers metric values until `query_metric_values(...)` or `query_metric_values_raw(...)` is called.
- Returned samples are consumed from the in-memory buffers.


## Quick Example

```python
from swchmonclient import (
    deploy_monitoring,
    query_metric_values,
    query_metric_values_raw,
    subscribe_metric,
    subscribe_metric_raw,
    undeploy_monitoring,
    unsubscribe_metric,
)

deploy_monitoring("tosca_metrics_ze.yaml", "http://optimusdb.example/swarmkb")
subscribe_metric("/topic/mysample_metric")
recent_values = query_metric_values("/topic/mysample_metric")
subscribe_metric_raw("/topic/myraw_metric", ["10.0.0.1", "10.0.0.2"])
subscribe_metric_raw("/topic/myraw_metric", "all")
subscribe_metric_raw("/topic/myraw_metric", "local")
raw_values = query_metric_values_raw("/topic/myraw_metric", seconds=60)
unsubscribe_metric("/topic/mysample_metric")
unsubscribe_metric("/topic/myraw_metric")
undeploy_monitoring(
    "tosca_metrics_ze.yaml",
    "http://optimusdb.example/swarmkb",
    namespace="default",
)
```

## Raw Metric Subscriptions

`subscribe_metric_raw(metric, node)` supports three selector modes:

- `["10.0.0.1", "10.0.0.2"]` starts one raw listener per explicit node/IP
- `"all"` resolves all Kubernetes VM private IPs internally
- `"local"` resolves the current Kubernetes node InternalIP and starts one raw listener for it

> **Important:** `node="all"` requires in-cluster Kubernetes config and RBAC permission to read Kubernetes nodes.
>
> **Important:** `node="local"` also uses the Kubernetes API in-cluster. It resolves the current pod, then the node backing that pod, so it needs RBAC permission to read the current pod and its node.

If you do **not** want to grant cluster-wide node-read access, do not use `node="all"`; pass explicit node IPs instead.

When testing `node="all"` from a temporary pod, start the pod with a service account so `load_incluster_config()` can use the mounted token. Some `kubectl` versions do **not** support `kubectl run --serviceaccount`, so use `--overrides` instead:

```bash
kubectl create serviceaccount mon-client

kubectl run python-shell \
  --rm -i --tty \
  --image=python:3.12-slim \
  --restart=Never \
  --overrides='
{
  "apiVersion": "v1",
  "spec": {
    "serviceAccountName": "mon-client"
  }
}' \
  -- bash
```

If your cluster or client version behaves badly with `--overrides`, use a Pod manifest instead:

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
    image: python:3.12-slim
    command: ["bash", "-lc", "sleep infinity"]
    stdin: true
    tty: true
EOF

kubectl exec -it python-shell -- bash
kubectl delete pod python-shell
```

If the raw `"all"` or `"local"` selector needs to resolve node IPs through the Kubernetes API, grant the service account permission to read the required resources. The simplest way is with imperative `kubectl` commands:

```bash
kubectl create serviceaccount mon-client
kubectl create clusterrole mon-client-node-reader --verb=get,list --resource=pods,nodes
kubectl create clusterrolebinding mon-client-node-reader \
  --clusterrole=mon-client-node-reader \
  --serviceaccount=default:mon-client
```

If you prefer YAML, use:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: mon-client-node-reader
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get"]
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
    namespace: default
```

If you see an error like `Failed to list Kubernetes nodes: Forbidden` or `Failed to determine current Kubernetes node IP: Forbidden`, the service account can authenticate but does not have enough RBAC to read the required Kubernetes resources.

You can verify the permission with:

```bash
kubectl auth can-i list nodes --as=system:serviceaccount:default:mon-client
kubectl auth can-i get nodes --as=system:serviceaccount:default:mon-client
kubectl auth can-i get pods --as=system:serviceaccount:default:mon-client
```

Inside the pod, verify that the service account token and Kubernetes service env vars are present:

```bash
ls /var/run/secrets/kubernetes.io/serviceaccount
env | grep KUBERNETES_SERVICE
```

Raw subscriptions connect directly to the resolved node IPs. Read buffered raw samples with:

```python
raw_values = query_metric_values_raw("cpu_util_instance", seconds=60)
```

The returned structure is grouped by node/IP:

```python
{
    "10.0.0.1": [
        {"timestamp": 1716712345.12, "value": 42.0},
    ],
}
```

Each raw metric on each node/IP keeps up to 1000 cached samples, dropping the oldest entries first when the buffer fills.

If you subscribe multiple raw metrics for the same node/IP, the library reuses the same raw listener thread for that node and dynamically subscribes the additional metric topics on that connection.

## API Reference

### `deploy_monitoring(sat_file: str, optimusdb_url: str, logger: logging.Logger | None = None) -> int`

Deploys the standard monitoring stack manifests.

If `./manifests/emsconfig.yaml` or `./manifests/ems+netdata-k3s_parametric.yaml` is missing locally, the library downloads it from the `v0.1.0` release assets before deployment. When a local copy already exists, it validates the content against the release asset, logs whether it matches, and replaces the file if it differs.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `sat_file` | Yes | `str` | SAT file path injected into the templated manifest. |
| `optimusdb_url` | Yes | `str` | OptimusDB URL injected into the templated manifest. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** process-style exit code: `0` on success, `1` if one or more deploy steps fail.

### `undeploy_monitoring(sat_file: str, optimusdb_url: str, namespace: str | None = None, logger: logging.Logger | None = None) -> int`

Undeploys the standard monitoring stack manifests and the related cleanup resources.

Like deployment, undeploy ensures the two required monitoring manifests are available locally, logs whether existing local copies match the published release assets, and refreshes differing files from the release.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `sat_file` | Yes | `str` | SAT file path used to render the templated manifest before undeploy. |
| `optimusdb_url` | Yes | `str` | OptimusDB URL used to render the templated manifest before undeploy. |
| `namespace` | No | `str \| None` | Namespace override for deleting namespaced resources. If omitted, manifest/default namespaces are used. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** process-style exit code: `0` on success, `1` if one or more undeploy steps fail.

### `subscribe_metric(metric: str) -> str`

Starts or reuses the shared standard metric listener for the requested metric topic.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. Plain names are normalized to `/topic/<metric>`. |

**Output:** `str` thread name of the shared listener, currently `metric-listener`.

### `subscribe_metric_raw(metric: str, node: list[str] | str, cache_size: int | None = None) -> dict[str, str]`

Starts raw metric listeners that connect directly to node IPs.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. Plain names are normalized to `/topic/<metric>`. |
| `node` | Yes | `list[str] \| str` | Raw node selector. Use an explicit node/IP list, `"all"` for all Kubernetes VM private IPs, or `"local"` for the current Kubernetes node InternalIP. |
| `cache_size` | No | `int \| None` | Per raw metric per node sample buffer size. If omitted, the default value `1000` is used. |

**Output:** `dict[str, str]` mapping each resolved node/IP to the listener thread name started for it.

**Notes:**
- Starts one raw listener thread per resolved node/IP and reuses it for additional raw metrics on that same node/IP.
- Raw subscriptions connect directly to each resolved node/IP instead of `MON_CLIENT_STOMP_HOST`.
- Mixing `subscribe_metric(...)` and `subscribe_metric_raw(...)` for the same metric is rejected.
- **Important:** `node="all"` requires in-cluster Kubernetes config loaded via `load_incluster_config()`.

### `query_metric_values(metric: str) -> list[Any]`

Returns all currently buffered metric values for the metric and consumes those returned samples.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |
```python
[42.0, 41.7]
```

### `query_metric_values_raw(metric: str, seconds: int) -> dict[str, list[dict[str, Any]]]`

Returns buffered raw metric values received within the last `seconds` seconds and consumes those returned samples.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |
| `seconds` | Yes | `int` | Time window to read from. Must be non-negative. |

**Output:**

```python
{
    "10.0.0.1": [
        {"timestamp": 1716712345.12, "value": 42.0},
    ],
}
```

### `unsubscribe_metric(metric: str, nodes: list[str] | None = None) -> None`

Stops metric listeners or removes node-specific subscriptions, depending on the subscription mode.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |
| `nodes` | No | `list[str] \| None` | For raw subscriptions, stops only the listed node/IP listeners. For standard subscriptions, blocks those nodes from future samples. If omitted, removes the full subscription. |

**Output:** no return value.

**Behavior summary:**
- Standard metric + no `nodes`: stop the shared listener when the last standard metric is removed.
- Standard metric + `nodes`: keep the listener running, but ignore future samples from those nodes.
- Raw metric + no `nodes`: stop all raw listeners for that metric.
- Raw metric + `nodes`: stop only the listed raw node/IP listeners and remove their cached data buckets.

## Examples

Runnable examples are available under `examples/`:

- `examples/deploy.py`
- `examples/undeploy.py`
- `examples/subscribe_cpu_util_instance.py`
- `examples/subscribe_cpu_util_instance_raw.py`

## Simple Snippets

### `deploy_monitoring`

```python
from swchmonclient import deploy_monitoring

exit_code = deploy_monitoring("tosca_metrics_ze.yaml", "http://optimusdb.example/swarmkb")
```

### `undeploy_monitoring`

```python
from swchmonclient import undeploy_monitoring

exit_code = undeploy_monitoring(
    "tosca_metrics_ze.yaml",
    "http://optimusdb.example/swarmkb",
    namespace="default",
)
```

### `subscribe_metric`

```python
from swchmonclient import subscribe_metric

thread_name = subscribe_metric("cpu_util_instance")
print(thread_name)
```

### `subscribe_metric_raw`

```python
from swchmonclient import subscribe_metric_raw

threads = subscribe_metric_raw("cpu_util_instance", "local")
# or: subscribe_metric_raw("cpu_util_instance", "local", cache_size=500)
# or: subscribe_metric_raw("cpu_util_instance", "all")  # IMPORTANT: requires Kubernetes config
# or: subscribe_metric_raw("cpu_util_instance", ["10.0.0.1", "10.0.0.2"])
print(threads)
```

### `query_metric_values`

```python
from swchmonclient import query_metric_values

standard_values = query_metric_values("cpu_util_instance")
```

### `query_metric_values_raw`

```python
from swchmonclient import query_metric_values_raw

raw_values = query_metric_values_raw("cpu_util_instance", seconds=60)
```

### `unsubscribe_metric`

```python
from swchmonclient import unsubscribe_metric

unsubscribe_metric("cpu_util_instance")
# or: unsubscribe_metric("cpu_util_instance", nodes=["10.0.0.1"])
```


## Development

```bash
uv sync
uv build
```

### Switching between in-cluster and local development

The STOMP listener reads `MON_CLIENT_STOMP_HOST` and `MON_CLIENT_STOMP_PORT` from `.env`, so you can switch environments without changing code.

Use the bundled profiles:

```bash
./scripts/use-cluster-env.sh
./scripts/use-dev-env.sh
```

- `use-cluster-env.sh` writes `.env` with `MON_CLIENT_STOMP_HOST=emsserver-ems-server` and `MON_CLIENT_STOMP_PORT=61610`
- `use-dev-env.sh` writes `.env` with `MON_CLIENT_STOMP_HOST=127.0.0.1` and `MON_CLIENT_STOMP_PORT=61610`

When running outside the cluster, start a port-forward to the ClusterIP service:

```bash
./scripts/use-dev-env.sh
./scripts/port-forward-emsserver.sh
```

If the service is not in `default`, pass the namespace explicitly:

```bash
./scripts/port-forward-emsserver.sh monitoring
```
