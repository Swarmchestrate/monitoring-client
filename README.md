# swchmonclient

A Python library for deploying the monitoring stack and consuming SLO, constraints and raw metric events over STOMP in the Swarmchestrate project.

## Install

```bash
pip install swchmonclient
```

OR

```bash
uv add swchmonclient
```

## Overview

- `deploy_monitoring(...)` and `undeploy_monitoring(...)` manage the monitoring manifests from inside Kubernetes.
- `subscribe_metric(...)` consumes standard metrics through a shared STOMP listener from EPM.
- `subscribe_metric_raw(...)` consumes raw metrics directly from resolved node IPs EPA.
- Metric values are buffered until `query_metric_values(...)` or `query_metric_values_raw(...)` is called.
- Returned samples are consumed from the in-memory buffers.

> **Important:** `deploy_monitoring(...)` and `undeploy_monitoring(...)` use in-cluster Kubernetes configuration. Run them from a pod that has a mounted service account token and RBAC permission to create, patch, get, list, and delete the Kubernetes resources referenced by the monitoring manifests.


## Getting started

See the [Step-by-step guide](step-by-step-guide.md) for the full deployment and subscription flow.

For runnable scripts, see [Examples](#examples). For the individual function signatures, see [Simple Snippets](#simple-snippets) and the [API Reference](#api-reference).

## Available Metrics

The following metrics are collected from Netdata or calculated from the collected raw metrics. Raw metrics represent values retrieved directly from Netdata, while composite metrics are derived using aggregation formulas over a sliding time window.

### Raw Metrics

| Metric name           | Description                                                                                        | Example collection frequency |
| --------------------- | -------------------------------------------------------------------------------------------------- | ---------------------------- |
| `cpu_idle_instance`   | CPU idle percentage reported by Netdata from the `system.cpu` context using the `idle` dimension.  | 30 sec                       |
| `ram_free_instance`   | Amount of free RAM reported by Netdata from the `system.ram` context using the `free` dimension.   | 30 sec                       |
| `ram_total_instance`  | Total RAM reported by Netdata from the `system.ram` context. Values are aggregated using `SUM`.    | 30 sec                       |
| `disk_read_activity`  | Disk read activity reported by Netdata from the `system.io` context using the `reads` dimension.   | 10 sec                       |
| `disk_write_activity` | Disk write activity reported by Netdata from the `system.io` context using the `writes` dimension. | 10 sec                       |

### Composite Metrics

Composite metrics are calculated globally using a **5-minute sliding window**.

| Metric name            | Description                                                                                                                                                                            | Formula                                                                           | Example collection frequency |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------- |
| `cpu_util_prct`        | Average CPU utilization calculated from the CPU idle percentage over the current 5-minute window.                                                                                      | `100 - mean(cpu_idle_instance)`                                                   | 30 sec                       |
| `ram_util_prct`        | RAM utilization calculated from the average total and free RAM values over the current 5-minute window.                                                                                | `(mean(ram_total_instance) - mean(ram_free_instance)) / mean(ram_total_instance)` | 30 sec                       |
| `avg_disk_utilization` | Combined average disk read and write activity over the current 5-minute window. Absolute values are used so that read and write activity contribute positively to the resulting value. | `abs(mean(disk_read_activity)) + abs(mean(disk_write_activity))`                  | 10 sec                       |

All metrics use `collection_output: all`. Composite metrics use `grouping: global`, meaning that the resulting values are aggregated globally rather than calculated separately per availability zone.

### Example Configuration

The following configuration shows an example of how the raw and composite metrics can be defined. Values such as collection frequency, window size, and grouping are examples and can be adjusted to suit the deployment.

```yaml
raw:
  - name: cpu_idle_instance
    sensor: "netdata"
    config:
      scope_contexts: system.cpu
      dimensions: idle
    collection_frequency: "30 sec"
    collection_output: "all"

  - name: ram_free_instance
    sensor: "netdata"
    config:
      scope_contexts: system.ram
      dimensions: free
    collection_frequency: "30 sec"
    collection_output: "all"

  - name: ram_total_instance
    sensor: "netdata"
    config:
      scope_contexts: system.ram
      results-aggregation: SUM
    collection_frequency: "30 sec"
    collection_output: "all"

  - name: disk_read_activity
    sensor: "netdata"
    config:
      scope_contexts: system.io
      dimensions: reads
    collection_frequency: "10 sec"
    collection_output: "all"

  - name: disk_write_activity
    sensor: "netdata"
    config:
      scope_contexts: system.io
      dimensions: writes
    collection_frequency: "10 sec"
    collection_output: "all"

composite:
  - name: cpu_util_prct
    formula: 100 - mean(cpu_idle_instance)
    collection_frequency: "30 sec"
    collection_output: "all"
    window_type: "sliding"
    window_size: "5 min"
    grouping: "global"

  - name: ram_util_prct
    formula: (mean(ram_total_instance)-mean(ram_free_instance))/mean(ram_total_instance)
    collection_frequency: "30 sec"
    collection_output: "all"
    window_type: "sliding"
    window_size: "5 min"
    grouping: "global"

  - name: avg_disk_utilization
    formula: abs(mean(disk_read_activity))+abs(mean(disk_write_activity))
    collection_frequency: "10 sec"
    collection_output: "all"
    window_type: "sliding"
    window_size: "5 min"
    grouping: "global"
```


## Raw Metric Subscriptions

`subscribe_metric_raw(metric, node)` supports three live selector modes:

- `["10.0.0.1", "10.0.0.2"]` starts one raw listener per explicit node/IP
- `"all"` resolves all Kubernetes VM private IPs internally
- `"local"` resolves the current Kubernetes node InternalIP and starts one raw listener for it
- Pass `source_file="./raw-metrics.json"` to replay values from a JSON file instead
  of opening node connections. With a file, omit `node` (or use `"all"`) for
  every file node defining the requested metric, select explicit `node_id`
  values, or use `"cluster"` to dynamically map file profiles onto current
  Kubernetes nodes.

> **Important:** An explicit node/IP list is the simplest option when running outside Kubernetes-aware environments because it does not require Kubernetes API access.
>
> **Important:** `node="all"` requires in-cluster Kubernetes config and RBAC permission to read Kubernetes nodes.
>
> **Important:** `node="local"` also uses the Kubernetes API in-cluster. It resolves the current pod, then the node backing that pod, so it needs RBAC permission to read the current pod and its node.
>
> **Important:** File mode normally requires no Kubernetes access, but
> `node="cluster"` requires permission to list Kubernetes nodes.

### File-backed raw metrics

Use a versioned JSON document so invalid fixtures fail at subscription time and
future schema changes remain explicit:

```json
{
  "version": 1,
  "nodes": [
    {
      "node_id": "simulated-node-a",
      "metrics": {
        "cpu_util_prct": {
          "interval_seconds": 5,
          "values": [42.0, 44.5, 41.2],
          "repeat": true
        }
      }
    }
  ]
}
```

```python
threads = subscribe_metric_raw(
    "cpu_util_prct",
    source_file="./raw-metrics.json",
)
```

One file may contain multiple nodes and metrics. One file per node is also valid.
Metric names may be plain names or full `/topic/...` destinations.

In file mode, `node` controls which file entries are replayed:

| File-mode selector | Behavior | Kubernetes access |
| --- | --- | --- |
| Omitted | Every file `node_id` defining the requested metric | None |
| `"all"` | Same as omitting `node` | None |
| `"node-a"` or `["node-a", "node-b"]` | Exactly the requested file IDs; every selected ID must define the metric | None |
| `"cluster"` | Dynamically maps unique file profiles onto current Kubernetes VM private IPs | `get,list nodes` |

Cluster mapping is one-to-one: a file profile is never reused for two cluster
nodes. Existing valid assignments for the same source file are preserved across
later subscriptions, exact profile/IP matches are preferred when initially
available, and remaining assignments are deterministic. If the file has more
profiles than the cluster, unused profiles remain idle. If the cluster has more
nodes than the file has profiles, the unmatched cluster nodes are logged and do
not receive replay data. The cluster snapshot is resolved when a cluster-mode
subscription is made and refreshed every 30 seconds. New nodes receive unused
profiles without changing valid assignments; departed nodes are unsubscribed
and release their profiles. If refresh-time Kubernetes discovery or file
validation fails, the last valid subscription state remains active and the
failure is logged. `"local"` is not supported in file mode.

The bundled ten-profile fixture can drive up to ten cluster nodes:

```python
from pathlib import Path

source_file = Path("./examples/raw_metrics_10_nodes.json")

cpu_threads = subscribe_metric_raw(
    "cpu_util_prct",
    "cluster",
    source_file=source_file,
)
ram_threads = subscribe_metric_raw(
    "ram_util_prct",
    "cluster",
    source_file=source_file,
)
```

Both metrics use the same preserved profile-to-cluster assignment. Returned
thread mappings and query results are keyed by the actual cluster VM private IP,
not by `profile-01` through `profile-10`.

#### Choosing between `values` and `samples`

Each metric definition must use exactly one replay format. Use `values` when all
readings are separated by the same amount of time, or use `samples` when each
reading needs an explicit replay time. Do not put both fields in the same metric
definition: they describe two different schedules, so the client rejects a
definition containing both. A definition containing neither field is also
rejected.

| Format | Use when | Required timing field | First emission |
| --- | --- | --- | --- |
| `values` | Readings have a fixed interval | `interval_seconds` | Immediately when replay starts |
| `samples` | Readings have irregular intervals | `offset_seconds` on every sample | At the first sample's offset, which must be `0` |

For regularly sampled data, provide a non-empty `values` array and a positive
`interval_seconds`. The first value is emitted immediately at offset `0`; each
following value is emitted after another interval:

```json
{
  "interval_seconds": 5,
  "values": [42.0, 44.5, 41.2],
  "repeat": true
}
```

This schedule emits `42.0` at 0 seconds, `44.5` at 5 seconds, and `41.2` at
10 seconds. Because `repeat` is `true`, the sequence starts again with `42.0`
at 15 seconds. A single value with `repeat: true` reports a constant value once
per interval. No `delta_time` is needed for this format.

For irregular data, use a non-empty `samples` array instead. Every entry must
contain a `value` and a non-negative `offset_seconds`. Offsets are relative to
the start of replay, not Unix timestamps; the first offset must be `0`, and all
following offsets must be strictly increasing:

```json
{
  "repeat": true,
  "cycle_duration_seconds": 10,
  "samples": [
    {"offset_seconds": 0, "value": 42.0},
    {"offset_seconds": 2.5, "value": 44.5},
    {"offset_seconds": 7, "value": 41.2}
  ]
}
```

This schedule emits `42.0` at 0 seconds, `44.5` at 2.5 seconds, and `41.2` at
7 seconds. When an irregular schedule repeats, `cycle_duration_seconds` is
required and must be greater than the final sample offset. In this example, the
next cycle starts at 10 seconds. When `repeat` is `false`,
`cycle_duration_seconds` is not required because the samples are emitted only
once.

`repeat` defaults to `true` in both formats. The following definition is invalid
because it attempts to specify both a fixed-interval schedule and an explicit
offset schedule:

```json
{
  "interval_seconds": 5,
  "values": [42.0, 44.5],
  "samples": [
    {"offset_seconds": 0, "value": 42.0},
    {"offset_seconds": 2.5, "value": 44.5}
  ]
}
```

The client assigns each emitted sample a wall-clock timestamp matching its
replay schedule, which keeps
`query_metric_values_raw(metric, seconds)` time-window behavior consistent with
live data.

Live and file-backed nodes can be combined for the same metric with separate
calls:

```python
live_threads = subscribe_metric_raw("cpu_util_prct", ["10.0.0.1"])
file_threads = subscribe_metric_raw(
    "cpu_util_prct",
    ["simulated-node-a"],
    source_file="./raw-metrics.json",
)
```

A given `node_id` can have only one source kind at a time. Unsubscribe that node
before switching it between live and file-backed data; this prevents duplicate,
ambiguous samples for the same node.

## Kubernetes access for `all`, `local`, and `cluster`

These selectors use the in-cluster Kubernetes API:

- explicit node/IP list: no Kubernetes API permission needed
- live `"all"`: cluster-wide `get` and `list` on `nodes`
- `"local"`: `get` on `pods` in the service account namespace, plus cluster-wide `get` and `list` on `nodes`
- file-backed `"cluster"`: cluster-wide `get` and `list` on `nodes`

Apply the bundled manifest:

```bash
kubectl apply -f ./manifests/mon-client-rbac.yaml
```

That manifest creates:

- `ServiceAccount` `mon-client`
- namespace `Role` + `RoleBinding` for `get pods`
- `ClusterRole` + `ClusterRoleBinding` for `get,list nodes`

If you deploy outside `default`, update the namespace fields in the manifest before applying it.

The same permissions can be created with imperative `kubectl` commands:

```bash
kubectl create serviceaccount mon-client -n default
kubectl create role mon-client-pod-reader --verb=get --resource=pods -n default
kubectl create rolebinding mon-client-pod-reader \
  --role=mon-client-pod-reader \
  --serviceaccount=default:mon-client \
  -n default
kubectl create clusterrole mon-client-node-reader --verb=get,list --resource=nodes
kubectl create clusterrolebinding mon-client-node-reader \
  --clusterrole=mon-client-node-reader \
  --serviceaccount=default:mon-client
```

Verify access with:

```bash
kubectl auth can-i get pods --as=system:serviceaccount:default:mon-client -n default
kubectl auth can-i get nodes --as=system:serviceaccount:default:mon-client
kubectl auth can-i list nodes --as=system:serviceaccount:default:mon-client
```

If you see an error like `Failed to list Kubernetes nodes: Forbidden` or `Failed to determine current Kubernetes node IP: Forbidden`, the service account can authenticate but does not have enough RBAC to read the required Kubernetes resources.

## Testing in a pod

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

### `deploy_monitoring(sat_file: str, optimusdb_url: str = "http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb", use_kb: bool = True, upload_kb: bool = False, logger: logging.Logger | None = None) -> int`

Deploys the standard monitoring stack manifests.

This helper uses in-cluster Kubernetes config loaded from the pod's service account. It does not read a local kubeconfig. The service account used by the pod must have RBAC permission to create or patch the Kubernetes resources defined by the monitoring manifests. The `sat_file`, `optimusdb_url`, `use_kb`, and `upload_kb` values are part of the deployment flow. The `sat_file` input is a local file path. During deployment, the SAT file is read locally, its basename is converted into a unique filename by appending a UTC timestamp, and that generated filename is injected into the rendered manifest as `SAT_FILE`. The SAT file content is also deployed as `ConfigMap/tosca-model-configmap` under the fixed key `test-tosca-model.yaml`, which is the filename expected by the application. `optimusdb_url` is optional and defaults to the Swarmchestrate OptimusDB endpoint. By default, `use_kb=True`, so EMS is configured to resolve the SAT through the knowledge base exposed by the configured `optimusdb_url`. Set `use_kb=False` to keep knowledge-base mode disabled in the rendered manifest and use the SAT content from that ConfigMap instead. When `upload_kb=True`, the SAT file is also uploaded to the knowledge base under that generated unique filename before the Kubernetes resources are deployed. If `./manifests/emsconfig.yaml` or `./manifests/ems+netdata-k3s_parametric.yaml` is missing locally, the library downloads it from the release assets before deployment. When a local copy already exists, it validates the content against the release asset, logs whether it matches, and keeps the local file if it differs.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `sat_file` | Yes | `str` | Local SAT file path. Its basename is converted to a unique timestamped filename for `SAT_FILE`, and its content is loaded into `ConfigMap/tosca-model-configmap` as `test-tosca-model.yaml`. |
| `optimusdb_url` | No | `str` | OptimusDB URL injected into the templated manifest. Defaults to the Swarmchestrate OptimusDB endpoint. |
| `use_kb` | No | `bool` | Controls the rendered `USE_KB` value in `emsconfig.yaml`. Defaults to `True`. Set to `False` to disable knowledge base mode. |
| `upload_kb` | No | `bool` | If `True`, uploads the SAT file to the knowledge base before deployment. Defaults to `False`. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** process-style exit code: `0` on success, `1` if one or more deploy steps fail.

### `undeploy_monitoring(namespace: str | None = None, logger: logging.Logger | None = None) -> int`

Undeploys the monitoring stack manifests and the related cleanup resources.

Like deployment, undeploy uses in-cluster Kubernetes config loaded from the pod's service account and does not read a local kubeconfig. That service account must have RBAC permission to delete the Kubernetes resources defined by the manifests and the additional cleanup resources. Unlike deployment, undeploy does not render `emsconfig.yaml` and does not require the original `sat_file`, `optimusdb_url`, or `use_kb` values. It deletes `ConfigMap/emsconfig` and `ConfigMap/tosca-model-configmap` directly by name and undeploys the remaining manifest-defined resources from the static manifest files.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `namespace` | No | `str \| None` | Namespace override for deleting namespaced resources. If omitted, manifest/default namespaces are used. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** process-style exit code: `0` on success, `1` if one or more undeploy steps fail.

### `subscribe_metric(metric: str) -> str`

Starts or reuses the shared metric listener for the requested metric topic.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. Plain names are normalized to `/topic/<metric>`. |

**Output:** `str` thread name of the shared listener, currently `metric-listener`.

### `subscribe_metric_raw(metric: str, node: list[str] | str | None = None, cache_size: int | None = None, *, source_file: str | Path | None = None) -> dict[str, str]`

Starts raw metric producers that either connect directly to node IPs or replay a
versioned JSON source file.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. Plain names are normalized to `/topic/<metric>`. |
| `node` | Live mode only | `list[str] \| str \| None` | In live mode, use an explicit node/IP list, `"all"`, or `"local"`; omitting it is an error. In file mode, omit it or use `"all"` for every matching file node, use explicit file IDs for a subset, or use `"cluster"` to map unique file profiles onto current Kubernetes VM private IPs. |
| `cache_size` | No | `int \| None` | Per raw metric per node sample buffer size. If omitted, the default value `1000` is used. |
| `source_file` | No | `str \| Path \| None` | Version 1 raw metric JSON file. If omitted, metrics come from live node connections. |

**Output:** `dict[str, str]` mapping each resolved node/IP to the worker thread name started for it.

**Notes:**
- Starts one worker thread per resolved node/IP and reuses it for additional raw
  metrics from the same source kind on that node/IP.
- Raw subscriptions connect directly to each resolved node/IP instead of `MON_CLIENT_STOMP_HOST`.
- File subscriptions start one replay thread per selected `node_id`; `"local"` is
  not supported for file sources.
- File `node="cluster"` discovers Kubernetes VM private IPs and dynamically
  assigns at most one file profile to each node without profile reuse; it
  requires node-list RBAC and reconciles changes every 30 seconds.
- File and live nodes may be subscribed to the same metric, but one `node_id`
  cannot use both source kinds concurrently.
- Mixing `subscribe_metric(...)` and `subscribe_metric_raw(...)` for the same metric is rejected.
- **Important:** In live mode, `node="all"` and `node="local"` use in-cluster Kubernetes API access.
- **Important:** In live mode, `node="local"` needs `get pods` in the current namespace and `get,list nodes` cluster-wide.

### `query_metric_values(metric: str) -> list[Any]`

Returns all currently buffered metric values for the metric and consumes those returned samples.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |

**Output:**

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
- `examples/subscribe_metric.py`
- `examples/subscribe_metric_raw.py`

## Simple Snippets

### `deploy_monitoring`

```python
from swchmonclient import deploy_monitoring

exit_code = deploy_monitoring(
    "./manifests/stressng.yaml",
    use_kb=True,
    upload_kb=False,
)
```

### `undeploy_monitoring`

```python
from swchmonclient import undeploy_monitoring

exit_code = undeploy_monitoring(
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
