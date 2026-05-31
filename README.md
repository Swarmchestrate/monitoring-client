# swchmonclient

A small Python library for deploying the monitoring stack and consuming metric events over STOMP.

## Install

```bash
uv add swchmonclient
```

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

deploy_monitoring("./k3s.yaml", "tosca_metrics_ze.yaml", "http://optimusdb.example/swarmkb")
subscribe_metric("/topic/mysample_metric")
recent_values = query_metric_values("/topic/mysample_metric", seconds=60)
subscribe_metric_raw("/topic/myraw_metric", ["10.0.0.1", "10.0.0.2"])
subscribe_metric_raw("/topic/myraw_metric", "all")
subscribe_metric_raw("/topic/myraw_metric", "local")
raw_values = query_metric_values_raw("/topic/myraw_metric", seconds=60)
unsubscribe_metric("/topic/mysample_metric")
unsubscribe_metric("/topic/myraw_metric")
undeploy_monitoring(
    "./k3s.yaml",
    "tosca_metrics_ze.yaml",
    "http://optimusdb.example/swarmkb",
    namespace="default",
)
```

## Raw Metric Subscriptions

`subscribe_metric_raw(metric, node)` supports three selector modes:

- `["10.0.0.1", "10.0.0.2"]` starts one raw listener per explicit node/IP
- `"all"` resolves all Kubernetes VM private IPs internally
- `"local"` resolves the current machine's private IP and starts one raw listener for it

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

Each node/IP keeps up to 1000 cached samples, dropping the oldest entries first when the buffer fills.

If you subscribe multiple raw metrics for the same node/IP, the library reuses the same raw listener thread for that node and dynamically subscribes the additional metric topics on that connection.

## Containerized raw local test

This repository includes a Docker image definition and a Kubernetes DaemonSet manifest for running `examples/subscribe_cpu_util_instance_raw_example.py` with the existing `"local"` selector.

Build the image from the repository root:

```bash
docker build -f docker/Dockerfile-subscribe-cpu-util-raw-local -t swchmonclient/raw-local-listener:latest .
```

The Kubernetes manifest is:

```text
manifests/raw-local-listener-daemonset.yaml
```

It uses `hostNetwork: true` so the `"local"` selector resolves the node IP instead of only the pod IP. The manifest is configured to pull the image from:

```text
cloud-193-225-250-99.sztaki.science-cloud.hu/swarchestrate/mon_lib:test
```

Before applying it, create the `registry-credentials` pull secret in the target namespace:

```bash
kubectl create secret docker-registry registry-credentials \
  --docker-server=cloud-193-225-250-99.sztaki.science-cloud.hu \
  --docker-username='<username>' \
  --docker-password='<password>'
```

Then apply the DaemonSet:

```bash
kubectl apply -f manifests/raw-local-listener-daemonset.yaml
```

If your broker requires credentials, create a `stomp-credentials` secret with `username` and `password` keys before deploying.

## API Reference

### `deploy_monitoring(kubeconfig_path: str | None, sat_file: str, optimusdb_url: str, logger: logging.Logger | None = None) -> int`

Deploys the standard monitoring stack manifests.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `kubeconfig_path` | No | `str \| None` | Path to the kubeconfig file. If omitted, the default kubeconfig / in-cluster fallback is used. |
| `sat_file` | Yes | `str` | SAT file path injected into the templated manifest. |
| `optimusdb_url` | Yes | `str` | OptimusDB URL injected into the templated manifest. |
| `logger` | No | `logging.Logger \| None` | Custom logger. If omitted, stdout logging is configured automatically. |

**Output:** process-style exit code: `0` on success, `1` if one or more deploy steps fail.

### `undeploy_monitoring(kubeconfig_path: str | None, sat_file: str, optimusdb_url: str, namespace: str | None = None, logger: logging.Logger | None = None) -> int`

Undeploys the standard monitoring stack manifests and the related cleanup resources.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `kubeconfig_path` | No | `str \| None` | Path to the kubeconfig file. If omitted, the default kubeconfig / in-cluster fallback is used. |
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
| `node` | Yes | `list[str] \| str` | Raw node selector. Use an explicit node/IP list, `"all"` for all Kubernetes VM private IPs, or `"local"` for the current machine's private IP. |
| `cache_size` | No | `int \| None` | Per raw metric per node sample buffer size. If omitted, the default value `1000` is used. |

**Output:** `dict[str, str]` mapping each resolved node/IP to the listener thread name started for it.

**Notes:**
- Starts one raw listener thread per resolved node/IP and reuses it for additional raw metrics on that same node/IP.
- Raw subscriptions connect directly to each resolved node/IP instead of `STOMP_HOST`.
- Mixing `subscribe_metric(...)` and `subscribe_metric_raw(...)` for the same metric is rejected.
- For `node="all"`, Kubernetes config is resolved automatically from the default kubeconfig, `KUBECONFIG`, common local files such as `./k3s.yaml`, or in-cluster config.

### `query_metric_values(metric: str, seconds: int) -> list[Any]`

Returns buffered metric values received within the last `seconds` seconds and consumes those returned samples.

| Parameter | Required | Type | Description |
| --- | --- | --- | --- |
| `metric` | Yes | `str` | Metric name or full topic destination. |
| `seconds` | Yes | `int` | Time window to read from. Must be non-negative. |

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

## Simple Snippets

### `deploy_monitoring`

```python
from swchmonclient import deploy_monitoring

exit_code = deploy_monitoring("./k3s.yaml", "tosca_metrics_ze.yaml", "http://optimusdb.example/swarmkb")
```

### `undeploy_monitoring`

```python
from swchmonclient import undeploy_monitoring

exit_code = undeploy_monitoring(
    "./k3s.yaml",
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
# or: subscribe_metric_raw("cpu_util_instance", "all")
# or: subscribe_metric_raw("cpu_util_instance", ["10.0.0.1", "10.0.0.2"])
print(threads)
```

### `query_metric_values`

```python
from swchmonclient import query_metric_values

standard_values = query_metric_values("cpu_util_instance", seconds=60)
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

The STOMP listener reads `STOMP_HOST` and `STOMP_PORT` from `.env`, so you can switch environments without changing code.

Use the bundled profiles:

```bash
./scripts/use-cluster-env.sh
./scripts/use-dev-env.sh
```

- `use-cluster-env.sh` writes `.env` with `emsserver-ems-server:61610`
- `use-dev-env.sh` writes `.env` with `127.0.0.1:61610`

When running outside the cluster, start a port-forward to the ClusterIP service:

```bash
./scripts/use-dev-env.sh
./scripts/port-forward-emsserver.sh
```

If the service is not in `default`, pass the namespace explicitly:

```bash
./scripts/port-forward-emsserver.sh monitoring
```
