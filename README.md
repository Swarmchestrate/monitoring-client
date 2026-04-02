# swchmonclient

A small Python library that manages a Kubernetes application lifecycle:

- Deploy Kubernetes manifests
- Wait for all newly created resources to become ready
- Start a monitoring function in a dedicated thread
- Destroy an application from Kubernetes
- Stop monitoring thread(s)

## Install

```bash
uv add swchmonclient
```

## Quick Example

```python
from swchmonclient import K8sDeployer, K8sMonitor, MonitoringThreadManager

# 1) Deploy
resources = K8sDeployer().deploy_manifest("./manifests/app.yaml", namespace="default")

# 2) Wait for readiness
K8sMonitor().wait_for_resources(resources, timeout_seconds=300)

# 3) Start monitoring thread
manager = MonitoringThreadManager()


def pull_metrics(stop_event):
    while not stop_event.is_set():
        # your existing monitoring script call
        pass

manager.start_monitoring_thread("metrics-poller", pull_metrics)

# 4) Destroy app (by label selector)
K8sDeployer().destroy_app("app=my-app", namespace="default")

# 5) Stop thread
manager.stop_monitoring_thread("metrics-poller")
```

## Development

```bash
uv sync
uv build
```

## Publish to PyPI

```bash
uv publish
```

To test first:

```bash
uv publish --repository testpypi
```
