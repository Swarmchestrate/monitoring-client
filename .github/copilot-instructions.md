# Copilot instructions for `swchmonclient`

## Build and test commands

Run commands from the repository root.

- Install/update the dev environment: `uv sync`
- Build distributables: `uv build`
- Run the full test suite: `PYTHONPATH=. uv run python -m pytest`
- Run a single test: `PYTHONPATH=. uv run python -m pytest tests/test_listener_threader.py::test_listener_reconnect_uses_exponential_backoff`

`PYTHONPATH=.` matters for tests in this repo because they import the example modules from `examples/` directly.

## High-level architecture

- The public package surface is exported from `src/swchmonclient/__init__.py`. The package has three main concerns: Kubernetes manifest deployment/cleanup, manifest rendering, and STOMP listener thread management.
- `src/swchmonclient/deployer.py` is the center of the Kubernetes flow. `K8sDeployer` wraps the Kubernetes dynamic client and applies manifests document-by-document. Deployments use create-first semantics and fall back to merge patch on HTTP 409 conflicts; deletes ignore HTTP 404s but raise `DeploymentError` for other failures.
- The higher-level `deploy_monitoring()` and `undeploy_monitoring()` helpers orchestrate the monitoring stack. `deploy_monitoring()` renders `./manifests/emsconfig.yaml` with Jinja variables, then applies a fixed manifest sequence. `undeploy_monitoring()` deletes `ConfigMap/emsconfig` directly by name, undeploys the remaining fixed manifest sequence, and removes a small set of explicitly named cleanup resources that are not handled purely by manifest deletion.
- `src/swchmonclient/renderer.py` renders Jinja-backed manifests into a temporary YAML file. The deploy/undeploy helpers are responsible for cleaning up that rendered file in a `finally` block.
- `src/swchmonclient/listener.py` owns the STOMP client loop. It reads connection settings from environment variables by default, creates the STOMP connection, subscribes, and reconnects with exponential backoff until a cooperative stop event is set.
- `src/swchmonclient/thread_manager.py` wraps long-running monitoring jobs. It injects a `stop_event` into targets that accept one, tracks thread liveness by name, records worker exceptions, and re-raises them when the thread is stopped.
- `src/swchmonclient/logging_utils.py` standardizes stdout logging for the deploy/undeploy entrypoints so their output is both human-readable and assertion-friendly in tests.

## Key conventions

- Keep the `examples/` scripts runnable without installation. They explicitly prepend `src/` to `sys.path`, and tests depend on importing some of them as modules.
- When changing deploy/undeploy behavior, preserve the current user-facing logging contract. Tests assert exact phrases such as the original manifest path, rendered template variables, resource summaries, and explicit cleanup resource messages.
- For templated manifests, log the original manifest path (`./manifests/emsconfig.yaml`), not the temporary rendered filename. Render once, use the rendered file only for the Kubernetes call, and always delete it afterward.
- `deploy_monitoring()` / `undeploy_monitoring()` should return process-style exit codes (`0` on overall success, `1` if any manifest/resource operation failed) instead of surfacing raw exceptions at the top level.
- The monitoring stack is driven by fixed manifest tuples and named extra cleanup resources in `deployer.py`. If that stack changes, update those constants first rather than scattering paths and resource names through callers.
- Threaded monitoring code in this repo is expected to be cooperative. Prefer targets that accept `stop_event`; if a target does not, `MonitoringThreadManager` will call it repeatedly until shutdown is requested.
