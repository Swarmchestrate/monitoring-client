from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from kubernetes import config, dynamic
from kubernetes.client import ApiClient

from .exceptions import DeploymentError, MonitorTimeout


class K8sMonitor:
    """Poll Kubernetes resources until they become ready."""

    def __init__(
        self,
        kubeconfig_path: Optional[str] = None,
        context: Optional[str] = None,
        in_cluster_fallback: bool = True,
    ) -> None:
        self._load_config(kubeconfig_path=kubeconfig_path, context=context, in_cluster_fallback=in_cluster_fallback)
        self.dynamic_client = dynamic.DynamicClient(ApiClient())

    def _load_config(
        self,
        kubeconfig_path: Optional[str],
        context: Optional[str],
        in_cluster_fallback: bool,
    ) -> None:
        try:
            config.load_kube_config(config_file=kubeconfig_path, context=context)
        except Exception as kubeconfig_error:
            if not in_cluster_fallback:
                raise DeploymentError(f"Unable to load kubeconfig: {kubeconfig_error}") from kubeconfig_error
            try:
                config.load_incluster_config()
            except Exception as incluster_error:
                raise DeploymentError(
                    "Unable to load Kubernetes configuration from kubeconfig or in-cluster environment"
                ) from incluster_error

    def wait_for_resources(
        self,
        resources: List[Dict[str, str]],
        timeout_seconds: int = 300,
        poll_seconds: float = 3.0,
    ) -> None:
        """Wait until all given resources are ready, or raise MonitorTimeout."""
        deadline = time.monotonic() + timeout_seconds
        pending = list(resources)

        while pending:
            unresolved: List[Dict[str, str]] = []
            for reference in pending:
                if not self.is_resource_ready(reference):
                    unresolved.append(reference)

            if not unresolved:
                return

            if time.monotonic() >= deadline:
                summary = ", ".join(
                    f"{ref.get('kind', '?')}/{ref.get('name', '?')}" for ref in unresolved
                )
                raise MonitorTimeout(f"Timeout waiting for resources to become ready: {summary}")

            pending = unresolved
            time.sleep(poll_seconds)

    def is_resource_ready(self, reference: Dict[str, str]) -> bool:
        """Return True when a single resource is considered ready."""
        resource = self._read_resource(reference)
        kind = reference["kind"].lower()

        if kind == "deployment":
            desired = getattr(resource.spec, "replicas", 1) or 1
            ready = getattr(resource.status, "readyReplicas", 0) or 0
            return ready >= desired

        if kind == "statefulset":
            desired = getattr(resource.spec, "replicas", 1) or 1
            ready = getattr(resource.status, "readyReplicas", 0) or 0
            return ready >= desired

        if kind == "daemonset":
            desired = getattr(resource.status, "desiredNumberScheduled", 0) or 0
            ready = getattr(resource.status, "numberReady", 0) or 0
            return desired == 0 or ready >= desired

        if kind == "pod":
            if getattr(resource.status, "phase", "") != "Running":
                return False
            container_statuses = getattr(resource.status, "containerStatuses", None) or []
            return all(getattr(status, "ready", False) for status in container_statuses)

        if kind == "service":
            service_type = getattr(resource.spec, "type", "ClusterIP")
            if service_type == "ExternalName":
                return True
            return bool(getattr(resource.spec, "clusterIP", None))

        if kind == "job":
            conditions = getattr(resource.status, "conditions", None) or []
            return any(getattr(condition, "type", "") == "Complete" and getattr(condition, "status", "") == "True" for condition in conditions)

        conditions = getattr(resource.status, "conditions", None) or []
        return any(getattr(condition, "type", "") == "Ready" and getattr(condition, "status", "") == "True" for condition in conditions)

    def _read_resource(self, reference: Dict[str, str]) -> Any:
        api_version = reference.get("apiVersion") or self._default_api_version(reference["kind"])
        kind = reference["kind"]
        name = reference["name"]
        namespace = reference.get("namespace") or "default"

        resource = self.dynamic_client.resources.get(api_version=api_version, kind=kind)
        if bool(getattr(resource, "namespaced", True)):
            return resource.get(name=name, namespace=namespace)
        return resource.get(name=name)

    @staticmethod
    def _default_api_version(kind: str) -> str:
        mapping = {
            "deployment": "apps/v1",
            "statefulset": "apps/v1",
            "daemonset": "apps/v1",
            "pod": "v1",
            "service": "v1",
            "job": "batch/v1",
        }
        return mapping.get(kind.lower(), "v1")
