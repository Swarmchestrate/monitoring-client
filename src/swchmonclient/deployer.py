import os
from collections.abc import Iterable, Sequence
from logging import Logger
from typing import Any

import yaml
from kubernetes import config, dynamic
from kubernetes.client import ApiClient, CoreV1Api
from kubernetes.client.exceptions import ApiException

from .exceptions import DeploymentError


MONITORING_TEMPLATE_MANIFEST = "./manifests/emsconfig.yaml"
MONITORING_DEPLOY_MANIFESTS = (
    MONITORING_TEMPLATE_MANIFEST,
    "./manifests/ems+netdata-k3s_parametric.yaml",
)
MONITORING_UNDEPLOY_MANIFESTS = (
    "./manifests/custom-metric-config.yaml",
    MONITORING_TEMPLATE_MANIFEST,
    "./manifests/ems+netdata-k3s_parametric.yaml",
    "./manifests/stomp-listener.yaml",
    "./manifests/python_manifest.yaml",
)
MONITORING_EXTRA_RESOURCES_TO_DELETE = (
    ("apps/v1", "DaemonSet", "ems-client-daemonset", "daemonset"),
    ("v1", "ConfigMap", "ems-client-configmap", "configmap"),
    ("v1", "ConfigMap", "monitoring-configmap", "configmap"),
)
DEFAULT_MONITORING_NAMESPACE = "default"


class K8sDeployer:
    """Deploy and remove Kubernetes resources from manifest files."""

    def __init__(
        self,
        kubeconfig_path: str | None = None,
        context: str | None = None,
        in_cluster_fallback: bool = True,
    ) -> None:
        self._load_config(kubeconfig_path=kubeconfig_path, context=context, in_cluster_fallback=in_cluster_fallback)
        self.api_client = ApiClient()
        self.core_v1_api = CoreV1Api(self.api_client)
        self.dynamic_client = dynamic.DynamicClient(self.api_client)

    def _load_config(
        self,
        kubeconfig_path: str | None,
        context: str | None,
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

    def _iter_manifest_documents(self, manifest_path: str) -> Iterable[dict[str, Any]]:
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                documents = list(yaml.safe_load_all(handle))
        except FileNotFoundError as error:
            raise DeploymentError(f"Manifest file not found: {manifest_path}") from error
        except yaml.YAMLError as error:
            raise DeploymentError(f"Invalid YAML in manifest file {manifest_path}: {error}") from error

        for document in documents:
            if isinstance(document, dict) and document.get("kind") and document.get("apiVersion"):
                yield document

    def _resource_from_doc(self, document: dict[str, Any]):
        return self.dynamic_client.resources.get(
            api_version=document["apiVersion"],
            kind=document["kind"],
        )

    def deploy_manifest(self, manifest_path: str, namespace: str | None = None) -> list[dict[str, str]]:
        """Create or patch resources from a manifest file and return resource references."""
        deployed: list[dict[str, str]] = []

        for document in self._iter_manifest_documents(manifest_path):
            resource = self._resource_from_doc(document)
            metadata = document.setdefault("metadata", {})
            name = metadata.get("name")
            if not name:
                raise DeploymentError("Each manifest document must include metadata.name")

            resource_namespace = namespace or metadata.get("namespace") or "default"
            namespaced = bool(getattr(resource, "namespaced", True))
            if namespaced:
                metadata["namespace"] = resource_namespace

            try:
                if namespaced:
                    resource.create(body=document, namespace=resource_namespace)
                else:
                    resource.create(body=document)
            except ApiException as error:
                if error.status == 409:
                    if namespaced:
                        resource.patch(
                            name=name,
                            namespace=resource_namespace,
                            body=document,
                            content_type="application/merge-patch+json",
                        )
                    else:
                        resource.patch(
                            name=name,
                            body=document,
                            content_type="application/merge-patch+json",
                        )
                else:
                    raise DeploymentError(
                        f"Failed to deploy {document['kind']}/{name}: {error.reason}"
                    ) from error
            except Exception as error:
                raise DeploymentError(f"Failed to deploy {document['kind']}/{name}: {error}") from error

            deployed.append(
                {
                    "apiVersion": document["apiVersion"],
                    "kind": document["kind"],
                    "name": name,
                    "namespace": resource_namespace if namespaced else "",
                }
            )

        return deployed

    def destroy_manifest(self, manifest_path: str, namespace: str | None = None) -> int:
        """Delete resources listed in the given manifest file."""
        deleted = 0

        for document in self._iter_manifest_documents(manifest_path):
            resource = self._resource_from_doc(document)
            metadata = document.get("metadata", {})
            name = metadata.get("name")
            if not name:
                continue

            resource_namespace = namespace or metadata.get("namespace") or "default"
            namespaced = bool(getattr(resource, "namespaced", True))

            try:
                if namespaced:
                    resource.delete(name=name, namespace=resource_namespace)
                else:
                    resource.delete(name=name)
                deleted += 1
            except ApiException as error:
                if error.status != 404:
                    raise DeploymentError(
                        f"Failed to delete {document['kind']}/{name}: {error.reason}"
                    ) from error
            except Exception as error:
                raise DeploymentError(f"Failed to delete {document['kind']}/{name}: {error}") from error

        return deleted

    def destroy_resource(
        self,
        api_version: str,
        kind: str,
        name: str,
        namespace: str | None = None,
    ) -> int:
        """Delete a single named resource and return 1 if it was deleted, else 0."""
        try:
            resource = self.dynamic_client.resources.get(api_version=api_version, kind=kind)
            namespaced = bool(getattr(resource, "namespaced", True))

            if namespaced:
                resource_namespace = namespace or "default"
                resource.delete(name=name, namespace=resource_namespace)
            else:
                resource.delete(name=name)
        except ApiException as error:
            if error.status == 404:
                return 0
            raise DeploymentError(f"Failed to delete {kind}/{name}: {error.reason}") from error
        except Exception as error:
            raise DeploymentError(f"Failed to delete {kind}/{name}: {error}") from error

        return 1

    def destroy_app(
        self,
        label_selector: str,
        namespace: str = "default",
        kinds: list[tuple[str, str]] | None = None,
    ) -> int:
        """Delete resources matching a label selector in a namespace."""
        default_kinds = [
            ("apps/v1", "Deployment"),
            ("apps/v1", "StatefulSet"),
            ("apps/v1", "DaemonSet"),
            ("batch/v1", "Job"),
            ("v1", "Service"),
            ("v1", "Pod"),
            ("v1", "ConfigMap"),
            ("v1", "Secret"),
        ]
        resolved_kinds = kinds or default_kinds

        deleted = 0
        for api_version, kind in resolved_kinds:
            try:
                resource = self.dynamic_client.resources.get(api_version=api_version, kind=kind)
                namespaced = bool(getattr(resource, "namespaced", True))
                if namespaced:
                    result = resource.get(namespace=namespace, label_selector=label_selector)
                else:
                    result = resource.get(label_selector=label_selector)

                for item in getattr(result, "items", []):
                    item_name = item.metadata.name
                    if namespaced:
                        resource.delete(name=item_name, namespace=namespace)
                    else:
                        resource.delete(name=item_name)
                    deleted += 1
            except ApiException as error:
                if error.status not in (403, 404):
                    raise DeploymentError(
                        f"Failed to destroy resources for {kind} with selector {label_selector}: {error.reason}"
                    ) from error
            except Exception as error:
                raise DeploymentError(
                    f"Failed to destroy resources for {kind} with selector {label_selector}: {error}"
                ) from error

        return deleted

    def get_vm_private_ips(self) -> list[str]:
        """Return Kubernetes node InternalIP addresses in API order, without duplicates."""
        try:
            nodes = self.core_v1_api.list_node().items
        except ApiException as error:
            raise DeploymentError(f"Failed to list Kubernetes nodes: {error.reason}") from error
        except Exception as error:
            raise DeploymentError(f"Failed to list Kubernetes nodes: {error}") from error

        private_ips: list[str] = []
        seen_ips: set[str] = set()
        for node in nodes:
            addresses = getattr(getattr(node, "status", None), "addresses", []) or []
            for address in addresses:
                if getattr(address, "type", None) != "InternalIP":
                    continue
                ip = getattr(address, "address", None)
                if not isinstance(ip, str) or not ip or ip in seen_ips:
                    continue
                seen_ips.add(ip)
                private_ips.append(ip)
                break

        return private_ips


def _resolve_manifests_with_optional_render(
    manifests: Sequence[str],
    template_manifest_path: str | None = None,
    template_variables: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    from .renderer import render_manifest

    rendered_template_path: str | None = None
    manifest_variables = template_variables or {}

    if template_manifest_path:
        rendered_template_path = render_manifest(template_manifest_path, **manifest_variables)

    resolved_manifests = [
        {
            "display_path": manifest,
            "actual_path": rendered_template_path if template_manifest_path and manifest == template_manifest_path else manifest,
            "variables": manifest_variables if template_manifest_path and manifest == template_manifest_path else None,
        }
        for manifest in manifests
    ]

    return resolved_manifests, rendered_template_path


def _deploy_manifests_with_optional_render(
    manifests: Sequence[str],
    kubeconfig_path: str | None = None,
    context: str | None = None,
    template_manifest_path: str | None = None,
    template_variables: dict[str, str] | None = None,
    logger: Logger | None = None,
    logger_name: str = "swchmonclient.deploy",
) -> int:
    """Deploy manifests and optionally render one manifest before deployment.

    If ``template_manifest_path`` is provided and it appears in ``manifests``, that
    manifest is rendered with ``template_variables`` and the rendered file is used
    for deployment, while logs still show the original manifest path.
    """
    from .logging_utils import configure_stdout_logger
    active_logger = logger or configure_stdout_logger(logger_name)
    deployer = K8sDeployer(kubeconfig_path=kubeconfig_path, context=context)
    overall_ok = True
    rendered_template_path: str | None = None

    try:
        resolved_manifests, rendered_template_path = _resolve_manifests_with_optional_render(
            manifests=manifests,
            template_manifest_path=template_manifest_path,
            template_variables=template_variables,
        )

        for manifest in resolved_manifests:
            manifest_path = manifest["actual_path"]
            display_path = manifest["display_path"]
            variables = manifest["variables"]

            if variables:
                active_logger.info("Deploying %s with variables:", display_path)
                for key, value in variables.items():
                    active_logger.info("    • %s: %s", key, value)
            else:
                active_logger.info("Deploying %s ...", display_path)

            try:
                deployed = deployer.deploy_manifest(manifest_path)
            except DeploymentError as error:
                active_logger.error("  ERROR: %s", error)
                overall_ok = False
                continue

            if not deployed:
                active_logger.info("No valid Kubernetes resources found in the manifest.")
                continue

            active_logger.info("  Created or patched resources:")
            for resource in deployed:
                namespace = resource.get("namespace") or "<cluster-scoped>"
                active_logger.info(
                    "  - %s/%s (apiVersion=%s, namespace=%s)",
                    resource["kind"],
                    resource["name"],
                    resource["apiVersion"],
                    namespace,
                )
    finally:
        if rendered_template_path and os.path.exists(rendered_template_path):
            os.unlink(rendered_template_path)

    if overall_ok:
        active_logger.info("All manifests deployed successfully.")
        return 0

    active_logger.info("One or more manifests failed to deploy.")
    return 1


def _undeploy_manifests_with_optional_render(
    manifests: Sequence[str],
    kubeconfig_path: str | None = None,
    context: str | None = None,
    namespace: str | None = None,
    template_manifest_path: str | None = None,
    template_variables: dict[str, str] | None = None,
    extra_resources_to_delete: Sequence[tuple[str, str, str, str]] = (),
    logger: Logger | None = None,
    logger_name: str = "swchmonclient.undeploy",
) -> int:
    from .logging_utils import configure_stdout_logger

    active_logger = logger or configure_stdout_logger(logger_name)
    deployer = K8sDeployer(kubeconfig_path=kubeconfig_path, context=context)
    overall_ok = True
    rendered_template_path: str | None = None

    try:
        resolved_manifests, rendered_template_path = _resolve_manifests_with_optional_render(
            manifests=manifests,
            template_manifest_path=template_manifest_path,
            template_variables=template_variables,
        )

        for manifest in resolved_manifests:
            manifest_path = manifest["actual_path"]
            display_path = manifest["display_path"]
            variables = manifest["variables"]

            if variables:
                active_logger.info("Undeploying %s with variables:", display_path)
                for key, value in variables.items():
                    active_logger.info("    • %s: %s", key, value)
            else:
                active_logger.info("Undeploying %s ...", display_path)

            try:
                deleted = deployer.destroy_manifest(manifest_path, namespace=namespace)
                active_logger.info("  Done. %s resource(s) deleted.", deleted)
            except DeploymentError as error:
                active_logger.error("  ERROR: %s", error)
                overall_ok = False

        for api_version, kind, name, resource_label in extra_resources_to_delete:
            active_logger.info("Undeploying %s/%s ...", kind, name)
            try:
                deleted = deployer.destroy_resource(
                    api_version=api_version,
                    kind=kind,
                    name=name,
                    namespace=namespace or DEFAULT_MONITORING_NAMESPACE,
                )
                active_logger.info("  Done. %s %s resource(s) deleted.", deleted, resource_label)
            except DeploymentError as error:
                active_logger.error("  ERROR: %s", error)
                overall_ok = False
    finally:
        if rendered_template_path and os.path.exists(rendered_template_path):
            os.unlink(rendered_template_path)

    if overall_ok:
        active_logger.info("All manifests undeployed successfully.")
        return 0

    active_logger.info("One or more manifests failed to undeploy.")
    return 1


def deploy_monitoring(
    kubeconfig_path: str | None,
    sat_file: str,
    optimusdb_url: str,
    logger: Logger | None = None,
) -> int:
    """Deploy the standard monitoring stack manifests."""
    return _deploy_manifests_with_optional_render(
        manifests=MONITORING_DEPLOY_MANIFESTS,
        kubeconfig_path=kubeconfig_path,
        template_manifest_path=MONITORING_TEMPLATE_MANIFEST,
        template_variables={
            "sat_file": sat_file,
            "optimusdb_url": optimusdb_url,
        },
        logger=logger,
        logger_name="swchmonclient.deploy_monitoring",
    )


def undeploy_monitoring(
    kubeconfig_path: str | None,
    sat_file: str,
    optimusdb_url: str,
    namespace: str | None = None,
    logger: Logger | None = None,
) -> int:
    """Undeploy the standard monitoring stack manifests and cleanup resources."""
    return _undeploy_manifests_with_optional_render(
        manifests=MONITORING_UNDEPLOY_MANIFESTS,
        kubeconfig_path=kubeconfig_path,
        namespace=namespace,
        template_manifest_path=MONITORING_TEMPLATE_MANIFEST,
        template_variables={
            "sat_file": sat_file,
            "optimusdb_url": optimusdb_url,
        },
        extra_resources_to_delete=MONITORING_EXTRA_RESOURCES_TO_DELETE,
        logger=logger,
        logger_name="swchmonclient.undeploy_monitoring",
    )


def get_vm_private_ips(
    kubeconfig_path: str | None = None,
    context: str | None = None,
) -> list[str]:
    """Load Kubernetes config and return the cluster nodes' private IP addresses."""
    deployer = K8sDeployer(kubeconfig_path=kubeconfig_path, context=context)
    return deployer.get_vm_private_ips()
