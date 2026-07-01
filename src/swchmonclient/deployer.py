import base64
import os
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from logging import Logger
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import requests
import yaml
from kubernetes import config, dynamic
from kubernetes.client import ApiClient, CoreV1Api
from kubernetes.client.exceptions import ApiException

from .exceptions import DeploymentError


MONITORING_TEMPLATE_MANIFEST = "./manifests/emsconfig.yaml"
TOSCA_MODEL_CONFIGMAP_NAME = "tosca-model-configmap"
TOSCA_MODEL_CONFIGMAP_KEY = "test-tosca-model.yaml"
DEFAULT_OPTIMUSDB_URL = "http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb"
MONITORING_DEPLOY_MANIFESTS = (
    MONITORING_TEMPLATE_MANIFEST,
    "./manifests/ems+netdata-k3s_parametric.yaml",
)
MONITORING_UNDEPLOY_MANIFESTS = (
    "./manifests/ems+netdata-k3s_parametric.yaml",
)
MONITORING_EXTRA_RESOURCES_TO_DELETE = (
    ("v1", "ConfigMap", "emsconfig", "configmap"),
    ("v1", "ConfigMap", TOSCA_MODEL_CONFIGMAP_NAME, "configmap"),
    ("apps/v1", "DaemonSet", "ems-client-daemonset", "daemonset"),
    ("v1", "ConfigMap", "ems-client-configmap", "configmap"),
    ("v1", "ConfigMap", "monitoring-configmap", "configmap"),
)
DEFAULT_MONITORING_NAMESPACE = "default"
MANIFEST_DOWNLOAD_TIMEOUT_SECONDS = 30
SERVICEACCOUNT_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
MONITORING_MANIFEST_RELEASE_URLS = {
    MONITORING_TEMPLATE_MANIFEST: (
        "https://github.com/Swarmchestrate/monitoring-client/releases/download/v0.1.0/emsconfig.yaml"
    ),
    "./manifests/ems+netdata-k3s_parametric.yaml": (
        "https://github.com/Swarmchestrate/monitoring-client/releases/download/v0.1.0/ems+netdata-k3s_parametric.yaml"
    ),
}


class K8sDeployer:
    """Deploy and remove Kubernetes resources from manifest files."""

    def __init__(self) -> None:
        self._load_config()
        self.api_client = ApiClient()
        self.core_v1_api = CoreV1Api(self.api_client)
        self.dynamic_client = dynamic.DynamicClient(self.api_client)

    def _load_config(self) -> None:
        try:
            config.load_incluster_config()
        except Exception as error:
            raise DeploymentError("Unable to load in-cluster Kubernetes configuration") from error

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
            ip = self._extract_node_internal_ip(node)
            if ip is None or ip in seen_ips:
                continue
            seen_ips.add(ip)
            private_ips.append(ip)

        return private_ips

    def get_current_vm_private_ip(self) -> str:
        """Return the current pod's Kubernetes node InternalIP."""
        pod_name = os.getenv("HOSTNAME")
        if not isinstance(pod_name, str) or not pod_name.strip():
            raise DeploymentError("Failed to determine current pod name from HOSTNAME")

        namespace = self._read_current_namespace()

        try:
            pod = self.core_v1_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            node_name = getattr(getattr(pod, "spec", None), "node_name", None)
            if not isinstance(node_name, str) or not node_name.strip():
                raise DeploymentError(f"Failed to determine Kubernetes node name for pod {pod_name}")

            node = self.core_v1_api.read_node(name=node_name)
        except ApiException as error:
            raise DeploymentError(f"Failed to determine current Kubernetes node IP: {error.reason}") from error
        except DeploymentError:
            raise
        except Exception as error:
            raise DeploymentError(f"Failed to determine current Kubernetes node IP: {error}") from error

        ip = self._extract_node_internal_ip(node)
        if ip is None:
            raise DeploymentError(f"Failed to determine InternalIP for Kubernetes node {node_name}")

        return ip

    @staticmethod
    def _extract_node_internal_ip(node: object) -> str | None:
        addresses = getattr(getattr(node, "status", None), "addresses", []) or []
        for address in addresses:
            if getattr(address, "type", None) != "InternalIP":
                continue
            ip = getattr(address, "address", None)
            if isinstance(ip, str) and ip:
                return ip
        return None

    @staticmethod
    def _read_current_namespace() -> str:
        try:
            with open(SERVICEACCOUNT_NAMESPACE_PATH, "r", encoding="utf-8") as handle:
                namespace = handle.read().strip()
        except OSError as error:
            raise DeploymentError(
                f"Failed to determine current namespace from {SERVICEACCOUNT_NAMESPACE_PATH}: {error}"
            ) from error

        if not namespace:
            raise DeploymentError(
                f"Failed to determine current namespace from {SERVICEACCOUNT_NAMESPACE_PATH}"
            )

        return namespace


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


def _fetch_manifest_release_bytes(url: str) -> bytes:
    try:
        with urlopen(url, timeout=MANIFEST_DOWNLOAD_TIMEOUT_SECONDS) as response:
            return response.read()
    except URLError as error:
        raise DeploymentError(f"Unable to download manifest from {url}: {error}") from error


def _write_manifest_file(manifest_path: str, content: bytes) -> None:
    destination = Path(manifest_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(dir=destination.parent, delete=False) as temp_file:
        temp_file.write(content)
        temp_path = temp_file.name

    os.replace(temp_path, destination)


def _ensure_monitoring_manifest(manifest_path: str, logger: Logger) -> None:
    release_url = MONITORING_MANIFEST_RELEASE_URLS[manifest_path]
    release_content: bytes | None = None

    if os.path.exists(manifest_path):
        logger.info("Found local manifest %s.", manifest_path)
        try:
            with open(manifest_path, "rb") as local_manifest:
                local_content = local_manifest.read()
        except OSError as error:
            raise DeploymentError(f"Unable to read local manifest {manifest_path}: {error}") from error

        release_content = _fetch_manifest_release_bytes(release_url)
        if local_content == release_content:
            logger.info("Local manifest %s matches release asset.", manifest_path)
        else:
            logger.warning(
                "Local manifest %s differs from release asset %s. Keeping local file.",
                manifest_path,
                release_url,
            )
        return

    logger.info("Fetching manifest %s from release asset %s.", manifest_path, release_url)
    release_content = _fetch_manifest_release_bytes(release_url)
    _write_manifest_file(manifest_path, release_content)
    logger.info("Wrote manifest to %s.", manifest_path)


def _ensure_monitoring_manifests(manifest_paths: Sequence[str], logger: Logger) -> None:
    for manifest_path in manifest_paths:
        _ensure_monitoring_manifest(manifest_path, logger)


def _load_sat_file(sat_file: str) -> tuple[str, str]:
    sat_path = Path(sat_file)
    sat_filename = sat_path.name
    if not sat_filename:
        raise DeploymentError(f"Unable to determine SAT filename from {sat_file}")

    try:
        sat_content = sat_path.read_text(encoding="utf-8")
    except OSError as error:
        raise DeploymentError(f"Unable to read SAT file {sat_file}: {error}") from error

    return sat_filename, sat_content


def _build_unique_sat_filename(sat_filename: str) -> str:
    sat_path = Path(sat_filename)
    if not sat_path.name:
        raise DeploymentError(f"Unable to determine SAT filename from {sat_filename}")

    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    if sat_path.suffix:
        return f"{sat_path.stem}-{timestamp}{sat_path.suffix}"
    return f"{sat_path.name}-{timestamp}"


def _upload_sat_to_kb(
    sat_file: str,
    sat_filename: str,
    sat_content: str,
    logger: Logger,
) -> None:
    base = os.environ.get("KB_BASE_URL", "http://optimusdb.swarmchestrate.sztaki.hu").rstrip("/")
    context = os.environ.get("KB_CONTEXT", "swarmkb").strip("/")

    try:
        timeout = int(os.environ.get("KB_TIMEOUT", "10"))
    except ValueError as error:
        raise DeploymentError("KB_TIMEOUT must be an integer") from error

    upload_url = f"{base}/optimusdb1/{context}/upload"
    payload = {
        "file": base64.b64encode(sat_content.encode("utf-8")).decode("utf-8"),
        "filename": sat_filename,
        "store_full_structure": True,
        "target_store": "dsswres",
    }

    logger.info("Uploading SAT file %s to the knowledge base as %s ...", sat_file, sat_filename)
    try:
        response = requests.post(upload_url, json=payload, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as error:
        raise DeploymentError(f"Failed to upload SAT file {sat_file} to the knowledge base: {error}") from error
    logger.info("Uploaded SAT file %s to the knowledge base.", sat_filename)


def _render_tosca_model_configmap_manifest(sat_content: str) -> str:

    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": TOSCA_MODEL_CONFIGMAP_NAME,
            "namespace": DEFAULT_MONITORING_NAMESPACE,
        },
        "data": {
            TOSCA_MODEL_CONFIGMAP_KEY: sat_content,
        },
    }

    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as temp_file:
        yaml.safe_dump(manifest, temp_file, sort_keys=False)
        return temp_file.name


def _deploy_tosca_model_configmap(
    sat_file: str,
    sat_content: str,
    logger: Logger,
) -> int:
    deployer = K8sDeployer()
    rendered_manifest_path: str | None = None

    try:
        rendered_manifest_path = _render_tosca_model_configmap_manifest(sat_content)
        logger.info(
            "Deploying ConfigMap/%s from SAT file %s ...",
            TOSCA_MODEL_CONFIGMAP_NAME,
            sat_file,
        )
        deployed = deployer.deploy_manifest(rendered_manifest_path)
    except DeploymentError as error:
        logger.error("  ERROR: %s", error)
        return 1
    finally:
        if rendered_manifest_path and os.path.exists(rendered_manifest_path):
            os.unlink(rendered_manifest_path)

    if not deployed:
        logger.info("No valid Kubernetes resources found in the SAT ConfigMap manifest.")
        return 0

    logger.info("  Created or patched resources:")
    for resource in deployed:
        namespace = resource.get("namespace") or "<cluster-scoped>"
        logger.info(
            "  - %s/%s (apiVersion=%s, namespace=%s)",
            resource["kind"],
            resource["name"],
            resource["apiVersion"],
            namespace,
        )

    return 0


def _deploy_manifests_with_optional_render(
    manifests: Sequence[str],
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
    deployer = K8sDeployer()
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
    namespace: str | None = None,
    template_manifest_path: str | None = None,
    template_variables: dict[str, str] | None = None,
    extra_resources_to_delete: Sequence[tuple[str, str, str, str]] = (),
    logger: Logger | None = None,
    logger_name: str = "swchmonclient.undeploy",
) -> int:
    from .logging_utils import configure_stdout_logger

    active_logger = logger or configure_stdout_logger(logger_name)
    deployer = K8sDeployer()
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
    sat_file: str,
    optimusdb_url: str = DEFAULT_OPTIMUSDB_URL,
    use_kb: bool = True,
    upload_kb: bool = False,
    logger: Logger | None = None,
) -> int:
    """Deploy the standard monitoring stack manifests."""
    from .logging_utils import configure_stdout_logger

    active_logger = logger or configure_stdout_logger("swchmonclient.deploy_monitoring")
    try:
        _ensure_monitoring_manifests(MONITORING_DEPLOY_MANIFESTS, active_logger)
        sat_filename, sat_content = _load_sat_file(sat_file)
        deployed_sat_filename = _build_unique_sat_filename(sat_filename)
    except DeploymentError as error:
        active_logger.error("  ERROR: %s", error)
        active_logger.info("One or more manifests failed to deploy.")
        return 1
    if upload_kb and not use_kb:
        active_logger.warning(
            "upload_kb=True is ignored because use_kb=False; skipping knowledge base upload."
        )
    elif upload_kb:
        try:
            _upload_sat_to_kb(sat_file, deployed_sat_filename, sat_content, active_logger)
        except DeploymentError as error:
            active_logger.error("  ERROR: %s", error)
            active_logger.info("One or more manifests failed to deploy.")
            return 1
    if _deploy_tosca_model_configmap(sat_file, sat_content, active_logger) != 0:
        active_logger.info("One or more manifests failed to deploy.")
        return 1
    return _deploy_manifests_with_optional_render(
        manifests=MONITORING_DEPLOY_MANIFESTS,
        template_manifest_path=MONITORING_TEMPLATE_MANIFEST,
        template_variables={
            "sat_file": deployed_sat_filename,
            "optimusdb_url": optimusdb_url,
            "use_kb": use_kb,
            "upload_kb": upload_kb,
        },
        logger=active_logger,
        logger_name="swchmonclient.deploy_monitoring",
    )


def undeploy_monitoring(
    namespace: str | None = None,
    logger: Logger | None = None,
) -> int:
    """Undeploy the standard monitoring stack manifests and cleanup resources."""
    from .logging_utils import configure_stdout_logger

    active_logger = logger or configure_stdout_logger("swchmonclient.undeploy_monitoring")
    try:
        _ensure_monitoring_manifests(MONITORING_UNDEPLOY_MANIFESTS, active_logger)
    except DeploymentError as error:
        active_logger.error("  ERROR: %s", error)
        active_logger.info("One or more manifests failed to undeploy.")
        return 1
    return _undeploy_manifests_with_optional_render(
        manifests=MONITORING_UNDEPLOY_MANIFESTS,
        namespace=namespace,
        extra_resources_to_delete=MONITORING_EXTRA_RESOURCES_TO_DELETE,
        logger=active_logger,
        logger_name="swchmonclient.undeploy_monitoring",
    )


def get_vm_private_ips() -> list[str]:
    """Load in-cluster Kubernetes config and return the cluster nodes' private IP addresses."""
    deployer = K8sDeployer()
    return deployer.get_vm_private_ips()


def get_current_vm_private_ip() -> str:
    """Load in-cluster Kubernetes config and return the current pod's node InternalIP."""
    deployer = K8sDeployer()
    return deployer.get_current_vm_private_ip()
