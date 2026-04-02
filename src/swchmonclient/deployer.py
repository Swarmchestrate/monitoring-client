from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from kubernetes import config, dynamic
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException

from .exceptions import DeploymentError


class K8sDeployer:
    """Deploy and remove Kubernetes resources from manifest files."""

    def __init__(
        self,
        kubeconfig_path: Optional[str] = None,
        context: Optional[str] = None,
        in_cluster_fallback: bool = True,
    ) -> None:
        self._load_config(kubeconfig_path=kubeconfig_path, context=context, in_cluster_fallback=in_cluster_fallback)
        self.api_client = ApiClient()
        self.dynamic_client = dynamic.DynamicClient(self.api_client)

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

    def _iter_manifest_documents(self, manifest_path: str) -> Iterable[Dict[str, Any]]:
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

    def _resource_from_doc(self, document: Dict[str, Any]):
        return self.dynamic_client.resources.get(
            api_version=document["apiVersion"],
            kind=document["kind"],
        )

    def deploy_manifest(self, manifest_path: str, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """Create or patch resources from a manifest file and return resource references."""
        deployed: List[Dict[str, str]] = []

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

    def destroy_manifest(self, manifest_path: str, namespace: Optional[str] = None) -> int:
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

    def destroy_app(
        self,
        label_selector: str,
        namespace: str = "default",
        kinds: Optional[List[Tuple[str, str]]] = None,
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
