import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import yaml
from kubernetes.client.exceptions import ApiException

from swchmonclient import deploy_monitoring
from swchmonclient.deployer import (
    DEFAULT_MONITORING_NAMESPACE,
    MONITORING_DEPLOY_MANIFESTS,
    MONITORING_TEMPLATE_MANIFEST,
    K8sDeployer,
    _render_tosca_model_configmap_manifest,
)
from swchmonclient.exceptions import DeploymentError


def test_deploy_monitoring_propagates_requested_namespace():
    with patch("swchmonclient.deployer._ensure_monitoring_manifests"), \
        patch(
            "swchmonclient.deployer._load_sat_file",
            return_value=("stressng.yaml", "topology: demo\n"),
        ), \
        patch(
            "swchmonclient.deployer._build_unique_sat_filename",
            return_value="stressng-20260904120000000000.yaml",
        ), \
        patch(
            "swchmonclient.deployer._deploy_tosca_model_configmap",
            return_value=0,
        ) as deploy_tosca, \
        patch(
            "swchmonclient.deployer._deploy_manifests_with_optional_render",
            return_value=0,
        ) as deploy_manifests:
        exit_code = deploy_monitoring(
            sat_file="/tmp/stressng.yaml",
            namespace="monitoring-test",
        )

    assert exit_code == 0
    deploy_tosca.assert_called_once()
    assert deploy_tosca.call_args.kwargs["namespace"] == "monitoring-test"
    deploy_manifests.assert_called_once_with(
        manifests=MONITORING_DEPLOY_MANIFESTS,
        namespace="monitoring-test",
        template_manifest_path=MONITORING_TEMPLATE_MANIFEST,
        template_variables={
            "sat_file": "stressng-20260904120000000000.yaml",
            "optimusdb_url": (
                "http://optimusdb.swarmchestrate.sztaki.hu/optimusdb1/swarmkb"
            ),
            "use_kb": True,
            "upload_kb": False,
        },
        logger=deploy_manifests.call_args.kwargs["logger"],
        logger_name="swchmonclient.deploy_monitoring",
    )


def test_deploy_manifest_overrides_namespaced_resources_and_service_account_subjects(tmp_path):
    manifest_path = tmp_path / "resources.yaml"
    manifest_path.write_text(
        """apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: application
subjects:
  - kind: ServiceAccount
    name: application
    namespace: default
  - kind: User
    name: administrator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: application
""",
        encoding="utf-8",
    )

    config_map_resource = Mock(namespaced=True)
    cluster_role_binding_resource = Mock(namespaced=False)
    resources = {
        "ConfigMap": config_map_resource,
        "ClusterRoleBinding": cluster_role_binding_resource,
    }

    deployer = K8sDeployer.__new__(K8sDeployer)
    deployer.dynamic_client = SimpleNamespace(
        resources=SimpleNamespace(
            get=lambda *, api_version, kind: resources[kind],
        )
    )

    deployed = deployer.deploy_manifest(
        str(manifest_path),
        namespace="swarm-system",
    )

    config_map_body = config_map_resource.create.call_args.kwargs["body"]
    assert config_map_resource.create.call_args.kwargs["namespace"] == "swarm-system"
    assert config_map_body["metadata"]["namespace"] == "swarm-system"

    binding_body = cluster_role_binding_resource.create.call_args.kwargs["body"]
    assert binding_body["subjects"][0]["namespace"] == "swarm-system"
    assert "namespace" not in binding_body["subjects"][1]
    assert deployed[0]["namespace"] == "swarm-system"
    assert deployed[1]["namespace"] == ""


def test_deploy_manifest_reports_missing_target_namespace(tmp_path):
    manifest_path = tmp_path / "configmap.yaml"
    manifest_path.write_text(
        """apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
""",
        encoding="utf-8",
    )

    config_map_resource = Mock(namespaced=True)
    config_map_resource.create.side_effect = ApiException(status=404, reason="Not Found")
    deployer = K8sDeployer.__new__(K8sDeployer)
    deployer.dynamic_client = SimpleNamespace(
        resources=SimpleNamespace(get=lambda *, api_version, kind: config_map_resource)
    )

    with pytest.raises(
        DeploymentError,
        match=(
            'Target namespace "missing-system" does not exist. '
            "Create it before deployment with: kubectl create namespace missing-system"
        ),
    ):
        deployer.deploy_manifest(
            str(manifest_path),
            namespace="missing-system",
        )


def test_tosca_model_configmap_uses_default_monitoring_namespace():
    manifest_path = _render_tosca_model_configmap_manifest("topology: demo\n")
    try:
        with open(manifest_path, encoding="utf-8") as manifest_file:
            manifest = yaml.safe_load(manifest_file)
    finally:
        os.unlink(manifest_path)

    assert DEFAULT_MONITORING_NAMESPACE == "swarm-system"
    assert manifest["metadata"]["namespace"] == DEFAULT_MONITORING_NAMESPACE


def test_bootstrap_manifests_default_to_monitoring_namespace():
    repository_root = Path(__file__).resolve().parents[1]
    manifest_paths = (
        repository_root / "manifests" / "mon-client-rbac.yaml",
        repository_root / "manifests" / "mon-client-test-pod.yaml",
    )

    namespaces = []
    for manifest_path in manifest_paths:
        with manifest_path.open(encoding="utf-8") as manifest_file:
            for document in yaml.safe_load_all(manifest_file):
                metadata_namespace = document.get("metadata", {}).get("namespace")
                if metadata_namespace:
                    namespaces.append(metadata_namespace)
                for subject in document.get("subjects", []):
                    if subject.get("kind") == "ServiceAccount":
                        namespaces.append(subject.get("namespace"))

    assert namespaces
    assert set(namespaces) == {DEFAULT_MONITORING_NAMESPACE}
