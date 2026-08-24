from types import SimpleNamespace
from unittest.mock import patch

import pytest

from swchmonclient.deployer import K8sDeployer, get_current_vm_private_ip, get_vm_private_ips
from swchmonclient.exceptions import DeploymentError


def test_k8s_deployer_get_vm_private_ips_returns_internal_ips_without_duplicates():
    deployer = object.__new__(K8sDeployer)
    deployer.core_v1_api = SimpleNamespace(
        list_node=lambda: SimpleNamespace(
            items=[
                SimpleNamespace(
                    status=SimpleNamespace(
                        addresses=[
                            SimpleNamespace(type="Hostname", address="worker-1"),
                            SimpleNamespace(type="InternalIP", address="10.0.0.11"),
                            SimpleNamespace(type="ExternalIP", address="34.1.1.1"),
                        ]
                    )
                ),
                SimpleNamespace(
                    status=SimpleNamespace(
                        addresses=[
                            SimpleNamespace(type="InternalIP", address="10.0.0.12"),
                        ]
                    )
                ),
                SimpleNamespace(
                    status=SimpleNamespace(
                        addresses=[
                            SimpleNamespace(type="InternalIP", address="10.0.0.11"),
                        ]
                    )
                ),
            ]
        )
    )

    assert deployer.get_vm_private_ips() == ["10.0.0.11", "10.0.0.12"]


def test_k8s_deployer_get_vm_private_ips_wraps_kubernetes_errors():
    deployer = object.__new__(K8sDeployer)
    deployer.core_v1_api = SimpleNamespace(
        list_node=lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(DeploymentError, match="Failed to list Kubernetes nodes: boom"):
        deployer.get_vm_private_ips()


def test_get_vm_private_ips_helper_uses_k8s_deployer():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls:
        mock_deployer_cls.return_value.get_vm_private_ips.return_value = [
            "10.0.0.11",
            "10.0.0.12",
        ]

        result = get_vm_private_ips()

    assert result == ["10.0.0.11", "10.0.0.12"]
    mock_deployer_cls.assert_called_once_with()


def test_k8s_deployer_get_current_vm_private_ip_reads_current_pod_node_ip(monkeypatch, tmp_path):
    namespace_path = tmp_path / "namespace"
    namespace_path.write_text("monitoring\n")
    monkeypatch.setattr("swchmonclient.deployer.SERVICEACCOUNT_NAMESPACE_PATH", str(namespace_path))
    monkeypatch.setenv("HOSTNAME", "python-shell")

    deployer = object.__new__(K8sDeployer)
    deployer.core_v1_api = SimpleNamespace(
        read_namespaced_pod=lambda name, namespace: SimpleNamespace(
            spec=SimpleNamespace(node_name="worker-1")
        ),
        read_node=lambda name: SimpleNamespace(
            status=SimpleNamespace(
                addresses=[
                    SimpleNamespace(type="Hostname", address="worker-1"),
                    SimpleNamespace(type="InternalIP", address="10.0.0.21"),
                ]
            )
        ),
    )

    assert deployer.get_current_vm_private_ip() == "10.0.0.21"


def test_k8s_deployer_get_current_vm_private_ip_wraps_lookup_errors(monkeypatch, tmp_path):
    namespace_path = tmp_path / "namespace"
    namespace_path.write_text("monitoring\n")
    monkeypatch.setattr("swchmonclient.deployer.SERVICEACCOUNT_NAMESPACE_PATH", str(namespace_path))
    monkeypatch.setenv("HOSTNAME", "python-shell")

    deployer = object.__new__(K8sDeployer)
    deployer.core_v1_api = SimpleNamespace(
        read_namespaced_pod=lambda name, namespace: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(DeploymentError, match="Failed to determine current Kubernetes node IP: boom"):
        deployer.get_current_vm_private_ip()


def test_get_current_vm_private_ip_helper_uses_k8s_deployer():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls:
        mock_deployer_cls.return_value.get_current_vm_private_ip.return_value = "10.0.0.21"

        result = get_current_vm_private_ip()

    assert result == "10.0.0.21"
    mock_deployer_cls.assert_called_once_with()


def test_k8s_deployer_load_config_uses_incluster_config(monkeypatch):
    incluster_calls = []

    monkeypatch.setattr(
        "swchmonclient.deployer.config.load_incluster_config",
        lambda: incluster_calls.append(True),
    )
    monkeypatch.setattr("swchmonclient.deployer.ApiClient", lambda: object())
    monkeypatch.setattr("swchmonclient.deployer.CoreV1Api", lambda api_client: object())
    monkeypatch.setattr(
        "swchmonclient.deployer.dynamic.DynamicClient",
        lambda api_client: object(),
    )

    K8sDeployer()

    assert incluster_calls == [True]


def test_k8s_deployer_load_config_wraps_incluster_failures(monkeypatch):
    monkeypatch.setattr(
        "swchmonclient.deployer.config.load_incluster_config",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(DeploymentError, match="Unable to load in-cluster Kubernetes configuration"):
        K8sDeployer()
