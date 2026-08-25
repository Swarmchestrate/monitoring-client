from unittest.mock import call, patch

from swchmonclient import undeploy_monitoring


def test_undeploy_monitoring_uses_requested_namespace():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls, \
        patch("swchmonclient.deployer._ensure_monitoring_manifests"):
        mock_deployer = mock_deployer_cls.return_value
        mock_deployer.destroy_manifest.return_value = 1
        mock_deployer.destroy_resource.return_value = 1

        exit_code = undeploy_monitoring(namespace="monitoring")

    assert exit_code == 0
    mock_deployer.destroy_manifest.assert_has_calls(
        [
            call("./manifests/ems+netdata-k3s_parametric.yaml", namespace="monitoring"),
        ]
    )
    mock_deployer.destroy_resource.assert_has_calls(
        [
            call(
                api_version="v1",
                kind="ConfigMap",
                name="emsconfig",
                namespace="monitoring",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="tosca-model-configmap",
                namespace="monitoring",
            ),
            call(
                api_version="apps/v1",
                kind="DaemonSet",
                name="ems-client-daemonset",
                namespace="monitoring",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="ems-client-configmap",
                namespace="monitoring",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="monitoring-configmap",
                namespace="monitoring",
            ),
        ]
    )
