import contextlib
import io
from unittest.mock import call, patch

from examples import undeploy


def test_undeploy_removes_named_cleanup_resources():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls, \
        patch("swchmonclient.deployer._ensure_monitoring_manifests"):
        mock_deployer = mock_deployer_cls.return_value
        mock_deployer.destroy_manifest.return_value = 1
        mock_deployer.destroy_resource.return_value = 1

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = undeploy.main()

    printed = output.getvalue()

    assert exit_code == 0
    mock_deployer.destroy_resource.assert_has_calls(
        [
            call(
                api_version="v1",
                kind="ConfigMap",
                name="emsconfig",
                namespace="default",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="tosca-model-configmap",
                namespace="default",
            ),
            call(
                api_version="apps/v1",
                kind="DaemonSet",
                name="ems-client-daemonset",
                namespace="default",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="ems-client-configmap",
                namespace="default",
            ),
            call(
                api_version="v1",
                kind="ConfigMap",
                name="monitoring-configmap",
                namespace="default",
            ),
        ]
    )
    assert mock_deployer.destroy_resource.call_count == 5
    assert "Undeploying ConfigMap/emsconfig ..." in printed
    assert "Done. 1 configmap resource(s) deleted." in printed
    assert "Undeploying ConfigMap/tosca-model-configmap ..." in printed
    assert "Done. 1 configmap resource(s) deleted." in printed
    assert "Undeploying DaemonSet/ems-client-daemonset ..." in printed
    assert "Done. 1 daemonset resource(s) deleted." in printed
    assert "Undeploying ConfigMap/ems-client-configmap ..." in printed
    assert "Done. 1 configmap resource(s) deleted." in printed
    assert "Undeploying ConfigMap/monitoring-configmap ..." in printed
