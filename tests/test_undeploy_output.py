import contextlib
import io
from unittest.mock import patch

from examples import undeploy


def test_undeploy_shows_original_manifest_name_and_variables():
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
    assert "Undeploying ./manifests/ems+netdata-k3s_parametric.yaml ..." in printed
    assert "Undeploying ConfigMap/emsconfig ..." in printed
    assert "Undeploying ConfigMap/tosca-model-configmap ..." in printed
