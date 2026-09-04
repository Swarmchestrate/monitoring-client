import contextlib
import io
from unittest.mock import call, patch

from examples import deploy
from swchmonclient import deploy_monitoring
from swchmonclient.deployer import DEFAULT_MONITORING_NAMESPACE, DEFAULT_OPTIMUSDB_URL


def test_deploy_shows_manifest_name_variables_and_resources():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls, \
        patch("swchmonclient.deployer._ensure_monitoring_manifests"), \
        patch("swchmonclient.deployer._load_sat_file", return_value=("stressng.yaml", "topology: demo\n")), \
        patch(
            "swchmonclient.deployer._build_unique_sat_filename",
            return_value="stressng-20260629123045123456.yaml",
        ), \
        patch("swchmonclient.deployer._upload_sat_to_kb") as mock_upload_sat_to_kb, \
        patch(
            "swchmonclient.deployer._render_tosca_model_configmap_manifest",
            return_value="/tmp/tosca-model-configmap.yaml",
        ), \
        patch("swchmonclient.renderer.render_manifest", return_value="/tmp/rendered.yaml"), \
        patch("os.path.exists", return_value=False):
        mock_deployer = mock_deployer_cls.return_value
        mock_deployer.deploy_manifest.side_effect = [
            [
                {
                    "kind": "ConfigMap",
                    "name": "tosca-model-configmap",
                    "apiVersion": "v1",
                    "namespace": DEFAULT_MONITORING_NAMESPACE,
                }
            ],
            [
                {
                    "kind": "ConfigMap",
                    "name": "emsconfig",
                    "apiVersion": "v1",
                    "namespace": DEFAULT_MONITORING_NAMESPACE,
                }
            ],
            [
                {
                    "kind": "Deployment",
                    "name": "demo",
                    "apiVersion": "apps/v1",
                    "namespace": DEFAULT_MONITORING_NAMESPACE,
                }
            ],
        ]

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = deploy.main()

    printed = output.getvalue()

    assert exit_code == 0
    if deploy.UPLOAD_KB:
        mock_upload_sat_to_kb.assert_called_once()
    else:
        mock_upload_sat_to_kb.assert_not_called()
    mock_deployer.deploy_manifest.assert_has_calls(
        [
            call(
                "/tmp/tosca-model-configmap.yaml",
                namespace=DEFAULT_MONITORING_NAMESPACE,
            ),
            call("/tmp/rendered.yaml", namespace=DEFAULT_MONITORING_NAMESPACE),
            call(
                "./manifests/ems+netdata-k3s_parametric.yaml",
                namespace=DEFAULT_MONITORING_NAMESPACE,
            ),
        ]
    )
    assert f"Deploying ConfigMap/tosca-model-configmap from SAT file {deploy.SAT_FILE} ..." in printed
    assert "Deploying ./manifests/emsconfig.yaml with variables:" in printed
    assert "    • sat_file: stressng-20260629123045123456.yaml" in printed
    assert f"    • optimusdb_url: {DEFAULT_OPTIMUSDB_URL}" in printed
    assert f"    • use_kb: {deploy.USE_KB}" in printed
    assert f"    • upload_kb: {deploy.UPLOAD_KB}" in printed
    assert "Created or patched resources:" in printed
    assert (
        f"ConfigMap/tosca-model-configmap (apiVersion=v1, namespace={DEFAULT_MONITORING_NAMESPACE})"
        in printed
    )
    assert f"ConfigMap/emsconfig (apiVersion=v1, namespace={DEFAULT_MONITORING_NAMESPACE})" in printed
    assert f"Deployment/demo (apiVersion=apps/v1, namespace={DEFAULT_MONITORING_NAMESPACE})" in printed


def test_deploy_skips_kb_upload_when_use_kb_is_false():
    with patch("swchmonclient.deployer.K8sDeployer") as mock_deployer_cls, \
        patch("swchmonclient.deployer._ensure_monitoring_manifests"), \
        patch("swchmonclient.deployer._load_sat_file", return_value=("stressng.yaml", "topology: demo\n")), \
        patch(
            "swchmonclient.deployer._build_unique_sat_filename",
            return_value="stressng-20260629123045123456.yaml",
        ), \
        patch("swchmonclient.deployer._upload_sat_to_kb") as mock_upload_sat_to_kb, \
        patch(
            "swchmonclient.deployer._render_tosca_model_configmap_manifest",
            return_value="/tmp/tosca-model-configmap.yaml",
        ), \
        patch("swchmonclient.renderer.render_manifest", return_value="/tmp/rendered.yaml"), \
        patch("os.path.exists", return_value=False):
        mock_deployer = mock_deployer_cls.return_value
        mock_deployer.deploy_manifest.side_effect = [
            [
                {
                    "kind": "ConfigMap",
                    "name": "tosca-model-configmap",
                    "apiVersion": "v1",
                    "namespace": "default",
                }
            ],
            [
                {
                    "kind": "ConfigMap",
                    "name": "emsconfig",
                    "apiVersion": "v1",
                    "namespace": "default",
                }
            ],
            [
                {
                    "kind": "Deployment",
                    "name": "demo",
                    "apiVersion": "apps/v1",
                    "namespace": "default",
                }
            ],
        ]

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = deploy_monitoring(
                sat_file="/tmp/stressng.yaml",
                use_kb=False,
                upload_kb=True,
            )

    printed = output.getvalue()

    assert exit_code == 0
    mock_upload_sat_to_kb.assert_not_called()
    assert (
        "upload_kb=True is ignored because use_kb=False; skipping knowledge base upload."
        in printed
    )
