import sys
import os
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Set to the path of your kubeconfig file, or None to use ~/.kube/config
KUBECONFIG_PATH = "./k3s.yaml"
# Set to a specific context name, or None to use the current context
CONTEXT = "default"

# Default values for the emsconfig template
EMS_SAT_FILE = "tosca_metrics_ze.yaml"
# EMS_SAT_FILE = "sat-innorenew.yaml"
EMS_OPTIMUSDB_URL = "http://193.225.250.240/optimusdb1/swarmkb"

MANIFESTS = [
    "./manifests/custom-metric-config.yaml",
    "./manifests/emsconfig.yaml",
    "./manifests/ems+netdata-k3s_parametric.yaml",
    "./manifests/stomp-listener.yaml",
    "./manifests/python_manifest.yaml",
]


def main() -> int:
    from swchmonclient.deployer import K8sDeployer
    from swchmonclient.exceptions import DeploymentError
    from swchmonclient.logging_utils import configure_stdout_logger
    from swchmonclient.renderer import render_manifest

    logger = configure_stdout_logger("undeploy_example")
    deployer = K8sDeployer(kubeconfig_path=KUBECONFIG_PATH, context=CONTEXT)
    overall_ok = True

    emsconfig_tmp: str | None = None
    rendered_variables = {
        "sat_file": EMS_SAT_FILE,
        "optimusdb_url": EMS_OPTIMUSDB_URL,
    }
    try:
        emsconfig_tmp = render_manifest(
            "./manifests/emsconfig.yaml",
            **rendered_variables,
        )

        manifests = [
            {
                "display_path": m,
                "actual_path": emsconfig_tmp if m == "./manifests/emsconfig.yaml" else m,
                "variables": rendered_variables if m == "./manifests/emsconfig.yaml" else None,
            }
            for m in MANIFESTS
        ]

        for manifest in manifests:
            manifest_path = manifest["actual_path"]
            display_path = manifest["display_path"]
            variables = manifest["variables"]

            if variables:
                logger.info("Undeploying %s with variables:", display_path)
                for key, value in variables.items():
                    logger.info("    • %s: %s", key, value)
            else:
                logger.info("Undeploying %s ...", display_path)
            try:
                deleted = deployer.destroy_manifest(manifest_path)
                logger.info("  Done. %s resource(s) deleted.", deleted)
            except DeploymentError as error:
                logger.error("  ERROR: %s", error)
                overall_ok = False

        logger.info("Undeploying DaemonSet/ems-client-daemonset ...")
        try:
            deleted = deployer.destroy_app(
                label_selector="app.kubernetes.io/name=ems-client-daemonset",
                kinds=[("apps/v1", "DaemonSet")],
            )
            logger.info("  Done. %s matching daemonset resource(s) deleted.", deleted)
        except DeploymentError as error:
            logger.error("  ERROR: %s", error)
            overall_ok = False
    finally:
        if emsconfig_tmp and os.path.exists(emsconfig_tmp):
            os.unlink(emsconfig_tmp)

    if overall_ok:
        logger.info("All manifests undeployed successfully.")
        return 0
    else:
        logger.info("One or more manifests failed to undeploy.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
