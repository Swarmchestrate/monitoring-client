import sys
import os
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from swchmonclient.deployer import K8sDeployer
from swchmonclient.exceptions import DeploymentError
from swchmonclient.renderer import render_manifest

# Set to the path of your kubeconfig file, or None to use ~/.kube/config
KUBECONFIG_PATH = "./k3s.yaml"
# Set to a specific context name, or None to use the current context
CONTEXT = "default"

# Default values for the emsconfig template
EMS_SAT_FILE = "SAT-test-ra_20260413_154456.152"
EMS_OPTIMUSDB_URL = "http://193.225.250.240/optimusdb1/swarmkb"

MANIFESTS = [
    "./manifests/emsconfig.yaml",
    "./manifests/ems+netdata-k3s_parametric.yaml",
]

    # "./manifests/custom-metric-config.yaml",
    # "./manifests/stomp-listener.yaml",
    # "./manifests_wip/python_manifest.yaml",

def main() -> int:
    deployer = K8sDeployer(kubeconfig_path=KUBECONFIG_PATH, context=CONTEXT)
    overall_ok = True

    emsconfig_tmp: str | None = None
    try:
        emsconfig_tmp = render_manifest(
            "./manifests/emsconfig.yaml",
            sat_file=EMS_SAT_FILE,
            optimusdb_url=EMS_OPTIMUSDB_URL,
        )

        manifests = [
            emsconfig_tmp if m == "./manifests/emsconfig.yaml" else m
            for m in MANIFESTS
        ]

        for manifest_path in manifests:
            print(f"\nDeploying {manifest_path} ...")
            try:
                deployed = deployer.deploy_manifest(manifest_path)
            except DeploymentError as error:
                print(f"  ERROR: {error}")
                overall_ok = False
                continue

            if not deployed:
                print("  No valid Kubernetes resources found in the manifest.")
                continue

            print("  Created or patched resources:")
            for resource in deployed:
                namespace = resource.get("namespace") or "<cluster-scoped>"
                print(
                    f"  - {resource['kind']}/{resource['name']} "
                    f"(apiVersion={resource['apiVersion']}, namespace={namespace})"
                )
    finally:
        if emsconfig_tmp and os.path.exists(emsconfig_tmp):
            os.unlink(emsconfig_tmp)

    if overall_ok:
        print("\nAll manifests deployed successfully.")
        return 0
    else:
        print("\nOne or more manifests failed to deploy.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
