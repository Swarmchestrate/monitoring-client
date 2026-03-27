import sys
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from k8slib.deployer import K8sDeployer
from k8slib.exceptions import DeploymentError

# Set to the path of your kubeconfig file, or None to use ~/.kube/config
KUBECONFIG_PATH = "./k3s.yaml"
# Set to a specific context name, or None to use the current context
CONTEXT = "default"

MANIFESTS = [
    "./manifest/custom-metric-config.yaml",
    "./manifest/emsconfig.yaml",
    "./manifest/ems+netdata-k3s_parametric.yaml",
    "./manifest/stomp-listener.yaml",
    "./manifest/python_manifest.yaml",
]


def main() -> int:
    deployer = K8sDeployer(kubeconfig_path=KUBECONFIG_PATH, context=CONTEXT)
    overall_ok = True

    for manifest_path in MANIFESTS:
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

    if overall_ok:
        print("\nAll manifests deployed successfully.")
        return 0
    else:
        print("\nOne or more manifests failed to deploy.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
