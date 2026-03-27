import argparse
import sys
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from k8slib.deployer import K8sDeployer
from k8slib.exceptions import DeploymentError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit a Kubernetes manifest using k8s-lifecycle deployer.")
    parser.add_argument("manifest", help="Path to manifest YAML file")
    parser.add_argument("--namespace", default=None, help="Override namespace for namespaced resources")
    parser.add_argument("--kubeconfig", default=None, help="Path to kubeconfig file")
    parser.add_argument("--context", default=None, help="Kubeconfig context name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    deployer = K8sDeployer(
        kubeconfig_path=args.kubeconfig,
        context=args.context,
    )

    try:
        deployed = deployer.deploy_manifest(args.manifest, namespace=args.namespace)
    except DeploymentError as error:
        print(f"Deployment failed: {error}")
        return 1

    if not deployed:
        print("No valid Kubernetes resources found in the manifest.")
        return 0

    print("Deployment submitted successfully.")
    print("Created or patched resources:")
    for resource in deployed:
        namespace = resource.get("namespace") or "<cluster-scoped>"
        print(
            f"- {resource['kind']}/{resource['name']} "
            f"(apiVersion={resource['apiVersion']}, namespace={namespace})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
