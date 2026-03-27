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
    parser = argparse.ArgumentParser(
        description="Remove a Kubernetes application using k8s-lifecycle deployer.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--manifest",
        metavar="FILE",
        help="Path to the manifest YAML file.\n"
             "Deletes exactly the resources listed in the file.",
    )
    mode.add_argument(
        "--selector",
        metavar="LABEL_SELECTOR",
        help="Label selector to match resources for deletion (e.g. app=my-app).\n"
             "Deletes all matching Deployments, StatefulSets, DaemonSets, Jobs,\n"
             "Services, Pods, ConfigMaps, and Secrets in the given namespace.",
    )

    parser.add_argument("--namespace", default="default", help="Namespace to target (default: default)")
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
        if args.manifest:
            print(f"Destroying resources from manifest: {args.manifest}")
            deleted = deployer.destroy_manifest(args.manifest, namespace=args.namespace)
            print(f"Done. {deleted} resource(s) deleted.")
        else:
            print(f"Destroying resources matching selector '{args.selector}' in namespace '{args.namespace}'...")
            deleted = deployer.destroy_app(args.selector, namespace=args.namespace)
            print(f"Done. {deleted} resource(s) deleted.")
    except DeploymentError as error:
        print(f"Undeploy failed: {error}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
