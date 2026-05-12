import sys
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Set to the path of your kubeconfig file, or None to use ~/.kube/config
KUBECONFIG_PATH = "./k3s.yaml"
# Default values for the emsconfig template
EMS_SAT_FILE = "tosca_metrics_ze.yaml"
# EMS_SAT_FILE = "sat-innorenew.yaml"
EMS_OPTIMUSDB_URL = "http://193.225.250.240/optimusdb1/swarmkb"


def main() -> int:
    from swchmonclient import undeploy_monitoring

    return undeploy_monitoring(
        kubeconfig_path=KUBECONFIG_PATH,
        sat_file=EMS_SAT_FILE,
        optimusdb_url=EMS_OPTIMUSDB_URL,
    )


if __name__ == "__main__":
    raise SystemExit(main())
