from pathlib import Path

from swchmonclient import deploy_monitoring

# Set to the path of your SAT file
SAT_FILE = str(Path(__file__).resolve().parents[1] / "manifests" / "stressng.yaml")
# Set to False to disable knowledge base mode in the rendered EMS config
USE_KB = False
# Set to True to upload the SAT file into the KB before deployment
UPLOAD_KB = False
# Namespace in which the monitoring stack will be deployed
NAMESPACE = "swarm-system"

def main() -> int:
    exit_code = deploy_monitoring(
        sat_file=SAT_FILE,
        use_kb=USE_KB,
        upload_kb=UPLOAD_KB,
        namespace=NAMESPACE,
    )

    if exit_code == 0:
        print("Monitoring deployed successfully.")
    else:
        print(f"Failed to deploy monitoring. Exit code: {exit_code}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
