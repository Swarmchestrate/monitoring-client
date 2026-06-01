from swchmonclient import undeploy_monitoring

# Set to the path of your SAT file
EMS_SAT_FILE = "tosca_metrics_ze.yaml"
# Set to the URL of your EMS OptimusDB instance
EMS_OPTIMUSDB_URL = "http://193.225.250.240/optimusdb1/swarmkb"

def main() -> int:
    exit_code = undeploy_monitoring(
        sat_file=EMS_SAT_FILE,
        optimusdb_url=EMS_OPTIMUSDB_URL,
    )

    if exit_code == 0:
        print("Monitoring undeployed successfully.")
    else:
        print(f"Failed to undeploy monitoring. Exit code: {exit_code}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
