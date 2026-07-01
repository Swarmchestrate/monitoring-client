from swchmonclient import undeploy_monitoring

def main() -> int:
    exit_code = undeploy_monitoring()

    if exit_code == 0:
        print("Monitoring undeployed successfully.")
    else:
        print(f"Failed to undeploy monitoring. Exit code: {exit_code}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
