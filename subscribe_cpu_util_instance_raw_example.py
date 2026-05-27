import logging
import signal
import sys
import time
from pathlib import Path

from swchmonclient.deployer import K8sDeployer

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from swchmonclient import query_metric_values_raw, subscribe_metric_raw, unsubscribe_metric

METRIC_NAME = "cpu_util_instance"
NODES = ["100.104.109.71", "100.118.84.34"]
RUN_SECONDS = 180
QUERY_WINDOW_SECONDS = 30
POLL_INTERVAL_SECONDS = 5


def main() -> int:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        if not stopping:
            logging.info("Stop requested, shutting down raw metric subscription...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logging.info("Subscribing to raw metric %s for nodes %s", METRIC_NAME, NODES)
    thread_names = subscribe_metric_raw(METRIC_NAME, NODES)
    logging.info("Started raw listener threads: %s", thread_names)

    # started_at = time.time()
    # try:
    #     while not stopping and (time.time() - started_at) < RUN_SECONDS:
    #         values_by_node = query_metric_values_raw(
    #             METRIC_NAME,
    #             QUERY_WINDOW_SECONDS,
    #         )
    #         logging.info(
    #             "Buffered raw values for %s from the last %s seconds: %s",
    #             METRIC_NAME,
    #             QUERY_WINDOW_SECONDS,
    #             values_by_node,
    #         )
    #         time.sleep(POLL_INTERVAL_SECONDS)
    # finally:
    #     logging.info("Unsubscribing from raw metric %s", METRIC_NAME)
    #     unsubscribe_metric(METRIC_NAME)
    #     logging.info("Raw metric subscription stopped")

    print(K8sDeployer().get_vm_private_ips())

    time.sleep(100)
    values_by_node = query_metric_values_raw(
        METRIC_NAME,
        QUERY_WINDOW_SECONDS,
    )
    logging.info(
        "Buffered raw values for %s from the last %s seconds: %s",
        METRIC_NAME,
        QUERY_WINDOW_SECONDS,
        values_by_node,
    )
    unsubscribe_metric(METRIC_NAME)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
