import logging
import signal
import time
from pathlib import Path

from swchmonclient import (
    query_metric_values_raw,
    subscribe_metric_raw,
    unsubscribe_metric,
)

METRIC_NAME = "cpu_util_prct"
NODES = ["100.104.109.71", "100.118.84.34"]
RUN_SECONDS = 180
QUERY_WINDOW_SECONDS = 30
POLL_INTERVAL_SECONDS = 5
RAW_METRICS_FILE = Path(__file__).with_name("raw_metrics.json")
CLUSTER_RAW_METRICS_FILE = Path(__file__).with_name("raw_metrics_10_nodes.json")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
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

    # Option 1: Subscribe to the raw metric for specific nodes.
    # Use this when you want metric data only from the nodes listed in the NODES variable.
    # thread_names = subscribe_metric_raw(METRIC_NAME, NODES)

    # Option 2: Subscribe to the raw metric for the local node only.
    # Use this when you want metric data only from the node running this code.
    # thread_names = subscribe_metric_raw(METRIC_NAME, "local")

    # Option 3: Subscribe to the raw metric for all nodes.
    # Use this when you want metric data from every available node.
    # thread_names = subscribe_metric_raw(METRIC_NAME, "all")

    # Option 4: Replay raw metrics for all node_id values in a JSON source file.
    # This does not connect to the monitoring system.
    # thread_names = subscribe_metric_raw(
    #     METRIC_NAME,
    #     source_file=RAW_METRICS_FILE,
    # )

    # Option 5: Map up to ten unique file profiles onto current cluster nodes.
    # This requires Kubernetes permission to list nodes and returns real node IPs.
    thread_names = subscribe_metric_raw(
        METRIC_NAME,
        "cluster",
        source_file=CLUSTER_RAW_METRICS_FILE,
    )

    logging.info("Started raw listener threads: %s", thread_names)

    started_at = time.time()
    try:
        while not stopping and (time.time() - started_at) < RUN_SECONDS:
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
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        logging.info("Unsubscribing from raw metric %s", METRIC_NAME)
        unsubscribe_metric(METRIC_NAME)
        logging.info("Raw metric subscription stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
