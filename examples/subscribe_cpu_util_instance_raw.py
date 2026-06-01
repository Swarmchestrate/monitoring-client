import logging
import signal
import time
from swchmonclient import (
    query_metric_values_raw,
    subscribe_metric_raw,
    unsubscribe_metric,
)

METRIC_NAME = "cpu_util_instance"
METRIC_NAME2 = "mean_cpu_util_prct"
NODES = ["100.104.109.71", "100.118.84.34"]
RUN_SECONDS = 180
QUERY_WINDOW_SECONDS = 30
POLL_INTERVAL_SECONDS = 5


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

    # Subscribe to the raw metric for the specified nodes (or "local" or "all")
    # logging.info("Subscribing to raw metric %s for nodes %s", METRIC_NAME, NODES)
    # thread_names = subscribe_metric_raw(METRIC_NAME, NODES)
    # thread_names = subscribe_metric_raw(METRIC_NAME, "local")
    thread_names = subscribe_metric_raw(METRIC_NAME, "all")
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
