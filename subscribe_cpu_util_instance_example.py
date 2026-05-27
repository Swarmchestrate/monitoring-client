import logging
import signal
import sys
import time
from pathlib import Path

# Allow running this script directly from the repo root without installing the package.
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from swchmonclient import query_metric_values, subscribe_metric, unsubscribe_metric

METRIC_NAME = "cpu_util_instance"
METRIC_NAME2 = "mean_cpu_util_prct"
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
            logging.info("Stop requested, shutting down metric subscription...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logging.info("Subscribing to metric %s", METRIC_NAME)
    subscribe_metric(METRIC_NAME)
    subscribe_metric(METRIC_NAME2)

    started_at = time.time()
    try:
        while not stopping and (time.time() - started_at) < RUN_SECONDS:
            values = query_metric_values(METRIC_NAME, QUERY_WINDOW_SECONDS)
            logging.info(
                "Latest buffered value for %s from the last %s seconds: %s",
                METRIC_NAME,
                QUERY_WINDOW_SECONDS,
                values,
            )
            values2 = query_metric_values(METRIC_NAME2, QUERY_WINDOW_SECONDS)
            logging.info(
                "Latest buffered value for %s from the last %s seconds: %s",
                METRIC_NAME2,
                QUERY_WINDOW_SECONDS,
                values2,
            )
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        logging.info("Unsubscribing from metric %s", METRIC_NAME)
        unsubscribe_metric(METRIC_NAME)
        logging.info("Unsubscribing from metric %s", METRIC_NAME2)
        unsubscribe_metric(METRIC_NAME2)
        logging.info("Metric subscription stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
