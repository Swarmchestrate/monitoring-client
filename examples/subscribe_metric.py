import logging
import signal
import time

from swchmonclient import query_metric_values, subscribe_metric, unsubscribe_metric

METRIC_NAME = "cpu_util_prct"
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
            logging.info("Stop requested, shutting down metric subscription...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logging.info("Subscribing to metric: %s", METRIC_NAME)
    subscribe_metric(METRIC_NAME)

    try:
        while not stopping:
            values = query_metric_values(METRIC_NAME)
            logging.info(
                "Latest buffered value for %s: %s",
                METRIC_NAME,
                values,
            )
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        logging.info("Unsubscribing from metric %s", METRIC_NAME)
        unsubscribe_metric(METRIC_NAME)
        logging.info("Metric subscription stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
