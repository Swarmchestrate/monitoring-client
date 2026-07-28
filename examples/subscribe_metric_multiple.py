import logging
import signal
import time

from swchmonclient import query_metric_values, subscribe_metric, unsubscribe_metric

METRIC_NAMES = [
    "cpu_util_prct",
    "ram_util_prct",
    "avg_disk_utilization",
]
POLL_INTERVAL_SECONDS = 30


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        if not stopping:
            logging.info("Stop requested, shutting down metric subscriptions...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    subscribed: list[str] = []
    try:
        for metric in METRIC_NAMES:
            logging.info("Subscribing to metric: %s", metric)
            subscribe_metric(metric)
            subscribed.append(metric)

        while not stopping:
            for metric in METRIC_NAMES:
                values = query_metric_values(metric)
                logging.info("Latest buffered value for %s: %s", metric, values)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for metric in subscribed:
            logging.info("Unsubscribing from metric %s", metric)
            try:
                unsubscribe_metric(metric)
            except Exception:
                logging.exception("Failed to unsubscribe from %s", metric)
        logging.info("Metric subscriptions stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())