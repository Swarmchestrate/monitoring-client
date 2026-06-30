import logging
import time

from swchmonclient import query_metric_values, subscribe_metric, unsubscribe_metric

METRIC_NAME = "cpu_util_prct"
RUN_SECONDS = 180
QUERY_WINDOW_SECONDS = 30
POLL_INTERVAL_SECONDS = 5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logging.info("Subscribing to metric: %s", METRIC_NAME)
subscribe_metric(METRIC_NAME)

started_at = time.time()
try:
    while (time.time() - started_at) < RUN_SECONDS:
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
