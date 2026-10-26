import logging
import signal
import time

from swchmonclient import query_slo_violations, subscribe_slo, unsubscribe_slo

SLO_NAME = "cpu_utilization"
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
            logging.info("Stop requested, shutting down SLO subscription...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logging.info("Subscribing to SLO violations: %s", SLO_NAME)
    subscribe_slo(SLO_NAME)

    try:
        while not stopping:
            violations = query_slo_violations(SLO_NAME)
            logging.info(
                "Buffered violations for %s: %s",
                SLO_NAME,
                violations,
            )
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        logging.info("Unsubscribing from SLO %s", SLO_NAME)
        unsubscribe_slo(SLO_NAME)
        logging.info("SLO subscription stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
