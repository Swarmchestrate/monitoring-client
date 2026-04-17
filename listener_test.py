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

from swchmonclient import stop_listener_thread, start_listener_thread

LISTENER_NAME = "demo-listener"
RUN_SECONDS = 180


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        if not stopping:
            logging.info("Stop requested, shutting down listener thread...")
            stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logging.info("Starting listener thread for up to %s seconds", RUN_SECONDS)
    start_listener_thread(name=LISTENER_NAME)

    started_at = time.time()
    try:
        while not stopping and (time.time() - started_at) < RUN_SECONDS:
            time.sleep(1)
    finally:
        logging.info("Stopping listener thread...")
        stop_listener_thread(name=LISTENER_NAME, timeout=10.0)
        logging.info("Listener thread stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
