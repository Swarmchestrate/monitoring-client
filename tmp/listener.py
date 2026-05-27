import logging
import os
import signal

from threading import Event

import stomp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)
stop_event = Event()


def handle_shutdown(signum, _frame):
    logger.info("Shutdown signal received (%s). Stopping listener...", signum)
    stop_event.set()


def get_env(name, default):
    value = os.getenv(name)
    return value if value not in (None, '') else default


def get_int_env(name, default):
    try:
        return int(get_env(name, str(default)))
    except ValueError:
        return default


class MyListener(stomp.ConnectionListener):
    def on_connected(self, frame):
        logger.info("Connected to ActiveMQ")

    def on_disconnected(self):
        logger.warning("Disconnected from ActiveMQ")

    def on_error(self, frame):
        logger.error("Received an error: %s", frame.body)

    def on_message(self, frame):
        destination = frame.headers.get('destination', '<unknown>')
        logger.info("Received a message from [%s]: %s", destination, frame.body)


# Broker details
host = get_env('STOMP_HOST', 'emsserver-ems-server')
port = get_int_env('STOMP_PORT', 61610)
username = get_env('STOMP_USERNAME', '')
password = get_env('STOMP_PASSWORD', '')
destination = get_env('STOMP_DESTINATION', '/topic/>')
reconnect_delay = get_int_env('STOMP_RECONNECT_DELAY', 5)


def create_connection():
    conn = stomp.Connection([(host, port)])
    conn.set_listener('', MyListener())
    return conn


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(
        "Starting listener for [%s] on %s:%s",
        destination,
        host,
        port,
    )

    while not stop_event.is_set():
        conn = None
        try:
            conn = create_connection()
            conn.connect(login=username, passcode=password, wait=True)
            conn.subscribe(destination=destination, id=1, ack='auto')
            logger.info("Subscription created successfully")

            while conn.is_connected() and not stop_event.wait(20):
                pass

            if stop_event.is_set():
                logger.info("Shutdown requested. Closing connection...")
                break

            logger.warning(
                "Connection lost. Reconnecting in %s seconds...",
                reconnect_delay,
            )
        except KeyboardInterrupt:
            logger.info("Listener interrupted by user")
            stop_event.set()
        except Exception:
            logger.exception(
                "Connection failed. Retrying in %s seconds...",
                reconnect_delay,
            )
        finally:
            if conn and conn.is_connected():
                try:
                    conn.disconnect()
                    logger.info("Disconnected cleanly")
                except Exception:
                    logger.debug("Disconnect cleanup failed", exc_info=True)

        if not stop_event.is_set():
            stop_event.wait(reconnect_delay)


if __name__ == '__main__':
    main()
