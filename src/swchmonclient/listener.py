import logging
import os
import threading
from typing import Callable, Optional

import stomp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class LoggingStompListener(stomp.ConnectionListener):
    """Simple STOMP listener that logs connection and message events."""

    def on_connected(self, frame) -> None:
        logger.info("Connected to ActiveMQ")

    def on_disconnected(self) -> None:
        logger.warning("Disconnected from ActiveMQ")

    def on_error(self, frame) -> None:
        logger.error("Received an error: %s", getattr(frame, "body", frame))

    def on_message(self, frame) -> None:
        destination = getattr(frame, "headers", {}).get("destination", "<unknown>")
        logger.info("Received a message from [%s]: %s", destination, getattr(frame, "body", frame))


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def get_int_env(name: str, default: int) -> int:
    try:
        return int(get_env(name, str(default)))
    except ValueError:
        return default


def create_stomp_connection(host: str, port: int):
    """Create a STOMP connection and attach the default logging listener."""
    conn = stomp.Connection([(host, port)])
    conn.set_listener("", LoggingStompListener())
    return conn


def run_stomp_listener(
    stop_event: Optional[threading.Event] = None,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    destination: Optional[str] = None,
    reconnect_delay: Optional[int] = None,
    max_reconnect_delay: Optional[int] = None,
    connection_factory: Optional[Callable[[str, int], object]] = None,
) -> None:
    """Run a reconnecting STOMP listener until the provided stop event is set."""
    resolved_stop_event = stop_event or threading.Event()
    resolved_host = host or get_env("STOMP_HOST", "emsserver-ems-server")
    resolved_port = port if port is not None else get_int_env("STOMP_PORT", 61610)
    resolved_username = username if username is not None else get_env("STOMP_USERNAME", "")
    resolved_password = password if password is not None else get_env("STOMP_PASSWORD", "")
    resolved_destination = destination or get_env("STOMP_DESTINATION", "/topic/>")
    resolved_reconnect_delay = (
        reconnect_delay if reconnect_delay is not None else get_int_env("STOMP_RECONNECT_DELAY", 5)
    )
    resolved_max_reconnect_delay = (
        max_reconnect_delay
        if max_reconnect_delay is not None
        else get_int_env("STOMP_MAX_RECONNECT_DELAY", max(resolved_reconnect_delay, 60))
    )
    current_reconnect_delay = max(0, resolved_reconnect_delay)
    resolved_max_reconnect_delay = max(current_reconnect_delay, resolved_max_reconnect_delay)

    logger.info(
        "Starting listener for [%s] on %s:%s",
        resolved_destination,
        resolved_host,
        resolved_port,
    )

    factory = connection_factory or create_stomp_connection

    while not resolved_stop_event.is_set():
        conn = None
        should_backoff = False
        connection_established = False
        try:
            conn = factory(resolved_host, resolved_port)
            conn.connect(login=resolved_username, passcode=resolved_password, wait=True)
            connection_established = True
            conn.subscribe(destination=resolved_destination, id=1, ack="auto")
            logger.info("Subscription created successfully")
            current_reconnect_delay = max(0, resolved_reconnect_delay)

            while not resolved_stop_event.is_set():
                is_connected = getattr(conn, "is_connected", None)
                if callable(is_connected) and not is_connected():
                    break
                resolved_stop_event.wait(0.2)

            if resolved_stop_event.is_set():
                logger.info("Shutdown requested. Closing connection...")
                break

            should_backoff = True
            logger.warning(
                "Connection lost. Reconnecting in %s seconds...",
                current_reconnect_delay,
            )
        except Exception:
            if resolved_stop_event.is_set():
                break
            should_backoff = True
            logger.exception(
                "Connection failed. Retrying in %s seconds...",
                current_reconnect_delay,
            )
        finally:
            if conn is not None:
                try:
                    disconnect = getattr(conn, "disconnect", None)
                    if callable(disconnect):
                        disconnect()
                        if connection_established:
                            logger.info("Connection cleanup completed")
                        else:
                            logger.debug("No active connection to clean up")
                except Exception:
                    logger.debug("Disconnect cleanup failed", exc_info=True)

        if not resolved_stop_event.is_set():
            resolved_stop_event.wait(current_reconnect_delay)
            if should_backoff and current_reconnect_delay > 0:
                current_reconnect_delay = min(current_reconnect_delay * 2, resolved_max_reconnect_delay)
