import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Callable

import stomp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _force_disconnect_connection(connection: object) -> None:
    transport = getattr(connection, "transport", None)
    disconnect_socket = getattr(transport, "disconnect_socket", None)
    if callable(disconnect_socket):
        disconnect_socket()
        return

    disconnect = getattr(connection, "disconnect", None)
    if callable(disconnect):
        disconnect()


@dataclass
class StompConnectionController:
    current_connection: object | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def set_connection(self, connection: object | None) -> None:
        with self._lock:
            self.current_connection = connection

    def clear_connection(self, connection: object) -> None:
        with self._lock:
            if self.current_connection is connection:
                self.current_connection = None

    def disconnect_active(self) -> None:
        with self._lock:
            connection = self.current_connection

        if connection is None:
            return

        _force_disconnect_connection(connection)


class LoggingStompListener(stomp.ConnectionListener):
    """Simple STOMP listener that logs connection and message events."""

    def on_connected(self, frame) -> None:
        logger.debug("Connected to ActiveMQ")

    def on_disconnected(self) -> None:
        logger.warning("Disconnected from ActiveMQ")

    def on_error(self, frame) -> None:
        logger.error("Received an error: %s", getattr(frame, "body", frame))

    def on_message(self, frame) -> None:
        destination = getattr(frame, "headers", {}).get("destination", "<unknown>")
        logger.debug("Received a message from [%s]: %s", destination, getattr(frame, "body", frame))


class CallbackStompListener(LoggingStompListener):
    """STOMP listener that logs messages and forwards frames to a callback."""

    def __init__(self, on_message_callback: Callable[[object], None]) -> None:
        self._on_message_callback = on_message_callback

    def on_message(self, frame) -> None:
        super().on_message(frame)
        self._on_message_callback(frame)


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


def _resolve_destinations(
    destination: str,
    destinations_provider: Callable[[], set[str]] | None,
) -> set[str]:
    if destinations_provider is None:
        return {destination}
    return {resolved for resolved in destinations_provider() if resolved}


def _sync_subscriptions(
    conn: object,
    desired_destinations: set[str],
    active_subscriptions: dict[str, int],
    next_subscription_id: int,
) -> int:
    unsubscribe = getattr(conn, "unsubscribe", None)
    for destination in sorted(active_subscriptions.keys() - desired_destinations):
        subscription_id = active_subscriptions.pop(destination)
        if callable(unsubscribe):
            unsubscribe(id=subscription_id)

    for destination in sorted(desired_destinations - active_subscriptions.keys()):
        conn.subscribe(destination=destination, id=next_subscription_id, ack="auto")
        active_subscriptions[destination] = next_subscription_id
        next_subscription_id += 1

    return next_subscription_id


def run_stomp_listener(
    stop_event: threading.Event | None = None,
    *,
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    destination: str | None = None,
    destinations_provider: Callable[[], set[str]] | None = None,
    reconnect_delay: int | None = None,
    max_reconnect_delay: int | None = None,
    connection_factory: Callable[[str, int], object] | None = None,
    listener: object | None = None,
    connection_controller: StompConnectionController | None = None,
) -> None:
    """Run a reconnecting STOMP listener until the provided stop event is set."""
    resolved_stop_event = stop_event or threading.Event()
    resolved_host = host or get_env("MON_CLIENT_STOMP_HOST", "emsserver-ems-server")
    resolved_port = port if port is not None else get_int_env("MON_CLIENT_STOMP_PORT", 61610)
    resolved_username = username if username is not None else get_env("MON_CLIENT_STOMP_USERNAME", "")
    resolved_password = password if password is not None else get_env("MON_CLIENT_STOMP_PASSWORD", "")
    resolved_destination = destination or get_env("MON_CLIENT_STOMP_DESTINATION", "/topic/>")
    resolved_reconnect_delay = (
        reconnect_delay
        if reconnect_delay is not None
        else get_int_env("MON_CLIENT_STOMP_RECONNECT_DELAY", 5)
    )
    resolved_max_reconnect_delay = (
        max_reconnect_delay
        if max_reconnect_delay is not None
        else get_int_env("MON_CLIENT_STOMP_MAX_RECONNECT_DELAY", max(resolved_reconnect_delay, 60))
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
            if connection_controller is not None:
                connection_controller.set_connection(conn)
            if listener is not None:
                conn.set_listener("", listener)
            conn.connect(login=resolved_username, passcode=resolved_password, wait=True)
            connection_established = True
            next_subscription_id = 1
            active_subscriptions: dict[str, int] = {}
            desired_destinations = _resolve_destinations(
                resolved_destination,
                destinations_provider,
            )
            next_subscription_id = _sync_subscriptions(
                conn,
                desired_destinations,
                active_subscriptions,
                next_subscription_id,
            )
            if active_subscriptions:
                logger.info("Subscription created successfully")
            current_reconnect_delay = max(0, resolved_reconnect_delay)

            while not resolved_stop_event.is_set():
                desired_destinations = _resolve_destinations(
                    resolved_destination,
                    destinations_provider,
                )
                next_subscription_id = _sync_subscriptions(
                    conn,
                    desired_destinations,
                    active_subscriptions,
                    next_subscription_id,
                )
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
                    _force_disconnect_connection(conn)
                    if connection_established:
                        logger.info("Connection cleanup completed")
                    else:
                        logger.debug("No active connection to clean up")
                except Exception:
                    logger.debug("Disconnect cleanup failed", exc_info=True)
                finally:
                    if connection_controller is not None:
                        connection_controller.clear_connection(conn)

        if not resolved_stop_event.is_set():
            resolved_stop_event.wait(current_reconnect_delay)
            if should_backoff and current_reconnect_delay > 0:
                current_reconnect_delay = min(current_reconnect_delay * 2, resolved_max_reconnect_delay)
