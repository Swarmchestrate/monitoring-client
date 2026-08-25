import logging
import threading
import time

from swchmonclient.listener import LoggingStompListener, run_stomp_listener
from swchmonclient.thread_manager import MonitoringThreadManager


class FakeConnection:
    def __init__(self):
        self.connected = False
        self.subscribed = False
        self.disconnected = False
        self.listener = None

    def set_listener(self, name, listener):
        self.listener = listener

    def connect(self, login="", passcode="", wait=True):
        self.connected = True

    def subscribe(self, destination, id, ack):
        self.subscribed = True

    def is_connected(self):
        return self.connected and not self.disconnected

    def disconnect(self):
        self.disconnected = True
        self.connected = False


def test_start_and_stop_listener_thread():
    manager = MonitoringThreadManager()
    created = []

    def connection_factory(host, port):
        conn = FakeConnection()
        created.append((host, port, conn))
        return conn

    thread_name = manager.start_listener_thread(
        name="test-listener",
        host="localhost",
        port=61613,
        destination="/topic/test",
        reconnect_delay=0,
        connection_factory=connection_factory,
    )

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    assert thread_name == "test-listener"
    assert created
    assert created[0][0] == "localhost"
    assert created[0][1] == 61613
    assert created[0][2].subscribed is True

    manager.stop_listener_thread("test-listener", timeout=2.0)

    assert created[0][2].disconnected is True
    assert "test-listener" not in manager.list_threads()


def test_listener_reconnect_uses_exponential_backoff():
    class RecordingStopEvent:
        def __init__(self):
            self._is_set = False
            self.wait_calls = []

        def is_set(self):
            return self._is_set

        def wait(self, timeout):
            self.wait_calls.append(timeout)
            if len(self.wait_calls) >= 3:
                self._is_set = True
            return self._is_set

    stop_event = RecordingStopEvent()

    def failing_connection_factory(host, port):
        raise RuntimeError("broker unavailable")

    run_stomp_listener(
        stop_event=stop_event,
        host="localhost",
        port=61613,
        reconnect_delay=1,
        max_reconnect_delay=4,
        connection_factory=failing_connection_factory,
    )

    assert stop_event.wait_calls == [1, 2, 4]


def test_listener_failed_connect_does_not_log_clean_disconnect(caplog):
    class FailingConnection:
        def connect(self, login="", passcode="", wait=True):
            raise RuntimeError("broker unavailable")

        def disconnect(self):
            return None

    class RecordingStopEvent:
        def __init__(self):
            self._is_set = False

        def is_set(self):
            return self._is_set

        def wait(self, timeout):
            self._is_set = True
            return True

    stop_event = RecordingStopEvent()

    def failing_connection_factory(host, port):
        return FailingConnection()

    with caplog.at_level(logging.DEBUG):
        run_stomp_listener(
            stop_event=stop_event,
            host="localhost",
            port=61613,
            reconnect_delay=1,
            max_reconnect_delay=4,
            connection_factory=failing_connection_factory,
        )

    assert "Connection failed. Retrying in 1 seconds..." in caplog.text
    assert "Disconnected cleanly" not in caplog.text


def test_listener_logs_messages_at_debug(caplog):
    frame = type(
        "Frame",
        (),
        {
            "body": "payload",
            "headers": {"destination": "/topic/cpu_util_instance"},
        },
    )()

    with caplog.at_level(logging.DEBUG):
        LoggingStompListener().on_message(frame)

    assert "Received a message from [/topic/cpu_util_instance]: payload" in caplog.text


def test_listener_uses_only_mon_client_env_prefix(monkeypatch):
    created = []

    monkeypatch.setenv("MON_CLIENT_STOMP_HOST", "mon-client-host")
    monkeypatch.setenv("MON_CLIENT_STOMP_PORT", "61612")
    monkeypatch.setenv("MON_CLIENT_STOMP_DESTINATION", "/topic/prefixed")
    monkeypatch.setenv("STOMP_HOST", "legacy-host")
    monkeypatch.setenv("STOMP_PORT", "61613")
    monkeypatch.setenv("STOMP_DESTINATION", "/topic/legacy")

    stop_event = threading.Event()

    def stopping_connection_factory(host, port):
        conn = FakeConnection()
        created.append((host, port, conn))
        stop_event.set()
        return conn

    run_stomp_listener(
        stop_event=stop_event,
        connection_factory=stopping_connection_factory,
    )

    assert created[0][0] == "mon-client-host"
    assert created[0][1] == 61612


def test_listener_ignores_legacy_env_names(monkeypatch):
    created = []

    monkeypatch.delenv("MON_CLIENT_STOMP_HOST", raising=False)
    monkeypatch.delenv("MON_CLIENT_STOMP_PORT", raising=False)
    monkeypatch.delenv("MON_CLIENT_STOMP_DESTINATION", raising=False)
    monkeypatch.setenv("STOMP_HOST", "legacy-host")
    monkeypatch.setenv("STOMP_PORT", "61613")
    monkeypatch.setenv("STOMP_DESTINATION", "/topic/legacy")

    stop_event = threading.Event()

    def stopping_connection_factory(host, port):
        conn = FakeConnection()
        created.append((host, port, conn))
        stop_event.set()
        return conn

    run_stomp_listener(
        stop_event=stop_event,
        connection_factory=stopping_connection_factory,
    )

    assert created[0][0] == "emsserver-ems-server"
    assert created[0][1] == 61610


def test_stop_listener_thread_forces_disconnect_on_blocked_connection():
    class BlockingConnection(FakeConnection):
        def __init__(self):
            super().__init__()
            self._disconnect_event = threading.Event()

        def is_connected(self):
            self._disconnect_event.wait(10)
            return self.connected and not self.disconnected

        def disconnect(self):
            super().disconnect()
            self._disconnect_event.set()

    manager = MonitoringThreadManager()
    created = []

    def connection_factory(host, port):
        conn = BlockingConnection()
        created.append(conn)
        return conn

    manager.start_listener_thread(
        name="blocked-listener",
        host="localhost",
        port=61613,
        destination="/topic/test",
        reconnect_delay=0,
        connection_factory=connection_factory,
    )

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    manager.stop_listener_thread("blocked-listener", timeout=2.0)

    assert created[0].disconnected is True
    assert "blocked-listener" not in manager.list_threads()


def test_stop_listener_thread_detaches_after_forced_disconnect_timeout():
    class StubbornConnection(FakeConnection):
        def is_connected(self):
            time.sleep(1.0)
            return self.connected and not self.disconnected

    manager = MonitoringThreadManager()
    created = []

    def connection_factory(host, port):
        conn = StubbornConnection()
        created.append(conn)
        return conn

    manager.start_listener_thread(
        name="stubborn-listener",
        host="localhost",
        port=61613,
        destination="/topic/test",
        reconnect_delay=0,
        connection_factory=connection_factory,
    )

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    manager.stop_listener_thread("stubborn-listener", timeout=0.01)

    assert created[0].disconnected is True
    assert "stubborn-listener" not in manager.list_threads()
