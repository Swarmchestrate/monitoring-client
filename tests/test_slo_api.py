import json
import threading
import time

import pytest

from swchmonclient import (
    query_metric_values,
    query_slo_violations,
    subscribe_metric,
    subscribe_metric_raw,
    subscribe_slo,
    unsubscribe_metric,
    unsubscribe_slo,
)
from swchmonclient.exceptions import MetricSubscriptionError
from swchmonclient.metrics import _default_metric_subscription_manager

# Timestamps of the two violations FakeConnection emits on subscribe, in seconds.
FIRST_TIMESTAMP = 1792749239.512
SECOND_TIMESTAMP = 1792795789.558
THIRD_RAW_TIMESTAMP = 1792800000000
THIRD_TIMESTAMP = 1792800000.0


class FakeFrame:
    def __init__(self, body, destination):
        self.body = body
        self.headers = {"destination": destination}


class FakeConnection:
    """Emits two SLO violations on ``*_CONSTRAINT`` topics and one sample elsewhere."""

    def __init__(self):
        self.connected = False
        self.disconnected = False
        self.listener = None
        self.subscribe_calls = []
        self.unsubscribe_calls = []

    def set_listener(self, name, listener):
        self.listener = listener

    def connect(self, login="", passcode="", wait=True):
        self.connected = True

    def subscribe(self, destination, id, ack):
        if destination.endswith("_CONSTRAINT"):
            payloads = [
                {"level": 3, "metricValue": 8.32034, "timestamp": 1792749239512},
                {"level": 2, "metricValue": 4.497942, "timestamp": 1792795789558},
            ]
        else:
            payloads = [{"metricValue": 12.5, "timestamp": int(time.time() * 1000)}]
        for payload in payloads:
            self.listener.on_message(FakeFrame(json.dumps(payload), destination))
        # Recorded last so tests waiting on subscribe_calls also see the emitted messages.
        self.subscribe_calls.append((destination, id, ack))

    def emit(self, body, destination):
        self.listener.on_message(FakeFrame(body, destination))

    def unsubscribe(self, id):
        self.unsubscribe_calls.append(id)

    def is_connected(self):
        return self.connected and not self.disconnected

    def disconnect(self):
        self.disconnected = True
        self.connected = False


@pytest.fixture
def created(monkeypatch):
    connections = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )
    return connections


@pytest.fixture(autouse=True)
def cleanup_subscriptions():
    yield
    for subscription in list(_default_metric_subscription_manager._slo_subscriptions.values()):
        unsubscribe_slo(subscription.slo_name)
    for metric in list(_default_metric_subscription_manager._subscriptions):
        unsubscribe_metric(metric)


def wait_for_subscribe_calls(created, count):
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if created and len(created[0].subscribe_calls) >= count:
            return
        time.sleep(0.01)
    raise AssertionError(f"expected {count} subscribe calls")


def test_subscribe_slo_buffers_all_violations(created):
    thread_name = subscribe_slo("high_cpu")
    wait_for_subscribe_calls(created, 1)

    assert thread_name == "metric-listener"
    assert created[0].subscribe_calls == [("/topic/high_cpu_CONSTRAINT", 1, "auto")]
    assert query_slo_violations("high_cpu") == [FIRST_TIMESTAMP, SECOND_TIMESTAMP]
    assert query_slo_violations("high_cpu") == []


@pytest.mark.parametrize(
    "slo_name",
    ["high_cpu", " high_cpu ", "high_cpu_CONSTRAINT", "/topic/high_cpu_CONSTRAINT"],
)
def test_subscribe_slo_normalizes_name_to_constraint_topic(created, slo_name):
    subscribe_slo(slo_name)
    wait_for_subscribe_calls(created, 1)

    assert created[0].subscribe_calls == [("/topic/high_cpu_CONSTRAINT", 1, "auto")]
    assert len(query_slo_violations("high_cpu")) == 2


@pytest.mark.parametrize("slo_name", ["", "   ", "_CONSTRAINT", "/queue/high_cpu"])
def test_subscribe_slo_rejects_invalid_names(slo_name):
    with pytest.raises(ValueError):
        subscribe_slo(slo_name)


def test_subscribe_slo_uses_receive_time_for_non_json_bodies(created):
    subscribe_slo("high_cpu")
    wait_for_subscribe_calls(created, 1)
    query_slo_violations("high_cpu")

    before = time.time()
    created[0].emit("not-json", "/topic/high_cpu_CONSTRAINT")
    after = time.time()

    [timestamp] = query_slo_violations("high_cpu")
    assert before <= timestamp <= after


def test_on_violation_callback_receives_timestamp(created):
    received = []
    subscribe_slo("high_cpu", on_violation=received.append)
    wait_for_subscribe_calls(created, 1)

    created[0].emit(
        json.dumps({"level": 1, "metricValue": 90.0, "timestamp": THIRD_RAW_TIMESTAMP, "zone": "a"}),
        "/topic/high_cpu_CONSTRAINT",
    )

    assert received == [FIRST_TIMESTAMP, SECOND_TIMESTAMP, THIRD_TIMESTAMP]


def test_subscribe_slo_uses_custom_cache_size(created):
    subscribe_slo("high_cpu", cache_size=1)
    wait_for_subscribe_calls(created, 1)

    assert query_slo_violations("high_cpu") == [SECOND_TIMESTAMP]


def test_subscribe_slo_rejects_non_positive_cache_size():
    with pytest.raises(ValueError, match="cache_size"):
        subscribe_slo("high_cpu", cache_size=0)


def test_subscribe_slo_invokes_callback_and_survives_callback_errors(created):
    received = []

    def on_violation(violation):
        received.append(violation)
        raise RuntimeError("boom")

    subscribe_slo("high_cpu", on_violation=on_violation)
    wait_for_subscribe_calls(created, 1)

    assert received == [FIRST_TIMESTAMP, SECOND_TIMESTAMP]
    assert query_slo_violations("high_cpu") == [FIRST_TIMESTAMP, SECOND_TIMESTAMP]


def test_subscribe_slo_shares_listener_with_metrics(created):
    subscribe_metric("cpu_util_prct")
    subscribe_slo("high_cpu")
    wait_for_subscribe_calls(created, 2)

    assert len(created) == 1
    assert created[0].subscribe_calls == [
        ("/topic/cpu_util_prct", 1, "auto"),
        ("/topic/high_cpu_CONSTRAINT", 2, "auto"),
    ]
    assert [sample["value"] for sample in query_metric_values("cpu_util_prct")] == [12.5]
    assert len(query_slo_violations("high_cpu")) == 2

    unsubscribe_metric("cpu_util_prct")
    deadline = time.time() + 2.0
    while time.time() < deadline and not created[0].unsubscribe_calls:
        time.sleep(0.01)

    assert created[0].unsubscribe_calls == [1]
    assert created[0].disconnected is False

    unsubscribe_slo("high_cpu")

    assert created[0].disconnected is True
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        query_slo_violations("high_cpu")


def test_unsubscribe_slo_keeps_listener_for_remaining_metric(created):
    subscribe_slo("high_cpu")
    subscribe_metric("cpu_util_prct")
    wait_for_subscribe_calls(created, 2)

    unsubscribe_slo("high_cpu")
    deadline = time.time() + 2.0
    while time.time() < deadline and not created[0].unsubscribe_calls:
        time.sleep(0.01)

    assert created[0].unsubscribe_calls == [1]
    assert created[0].disconnected is False
    assert [sample["value"] for sample in query_metric_values("cpu_util_prct")] == [12.5]


def test_subscribe_slo_rejects_topic_already_subscribed_as_metric(created):
    subscribe_metric("high_cpu_CONSTRAINT")

    with pytest.raises(MetricSubscriptionError, match="already subscribed as a metric"):
        subscribe_slo("high_cpu")


def test_subscribe_metric_rejects_topic_already_subscribed_as_slo(created):
    subscribe_slo("high_cpu")

    with pytest.raises(MetricSubscriptionError, match="already subscribed as an SLO"):
        subscribe_metric("high_cpu_CONSTRAINT")
    with pytest.raises(MetricSubscriptionError, match="already subscribed as an SLO"):
        subscribe_metric_raw("high_cpu_CONSTRAINT", ["10.0.0.1"])


def test_query_and_unsubscribe_unknown_slo_raise():
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        query_slo_violations("high_cpu")
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        unsubscribe_slo("high_cpu")


def test_resubscribe_slo_applies_new_settings_and_keeps_buffer(created):
    received = []
    subscribe_slo("high_cpu")
    wait_for_subscribe_calls(created, 1)

    subscribe_slo("high_cpu", cache_size=1, on_violation=received.append)
    created[0].emit(
        json.dumps({"level": 1, "metricValue": 90.0, "timestamp": THIRD_RAW_TIMESTAMP}),
        "/topic/high_cpu_CONSTRAINT",
    )

    assert received == [THIRD_TIMESTAMP]
    assert query_slo_violations("high_cpu") == [THIRD_TIMESTAMP]


def test_resubscribe_slo_without_arguments_keeps_existing_settings(created):
    received = []
    subscribe_slo("high_cpu", cache_size=1, on_violation=received.append)
    wait_for_subscribe_calls(created, 1)

    subscribe_slo("high_cpu")

    assert query_slo_violations("high_cpu") == [SECOND_TIMESTAMP]
    assert received == [FIRST_TIMESTAMP, SECOND_TIMESTAMP]


@pytest.mark.parametrize(
    ("subscribe", "query"),
    [
        (lambda: subscribe_slo("high_cpu"), lambda: query_slo_violations("high_cpu")),
        (lambda: subscribe_metric("cpu_util_prct"), lambda: query_metric_values("cpu_util_prct")),
    ],
)
def test_subscribe_rolls_back_when_stale_listener_cannot_be_cleared(
    monkeypatch,
    subscribe,
    query,
):
    def fail_clear(thread_name):
        raise MetricSubscriptionError(f"Failed to restart listener '{thread_name}'")

    monkeypatch.setattr(
        _default_metric_subscription_manager,
        "_clear_stale_listener_thread",
        fail_clear,
    )

    with pytest.raises(MetricSubscriptionError, match="Failed to restart listener"):
        subscribe()
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        query()


def test_subscribe_during_last_unsubscribe_restarts_listener(created, monkeypatch):
    manager = _default_metric_subscription_manager
    subscribe_metric("cpu_util_prct")
    wait_for_subscribe_calls(created, 1)

    stopping = threading.Event()
    release = threading.Event()
    original_stop = manager._thread_manager.stop_listener_thread

    def slow_stop(name, *args, **kwargs):
        stopping.set()
        release.wait(2.0)
        return original_stop(name, *args, **kwargs)

    monkeypatch.setattr(manager._thread_manager, "stop_listener_thread", slow_stop)

    unsubscriber = threading.Thread(target=unsubscribe_metric, args=("cpu_util_prct",))
    unsubscriber.start()
    assert stopping.wait(2.0)

    subscriber = threading.Thread(target=subscribe_slo, args=("high_cpu",))
    subscriber.start()
    subscriber.join(0.2)
    assert subscriber.is_alive(), "subscribe must wait while the listener is being stopped"

    release.set()
    unsubscriber.join(2.0)
    subscriber.join(2.0)

    deadline = time.time() + 2.0
    while time.time() < deadline and len(created) < 2:
        time.sleep(0.01)

    assert manager._thread_manager.list_threads().get("metric-listener") is True
    assert created[0].disconnected is True
    wait_for_subscribe_calls(created[1:], 1)
    assert created[1].subscribe_calls == [("/topic/high_cpu_CONSTRAINT", 1, "auto")]
