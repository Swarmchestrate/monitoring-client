import json
import logging
import threading
import time

import pytest

from swchmonclient import (
    query_metric_values,
    query_metric_values_raw,
    subscribe_metric,
    subscribe_metric_raw,
    unsubscribe_metric,
)
from swchmonclient.exceptions import MetricSubscriptionError
from swchmonclient.metrics import _default_metric_subscription_manager


class FakeFrame:
    def __init__(self, body, destination, headers=None):
        self.body = body
        self.headers = {"destination": destination}
        if headers:
            self.headers.update(headers)


class FakeConnection:
    def __init__(self):
        self.connected = False
        self.disconnected = False
        self.listener = None
        self.subscribe_calls = []
        self.subscription_ids = {}
        self.unsubscribe_calls = []

    def set_listener(self, name, listener):
        self.listener = listener

    def connect(self, login="", passcode="", wait=True):
        self.connected = True

    def subscribe(self, destination, id, ack):
        self.subscribe_calls.append((destination, id, ack))
        self.subscription_ids[destination] = id
        self.listener.on_message(
            FakeFrame(
                json.dumps(
                    {
                        "metricValue": 12.5,
                        "timestamp": int(time.time() * 1000),
                        "node": "node-a",
                    }
                ),
                destination,
            )
        )
        self.listener.on_message(
            FakeFrame(
                json.dumps(
                    {
                        "metricValue": 15.0,
                        "timestamp": int(time.time() * 1000),
                        "node": "node-b",
                    }
                ),
                destination,
            )
        )

    def unsubscribe(self, id):
        self.unsubscribe_calls.append(id)

    def is_connected(self):
        return self.connected and not self.disconnected

    def disconnect(self):
        self.disconnected = True
        self.connected = False


@pytest.fixture(autouse=True)
def cleanup_subscriptions():
    yield
    for metric in list(_default_metric_subscription_manager._subscriptions):
        unsubscribe_metric(metric)
    for metric in list(_default_metric_subscription_manager._raw_metric_nodes):
        unsubscribe_metric(metric)


def test_subscribe_and_query_metric_values(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        connection.port = port
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    thread_name = subscribe_metric("/topic/mysample_metric")

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    assert thread_name == "metric-listener"
    assert created
    assert created[0].subscribe_calls == [("/topic/mysample_metric", 1, "auto")]
    assert query_metric_values("/topic/mysample_metric") == [15.0]
    assert query_metric_values("/topic/mysample_metric") == []


def test_subscribe_metric_normalizes_plain_metric_name(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric("cpu_util_instance")

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    assert created
    assert created[0].subscribe_calls == [("/topic/cpu_util_instance", 1, "auto")]


def test_subscribe_metric_adds_metric_to_shared_listener(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    first_thread_name = subscribe_metric("/topic/cpu_util_instance")
    second_thread_name = subscribe_metric("/topic/mem_util_instance")

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if created and len(created[0].subscribe_calls) == 2:
            break
        time.sleep(0.01)

    assert first_thread_name == "metric-listener"
    assert second_thread_name == "metric-listener"
    assert len(created) == 1
    assert created[0].subscribe_calls == [
        ("/topic/cpu_util_instance", 1, "auto"),
        ("/topic/mem_util_instance", 2, "auto"),
    ]
    assert query_metric_values("/topic/cpu_util_instance") == [15.0]
    assert query_metric_values("/topic/mem_util_instance") == [15.0]


def test_subscribe_metric_raw_starts_listener_per_node(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        connection.port = port
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    thread_names = subscribe_metric_raw("/topic/mysample_metric", ["node-a", "node-b"])

    deadline = time.time() + 2.0
    while time.time() < deadline and len(created) < 2:
        time.sleep(0.01)

    assert thread_names == {
        "node-a": "metric-raw-listener:node-a",
        "node-b": "metric-raw-listener:node-b",
    }
    assert len(created) == 2
    assert all(
        connection.subscribe_calls == [("/topic/mysample_metric", 1, "auto")]
        for connection in created
    )
    assert {connection.host for connection in created} == {"node-a", "node-b"}

    raw_values = query_metric_values_raw("/topic/mysample_metric", 60)

    assert [entry["value"] for entry in raw_values["node-a"]] == [12.5]
    assert [entry["value"] for entry in raw_values["node-b"]] == [15.0]
    assert all(
        isinstance(entry["timestamp"], float)
        for entries in raw_values.values()
        for entry in entries
    )
    assert query_metric_values_raw("/topic/mysample_metric", 60) == {
        "node-a": [],
        "node-b": [],
    }


def test_subscribe_metric_raw_uses_connection_target_when_payload_has_different_ip(monkeypatch):
    class HeaderOnlyConnection(FakeConnection):
        def subscribe(self, destination, id, ack):
            self.subscribe_calls.append((destination, id, ack))
            self.subscription_ids[destination] = id
            self.listener.on_message(
                FakeFrame(
                    json.dumps(
                        {
                            "metricValue": 37.5,
                            "timestamp": int(time.time() * 1000),
                        }
                    ),
                    destination,
                    headers={
                        "host": "192.168.0.217",
                        "instance": "192.168.0.217",
                        "private-ip": "192.168.0.217",
                    },
                )
            )

    created = []

    def fake_connection_factory(host, port):
        connection = HeaderOnlyConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/mysample_metric", ["100.104.109.71"])

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    raw_values = query_metric_values_raw("/topic/mysample_metric", 60)

    assert list(raw_values) == ["100.104.109.71"]
    assert [entry["value"] for entry in raw_values["100.104.109.71"]] == [37.5]


def test_subscribe_metric_raw_all_resolves_cluster_vm_ips(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )
    monkeypatch.setattr(
        "swchmonclient.metrics.get_vm_private_ips",
        lambda: ["10.0.0.11", "10.0.0.12"],
    )

    thread_names = subscribe_metric_raw("/topic/mysample_metric", "all")

    deadline = time.time() + 2.0
    while time.time() < deadline and len(created) < 2:
        time.sleep(0.01)

    assert thread_names == {
        "10.0.0.11": "metric-raw-listener:10.0.0.11",
        "10.0.0.12": "metric-raw-listener:10.0.0.12",
    }
    assert {connection.host for connection in created} == {"10.0.0.11", "10.0.0.12"}


@pytest.mark.parametrize(
    ("selector", "resolved_nodes"),
    [
        ("all", ["10.0.0.11", "10.0.0.12"]),
        ("local", ["10.0.0.42"]),
    ],
)
def test_subscribe_metric_raw_logs_selector_resolution(
    monkeypatch,
    caplog,
    selector,
    resolved_nodes,
):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )
    if selector == "all":
        monkeypatch.setattr(
            "swchmonclient.metrics.get_vm_private_ips",
            lambda: resolved_nodes,
        )
    else:
        monkeypatch.setattr(
            "swchmonclient.metrics.MetricSubscriptionManager._get_local_private_ip",
            staticmethod(lambda: resolved_nodes[0]),
        )

    with caplog.at_level(logging.INFO):
        subscribe_metric_raw("/topic/mysample_metric", selector)

    assert (
        f"Subscribing to raw metric '/topic/mysample_metric' using node selector "
        f"'{selector}' resolved to nodes {resolved_nodes}"
    ) in caplog.text


def test_subscribe_metric_raw_local_resolves_current_private_ip(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )
    monkeypatch.setattr(
        "swchmonclient.metrics.MetricSubscriptionManager._get_local_private_ip",
        staticmethod(lambda: "10.0.0.77"),
    )

    thread_names = subscribe_metric_raw("/topic/mysample_metric", "local")

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    assert thread_names == {
        "10.0.0.77": "metric-raw-listener:10.0.0.77",
    }
    assert [connection.host for connection in created] == ["10.0.0.77"]


def test_subscribe_metric_raw_reuses_same_node_thread_for_different_metrics(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    first_threads = subscribe_metric_raw("/topic/cpu_util_instance", ["node-a"])
    second_threads = subscribe_metric_raw("/topic/mem_util_instance", ["node-a"])

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if created and len(created[0].subscribe_calls) == 2:
            break
        time.sleep(0.01)

    assert first_threads == {"node-a": "metric-raw-listener:node-a"}
    assert second_threads == {"node-a": "metric-raw-listener:node-a"}
    assert len(created) == 1
    assert created[0].subscribe_calls == [
        ("/topic/cpu_util_instance", 1, "auto"),
        ("/topic/mem_util_instance", 2, "auto"),
    ]
    cpu_values = query_metric_values_raw("/topic/cpu_util_instance", 60)
    mem_values = query_metric_values_raw("/topic/mem_util_instance", 60)

    assert [entry["value"] for entry in cpu_values["node-a"]] == [12.5]
    assert [entry["value"] for entry in mem_values["node-a"]] == [12.5]


def test_unsubscribe_metric_raw_keeps_shared_node_thread_for_other_metrics(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/cpu_util_instance", ["node-a"])
    subscribe_metric_raw("/topic/mem_util_instance", ["node-a"])

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if created and len(created[0].subscribe_calls) == 2:
            break
        time.sleep(0.01)

    unsubscribe_metric("/topic/cpu_util_instance", nodes=["node-a"])

    deadline = time.time() + 2.0
    while time.time() < deadline and not created[0].unsubscribe_calls:
        time.sleep(0.01)

    assert len(created) == 1
    assert created[0].unsubscribe_calls == [1]
    assert created[0].disconnected is False
    mem_values = query_metric_values_raw("/topic/mem_util_instance", 60)

    assert [entry["value"] for entry in mem_values["node-a"]] == [12.5]
    with pytest.raises(MetricSubscriptionError, match="not subscribed in raw mode"):
        query_metric_values_raw("/topic/cpu_util_instance", 60)


def test_unsubscribe_metric_raw_stops_last_node_without_unsubscribe_hang(monkeypatch):
    class HangOnUnsubscribeConnection(FakeConnection):
        def __init__(self):
            super().__init__()
            self.unsubscribe_attempted = threading.Event()

        def unsubscribe(self, id):
            self.unsubscribe_attempted.set()
            raise AssertionError("unsubscribe should not be called when stopping the last raw destination")

        def is_connected(self):
            return self.connected and not self.disconnected

    created = []

    def fake_connection_factory(host, port):
        connection = HangOnUnsubscribeConnection()
        connection.host = host
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/cpu_util_instance", ["node-a"])

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    unsubscribe_metric("/topic/cpu_util_instance")

    assert len(created) == 1
    assert created[0].disconnected is True
    assert created[0].unsubscribe_attempted.is_set() is False


def test_subscribe_metric_raw_keeps_last_1000_values(monkeypatch):
    class BurstConnection(FakeConnection):
        def subscribe(self, destination, id, ack):
            self.subscribe_calls.append((destination, id, ack))
            self.subscription_ids[destination] = id
            base_timestamp = int(time.time() * 1000)
            for value in range(1001):
                self.listener.on_message(
                    FakeFrame(
                        json.dumps(
                            {
                                "metricValue": value,
                                "timestamp": base_timestamp + value,
                                "node": "node-a",
                            }
                        ),
                        destination,
                    )
                )

    created = []

    def fake_connection_factory(host, port):
        connection = BurstConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/mysample_metric", ["node-a"])

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    raw_values = query_metric_values_raw("/topic/mysample_metric", 60)
    node_values = [entry["value"] for entry in raw_values["node-a"]]

    assert len(node_values) == 1000
    assert node_values[0] == 1
    assert node_values[-1] == 1000


def test_subscribe_metric_raw_uses_custom_cache_size(monkeypatch):
    class BurstConnection(FakeConnection):
        def subscribe(self, destination, id, ack):
            self.subscribe_calls.append((destination, id, ack))
            self.subscription_ids[destination] = id
            base_timestamp = int(time.time() * 1000)
            for value in range(10):
                self.listener.on_message(
                    FakeFrame(
                        json.dumps(
                            {
                                "metricValue": value,
                                "timestamp": base_timestamp + value,
                                "node": "node-a",
                            }
                        ),
                        destination,
                    )
                )

    created = []

    def fake_connection_factory(host, port):
        connection = BurstConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/mysample_metric", ["node-a"], cache_size=5)

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    raw_values = query_metric_values_raw("/topic/mysample_metric", 60)
    node_values = [entry["value"] for entry in raw_values["node-a"]]

    assert node_values == [5, 6, 7, 8, 9]


def test_subscribe_metric_raw_rejects_mixing_with_standard_subscription(monkeypatch):
    def fake_connection_factory(host, port):
        return FakeConnection()

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric("/topic/mysample_metric")

    with pytest.raises(MetricSubscriptionError, match="standard mode"):
        subscribe_metric_raw("/topic/mysample_metric", ["node-a"])


def test_unsubscribe_metric_blocks_selected_nodes(monkeypatch):
    def fake_connection_factory(host, port):
        return FakeConnection()

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric("/topic/mysample_metric")
    unsubscribe_metric("/topic/mysample_metric", nodes=["node-a"])

    assert query_metric_values("/topic/mysample_metric") == [15.0]


def test_unsubscribe_metric_stops_listener(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric("/topic/mysample_metric")
    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    unsubscribe_metric("/topic/mysample_metric")

    assert created[0].disconnected is True
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        query_metric_values("/topic/mysample_metric")


def test_unsubscribe_metric_keeps_shared_listener_running(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric("/topic/cpu_util_instance")
    subscribe_metric("/topic/mem_util_instance")

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if created and len(created[0].subscribe_calls) == 2:
            break
        time.sleep(0.01)

    unsubscribe_metric("/topic/cpu_util_instance")

    deadline = time.time() + 2.0
    while time.time() < deadline and not created[0].unsubscribe_calls:
        time.sleep(0.01)

    assert created[0].unsubscribe_calls == [1]
    assert created[0].disconnected is False
    assert query_metric_values("/topic/mem_util_instance") == [15.0]
    with pytest.raises(MetricSubscriptionError, match="not subscribed"):
        query_metric_values("/topic/cpu_util_instance")


def test_unsubscribe_metric_stops_selected_raw_listener(monkeypatch):
    created = []

    def fake_connection_factory(host, port):
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        fake_connection_factory,
    )

    subscribe_metric_raw("/topic/mysample_metric", ["node-a", "node-b"])

    deadline = time.time() + 2.0
    while time.time() < deadline and len(created) < 2:
        time.sleep(0.01)

    unsubscribe_metric("/topic/mysample_metric", nodes=["node-a"])

    assert created[0].disconnected is True
    assert created[1].disconnected is False
    raw_values = query_metric_values_raw("/topic/mysample_metric", 60)

    assert set(raw_values) == {"node-b"}
    assert [entry["value"] for entry in raw_values["node-b"]] == [15.0]
