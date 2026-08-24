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
            "swchmonclient.metrics.get_current_vm_private_ip",
            lambda: resolved_nodes[0],
        )

    with caplog.at_level(logging.INFO):
        subscribe_metric_raw("/topic/mysample_metric", selector)

    assert (
        f"Subscribing to raw metric '/topic/mysample_metric' using node selector "
        f"'{selector}' resolved to nodes {resolved_nodes}"
    ) in caplog.text


def test_subscribe_metric_raw_local_resolves_current_node_ip(monkeypatch):
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
        "swchmonclient.metrics.get_current_vm_private_ip",
        lambda: "10.0.0.77",
    )

    thread_names = subscribe_metric_raw("/topic/mysample_metric", "local")

    deadline = time.time() + 2.0
    while time.time() < deadline and not created:
        time.sleep(0.01)

    assert thread_names == {
        "10.0.0.77": "metric-raw-listener:10.0.0.77",
    }
    assert [connection.host for connection in created] == ["10.0.0.77"]


def test_subscribe_metric_raw_local_wraps_current_node_ip_errors(monkeypatch):
    monkeypatch.setattr(
        "swchmonclient.metrics.get_current_vm_private_ip",
        lambda: (_ for _ in ()).throw(RuntimeError("forbidden")),
    )

    with pytest.raises(
        MetricSubscriptionError,
        match="Failed to resolve raw metric nodes for 'local': forbidden",
    ):
        subscribe_metric_raw("/topic/mysample_metric", "local")


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


def test_subscribe_metric_raw_replays_regular_values_from_file(tmp_path):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "simulated-node",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 0.02,
                                "values": [10, 20],
                                "repeat": True,
                            }
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    thread_names = subscribe_metric_raw(
        "cpu_util_prct",
        "all",
        source_file=source_file,
    )
    time.sleep(0.07)

    assert thread_names == {
        "simulated-node": "metric-raw-file:simulated-node",
    }
    values = query_metric_values_raw("cpu_util_prct", 60)["simulated-node"]
    assert [sample["value"] for sample in values][:3] == [10, 20, 10]
    assert all(isinstance(sample["timestamp"], float) for sample in values)


def test_subscribe_metric_raw_file_defaults_to_all_matching_nodes(tmp_path):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "node-a",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [10],
                                "repeat": False,
                            }
                        },
                    },
                    {
                        "node_id": "node-b",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [20],
                                "repeat": False,
                            }
                        },
                    },
                    {
                        "node_id": "node-c",
                        "metrics": {
                            "mem_util_prct": {
                                "interval_seconds": 1,
                                "values": [30],
                                "repeat": False,
                            }
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    thread_names = subscribe_metric_raw(
        "cpu_util_prct",
        source_file=source_file,
    )

    assert thread_names == {
        "node-a": "metric-raw-file:node-a",
        "node-b": "metric-raw-file:node-b",
    }


def test_subscribe_metric_raw_file_cluster_maps_profiles_to_cluster_nodes(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": node_id,
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [value],
                                "repeat": False,
                            }
                        },
                    }
                    for node_id, value in (("node-a", 10), ("node-b", 20))
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "swchmonclient.metrics.get_vm_private_ips",
        lambda: ["node-b", "node-not-in-file"],
    )

    thread_names = subscribe_metric_raw(
        "cpu_util_prct",
        "cluster",
        source_file=source_file,
    )

    assert thread_names == {
        "node-b": "metric-raw-file:node-b",
        "node-not-in-file": "metric-raw-file:node-not-in-file",
    }
    time.sleep(0.05)
    values = query_metric_values_raw("cpu_util_prct", 60)
    assert [sample["value"] for sample in values["node-b"]] == [20]
    assert [sample["value"] for sample in values["node-not-in-file"]] == [10]


def test_subscribe_metric_raw_file_cluster_preserves_mapping_when_node_joins(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": f"profile-{index}",
                        "metrics": {
                            metric: {
                                "interval_seconds": 1,
                                "values": [index],
                                "repeat": False,
                            }
                            for metric in ("cpu_util_prct", "mem_util_prct")
                        },
                    }
                    for index in range(1, 4)
                ],
            }
        ),
        encoding="utf-8",
    )
    cluster_nodes = ["cluster-a", "cluster-b"]
    monkeypatch.setattr(
        "swchmonclient.metrics.get_vm_private_ips",
        lambda: list(cluster_nodes),
    )

    subscribe_metric_raw("cpu_util_prct", "cluster", source_file=source_file)
    source_key = str(source_file.resolve())
    initial_mapping = dict(
        _default_metric_subscription_manager._file_cluster_mappings[source_key]
    )

    cluster_nodes.append("cluster-c")
    _default_metric_subscription_manager._refresh_cluster_file_subscriptions_once()
    assert _default_metric_subscription_manager._raw_metric_nodes[
        "/topic/cpu_util_prct"
    ] == {"cluster-a", "cluster-b", "cluster-c"}

    subscribe_metric_raw("mem_util_prct", "cluster", source_file=source_file)
    expanded_mapping = (
        _default_metric_subscription_manager._file_cluster_mappings[source_key]
    )

    assert len(expanded_mapping) == 3
    assert len(set(expanded_mapping.values())) == 3
    assert all(expanded_mapping[node] == profile for node, profile in initial_mapping.items())

    cluster_nodes.remove("cluster-b")
    _default_metric_subscription_manager._refresh_cluster_file_subscriptions_once()

    assert _default_metric_subscription_manager._raw_metric_nodes[
        "/topic/cpu_util_prct"
    ] == {"cluster-a", "cluster-c"}
    assert _default_metric_subscription_manager._raw_metric_nodes[
        "/topic/mem_util_prct"
    ] == {"cluster-a", "cluster-c"}


def test_subscribe_metric_raw_file_cluster_does_not_reuse_profiles(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": f"profile-{index}",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [index],
                                "repeat": False,
                            }
                        },
                    }
                    for index in range(1, 3)
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "swchmonclient.metrics.get_vm_private_ips",
        lambda: ["cluster-a", "cluster-b", "cluster-c"],
    )

    thread_names = subscribe_metric_raw(
        "cpu_util_prct",
        "cluster",
        source_file=source_file,
    )
    mapping = _default_metric_subscription_manager._file_cluster_mappings[
        str(source_file.resolve())
    ]

    assert len(thread_names) == 2
    assert len(set(mapping.values())) == 2


def test_subscribe_metric_raw_live_mode_requires_node():
    with pytest.raises(
        ValueError,
        match="node is required when source_file is not provided",
    ):
        subscribe_metric_raw("cpu_util_prct")


def test_subscribe_metric_raw_replays_irregular_non_repeating_samples(tmp_path):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "simulated-node",
                        "metrics": {
                            "/topic/cpu_util_prct": {
                                "repeat": False,
                                "samples": [
                                    {"offset_seconds": 0, "value": 5},
                                    {"offset_seconds": 0.03, "value": 8},
                                ],
                            }
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    subscribe_metric_raw(
        "/topic/cpu_util_prct",
        ["simulated-node"],
        source_file=source_file,
    )
    time.sleep(0.08)

    values = query_metric_values_raw("cpu_util_prct", 60)["simulated-node"]
    assert [sample["value"] for sample in values] == [5, 8]
    assert values[1]["timestamp"] - values[0]["timestamp"] == pytest.approx(
        0.03,
        abs=0.015,
    )


def test_subscribe_metric_raw_can_mix_file_and_live_nodes(monkeypatch, tmp_path):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "simulated-node",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [99],
                                "repeat": False,
                            }
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        lambda host, port: FakeConnection(),
    )

    subscribe_metric_raw(
        "cpu_util_prct",
        ["simulated-node"],
        source_file=source_file,
    )
    subscribe_metric_raw("cpu_util_prct", ["node-a"])
    time.sleep(0.05)

    values = query_metric_values_raw("cpu_util_prct", 60)
    assert [sample["value"] for sample in values["simulated-node"]] == [99]
    assert [sample["value"] for sample in values["node-a"]] == [12.5]


def test_subscribe_metric_raw_file_validates_requested_metric(tmp_path):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "simulated-node",
                        "metrics": {
                            "mem_util_prct": {
                                "interval_seconds": 1,
                                "values": [50],
                            }
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        MetricSubscriptionError,
        match="Metric '/topic/cpu_util_prct' is not configured",
    ):
        subscribe_metric_raw(
            "cpu_util_prct",
            "all",
            source_file=source_file,
        )


def test_subscribe_metric_raw_rejects_different_sources_for_same_node(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "raw-metrics.json"
    source_file.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "node-a",
                        "metrics": {
                            "cpu_util_prct": {
                                "interval_seconds": 1,
                                "values": [99],
                            }
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "swchmonclient.listener.create_stomp_connection",
        lambda host, port: FakeConnection(),
    )

    subscribe_metric_raw("cpu_util_prct", ["node-a"])

    with pytest.raises(
        MetricSubscriptionError,
        match="Node 'node-a' is already subscribed from live",
    ):
        subscribe_metric_raw(
            "cpu_util_prct",
            ["node-a"],
            source_file=source_file,
        )


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
