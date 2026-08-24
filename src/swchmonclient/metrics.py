import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Deque

from .deployer import get_current_vm_private_ip, get_vm_private_ips
from .exceptions import MetricSubscriptionError, ThreadManagementError
from .listener import CallbackStompListener
from .thread_manager import MonitoringThreadManager

logger = logging.getLogger(__name__)

MAX_SAMPLES_PER_METRIC = 1
RAW_MAX_SAMPLES_PER_RAW_METRIC = 1000
LISTENER_THREAD_NAME = "metric-listener"
RAW_LISTENER_THREAD_NAME_PREFIX = "metric-raw-listener"
RAW_FILE_THREAD_NAME_PREFIX = "metric-raw-file"
RAW_METRIC_FILE_SCHEMA_VERSION = 1


@dataclass
class _MetricSample:
    timestamp: float
    value: Any
    node: str | None


@dataclass
class _MetricSubscription:
    thread_name: str
    samples: Deque[_MetricSample] = field(
        default_factory=lambda: deque(maxlen=MAX_SAMPLES_PER_METRIC)
    )
    blocked_nodes: set[str] = field(default_factory=set)


@dataclass
class _RawMetricSample:
    timestamp: float
    value: Any


@dataclass(frozen=True)
class _FileReplayPoint:
    offset_seconds: float
    value: Any


@dataclass(frozen=True)
class _FileMetricDefinition:
    points: tuple[_FileReplayPoint, ...]
    repeat: bool
    cycle_duration_seconds: float


@dataclass
class _FileReplayState:
    definition: _FileMetricDefinition
    point_index: int
    cycle_started_at: float


@dataclass
class _RawNodeSubscription:
    thread_name: str
    source_kind: str = "live"
    destinations: set[str] = field(default_factory=set)
    samples_by_destination: dict[str, Deque[_RawMetricSample]] = field(default_factory=dict)
    file_definitions: dict[str, _FileMetricDefinition] = field(default_factory=dict)


def _normalize_timestamp(raw_timestamp: Any) -> float:
    if isinstance(raw_timestamp, (int, float)):
        if raw_timestamp > 10_000_000_000:
            return float(raw_timestamp) / 1000.0
        return float(raw_timestamp)
    return time.time()


def _extract_node(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    for key in (
        "node",
        "node_name",
        "nodeName",
        "instance",
        "source",
        "host",
        "private-ip",
        "public-ip",
        "producer-host",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _deserialize_metric_payload(body: Any) -> Any:
    resolved_body = body.decode("utf-8") if isinstance(body, bytes) else body
    payload: Any = resolved_body

    if isinstance(resolved_body, str):
        try:
            payload = json.loads(resolved_body)
        except json.JSONDecodeError:
            payload = resolved_body

    return payload


def _parse_metric_payload(
    body: Any,
    headers: dict[str, Any] | None = None,
) -> tuple[Any, float, str | None]:
    payload = _deserialize_metric_payload(body)

    if isinstance(payload, dict):
        timestamp = _normalize_timestamp(payload.get("timestamp"))
        value = payload.get("metricValue", payload.get("value", payload))
        node = _extract_node(payload) or _extract_node(headers)
        return value, timestamp, node

    return payload, time.time(), _extract_node(headers)


class MetricSubscriptionManager:
    """Manage standard and raw metric listener threads plus in-memory sample buffers."""

    def __init__(self) -> None:
        self._thread_manager = MonitoringThreadManager()
        self._subscriptions: dict[str, _MetricSubscription] = {}
        self._raw_node_subscriptions: dict[str, _RawNodeSubscription] = {}
        self._raw_metric_nodes: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    def subscribe_metric(self, metric: str) -> str:
        destination = self._normalize_metric(metric)

        with self._lock:
            if destination in self._raw_metric_nodes:
                raise MetricSubscriptionError(
                    f"Metric '{metric}' is already subscribed in raw mode"
                )
            existing = self._subscriptions.get(destination)
            if existing is not None:
                if self._thread_manager.list_threads().get(existing.thread_name, False):
                    return existing.thread_name
                self._subscriptions.pop(destination, None)

            self._subscriptions[destination] = _MetricSubscription(
                thread_name=LISTENER_THREAD_NAME
            )
            should_start_listener = not self._thread_manager.list_threads().get(
                LISTENER_THREAD_NAME,
                False,
            )

        if not should_start_listener:
            return LISTENER_THREAD_NAME

        self._clear_stale_listener_thread(LISTENER_THREAD_NAME)
        listener = CallbackStompListener(self._build_metric_handler())

        try:
            return self._thread_manager.start_listener_thread(
                name=LISTENER_THREAD_NAME,
                destinations_provider=self.get_destinations,
                listener=listener,
            )
        except ThreadManagementError as error:
            with self._lock:
                self._subscriptions.pop(destination, None)
            raise MetricSubscriptionError(
                f"Failed to subscribe to metric '{metric}': {error}"
            ) from error

    def subscribe_metric_raw(
        self,
        metric: str,
        node: list[str] | str,
        cache_size: int | None = None,
        *,
        source_file: str | Path | None = None,
    ) -> dict[str, str]:
        destination = self._normalize_metric(metric)
        if source_file is None:
            source_kind = "live"
            normalized_nodes = self._resolve_raw_nodes(node)
            file_definitions: dict[str, _FileMetricDefinition] = {}
        else:
            source_kind = "file"
            normalized_nodes, file_definitions = self._load_file_metric_definitions(
                source_file,
                destination,
                node,
            )
        resolved_cache_size = self._resolve_raw_cache_size(cache_size)
        if isinstance(node, str) and node.strip().lower() in {"all", "local"}:
            if source_kind == "live":
                logger.info(
                    "Subscribing to raw metric '%s' using node selector '%s' "
                    "resolved to nodes %s",
                    metric,
                    node.strip().lower(),
                    normalized_nodes,
                )
            else:
                logger.info(
                    "Subscribing to raw metric '%s' from file using node selector "
                    "'%s' resolved to nodes %s",
                    metric,
                    node.strip().lower(),
                    normalized_nodes,
                )

        with self._lock:
            if destination in self._subscriptions:
                raise MetricSubscriptionError(
                    f"Metric '{metric}' is already subscribed in standard mode"
                )
            for current_node in normalized_nodes:
                existing_node = self._raw_node_subscriptions.get(current_node)
                if (
                    existing_node is not None
                    and existing_node.source_kind != source_kind
                ):
                    raise MetricSubscriptionError(
                        f"Node '{current_node}' is already subscribed from "
                        f"{existing_node.source_kind}; unsubscribe it before "
                        f"switching to {source_kind}"
                    )

            raw_metric_nodes = self._raw_metric_nodes.setdefault(destination, set())
            planned_nodes: list[tuple[str, str, bool]] = []
            thread_names: dict[str, str] = {}
            rollback_records: list[tuple[str, bool, bool]] = []

            for current_node in normalized_nodes:
                node_subscription = self._raw_node_subscriptions.get(current_node)
                node_created = False
                if node_subscription is None:
                    node_subscription = _RawNodeSubscription(
                        thread_name=(
                            self._build_raw_thread_name(current_node)
                            if source_kind == "live"
                            else self._build_raw_file_thread_name(current_node)
                        ),
                        source_kind=source_kind,
                    )
                    self._raw_node_subscriptions[current_node] = node_subscription
                    node_created = True

                destination_added = False
                if destination not in node_subscription.destinations:
                    node_subscription.destinations.add(destination)
                    node_subscription.samples_by_destination[destination] = deque(
                        maxlen=resolved_cache_size
                    )
                    raw_metric_nodes.add(current_node)
                    destination_added = True
                else:
                    existing_samples = node_subscription.samples_by_destination.get(
                        destination,
                        deque(maxlen=resolved_cache_size),
                    )
                    node_subscription.samples_by_destination[destination] = deque(
                        existing_samples,
                        maxlen=resolved_cache_size,
                    )
                if source_kind == "file":
                    node_subscription.file_definitions[destination] = file_definitions[
                        current_node
                    ]

                thread_name = node_subscription.thread_name
                thread_names[current_node] = thread_name
                rollback_records.append((current_node, node_created, destination_added))

                thread_state = self._thread_manager.list_threads().get(thread_name)
                if thread_state is True:
                    continue
                planned_nodes.append((current_node, thread_name, thread_state is False))

        started_nodes: list[tuple[str, str]] = []
        try:
            for current_node, thread_name, is_stale in planned_nodes:
                if is_stale:
                    self._clear_stale_listener_thread(thread_name)
                if source_kind == "live":
                    listener = CallbackStompListener(
                        self._build_raw_metric_handler(current_node)
                    )
                    self._thread_manager.start_listener_thread(
                        name=thread_name,
                        host=current_node,
                        destinations_provider=lambda node=current_node: self.get_raw_destinations(node),
                        listener=listener,
                    )
                else:
                    self._thread_manager.start_monitoring_thread(
                        name=thread_name,
                        target=self._run_raw_file_source,
                        node=current_node,
                    )
                started_nodes.append((current_node, thread_name))
        except (MetricSubscriptionError, ThreadManagementError) as error:
            for _started_node, started_thread_name in started_nodes:
                try:
                    self._thread_manager.stop_listener_thread(started_thread_name)
                except ThreadManagementError:
                    pass

            with self._lock:
                raw_metric_nodes = self._raw_metric_nodes.get(destination)
                for current_node, node_created, destination_added in rollback_records:
                    node_subscription = self._raw_node_subscriptions.get(current_node)
                    if node_subscription is None:
                        continue
                    if destination_added:
                        node_subscription.destinations.discard(destination)
                        node_subscription.samples_by_destination.pop(destination, None)
                        node_subscription.file_definitions.pop(destination, None)
                        if raw_metric_nodes is not None:
                            raw_metric_nodes.discard(current_node)
                    if node_created and not node_subscription.destinations:
                        self._raw_node_subscriptions.pop(current_node, None)
                if raw_metric_nodes is not None and not raw_metric_nodes:
                    self._raw_metric_nodes.pop(destination, None)

            raise MetricSubscriptionError(
                f"Failed to subscribe to metric '{metric}' in raw mode: {error}"
            ) from error

        return thread_names

    def query_metric_values(
        self,
        metric: str,
    ) -> list[Any]:
        destination = self._normalize_metric(metric)
        with self._lock:
            subscription = self._subscriptions.get(destination)
            if subscription is None:
                raise MetricSubscriptionError(f"Metric '{metric}' is not subscribed")
            matching_samples = list(subscription.samples)
            subscription.samples = deque(maxlen=MAX_SAMPLES_PER_METRIC)

        return [sample.value for sample in matching_samples]

    def query_metric_values_raw(
        self,
        metric: str,
        seconds: int,
    ) -> dict[str, list[dict[str, Any]]]:
        return self._query_raw_metric_values(metric, seconds)

    def unsubscribe_metric(self, metric: str, nodes: list[str] | None = None) -> None:
        destination = self._normalize_metric(metric)

        with self._lock:
            has_raw_subscription = destination in self._raw_metric_nodes

        if has_raw_subscription:
            self._unsubscribe_metric_raw(metric, destination, nodes)
            return

        if nodes is not None:
            normalized_nodes = {node for node in nodes if node}
            if not normalized_nodes:
                raise ValueError("nodes must contain at least one non-empty node identifier")
            self._block_nodes(destination, metric, normalized_nodes)
            return

        with self._lock:
            subscription = self._subscriptions.get(destination)
            if subscription is None:
                raise MetricSubscriptionError(f"Metric '{metric}' is not subscribed")
            thread_name = subscription.thread_name
            should_stop_listener = len(self._subscriptions) == 1
            removed_subscription = self._subscriptions.pop(destination)

        if not should_stop_listener:
            return

        try:
            self._thread_manager.stop_listener_thread(thread_name)
        except ThreadManagementError as error:
            with self._lock:
                self._subscriptions[destination] = removed_subscription
            raise MetricSubscriptionError(
                f"Failed to unsubscribe from metric '{metric}': {error}"
            ) from error

    def get_destinations(self) -> set[str]:
        with self._lock:
            return set(self._subscriptions)

    def get_raw_destinations(self, node: str) -> set[str]:
        with self._lock:
            node_subscription = self._raw_node_subscriptions.get(node)
            if node_subscription is None:
                return set()
            return set(node_subscription.destinations)

    def _query_raw_metric_values(
        self,
        metric: str,
        seconds: int,
    ) -> dict[str, list[dict[str, Any]]]:
        destination = self._normalize_metric(metric)
        if seconds < 0:
            raise ValueError("seconds must be non-negative")

        cutoff = time.time() - seconds
        with self._lock:
            raw_metric_nodes = self._raw_metric_nodes.get(destination)
            if raw_metric_nodes is None:
                raise MetricSubscriptionError(
                    f"Metric '{metric}' is not subscribed in raw mode"
                )

            results: dict[str, list[dict[str, Any]]] = {}
            for node in raw_metric_nodes:
                node_subscription = self._raw_node_subscriptions.get(node)
                if node_subscription is None:
                    continue
                samples = node_subscription.samples_by_destination.get(
                    destination,
                    deque(maxlen=RAW_MAX_SAMPLES_PER_RAW_METRIC),
                )
                matching_samples = [
                    sample for sample in samples if sample.timestamp >= cutoff
                ]
                remaining_samples = [
                    sample for sample in samples if sample.timestamp < cutoff
                ]
                node_subscription.samples_by_destination[destination] = deque(
                    remaining_samples,
                    maxlen=samples.maxlen,
                )
                results[node] = [
                    {"timestamp": sample.timestamp, "value": sample.value}
                    for sample in matching_samples
                ]

        return results

    def _block_nodes(self, destination: str, metric: str, blocked_nodes: set[str]) -> None:
        with self._lock:
            subscription = self._subscriptions.get(destination)
            if subscription is None:
                raise MetricSubscriptionError(f"Metric '{metric}' is not subscribed")
            subscription.blocked_nodes.update(blocked_nodes)
            filtered_samples = [
                sample for sample in subscription.samples if sample.node not in blocked_nodes
            ]
            subscription.samples = deque(filtered_samples, maxlen=MAX_SAMPLES_PER_METRIC)

    def _build_metric_handler(self):
        def handle_message(frame: object) -> None:
            body = getattr(frame, "body", frame)
            destination = self._extract_destination(frame)
            if destination is None:
                return
            value, timestamp, node = _parse_metric_payload(
                body,
                self._extract_headers(frame),
            )

            with self._lock:
                subscription = self._subscriptions.get(destination)
                if subscription is None:
                    return
                if node is not None and node in subscription.blocked_nodes:
                    return
                subscription.samples.append(
                    _MetricSample(timestamp=timestamp, value=value, node=node)
                )

        return handle_message

    def _build_raw_metric_handler(self, target_node: str):
        def handle_message(frame: object) -> None:
            resolved_destination = self._extract_destination(frame)
            if resolved_destination is None:
                return

            body = getattr(frame, "body", frame)
            value, timestamp, node = _parse_metric_payload(
                body,
                self._extract_headers(frame),
            )
            if not self._should_store_raw_sample(target_node, node):
                return

            with self._lock:
                node_subscription = self._raw_node_subscriptions.get(target_node)
                if node_subscription is None:
                    return
                samples = node_subscription.samples_by_destination.get(resolved_destination)
                if samples is None:
                    return
                samples.append(
                    _RawMetricSample(timestamp=timestamp, value=value)
                )

        return handle_message

    def _run_raw_file_source(
        self,
        node: str,
        *,
        stop_event: threading.Event,
    ) -> None:
        states: dict[str, _FileReplayState] = {}

        while not stop_event.is_set():
            monotonic_now = time.monotonic()
            wall_now = time.time()
            emissions: list[tuple[str, _FileMetricDefinition, float, Any]] = []
            next_due_at: float | None = None

            with self._lock:
                node_subscription = self._raw_node_subscriptions.get(node)
                if node_subscription is None or node_subscription.source_kind != "file":
                    return
                definitions = dict(node_subscription.file_definitions)

            for destination in set(states) - set(definitions):
                states.pop(destination, None)

            for destination, definition in definitions.items():
                state = states.get(destination)
                if state is None or state.definition != definition:
                    state = _FileReplayState(
                        definition=definition,
                        point_index=0,
                        cycle_started_at=monotonic_now,
                    )
                    states[destination] = state

                if state.point_index < 0:
                    continue

                due_at = (
                    state.cycle_started_at
                    + definition.points[state.point_index].offset_seconds
                )
                while due_at <= monotonic_now:
                    point = definition.points[state.point_index]
                    sample_timestamp = wall_now + (due_at - monotonic_now)
                    emissions.append(
                        (destination, definition, sample_timestamp, point.value)
                    )

                    state.point_index += 1
                    if state.point_index == len(definition.points):
                        if not definition.repeat:
                            state.point_index = -1
                            break
                        state.point_index = 0
                        state.cycle_started_at += definition.cycle_duration_seconds

                    due_at = (
                        state.cycle_started_at
                        + definition.points[state.point_index].offset_seconds
                    )

                if state.point_index >= 0:
                    next_due_at = (
                        due_at if next_due_at is None else min(next_due_at, due_at)
                    )

            if emissions:
                with self._lock:
                    node_subscription = self._raw_node_subscriptions.get(node)
                    if node_subscription is None:
                        return
                    for destination, definition, timestamp, value in emissions:
                        if (
                            node_subscription.file_definitions.get(destination)
                            != definition
                        ):
                            continue
                        samples = node_subscription.samples_by_destination.get(destination)
                        if samples is not None:
                            samples.append(
                                _RawMetricSample(timestamp=timestamp, value=value)
                            )

            wait_seconds = 0.1
            if next_due_at is not None:
                wait_seconds = min(wait_seconds, max(0.0, next_due_at - time.monotonic()))
            stop_event.wait(wait_seconds)

    def _unsubscribe_metric_raw(
        self,
        metric: str,
        destination: str,
        nodes: list[str] | None,
    ) -> None:
        if nodes is None:
            normalized_nodes = None
        else:
            normalized_nodes = self._normalize_nodes(nodes)

        with self._lock:
            raw_metric_nodes = self._raw_metric_nodes.get(destination)
            if raw_metric_nodes is None:
                raise MetricSubscriptionError(
                    f"Metric '{metric}' is not subscribed in raw mode"
                )

            if normalized_nodes is None:
                target_nodes = list(raw_metric_nodes)
            else:
                missing_nodes = [
                    current_node
                    for current_node in normalized_nodes
                    if current_node not in raw_metric_nodes
                ]
                if missing_nodes:
                    missing_nodes_text = ", ".join(sorted(missing_nodes))
                    raise MetricSubscriptionError(
                        f"Metric '{metric}' is not subscribed for nodes: {missing_nodes_text}"
                    )
                target_nodes = normalized_nodes

            nodes_to_stop: list[tuple[str, str]] = []
            for current_node in target_nodes:
                node_subscription = self._raw_node_subscriptions.get(current_node)
                if node_subscription is None:
                    continue

                if node_subscription.destinations == {destination}:
                    nodes_to_stop.append((current_node, node_subscription.thread_name))
                    continue

                node_subscription.samples_by_destination.pop(
                    destination,
                    deque(maxlen=RAW_MAX_SAMPLES_PER_RAW_METRIC),
                )
                node_subscription.destinations.discard(destination)
                node_subscription.file_definitions.pop(destination, None)
                raw_metric_nodes.discard(current_node)

            if not raw_metric_nodes and not nodes_to_stop:
                self._raw_metric_nodes.pop(destination, None)

        stopped_nodes: set[str] = set()
        try:
            for current_node, thread_name in nodes_to_stop:
                self._thread_manager.stop_listener_thread(
                    thread_name
                )
                stopped_nodes.add(current_node)
        except ThreadManagementError as error:
            with self._lock:
                self._finalize_stopped_raw_nodes(destination, stopped_nodes)

            raise MetricSubscriptionError(
                f"Failed to unsubscribe from metric '{metric}' in raw mode: {error}"
            ) from error

        with self._lock:
            self._finalize_stopped_raw_nodes(destination, stopped_nodes)

    def _clear_stale_listener_thread(self, thread_name: str) -> None:
        thread_state = self._thread_manager.list_threads().get(thread_name)
        if thread_state is not False:
            return

        try:
            self._thread_manager.stop_listener_thread(thread_name, timeout=0)
        except ThreadManagementError as error:
            raise MetricSubscriptionError(
                f"Failed to restart listener '{thread_name}': {error}"
            ) from error

    def _finalize_stopped_raw_nodes(
        self,
        destination: str,
        stopped_nodes: set[str],
    ) -> None:
        if not stopped_nodes:
            return

        raw_metric_nodes = self._raw_metric_nodes.get(destination)
        for current_node in stopped_nodes:
            node_subscription = self._raw_node_subscriptions.get(current_node)
            if node_subscription is None:
                continue

            node_subscription.samples_by_destination.pop(destination, None)
            node_subscription.destinations.discard(destination)
            node_subscription.file_definitions.pop(destination, None)
            if raw_metric_nodes is not None:
                raw_metric_nodes.discard(current_node)
            if not node_subscription.destinations:
                self._raw_node_subscriptions.pop(current_node, None)

        if raw_metric_nodes is not None and not raw_metric_nodes:
            self._raw_metric_nodes.pop(destination, None)

    @classmethod
    def _normalize_metric(cls, metric: str) -> str:
        cls._validate_metric(metric)
        return cls._resolve_destination(metric)

    @classmethod
    def _normalize_nodes(cls, nodes: list[str]) -> list[str]:
        if not nodes:
            raise ValueError("nodes must contain at least one non-empty node identifier")

        normalized_nodes: list[str] = []
        seen_nodes: set[str] = set()
        for node in nodes:
            if not isinstance(node, str):
                raise ValueError("nodes must contain at least one non-empty node identifier")
            normalized_node = node.strip()
            if not normalized_node:
                raise ValueError("nodes must contain at least one non-empty node identifier")
            if normalized_node in seen_nodes:
                continue
            seen_nodes.add(normalized_node)
            normalized_nodes.append(normalized_node)

        return normalized_nodes

    @classmethod
    def _resolve_raw_nodes(
        cls,
        node: list[str] | str,
    ) -> list[str]:
        if isinstance(node, str):
            selector = node.strip().lower()
            if selector == "all":
                try:
                    return cls._normalize_nodes(get_vm_private_ips())
                except Exception as error:
                    raise MetricSubscriptionError(
                        f"Failed to resolve raw metric nodes for 'all': {error}"
                    ) from error
            if selector == "local":
                try:
                    return cls._normalize_nodes([get_current_vm_private_ip()])
                except Exception as error:
                    raise MetricSubscriptionError(
                        f"Failed to resolve raw metric nodes for 'local': {error}"
                    ) from error
            return cls._normalize_nodes([node])

        return cls._normalize_nodes(node)

    @classmethod
    def _load_file_metric_definitions(
        cls,
        source_file: str | Path,
        destination: str,
        node: list[str] | str,
    ) -> tuple[list[str], dict[str, _FileMetricDefinition]]:
        path = Path(source_file).expanduser()
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise MetricSubscriptionError(
                f"Failed to read raw metric source file '{path}': {error}"
            ) from error

        if not isinstance(document, dict):
            raise MetricSubscriptionError(
                "Raw metric source file must contain a JSON object"
            )
        if document.get("version") != RAW_METRIC_FILE_SCHEMA_VERSION:
            raise MetricSubscriptionError(
                f"Raw metric source file version must be "
                f"{RAW_METRIC_FILE_SCHEMA_VERSION}"
            )

        raw_nodes = document.get("nodes")
        if not isinstance(raw_nodes, list) or not raw_nodes:
            raise MetricSubscriptionError(
                "Raw metric source file 'nodes' must be a non-empty array"
            )

        metrics_by_node: dict[str, dict[str, Any]] = {}
        for raw_node in raw_nodes:
            if not isinstance(raw_node, dict):
                raise MetricSubscriptionError(
                    "Each raw metric source node must be a JSON object"
                )
            node_id = raw_node.get("node_id")
            if not isinstance(node_id, str) or not node_id.strip():
                raise MetricSubscriptionError(
                    "Each raw metric source node must have a non-empty 'node_id'"
                )
            normalized_node_id = node_id.strip()
            if normalized_node_id in metrics_by_node:
                raise MetricSubscriptionError(
                    f"Duplicate node_id '{normalized_node_id}' in raw metric source file"
                )
            metrics = raw_node.get("metrics")
            if not isinstance(metrics, dict):
                raise MetricSubscriptionError(
                    f"Raw metric source node '{normalized_node_id}' must have a "
                    "'metrics' object"
                )
            normalized_metrics: dict[str, Any] = {}
            for raw_metric, raw_definition in metrics.items():
                if not isinstance(raw_metric, str) or not raw_metric.strip():
                    raise MetricSubscriptionError(
                        f"Raw metric source node '{normalized_node_id}' has an "
                        "invalid metric name"
                    )
                normalized_metric = cls._normalize_metric(raw_metric)
                if normalized_metric in normalized_metrics:
                    raise MetricSubscriptionError(
                        f"Metric '{normalized_metric}' is configured more than once "
                        f"for node '{normalized_node_id}'"
                    )
                normalized_metrics[normalized_metric] = raw_definition
            metrics_by_node[normalized_node_id] = normalized_metrics

        if isinstance(node, str) and node.strip().lower() == "all":
            normalized_nodes = [
                node_id
                for node_id, metrics in metrics_by_node.items()
                if destination in metrics
            ]
            if not normalized_nodes:
                raise MetricSubscriptionError(
                    f"Metric '{destination}' is not configured for any node in "
                    f"raw metric source file '{path}'"
                )
        elif isinstance(node, str) and node.strip().lower() == "local":
            raise MetricSubscriptionError(
                "The 'local' selector is not supported with source_file; "
                "use an explicit node_id or 'all'"
            )
        elif isinstance(node, str):
            normalized_nodes = cls._normalize_nodes([node])
        else:
            normalized_nodes = cls._normalize_nodes(node)

        definitions: dict[str, _FileMetricDefinition] = {}
        for node_id in normalized_nodes:
            metrics = metrics_by_node.get(node_id)
            if metrics is None:
                raise MetricSubscriptionError(
                    f"Node '{node_id}' is not present in raw metric source file '{path}'"
                )

            raw_definition = metrics.get(destination)
            if raw_definition is None:
                raise MetricSubscriptionError(
                    f"Metric '{destination}' is not configured for node '{node_id}' "
                    f"in raw metric source file '{path}'"
                )
            definitions[node_id] = cls._parse_file_metric_definition(
                raw_definition,
                node_id,
                destination,
            )

        return normalized_nodes, definitions

    @classmethod
    def _parse_file_metric_definition(
        cls,
        raw_definition: Any,
        node_id: str,
        destination: str,
    ) -> _FileMetricDefinition:
        context = f"metric '{destination}' for node '{node_id}'"
        if not isinstance(raw_definition, dict):
            raise MetricSubscriptionError(f"Raw file {context} must be a JSON object")

        repeat = raw_definition.get("repeat", True)
        if not isinstance(repeat, bool):
            raise MetricSubscriptionError(f"Raw file {context} 'repeat' must be boolean")

        has_values = "values" in raw_definition
        has_samples = "samples" in raw_definition
        if has_values == has_samples:
            raise MetricSubscriptionError(
                f"Raw file {context} must contain exactly one of 'values' or 'samples'"
            )

        if has_values:
            values = raw_definition["values"]
            if not isinstance(values, list) or not values:
                raise MetricSubscriptionError(
                    f"Raw file {context} 'values' must be a non-empty array"
                )
            interval = cls._positive_number(
                raw_definition.get("interval_seconds"),
                f"Raw file {context} 'interval_seconds'",
            )
            points = tuple(
                _FileReplayPoint(offset_seconds=index * interval, value=value)
                for index, value in enumerate(values)
            )
            return _FileMetricDefinition(
                points=points,
                repeat=repeat,
                cycle_duration_seconds=len(points) * interval,
            )

        raw_samples = raw_definition["samples"]
        if not isinstance(raw_samples, list) or not raw_samples:
            raise MetricSubscriptionError(
                f"Raw file {context} 'samples' must be a non-empty array"
            )

        points_list: list[_FileReplayPoint] = []
        previous_offset = -1.0
        for raw_sample in raw_samples:
            if not isinstance(raw_sample, dict) or "value" not in raw_sample:
                raise MetricSubscriptionError(
                    f"Raw file {context} samples must be objects containing 'value'"
                )
            offset = cls._non_negative_number(
                raw_sample.get("offset_seconds"),
                f"Raw file {context} sample 'offset_seconds'",
            )
            if offset <= previous_offset:
                raise MetricSubscriptionError(
                    f"Raw file {context} sample offsets must be strictly increasing"
                )
            points_list.append(
                _FileReplayPoint(offset_seconds=offset, value=raw_sample["value"])
            )
            previous_offset = offset

        if points_list[0].offset_seconds != 0:
            raise MetricSubscriptionError(
                f"Raw file {context} first sample offset must be 0"
            )

        if repeat:
            cycle_duration = cls._positive_number(
                raw_definition.get("cycle_duration_seconds"),
                f"Raw file {context} 'cycle_duration_seconds'",
            )
            if cycle_duration <= points_list[-1].offset_seconds:
                raise MetricSubscriptionError(
                    f"Raw file {context} 'cycle_duration_seconds' must be greater "
                    "than the final sample offset"
                )
        else:
            cycle_duration = points_list[-1].offset_seconds

        return _FileMetricDefinition(
            points=tuple(points_list),
            repeat=repeat,
            cycle_duration_seconds=cycle_duration,
        )

    @staticmethod
    def _positive_number(value: Any, label: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise MetricSubscriptionError(f"{label} must be a positive number")
        return float(value)

    @staticmethod
    def _non_negative_number(value: Any, label: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise MetricSubscriptionError(f"{label} must be a non-negative number")
        return float(value)

    @staticmethod
    def _resolve_raw_cache_size(cache_size: int | None) -> int:
        if cache_size is None:
            return RAW_MAX_SAMPLES_PER_RAW_METRIC
        if cache_size <= 0:
            raise ValueError("cache_size must be a positive integer")
        return cache_size

    @classmethod
    def _extract_destination(cls, frame: object) -> str | None:
        headers = cls._extract_headers(frame)
        destination = headers.get("destination") if isinstance(headers, dict) else None
        if not isinstance(destination, str) or not destination.strip():
            return None
        return cls._resolve_destination(destination)

    @staticmethod
    def _extract_headers(frame: object) -> dict[str, Any]:
        headers = getattr(frame, "headers", {})
        return headers if isinstance(headers, dict) else {}

    @staticmethod
    def _validate_metric(metric: str) -> None:
        if not metric or not metric.strip():
            raise ValueError("metric must be a non-empty string")

    @staticmethod
    def _resolve_destination(metric: str) -> str:
        normalized_metric = metric.strip()
        if normalized_metric.startswith("/"):
            return normalized_metric
        return f"/topic/{normalized_metric}"

    @staticmethod
    def _build_raw_thread_name(node: str) -> str:
        return f"{RAW_LISTENER_THREAD_NAME_PREFIX}:{node}"

    @staticmethod
    def _build_raw_file_thread_name(node: str) -> str:
        return f"{RAW_FILE_THREAD_NAME_PREFIX}:{node}"

    @staticmethod
    def _should_store_raw_sample(target_node: str, parsed_node: str | None) -> bool:
        if parsed_node is None:
            return True
        if parsed_node == target_node:
            return True
        if MetricSubscriptionManager._is_ip_address(target_node):
            return True
        return False

    @staticmethod
    def _is_ip_address(value: str) -> bool:
        try:
            ip_address(value)
        except ValueError:
            return False
        return True


_default_metric_subscription_manager = MetricSubscriptionManager()


def subscribe_metric(metric: str) -> str:
    """Subscribe to a metric destination and start a listener thread for it."""
    return _default_metric_subscription_manager.subscribe_metric(metric)


def subscribe_metric_raw(
    metric: str,
    node: list[str] | str,
    cache_size: int | None = None,
    *,
    source_file: str | Path | None = None,
) -> dict[str, str]:
    """Subscribe to live raw metric streams or replay them from a JSON source file."""
    return _default_metric_subscription_manager.subscribe_metric_raw(
        metric,
        node,
        cache_size=cache_size,
        source_file=source_file,
    )


def query_metric_values(metric: str) -> list[Any]:
    """Return and consume all buffered metric values for ``metric``."""
    return _default_metric_subscription_manager.query_metric_values(metric)


def query_metric_values_raw(
    metric: str,
    seconds: int,
) -> dict[str, list[dict[str, Any]]]:
    """Return and consume raw metric values received within the last ``seconds`` seconds."""
    return _default_metric_subscription_manager.query_metric_values_raw(metric, seconds)


def unsubscribe_metric(metric: str, nodes: list[str] | None = None) -> None:
    """Stop a metric listener, stop raw node listeners, or block nodes from a standard subscription."""
    _default_metric_subscription_manager.unsubscribe_metric(metric, nodes=nodes)
