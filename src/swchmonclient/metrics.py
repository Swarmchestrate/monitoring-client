import json
import logging
import threading
import time
from ipaddress import ip_address
from collections import deque
from dataclasses import dataclass, field
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


@dataclass
class _RawNodeSubscription:
    thread_name: str
    destinations: set[str] = field(default_factory=set)
    samples_by_destination: dict[str, Deque[_RawMetricSample]] = field(default_factory=dict)


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
    ) -> dict[str, str]:
        destination = self._normalize_metric(metric)
        normalized_nodes = self._resolve_raw_nodes(node)
        resolved_cache_size = self._resolve_raw_cache_size(cache_size)
        if isinstance(node, str) and node.strip().lower() in {"all", "local"}:
            logger.info(
                "Subscribing to raw metric '%s' using node selector '%s' resolved to nodes %s",
                metric,
                node.strip().lower(),
                normalized_nodes,
            )

        with self._lock:
            if destination in self._subscriptions:
                raise MetricSubscriptionError(
                    f"Metric '{metric}' is already subscribed in standard mode"
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
                        thread_name=self._build_raw_thread_name(current_node)
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

                thread_name = node_subscription.thread_name
                thread_names[current_node] = thread_name
                rollback_records.append((current_node, node_created, destination_added))

                thread_state = self._thread_manager.list_threads().get(thread_name)
                if thread_state is True:
                    continue
                planned_nodes.append((current_node, thread_name, thread_state is False))

        started_nodes: list[str] = []
        try:
            for current_node, thread_name, is_stale in planned_nodes:
                if is_stale:
                    self._clear_stale_listener_thread(thread_name)
                listener = CallbackStompListener(
                    self._build_raw_metric_handler(current_node)
                )
                self._thread_manager.start_listener_thread(
                    name=thread_name,
                    host=current_node,
                    destinations_provider=lambda node=current_node: self.get_raw_destinations(node),
                    listener=listener,
                )
                started_nodes.append(current_node)
        except (MetricSubscriptionError, ThreadManagementError) as error:
            for started_node in started_nodes:
                started_thread_name = self._build_raw_thread_name(started_node)
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
                    maxlen=RAW_MAX_SAMPLES_PER_RAW_METRIC,
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
) -> dict[str, str]:
    """Subscribe to raw metric streams for explicit nodes, ``all`` nodes, or the ``local`` node."""
    return _default_metric_subscription_manager.subscribe_metric_raw(
        metric,
        node,
        cache_size=cache_size,
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
