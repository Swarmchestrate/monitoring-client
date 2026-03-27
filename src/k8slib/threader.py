from __future__ import annotations

import inspect
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict

from .exceptions import ThreadManagementError


@dataclass
class _ManagedThread:
    thread: threading.Thread
    stop_event: threading.Event


class MonitoringThreadManager:
    """Create, track, and stop cooperative monitoring threads."""

    def __init__(self) -> None:
        self._threads: Dict[str, _ManagedThread] = {}
        self._errors: Dict[str, Exception] = {}
        self._lock = threading.Lock()

    def start_monitoring_thread(
        self,
        name: str,
        target: Callable[..., Any],
        *args: Any,
        daemon: bool = True,
        **kwargs: Any,
    ) -> str:
        with self._lock:
            if name in self._threads:
                raise ThreadManagementError(f"Thread '{name}' already exists")

            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._build_runner(name, target, stop_event, args, kwargs),
                name=name,
                daemon=daemon,
            )
            self._threads[name] = _ManagedThread(thread=thread, stop_event=stop_event)
            thread.start()
            return name

    def stop_monitoring_thread(self, name: str, timeout: float = 30.0) -> None:
        with self._lock:
            managed = self._threads.get(name)
            if managed is None:
                raise ThreadManagementError(f"Thread '{name}' not found")
            managed.stop_event.set()
            thread = managed.thread

        thread.join(timeout)
        if thread.is_alive():
            raise ThreadManagementError(
                f"Thread '{name}' did not stop within {timeout} seconds. "
                "Ensure your monitoring function cooperates with stop_event."
            )

        with self._lock:
            self._threads.pop(name, None)
            error = self._errors.pop(name, None)

        if error is not None:
            raise ThreadManagementError(f"Thread '{name}' exited with error: {error}") from error

    def stop_all(self, timeout_per_thread: float = 30.0) -> None:
        for name in list(self.list_threads()):
            self.stop_monitoring_thread(name, timeout=timeout_per_thread)

    def list_threads(self) -> Dict[str, bool]:
        with self._lock:
            return {name: managed.thread.is_alive() for name, managed in self._threads.items()}

    def _build_runner(
        self,
        name: str,
        target: Callable[..., Any],
        stop_event: threading.Event,
        args: Any,
        kwargs: Any,
    ) -> Callable[[], None]:
        signature = inspect.signature(target)
        accepts_stop_event = "stop_event" in signature.parameters

        def runner() -> None:
            try:
                if accepts_stop_event:
                    if "stop_event" in kwargs:
                        target(*args, **kwargs)
                    else:
                        target(*args, stop_event=stop_event, **kwargs)
                else:
                    while not stop_event.is_set():
                        target(*args, **kwargs)
            except Exception as error:  # pragma: no cover - surfaced on stop
                with self._lock:
                    self._errors[name] = error

        return runner
