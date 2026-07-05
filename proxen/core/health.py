from __future__ import annotations

import time
from typing import Hashable


class HealthCheck:
    """Per-target circuit breaker with exponential backoff.

    Isolated tracker with no knowledge of upstreams, models, or routing.
    Callers record outcomes by arbitrary hashable key and ask whether a
    target should be tried (pass 1) or retried as last-resort (pass 2).

    Derived states (never stored explicitly):
    - healthy:  failure points below threshold
    - failing:  points >= threshold, backoff not yet elapsed
    - probing:  points >= threshold, backoff elapsed
    """

    _MAX = 300.0

    def __init__(self, failure_threshold: int = 5, backoff_base: float = 5.0) -> None:
        self._threshold = failure_threshold if failure_threshold > 0 else float("inf")
        self._base = backoff_base
        self._points: dict[Hashable, int] = {}
        self._probes: dict[Hashable, int] = {}
        self._next: dict[Hashable, float] = {}

    def should_try(self, key: Hashable) -> bool:
        return self._points.get(key, 0) < self._threshold

    def should_retry(self, key: Hashable) -> bool:
        return (
            self._points.get(key, 0) >= self._threshold
            and time.monotonic() >= self._next.get(key, 0.0)
        )

    def record_failure(self, key: Hashable, *, weight: int = 1) -> None:
        now = time.monotonic()
        if self._points.get(key, 0) >= self._threshold:
            k = self._probes.get(key, 0) + 1
            self._probes[key] = k
            self._next[key] = now + min(self._base * (2 ** k), self._MAX)
        else:
            pts = self._points.get(key, 0) + weight
            self._points[key] = pts
            if pts >= self._threshold:
                self._probes[key] = 0
                self._next[key] = now + self._base

    def record_success(self, key: Hashable) -> None:
        self._points.pop(key, None)
        self._probes.pop(key, None)
        self._next.pop(key, None)

    def state(self, key: Hashable) -> str:
        if self._points.get(key, 0) < self._threshold:
            return "healthy"
        return "probing" if time.monotonic() >= self._next.get(key, 0.0) else "failing"

    def failing_states(self) -> dict[Hashable, str]:
        now = time.monotonic()
        return {
            k: ("probing" if now >= self._next.get(k, 0.0) else "failing")
            for k, pts in self._points.items()
            if pts >= self._threshold
        }
