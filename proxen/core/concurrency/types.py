"""Shared types for the admission-control system.

All types live here so that `gate.py`, `key_limits.py` and `provider.py`
can import from a single dependency-free module - no circular imports.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field


# ─── Exceptions ─────────────────────────────────────────────────────


class QueueOverflow(Exception):
    """Raised when the waiting queue is full (caller should return 429)."""


class QueueTimeout(Exception):
    """Raised when a waiter exceeded the configured queue timeout (-> 408)."""


class RateLimitExceeded(Exception):
    """Raised when a per-key rate limit is exceeded (caller should return 429)."""

    def __init__(self, limit_type: str, limit: int) -> None:
        self.limit_type = limit_type
        self.limit = limit
        super().__init__(f"{limit_type} limit ({limit}) exceeded")


# ─── Sliding-window counter ─────────────────────────────────────────


@dataclass
class _Window:
    """Sliding-window counter with amortised O(1) admission checks."""

    entries: deque[tuple[float, int]] = field(default_factory=deque)
    total: int = 0

    def _evict(self, window_s: float) -> None:
        cutoff = time.monotonic() - window_s
        while self.entries and self.entries[0][0] < cutoff:
            self.total -= self.entries.popleft()[1]

    def allows(self, limit: int, window_s: float) -> bool:
        self._evict(window_s)
        return self.total < limit

    def record(self, value: int = 1) -> float:
        now = time.monotonic()
        self.entries.append((now, value))
        self.total += value
        return now

    def rollback(self, ts: float) -> None:
        for i, (t, v) in enumerate(self.entries):
            if t == ts:
                del self.entries[i]
                self.total -= v
                return


# ─── Per-key limit configuration ────────────────────────────────────


@dataclass
class KeyLimits:
    """Per-key rate limits.  `None` means "no limit"."""

    max_inflight: int | None = None
    max_requests: int | None = None
    max_requests_window_s: float | None = None
    max_tokens: int | None = None
    max_tokens_window_s: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> KeyLimits:
        return cls(
            max_inflight=data.get("max_inflight"),
            max_requests=data.get("max_requests"),
            max_requests_window_s=data.get("max_requests_window_s"),
            max_tokens=data.get("max_tokens"),
            max_tokens_window_s=data.get("max_tokens_window_s"),
        )

    def validate(self) -> None:
        if (self.max_requests is None) != (self.max_requests_window_s is None):
            raise ValueError(
                "max_requests and max_requests_window_s must be set together"
            )
        if (self.max_tokens is None) != (self.max_tokens_window_s is None):
            raise ValueError(
                "max_tokens and max_tokens_window_s must be set together"
            )
        for name, val in (
            ("max_inflight", self.max_inflight),
            ("max_requests", self.max_requests),
            ("max_tokens", self.max_tokens),
        ):
            if val is not None and val < 0:
                raise ValueError(f"{name} must be non-negative")
        for name, val in (
            ("max_requests_window_s", self.max_requests_window_s),
            ("max_tokens_window_s", self.max_tokens_window_s),
        ):
            if val is not None and val <= 0:
                raise ValueError(f"{name} must be positive")


# ─── In-flight slot ─────────────────────────────────────────────────


@dataclass
class InflightSlot:
    """Mutable handle held by the endpoint layer while a request is in-flight."""

    seq_id: int = 0
    key_id: str = ""
    model: str = ""
    upstream: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    last_byte_time: float = 0.0
    wall_start: float = 0.0
    ttft: float = 0.0
    phase: str = "requesting"
    _released: bool = field(default=False)
    _idle_handle: asyncio.TimerHandle | None = field(default=None, repr=False)
    _idle_notified: bool = field(default=False, repr=False)
    _notify_cb: Callable[[], None] | None = field(default=None, repr=False)

    def notify(self) -> None:
        cb = self._notify_cb
        if cb is not None:
            cb()

    def mark_receiving(self) -> None:
        self.phase = "receiving"
        self.notify()

    def reset_idle(self) -> None:
        if self._idle_notified:
            self._idle_notified = False
            self.notify()

    def record_ttft(self, ttft: float) -> None:
        self.ttft = ttft
        self.notify()


# ─── Gate snapshot ──────────────────────────────────────────────────


@dataclass
class GateSnapshot:
    active: int
    waiting: int
    max_inflight: int
    max_waiting: int
    inflight: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)
