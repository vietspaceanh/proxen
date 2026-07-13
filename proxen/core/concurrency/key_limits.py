"""Per-key rate limiting: concurrency, request-window, and token-window limits.

Checked *before* the global gate in `acquire()`.  If a per-key limit is
exceeded, `RateLimitExceeded` is raised immediately - the request is
never queued.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .types import InflightSlot, KeyLimits, RateLimitExceeded, _Window

log = logging.getLogger(__name__)


class KeyLimiter:
    """Per-key rate limits: concurrency, request-window, token-window.

    State: `key_inflight` (active count per key), `key_limits` (config),
    `req_windows` and `tok_windows` (sliding-window counters).
    """

    def __init__(self) -> None:
        self.key_inflight: dict[str, int] = {}
        self.key_limits: dict[str, KeyLimits] = {}
        self.req_windows: dict[str, _Window] = {}
        self.tok_windows: dict[str, _Window] = {}
        self._on_change: Callable[[], None] | None = None

    def set_on_change(self, cb: Callable[[], None]) -> None:
        self._on_change = cb

    def _notify(self) -> None:
        cb = self._on_change
        if cb is not None:
            try:
                cb()
            except Exception:
                log.debug("on_change callback error", exc_info=True)

    # ─── Sliding-window helpers ─────────────────────────────────────

    @staticmethod
    def _window_allows(store: dict, key_id: str, limit: int, window_s: float) -> bool:
        w = store.get(key_id)
        if w is None:
            return True
        return w.allows(limit, window_s)

    @staticmethod
    def _window_record(store: dict, key_id: str, value: int = 1) -> float:
        w = store.get(key_id)
        if w is None:
            w = _Window()
            store[key_id] = w
        return w.record(value)

    @staticmethod
    def _window_rollback(store: dict, key_id: str, ts: float) -> None:
        w = store.get(key_id)
        if w is not None:
            w.rollback(ts)

    # ─── Check / record / rollback ──────────────────────────────────

    def check(self, key_id: str) -> float | None:
        """Check per-key limits.  Raises :class:`RateLimitExceeded` if exceeded.

        Returns `req_ts` for rollback (or `None` if no request window).
        """
        limits = self.key_limits.get(key_id)
        if limits is None:
            return None
        req_ts: float | None = None
        if limits.max_inflight is not None:
            if self.key_inflight.get(key_id, 0) >= limits.max_inflight:
                raise RateLimitExceeded("concurrency", limits.max_inflight)
        if limits.max_requests is not None and limits.max_requests_window_s is not None:
            if not self._window_allows(
                self.req_windows, key_id,
                limits.max_requests, limits.max_requests_window_s,
            ):
                raise RateLimitExceeded("requests", limits.max_requests)
            req_ts = self._window_record(self.req_windows, key_id)
        if limits.max_tokens is not None and limits.max_tokens_window_s is not None:
            if not self._window_allows(
                self.tok_windows, key_id,
                limits.max_tokens, limits.max_tokens_window_s,
            ):
                if req_ts is not None:
                    self._window_rollback(self.req_windows, key_id, req_ts)
                raise RateLimitExceeded("tokens", limits.max_tokens)
        return req_ts

    def record(self, key_id: str) -> None:
        self.key_inflight[key_id] = self.key_inflight.get(key_id, 0) + 1

    def rollback(self, key_id: str, req_ts: float | None) -> None:
        self.key_inflight[key_id] = self.key_inflight.get(key_id, 0) - 1
        if req_ts is not None:
            self._window_rollback(self.req_windows, key_id, req_ts)

    def on_release(self, slot: InflightSlot) -> None:
        """Decrement inflight and record token usage on slot release."""
        key = slot.key_id
        if key in self.key_inflight:
            self.key_inflight[key] -= 1
            if self.key_inflight[key] <= 0:
                del self.key_inflight[key]
        limits = self.key_limits.get(key)
        if (
            limits is not None
            and limits.max_tokens is not None
            and limits.max_tokens_window_s is not None
        ):
            tokens = slot.input_tokens + slot.output_tokens
            if tokens > 0:
                self._window_record(self.tok_windows, key, tokens)

    # ─── Configuration ─────────────────────────────────────────────

    def set_key_limits(self, key_id: str, limits: KeyLimits) -> None:
        self.key_limits[key_id] = limits
        self._notify()

    def get_key_limits(self, key_id: str) -> KeyLimits:
        return self.key_limits.get(key_id, KeyLimits())

    def remove_key_limits(self, key_id: str) -> None:
        self.key_limits.pop(key_id, None)
        self._notify()

    def key_snapshot(self) -> list[dict]:
        out = []
        for key_id, limits in self.key_limits.items():
            out.append(
                {
                    "key_id": key_id,
                    "inflight": self.key_inflight.get(key_id, 0),
                    "max_inflight": limits.max_inflight,
                    "max_requests": limits.max_requests,
                    "max_requests_window_s": limits.max_requests_window_s,
                    "max_tokens": limits.max_tokens,
                    "max_tokens_window_s": limits.max_tokens_window_s,
                }
            )
        return out
