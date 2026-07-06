from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field

log = logging.getLogger(__name__)

_IDLE_THRESHOLD = 30.0  # seconds without upstream data before "no signal"


# ─── Concurrency gate exceptions ─────────────────────────────────────


class QueueOverflow(Exception):
    """Raised when the waiting queue is full (caller should return 429)."""


class QueueTimeout(Exception):
    """Raised when a waiter exceeded the configured queue timeout (-> 408)."""


class RateLimitExceeded(Exception):
    """Raised when a per-key rate limit is exceeded (caller should return 429).

    `limit_type` is one of `"concurrency"`, `"requests"`, `"tokens"`.
    """

    def __init__(self, limit_type: str, limit: int) -> None:
        self.limit_type = limit_type
        self.limit = limit
        super().__init__(f"{limit_type} limit ({limit}) exceeded")


# ─── Sliding-window counter ──────────────────────────────────────────


@dataclass
class _Window:
    """Sliding-window counter with amortised O(1) admission checks.

    Stores `(timestamp, value)` entries, `value` is `1` for request
    counting and the token delta for token counting.  A running `total`
    keeps the admission check O(1) amortised (each entry is appended and
    evicted exactly once). `rollback` is O(n) but only used on the
    rare path where a request passes admission but is then rejected before
    it is actually served (so it does not consume budget).
    """

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


# ─── Per-key limit configuration ─────────────────────────────────────


@dataclass
class KeyLimits:
    """Per-key rate limits.  `None` means "no limit".

    Windowed limits (`max_requests` / `max_tokens`) use a configurable
    rolling window given in seconds, e.g. 200 requests over a
    `5 * 3600` second (5-hour) window.  A windowed limit is only active
    when *both* its `max` and `window_s` fields are set, so the two must
    always be supplied together (see `validate`).
    """

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
        """Raise `ValueError` if the limits are inconsistent.

        Each windowed limit must be specified as a (max, window) pair, and
        every configured value must be positive.
        """
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


# ─── In-flight slot ──────────────────────────────────────────────────


@dataclass
class InflightSlot:
    """Mutable handle held by the endpoint layer while a request is in-flight.

    The proxy fills in `model`, `upstream`, `input_tokens` and
    `output_tokens` once they are known.  The gate's
    `ConcurrencyGate.snapshot` reads these fields to report in-flight
    requests to the dashboard, and `ConcurrencyGate.release` reads
    token counts for per-key token-window tracking.

    `last_byte_time` is updated by the proxy on each received chunk so
    the idle-watch timer can detect "no signal" after 30 s of silence.
    Defaults to `0.0` (no chunks yet), the idle watch treats `0.0` as
    "waiting for first token" and does not flag it as no-signal.

    `wall_start` is the epoch timestamp (`time.time()`) at acquisition
    so the dashboard can compute elapsed time client-side without periodic
    server pushes.

    `phase` tracks the request lifecycle for the dashboard: `"requesting"`
    (POST sent, no upstream response yet) -> `"receiving"` (upstream response
    obtained, body in progress).  `ttft` is populated once the first byte
    streams.

    `_notify_cb` is set by the gate at acquisition so the proxy can trigger a
    dashboard push when a slot field changes mid-flight, without holding a
    gate reference (e.g. from `forward_simple`).

    `_idle_handle` / `_idle_notified` are managed by the gate's idle
    watch.      `_released` makes `ConcurrencyGate.release` idempotent.

    `seq_id` is a process-unique monotonic identifier assigned at acquisition.
    It is used as the dashboard's row `id` so the frontend React key is never
    reused after the slot is garbage-collected (unlike `id(slot)`, which
    returns a memory address that CPython may recycle).
    """

    seq_id: int = 0
    key_id: str = ""
    model: str = ""
    upstream: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    last_byte_time: float = 0.0  # monotonic timestamp of last received chunk
    wall_start: float = 0.0  # epoch timestamp at acquisition (for dashboard)
    ttft: float = 0.0  # seconds to first byte; 0.0 = first byte not yet received
    phase: str = "requesting"  # "requesting" -> "receiving" once response obtained
    _released: bool = field(default=False)
    _idle_handle: asyncio.TimerHandle | None = field(default=None, repr=False)
    _idle_notified: bool = field(default=False, repr=False)
    _notify_cb: Callable[[], None] | None = field(default=None, repr=False)

    def notify(self) -> None:
        """Trigger a dashboard push for this slot's current state."""
        cb = self._notify_cb
        if cb is not None:
            cb()

    def mark_receiving(self) -> None:
        """Transition to the ``receiving`` phase and push an update."""
        self.phase = "receiving"
        self.notify()

    def record_ttft(self, ttft: float) -> None:
        """Record time-to-first-byte and push an update."""
        self.ttft = ttft
        self.notify()


@dataclass
class GateSnapshot:
    active: int
    waiting: int
    max_inflight: int
    max_waiting: int
    inflight: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


# ─── Concurrency gate ────────────────────────────────────────────────


class ConcurrencyGate:
    """Two-tier FIFO gate: at most `max_inflight` active, `max_waiting` queued.

    Beyond both limits, `acquire` raises :class:`QueueOverflow`. Waiters
    are FIFO and time out after `timeout` seconds with :class:`QueueTimeout`.

    Per-key limits (concurrency, requests, tokens) are checked *before* the
    global gate.  If a per-key limit is exceeded, :class:`RateLimitExceeded`
    is raised immediately, the request is never queued.

    Notification contract: every method that mutates observable state
    (`_active`, `_waiting`, `inflight`, `_max_*`, `_key_limits`)
    must end up calling `_notify()`.  Promotion sites delegate to
    `_promote_waiters()` which handles both the promotion and the
    notification.  In-place deque/dict mutations (`_waiting.append`,
    `inflight[...]`, `_key_limits[...]`) call `_notify()` manually.
    """

    def __init__(
        self,
        max_inflight: int,
        max_waiting: int,
        timeout: float,
    ) -> None:
        if max_inflight < 1:
            raise ValueError("max_inflight must be >= 1")
        if max_waiting < 0:
            raise ValueError("max_waiting must be >= 0")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        self._max_inflight = max_inflight
        self._max_waiting = max_waiting
        self._timeout = timeout
        self._active = 0
        self._waiting: deque[asyncio.Future[None]] = deque()
        self.inflight: dict[int, InflightSlot] = {}
        self._seq = 0
        self._on_change: Callable[[], None] | None = None

        # Per-key state.
        self._key_inflight: dict[str, int] = {}
        self._key_limits: dict[str, KeyLimits] = {}
        self._req_windows: dict[str, _Window] = {}
        self._tok_windows: dict[str, _Window] = {}

    def set_on_change(self, cb: Callable[[], None]) -> None:
        """Register a synchronous callback fired on acquire/release so the
        dashboard can push an immediate snapshot instead of waiting for the
        next heartbeat tick."""
        self._on_change = cb

    def _notify(self) -> None:
        cb = self._on_change
        if cb is not None:
            try:
                cb()
            except Exception:
                log.debug("on_change callback error", exc_info=True)

    def _promote_waiters(self) -> None:
        """Promote waiting requests to active slots when capacity is available.

        Resolves as many waiters as the current `_max_inflight` allows,
        incrementing `_active` for each.  Always calls `_notify()` at
        the end so the dashboard reflects the final state.
        """
        while self._active < self._max_inflight and self._waiting:
            future = self._waiting.popleft()
            if future.cancelled() or future.done():
                continue
            self._active += 1
            future.set_result(None)
        self._notify()

    # ── Idle watch (no-signal detection) ──────────────────────────────

    def _schedule_idle_check(self, slot: InflightSlot, delay: float = _IDLE_THRESHOLD) -> None:
        """(Re)schedule the idle timer for *slot* to fire after *delay* seconds."""
        if slot._idle_handle is not None:
            slot._idle_handle.cancel()
        loop = asyncio.get_running_loop()
        slot._idle_handle = loop.call_later(delay, self._on_idle_timeout, slot)

    def _on_idle_timeout(self, slot: InflightSlot) -> None:
        """Timer callback: toggle `_idle_notified` if the no-signal state changed.

        When `last_byte_time` is 0 (waiting for first token), `idle`
        is 0 and the slot is treated as not-yet-idle, the timer simply
        reschedules for another threshold interval.
        """
        now = time.monotonic()
        idle = now - slot.last_byte_time if slot.last_byte_time else 0.0
        is_idle = idle >= _IDLE_THRESHOLD
        if is_idle != slot._idle_notified:
            slot._idle_notified = is_idle
            self._notify()
        self._schedule_idle_check(slot, _IDLE_THRESHOLD if is_idle else _IDLE_THRESHOLD - idle)

    def reset_idle(self, slot: InflightSlot) -> None:
        """Called by the proxy on each upstream chunk.  If the slot was
        flagged as no-signal, clears the flag and pushes a recovery update.
        """
        if slot._idle_notified:
            slot._idle_notified = False
            self._notify()

    @staticmethod
    def _cancel_idle_check(slot: InflightSlot) -> None:
        """Cancel the idle timer and clear the no-signal flag."""
        if slot._idle_handle is not None:
            slot._idle_handle.cancel()
            slot._idle_handle = None
        slot._idle_notified = False

    # ── Per-key limit configuration ──────────────────────────────────

    def set_key_limits(self, key_id: str, limits: KeyLimits) -> None:
        self._key_limits[key_id] = limits
        self._notify()

    def get_key_limits(self, key_id: str) -> KeyLimits:
        return self._key_limits.get(key_id, KeyLimits())

    def remove_key_limits(self, key_id: str) -> None:
        self._key_limits.pop(key_id, None)
        self._notify()

    def key_snapshot(self) -> list[dict]:
        """Per-key inflight counts and configured limits."""
        out = []
        for key_id, limits in self._key_limits.items():
            out.append(
                {
                    "key_id": key_id,
                    "inflight": self._key_inflight.get(key_id, 0),
                    "max_inflight": limits.max_inflight,
                    "max_requests": limits.max_requests,
                    "max_requests_window_s": limits.max_requests_window_s,
                    "max_tokens": limits.max_tokens,
                    "max_tokens_window_s": limits.max_tokens_window_s,
                }
            )
        return out

    # ── Sliding-window helpers (amortised O(1)) ──────────────────────

    def _window_allows(
        self, store: dict[str, _Window], key_id: str, limit: int, window_s: float
    ) -> bool:
        w = store.get(key_id)
        if w is None:
            return True
        return w.allows(limit, window_s)

    def _window_record(
        self, store: dict[str, _Window], key_id: str, value: int = 1
    ) -> float:
        w = store.get(key_id)
        if w is None:
            w = _Window()
            store[key_id] = w
        return w.record(value)

    def _window_rollback(
        self, store: dict[str, _Window], key_id: str, ts: float
    ) -> None:
        """Remove a previously-recorded entry (e.g. when a request that
        passed the check failed to actually be admitted)."""
        w = store.get(key_id)
        if w is not None:
            w.rollback(ts)

    # ── Acquire / release ────────────────────────────────────────────

    async def acquire(self, key_id: str = "") -> InflightSlot:
        # Per-key rate-limit checks (before global gate, 429 if exceeded).
        req_ts: float | None = None
        limits = self._key_limits.get(key_id)
        if limits is not None:
            if limits.max_inflight is not None:
                if self._key_inflight.get(key_id, 0) >= limits.max_inflight:
                    raise RateLimitExceeded("concurrency", limits.max_inflight)
            if limits.max_requests is not None and limits.max_requests_window_s is not None:
                if not self._window_allows(
                    self._req_windows,
                    key_id,
                    limits.max_requests,
                    limits.max_requests_window_s,
                ):
                    raise RateLimitExceeded("requests", limits.max_requests)
                # Record optimistically; undone below if not actually admitted.
                req_ts = self._window_record(self._req_windows, key_id)
            if limits.max_tokens is not None and limits.max_tokens_window_s is not None:
                if not self._window_allows(
                    self._tok_windows,
                    key_id,
                    limits.max_tokens,
                    limits.max_tokens_window_s,
                ):
                    if req_ts is not None:
                        self._window_rollback(self._req_windows, key_id, req_ts)
                    raise RateLimitExceeded("tokens", limits.max_tokens)

        self._key_inflight[key_id] = self._key_inflight.get(key_id, 0) + 1
        try:
            slot = await self._gate_acquire(key_id)
        except BaseException:
            self._key_inflight[key_id] -= 1
            # Request was never admitted, don't let it count against budget.
            if req_ts is not None:
                self._window_rollback(self._req_windows, key_id, req_ts)
            raise
        return slot

    async def _gate_acquire(self, key_id: str) -> InflightSlot:
        if self._active < self._max_inflight:
            self._active += 1
            self._seq += 1
            slot = InflightSlot(seq_id=self._seq, key_id=key_id, wall_start=time.time())
            slot._notify_cb = self._notify
            self.inflight[slot.seq_id] = slot
            self._schedule_idle_check(slot)
            self._notify()
            return slot
        if len(self._waiting) >= self._max_waiting:
            raise QueueOverflow()
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiting.append(future)
        self._notify()

        try:
            await asyncio.wait_for(asyncio.shield(future), self._timeout)
        except asyncio.TimeoutError:
            self._cancel_waiter(future)
            raise QueueTimeout() from None
        except asyncio.CancelledError:
            self._cancel_waiter(future)
            raise

        # We've been granted the slot, register it synchronously.
        # No `await` here: no yield point where CancelledError can be
        # injected between `wait_for` returning and the slot being
        # registered.  Dict assignment and `_notify` are both sync.
        self._seq += 1
        slot = InflightSlot(seq_id=self._seq, key_id=key_id, wall_start=time.time())
        slot._notify_cb = self._notify
        self.inflight[slot.seq_id] = slot
        self._schedule_idle_check(slot)
        self._notify()
        return slot

    def _cancel_waiter(self, future: asyncio.Future[None]) -> None:
        if not future.done():
            future.cancel()
            try:
                self._waiting.remove(future)
            except ValueError:
                pass
            self._notify()
        else:
            try:
                self._waiting.remove(future)
            except ValueError:
                pass
            self._active -= 1
            self._promote_waiters()

    def release(self, slot: InflightSlot) -> None:
        # Idempotent: a second release (e.g. from an error path plus the
        # streaming finally) is a silent no-op.
        if slot._released:
            return
        slot._released = True

        # Per-key bookkeeping.
        key = slot.key_id
        if key in self._key_inflight:
            self._key_inflight[key] -= 1
        limits = self._key_limits.get(key)
        if (
            limits is not None
            and limits.max_tokens is not None
            and limits.max_tokens_window_s is not None
        ):
            tokens = slot.input_tokens + slot.output_tokens
            if tokens > 0:
                self._window_record(self._tok_windows, key, tokens)

        # Global gate release.
        # Synchronous, safe to call from `finally` blocks that may run
        # under an active anyio cancel scope (e.g. Starlette's
        # `listen_for_disconnect` path).  `future.set_result(None)` is
        # synchronous: the woken waiter resumes on the next event-loop
        # tick, so no `await` is needed and no yield point exists where
        # `CancelledError` could be injected.
        self._active -= 1
        self._cancel_idle_check(slot)
        self.inflight.pop(slot.seq_id, None)
        self._promote_waiters()

    def snapshot(self) -> GateSnapshot:
        inflight = [
            {
                "id": s.seq_id,
                "model": s.model or "",
                "upstream": s.upstream or "",
                "started_at": int(s.wall_start * 1000),
                "key_id": s.key_id,
                "no_signal": s._idle_notified,
                "ttft": s.ttft,
                "phase": s.phase,
            }
            for s in self.inflight.values()
        ]
        return GateSnapshot(
            active=self._active,
            waiting=len(self._waiting),
            max_inflight=self._max_inflight,
            max_waiting=self._max_waiting,
            inflight=inflight,
        )

    def set_limits(self, max_inflight: int, max_waiting: int) -> None:
        """Update global limits at runtime. Existing active/waiting requests are not affected."""
        self._max_inflight = max(max_inflight, 1)
        self._max_waiting = max(max_waiting, 0)
        self._promote_waiters()
