"""Global concurrency gate: FIFO acquire/release with idle watch.

Holds the global `max_inflight` / `max_waiting` / `timeout` config
and the active/waiting state.  Idle-watch timers are scheduled on acquire
and cancelled on release.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

from .types import (
    GateSnapshot,
    InflightSlot,
    QueueOverflow,
    QueueTimeout,
)

log = logging.getLogger(__name__)

_IDLE_THRESHOLD = 30.0


class GlobalGate:
    """Two-tier FIFO gate: at most `max_inflight` active, `max_waiting` queued.

    Beyond both limits, `acquire` raises :class:`QueueOverflow`.  Waiters
    are FIFO and time out after `timeout` seconds with :class:`QueueTimeout`.
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
        self.max_inflight = max_inflight
        self.max_waiting = max_waiting
        self.timeout = timeout
        self.active = 0
        self.waiters: deque[asyncio.Future[None]] = deque()
        self.inflight: dict[int, InflightSlot] = {}
        self._seq = 0
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

    def _promote_waiters(self) -> None:
        while self.active < self.max_inflight and self.waiters:
            future = self.waiters.popleft()
            if future.cancelled() or future.done():
                continue
            self.active += 1
            future.set_result(None)
        self._notify()

    # ── Idle watch ──────────────────────────────────────────────────

    def _schedule_idle_check(
        self, slot: InflightSlot, delay: float = _IDLE_THRESHOLD
    ) -> None:
        if slot._idle_handle is not None:
            slot._idle_handle.cancel()
        loop = asyncio.get_running_loop()
        slot._idle_handle = loop.call_later(delay, self._on_idle_timeout, slot)

    def _on_idle_timeout(self, slot: InflightSlot) -> None:
        now = time.monotonic()
        idle = now - slot.last_byte_time if slot.last_byte_time else 0.0
        is_idle = idle >= _IDLE_THRESHOLD
        if is_idle != slot._idle_notified:
            slot._idle_notified = is_idle
            self._notify()
        self._schedule_idle_check(
            slot, _IDLE_THRESHOLD if is_idle else _IDLE_THRESHOLD - idle
        )

    @staticmethod
    def _cancel_idle_check(slot: InflightSlot) -> None:
        if slot._idle_handle is not None:
            slot._idle_handle.cancel()
            slot._idle_handle = None
        slot._idle_notified = False

    # ── Acquire / release ───────────────────────────────────────────

    async def acquire(self, key_id: str = "") -> InflightSlot:
        if self.active < self.max_inflight:
            self.active += 1
            self._seq += 1
            slot = InflightSlot(seq_id=self._seq, key_id=key_id, wall_start=time.time())
            slot._notify_cb = self._notify
            self.inflight[slot.seq_id] = slot
            self._schedule_idle_check(slot)
            self._notify()
            return slot
        if len(self.waiters) >= self.max_waiting:
            raise QueueOverflow()
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiters.append(future)
        self._notify()

        try:
            await asyncio.wait_for(asyncio.shield(future), self.timeout)
        except asyncio.TimeoutError:
            self._cancel_waiter(future)
            raise QueueTimeout() from None
        except asyncio.CancelledError:
            self._cancel_waiter(future)
            raise

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
                self.waiters.remove(future)
            except ValueError:
                pass
            self._notify()
        else:
            try:
                self.waiters.remove(future)
            except ValueError:
                pass
            self.active -= 1
            self._promote_waiters()

    def release(self, slot: InflightSlot) -> None:
        self.active -= 1
        self._cancel_idle_check(slot)
        self.inflight.pop(slot.seq_id, None)
        self._promote_waiters()

    # ── Snapshot / config ──────────────────────────────────────────

    def snapshot(self) -> GateSnapshot:
        inflight = []
        queued = 0
        for s in self.inflight.values():
            if s.phase == "queued":
                queued += 1
            else:
                inflight.append(
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
                )
        return GateSnapshot(
            active=len(inflight),
            waiting=len(self.waiters) + queued,
            max_inflight=self.max_inflight,
            max_waiting=self.max_waiting,
            inflight=inflight,
        )

    def set_limits(self, max_inflight: int, max_waiting: int) -> None:
        self.max_inflight = max(max_inflight, 1)
        self.max_waiting = max(max_waiting, 0)
        self._promote_waiters()
