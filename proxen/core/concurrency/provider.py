"""Per-provider concurrency: non-blocking try, queue-race wait, release.

Each provider has its own `max_inflight` limit and FIFO wait queue.
`wait_provider` races waiters across multiple providers plus the
client-disconnect event.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from contextlib import suppress

from .types import QueueOverflow, QueueTimeout

log = logging.getLogger(__name__)


class ProviderGate:
    """Per-provider concurrency with queue-race support.

    Shares `max_waiting` and `timeout` config with the global gate
    so that all queues have the same wait policy.
    """

    def __init__(self, max_waiting: int, timeout: float) -> None:
        self.max_waiting = max_waiting
        self.timeout = timeout
        self.limits: dict[str, int | None] = {}
        self.active: dict[str, int] = {}
        self.waiters: dict[str, deque[asyncio.Future[None]]] = {}
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

    # ─── Configuration ─────────────────────────────────────────────

    def set_provider_limit(self, name: str, max_inflight: int | None) -> None:
        self.limits[name] = max_inflight
        self.active.setdefault(name, 0)
        self.waiters.setdefault(name, deque())
        self._promote_waiters(name)
        self._notify()

    def rename_provider(self, old_name: str, new_name: str) -> None:
        if old_name in self.limits:
            self.limits[new_name] = self.limits.pop(old_name)
        if old_name in self.active:
            self.active[new_name] = self.active.pop(old_name)
        if old_name in self.waiters:
            self.waiters[new_name] = self.waiters.pop(old_name)

    def remove_provider(self, name: str) -> None:
        self.limits.pop(name, None)
        self.active.pop(name, None)
        waiters = self.waiters.pop(name, None)
        if waiters:
            for fut in waiters:
                if not fut.done():
                    fut.cancel()
            self._notify()

    # ─── Try / wait / release ──────────────────────────────────────

    def try_provider(self, name: str) -> bool:
        """Non-blocking try to acquire a provider slot."""
        limit = self.limits.get(name)
        if limit is None:
            self.active[name] = self.active.get(name, 0) + 1
            self._notify()
            return True
        active = self.active.get(name, 0)
        if active < limit:
            self.active[name] = active + 1
            self._notify()
            return True
        return False

    async def wait_provider(
        self, names: list[str], disconnect: asyncio.Event
    ) -> str | None:
        """Race-acquire a slot on any of the named providers.

        Phase 1 (non-blocking): the first provider with free capacity wins.
        Phase 2 (queue): enqueue on every eligible provider and race.

        Returns the winning provider name, or `None` if the client
        disconnected.  Raises `QueueOverflow` when every queue is full,
        `QueueTimeout` on timeout.
        """
        for name in names:
            if self.try_provider(name):
                return name

        entries: list[tuple[str, asyncio.Future[None]]] = []
        for name in names:
            waiters = self.waiters.get(name)
            if waiters is None:
                waiters = deque()
                self.waiters[name] = waiters
            if len(waiters) < self.max_waiting:
                fut = asyncio.get_running_loop().create_future()
                waiters.append(fut)
                entries.append((name, fut))

        if not entries:
            raise QueueOverflow()
        self._notify()

        disc_task = asyncio.ensure_future(disconnect.wait())
        futs = [f for _, f in entries] + [disc_task]
        wait_timeout = self.timeout if self.timeout > 0 else None
        try:
            await asyncio.wait(
                futs, timeout=wait_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            for name, fut in entries:
                self._cancel_waiter(name, fut)
            if not disc_task.done():
                disc_task.cancel()
            raise

        winner: str | None = None
        for name, fut in entries:
            if winner is None and fut.done() and not fut.cancelled():
                winner = name
            else:
                self._cancel_waiter(name, fut)

        disconnected = disc_task.done() or disconnect.is_set()
        if not disc_task.done():
            disc_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await disc_task

        if winner is not None and disconnected:
            self.release_provider(winner)
            return None
        if winner is not None:
            return winner
        if disconnected:
            return None
        raise QueueTimeout()

    def release_provider(self, name: str) -> None:
        active = self.active.get(name, 0)
        if active <= 0:
            return
        self.active[name] = active - 1
        self._promote_waiters(name)
        self._notify()

    # ─── Internal ──────────────────────────────────────────────────

    def _cancel_waiter(self, name: str, fut: asyncio.Future[None]) -> None:
        if not fut.done():
            fut.cancel()
        elif not fut.cancelled():
            self.release_provider(name)
        try:
            self.waiters[name].remove(fut)
        except (KeyError, ValueError):
            pass
        self._notify()

    def _promote_waiters(self, name: str) -> None:
        limit = self.limits.get(name)
        waiters = self.waiters.get(name)
        if not waiters:
            return
        while (limit is None or self.active.get(name, 0) < limit) and waiters:
            fut = waiters.popleft()
            if fut.cancelled() or fut.done():
                continue
            self.active[name] = self.active.get(name, 0) + 1
            fut.set_result(None)

    # ─── Read-only queries ─────────────────────────────────────────

    def provider_inflight(self) -> dict[str, int]:
        return dict(self.active)

    def provider_status(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for name, limit in self.limits.items():
            waiters = self.waiters.get(name, ())
            out[name] = {
                "inflight": self.active.get(name, 0),
                "waiting": len(waiters),
                "max_inflight": limit,
                "max_waiting": self.max_waiting,
                "routes": {},
            }
        return out
