"""Admission-control sub-package.

Exposes :class:`ConcurrencyGate` - a facade that coordinates three
focused sub-components:

- :class:`.gate.GlobalGate` - global FIFO acquire/release + idle watch
- :class:`.key_limits.KeyLimiter` - per-key rate limiting
- :class:`.provider.ProviderGate` - per-provider concurrency + queue racing

Each sub-component is independently testable.  The facade coordinates
them in `acquire()` and `release()`; all other methods are thin
delegates that form the unified API callers expect.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable

from .gate import GlobalGate
from .key_limits import KeyLimiter
from .provider import ProviderGate
from .types import (
    GateSnapshot,
    InflightSlot,
    KeyLimits,
    QueueOverflow,
    QueueTimeout,
    RateLimitExceeded,
)

__all__ = [
    "ConcurrencyGate",
    "GlobalGate",
    "KeyLimiter",
    "ProviderGate",
    "InflightSlot",
    "GateSnapshot",
    "KeyLimits",
    "QueueOverflow",
    "QueueTimeout",
    "RateLimitExceeded",
]


class ConcurrencyGate:
    """Facade: coordinates global gate, key limiter, and provider gate.

    Callers use this unified API.  The coordination logic lives in
    `acquire()` and `release()`; everything else delegates directly.
    """

    def __init__(
        self,
        max_inflight: int,
        max_waiting: int,
        timeout: float,
    ) -> None:
        self._global = GlobalGate(max_inflight, max_waiting, timeout)
        self._keys = KeyLimiter()
        self._providers = ProviderGate(max_waiting, timeout)

    @property
    def inflight(self) -> dict[int, InflightSlot]:
        return self._global.inflight

    def set_on_change(self, cb: Callable[[], None]) -> None:
        self._global.set_on_change(cb)
        self._keys.set_on_change(cb)
        self._providers.set_on_change(cb)

    # ── Global gate (coordinated with key limiter) ──────────────────

    async def acquire(self, key_id: str = "") -> InflightSlot:
        req_ts = self._keys.check(key_id)
        self._keys.record(key_id)
        try:
            slot = await self._global.acquire(key_id)
        except BaseException:
            self._keys.rollback(key_id, req_ts)
            raise
        return slot

    def release(self, slot: InflightSlot) -> None:
        if slot._released:
            return
        slot._released = True
        self._keys.on_release(slot)
        self._global.release(slot)

    def snapshot(self) -> GateSnapshot:
        return self._global.snapshot()

    def set_limits(self, max_inflight: int, max_waiting: int) -> None:
        self._global.set_limits(max_inflight, max_waiting)
        self._providers.max_waiting = max(max_waiting, 0)

    # ── Per-key limits (thin delegates) ─────────────────────────────

    def set_key_limits(self, key_id: str, limits: KeyLimits) -> None:
        self._keys.set_key_limits(key_id, limits)

    def get_key_limits(self, key_id: str) -> KeyLimits:
        return self._keys.get_key_limits(key_id)

    def remove_key_limits(self, key_id: str) -> None:
        self._keys.remove_key_limits(key_id)

    def key_snapshot(self) -> list[dict]:
        return self._keys.key_snapshot()

    # ── Per-provider (thin delegates) ───────────────────────────────

    def set_provider_limit(self, name: str, max_inflight: int | None) -> None:
        self._providers.set_provider_limit(name, max_inflight)

    def rename_provider(self, old_name: str, new_name: str) -> None:
        self._providers.rename_provider(old_name, new_name)

    def remove_provider(self, name: str) -> None:
        self._providers.remove_provider(name)

    def try_provider(self, name: str) -> bool:
        return self._providers.try_provider(name)

    async def wait_provider(
        self, names: list[str], disconnect: asyncio.Event
    ) -> str | None:
        return await self._providers.wait_provider(names, disconnect)

    def release_provider(self, name: str) -> None:
        self._providers.release_provider(name)

    def provider_inflight(self) -> dict[str, int]:
        return self._providers.provider_inflight()

    def provider_status(self) -> dict[str, dict]:
        return self._providers.provider_status()
