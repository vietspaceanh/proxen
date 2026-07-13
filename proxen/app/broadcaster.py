from __future__ import annotations

import asyncio
import logging
import time
import weakref

from ..core.concurrency import ConcurrencyGate
from ..services.management import Management
from ..services.telemetry import Database, TelemetryWriter
from ..services.upstream import UpstreamManager

log = logging.getLogger("proxen.broadcaster")

_CHART_TTL = 10.0


class StatsBroadcaster:
    """Event-driven full-state push to WebSocket dashboard clients.

    Producers (gate changes, record flushes, management edits, idle-watch
    no-signal/recovery signals) call `mark_dirty`.  A background task
    coalesces dirty signals and pushes the complete dashboard state at most
    every 300 ms, so clients always see consistent data with a single
    message type.  No polling, the loop blocks on a `Queue.get()` and
    only wakes when something actually changed.

    A `Queue(maxsize=1)` (instead of `Event`) ensures no signal is
    ever lost: if `mark_dirty` fires during the 300 ms debounce sleep,
    the item stays in the queue and triggers an immediate second push.
    """

    def __init__(
        self,
        gate: ConcurrencyGate,
        db: Database,
        writer: TelemetryWriter,
        management: Management,
        upstream_mgr: UpstreamManager,
    ) -> None:
        self._gate = gate
        self._db = db
        self._writer = writer
        self._management = management
        self._upstream_mgr = upstream_mgr
        self._dirty: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
        self._clients: weakref.WeakSet[asyncio.Queue[dict]] = weakref.WeakSet()
        self._chart_cache: dict = {"data": None, "cost": None, "ts": 0.0}
        self._recent_cache: list[dict] | None = None

    # ── Producer interface ───────────────────────────────────────────

    def mark_dirty(self) -> None:
        """Signal that dashboard state changed (non-blocking, idempotent)."""
        self._recent_cache = None
        try:
            self._dirty.put_nowait(None)
        except asyncio.QueueFull:
            pass

    def invalidate_chart_cache(self) -> None:
        """Force chart data refresh on the next push (e.g. after pricing edit).
        Also signals dirty + clears recent cache."""
        self._chart_cache["data"] = None
        self.mark_dirty()

    # ── Subscriber interface ─────────────────────────────────────────

    def subscribe(self) -> asyncio.Queue[dict]:
        q: asyncio.Queue[dict] = asyncio.Queue(maxsize=10)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict]) -> None:
        self._clients.discard(q)

    # ── State building ───────────────────────────────────────────────

    async def _refresh_cache(self) -> None:
        now = time.monotonic()
        cached = self._chart_cache
        if cached["data"] is not None and now - cached["ts"] < _CHART_TTL:
            return
        tps_ttft, daily_tok, daily_req, cost = await asyncio.gather(
            self._db.tps_ttft_24h(),
            self._db.daily_tokens(30),
            self._db.daily_requests(30),
            self._db.total_cost(),
        )
        cached["data"] = {
            "tps_ttft_24h": tps_ttft,
            "daily_tokens": daily_tok,
            "daily_requests": daily_req,
        }
        cached["cost"] = cost
        cached["ts"] = now

    async def get_stats(self) -> dict:
        snapshot = self._gate.snapshot()
        if self._recent_cache is None:
            self._recent_cache = await self._db.recent(50)
        await self._refresh_cache()
        return {
            "gate": snapshot.as_dict(),
            "totals": {**self._writer.totals, "total_cost": self._chart_cache["cost"]},
            "recent": self._recent_cache,
            "key_map": self._management.key_label_map(),
            "providers": self._upstream_mgr.provider_status(),
            **self._chart_cache["data"],
        }

    # ── Background loop ──────────────────────────────────────────────

    async def run(self) -> None:
        while True:
            try:
                await self._dirty.get()
                await asyncio.sleep(0.3)
                if not self._clients:
                    continue
                stats = await self.get_stats()
                stale: list[asyncio.Queue[dict]] = []
                for q in list(self._clients):
                    try:
                        q.put_nowait(stats)
                    except asyncio.QueueFull:
                        stale.append(q)
                for q in stale:
                    self._clients.discard(q)
            except Exception:
                log.exception("stats broadcaster error")
                await asyncio.sleep(1)
