"""Proxy orchestrator: pipeline seams, telemetry, and forward dispatch.

Routing logic lives in :mod:`proxen.services.routing`, streaming in
:mod:`proxen.services.streaming`, and HTTP utilities in
:mod:`proxen.core.httputil`.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Any

import aiohttp

from ..core.config import Settings
from ..core.contracts import TelemetrySink, UpstreamCatalog
from ..core.gate import ConcurrencyGate
from ..core.httputil import filter_headers, race_disconnect, speed_metrics
from ..core.models import RequestRecord
from ..core.sse import UsageStats, parse_json_usage
from .context import (
    AdmissionError,
    ModelNotFound,
    NoRoutes,
    ProxyError,
    RequestContext,
    UpstreamUnavailable,
)
from .routing import Router, _SIMPLE_TIMEOUT
from .streaming import StreamForwarder
from .upstream import UpstreamManager

log = logging.getLogger("proxen.proxy")


class Proxy:
    """Routes client requests to upstreams with fallback.

    The core orchestrator.  Cross-cutting concerns are injected:
    ``admit_hooks`` for pre-acquisition admission checks,
    ``complete_hooks`` for post-request accounting.  Both receive
    clean data (RequestContext / RequestRecord) and are simple
    callables - no protocol classes needed.
    """

    def __init__(
        self,
        settings: Settings,
        upstream_mgr: UpstreamManager,
        catalog: UpstreamCatalog,
        sink: TelemetrySink,
        gate: ConcurrencyGate,
        admit_hooks: list | None = None,
        complete_hooks: list | None = None,
    ) -> None:
        self.settings = settings
        self.upstream_mgr = upstream_mgr
        self._catalog = catalog
        self._sink = sink
        self._gate = gate
        self.admit_hooks = admit_hooks or []
        self.complete_hooks = complete_hooks or []
        self._router = Router(catalog, upstream_mgr, self._cancel_telemetry)

    # ── Pipeline seams ────────────────────────────────────────────────

    def admit(self, ctx: RequestContext) -> None:
        """Run admission hooks. Raises :class:`AdmissionError` to deny."""
        for hook in self.admit_hooks:
            hook(ctx)

    async def acquire(self, ctx: RequestContext) -> None:
        """Acquire the global concurrency slot."""
        ctx.slot = await self._gate.acquire(ctx.key_hash)

    def release(self, ctx: RequestContext) -> None:
        """Release all held resources. Idempotent - safe in ``finally``."""
        if ctx.provider:
            self.upstream_mgr.release_provider(ctx.provider)
            ctx.provider = ""
        if ctx.slot is not None:
            self._gate.release(ctx.slot)
            ctx.slot = None

    # ── Telemetry ─────────────────────────────────────────────────────

    def _record(
        self, *, wall_start, model, upstream, key_id, ttft, tps,
        usage, status, duration, stream, disconnected=False, completed=True,
    ) -> None:
        """Build a RequestRecord, enqueue to sink, and call complete hooks."""
        no_usage = not (usage.input_tokens or usage.output_tokens)
        record = RequestRecord(
            timestamp=wall_start,
            model=model or "unknown",
            upstream=upstream,
            key_id=key_id,
            ttft=ttft,
            tps=tps,
            input_tokens=usage.input_tokens,
            cached_input_tokens=usage.cached_input_tokens,
            output_tokens=usage.output_tokens,
            status=status,
            duration=duration,
            stream=stream,
            client_disconnect=disconnected and (not completed or no_usage),
            upstream_dropped=not completed and not disconnected,
            needs_review=self._needs_review(status, usage, completed, disconnected),
        )
        self._sink.enqueue(record)
        for hook in self.complete_hooks:
            hook(record)

    @staticmethod
    def _needs_review(status, usage, completed, disconnected) -> bool:
        if status >= 400 or not completed or disconnected:
            return False
        return not (usage.input_tokens or usage.output_tokens)

    def _error_telemetry(
        self, wall_start, start, model, key_id, upstream, status, *, stream,
    ) -> None:
        """Record a setup-phase error (model not found, no routes, etc.)."""
        self._record(
            wall_start=wall_start, model=model or "unknown",
            upstream=upstream, key_id=key_id, ttft=0.0, tps=0.0,
            usage=UsageStats(), status=status,
            duration=time.perf_counter() - start,
            stream=stream, completed=True,
        )

    def _cancel_telemetry(
        self, wall_start, start, model, key_id, upstream, status, *, stream,
    ) -> None:
        """Record a receiving-phase client disconnect (status already known)."""
        self._record(
            wall_start=wall_start, model=model or "unknown",
            upstream=upstream, key_id=key_id, ttft=0.0, tps=0.0,
            usage=UsageStats(), status=status,
            duration=time.perf_counter() - start,
            stream=stream, disconnected=True, completed=False,
        )

    # ── Forward: streaming ────────────────────────────────────────────

    async def forward_stream(
        self,
        ctx: RequestContext,
        disconnect: asyncio.Event,
        watcher: asyncio.Task,
    ) -> tuple[int, dict[str, str], Any] | None:
        """Forward a streaming request. Returns (status, headers, gen) or None."""
        wall_start = time.time()
        wall_perf = time.perf_counter()

        try:
            body_or_payload = self._router.prepare_request(ctx)
            if ctx.slot:
                ctx.slot.model = ctx.model

            ttft_timeout = self.settings.upstream_ttft_timeout or None
            result = await self._router.try_routes(
                ctx, body_or_payload, disconnect, ttft_timeout=ttft_timeout,
            )
        except ProxyError as exc:
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                exc.upstream, exc.status, stream=True,
            )
            raise
        if result is None:
            return None

        resp, upstream_name, upstream_model_id, start, first_chunk = result

        fwd = StreamForwarder(
            proxy=self, resp=resp, ctx=ctx,
            wall_start=wall_start, start=start,
            first_chunk=first_chunk,
            disconnect=disconnect, watcher=watcher,
            upstream_name=upstream_name,
            upstream_model_id=upstream_model_id,
        )
        fwd.start_watch()
        return resp.status, filter_headers(resp.headers), fwd.stream

    # ── Forward: non-streaming ────────────────────────────────────────

    async def forward_simple(
        self,
        ctx: RequestContext,
        disconnect: asyncio.Event,
    ) -> tuple[int, dict[str, str], bytes] | None:
        """Forward a non-streaming request. Returns (status, headers, body) or None."""
        wall_start = time.time()
        wall_perf = time.perf_counter()
        slot = ctx.slot

        try:
            body_or_payload = self._router.prepare_request(ctx)
            if slot:
                slot.model = ctx.model

            result = await self._router.try_routes(
                ctx, body_or_payload, disconnect, simple_timeout=_SIMPLE_TIMEOUT,
            )
        except ProxyError as exc:
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                exc.upstream, exc.status, stream=False,
            )
            raise
        if result is None:
            return None

        resp, upstream_name, _upstream_model_id, start, _ = result

        read_task = asyncio.ensure_future(resp.read())
        if await race_disconnect(read_task, disconnect):
            if not read_task.done():
                read_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await read_task
            with suppress(Exception):
                resp.close()
            self.upstream_mgr.release_provider(upstream_name)
            self._cancel_telemetry(
                wall_start, start, ctx.model, ctx.key_hash,
                upstream_name, resp.status, stream=False,
            )
            ctx.provider = ""
            return None

        try:
            content = read_task.result()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            with suppress(Exception):
                resp.close()
            self.upstream_mgr.release_provider(upstream_name)
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                upstream_name, 502, stream=False,
            )
            ctx.provider = ""
            raise UpstreamUnavailable(
                f"upstream read failed: {exc}", upstream=upstream_name,
            ) from exc

        usage = parse_json_usage(content, ctx.protocol)
        if slot:
            slot.input_tokens = usage.input_tokens
            slot.output_tokens = usage.output_tokens
        duration = time.perf_counter() - start
        ttft, tps = speed_metrics(resp.status, duration, duration, usage.output_tokens)

        self._record(
            wall_start=wall_start, model=ctx.model, upstream=upstream_name,
            key_id=ctx.key_hash, ttft=ttft, tps=tps, usage=usage,
            status=resp.status, duration=duration, stream=False,
        )

        with suppress(Exception):
            resp.release()
        self.upstream_mgr.release_provider(upstream_name)
        ctx.provider = ""

        return resp.status, filter_headers(resp.headers), content
