"""Proxy orchestrator: pipeline seams, telemetry, and forward dispatch.

Routing logic lives in :mod:`.routing`, streaming in :mod:`.forwarding`,
and HTTP utilities in :mod:`proxen.core.headers`, :mod:`proxen.core.body`,
and :mod:`proxen.core.asgi`.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpcore

from ...core.config import Settings
from ...core.concurrency import ConcurrencyGate
from ...core.headers import filter_headers
from ...core.models import RequestRecord
from ...core.sse import UsageStats, parse_json_usage
from .context import (
    ProxyError,
    RequestContext,
    UpstreamUnavailable,
)
from .routing import RouteResult, Router
from .forwarding import StreamForwarder, cancel_and_await, safe_aclose, speed_metrics
from ..upstream import UpstreamManager
from ..management import Management
from ..telemetry import TelemetryWriter

log = logging.getLogger("proxen.proxy")


class Proxy:
    """Routes client requests to upstreams with fallback.

    The core orchestrator.  Cross-cutting concerns are injected:
    `admit_hooks` for pre-acquisition admission checks,
    `complete_hooks` for post-request accounting.  Both receive
    clean data (RequestContext / RequestRecord) and are simple
    callables - no protocol classes needed.
    """

    def __init__(
        self,
        settings: Settings,
        upstream_mgr: UpstreamManager,
        management: Management,
        writer: TelemetryWriter,
        gate: ConcurrencyGate,
        admit_hooks: list | None = None,
        complete_hooks: list | None = None,
    ) -> None:
        self.settings = settings
        self.upstream_mgr = upstream_mgr
        self.management = management
        self._writer = writer
        self._gate = gate
        self.admit_hooks = admit_hooks or []
        self.complete_hooks = complete_hooks or []
        self._router = Router(management, upstream_mgr, self._cancel_telemetry)

    # ── Pipeline seams ────────────────────────────────────────────────

    def admit(self, ctx: RequestContext) -> None:
        """Run admission hooks. Raises :class:`AdmissionError` to deny."""
        for hook in self.admit_hooks:
            hook(ctx)

    async def acquire(self, ctx: RequestContext) -> None:
        """Acquire the global concurrency slot."""
        ctx.slot = await self._gate.acquire(ctx.key_hash)

    def release(self, ctx: RequestContext, *, cooldown: bool = False) -> None:
        """Release all held resources. Idempotent - safe in `finally`."""
        if ctx.provider:
            self.upstream_mgr.gate.release_provider(ctx.provider, cooldown=cooldown)
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
        self._writer.enqueue(record)
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

    # ── Forward helpers ──────────────────────────────────────────────

    async def _route(
        self,
        ctx: RequestContext,
        disconnect: asyncio.Event,
        *,
        wall_start: float,
        wall_perf: float,
        stream: bool,
        read_timeout: float,
        ttft_timeout: float = 0.0,
    ) -> RouteResult | None:
        """Prepare request and try routes. Handles error telemetry."""
        try:
            body_or_payload = self._router.prepare_request(ctx)
            if ctx.slot:
                ctx.slot.model = ctx.model
            return await self._router.try_routes(
                ctx, body_or_payload, disconnect,
                read_timeout=read_timeout,
                ttft_timeout=ttft_timeout,
            )
        except ProxyError as exc:
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                exc.upstream, exc.status, stream=stream,
            )
            raise

    async def _close_upstream(
        self,
        resp: httpcore.Response,
        ctx: RequestContext,
        upstream_name: str,
        *,
        cooldown: bool = False,
    ) -> None:
        """Close the response and release the provider slot."""
        await safe_aclose(resp)
        self.upstream_mgr.gate.release_provider(upstream_name, cooldown=cooldown)
        ctx.provider = ""

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

        route = await self._route(
            ctx, disconnect,
            wall_start=wall_start, wall_perf=wall_perf, stream=True,
            read_timeout=self.settings.upstream_sock_read,
            ttft_timeout=self.settings.upstream_ttft_timeout or 0.0,
        )
        if route is None:
            return None

        fwd = StreamForwarder.from_route(
            self, ctx, route, wall_start, disconnect, watcher,
            stall_timeout=self.settings.upstream_sock_read,
        )
        fwd.start_watch()
        return route.resp.status, filter_headers(route.resp.headers), fwd.stream

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

        route = await self._route(
            ctx, disconnect,
            wall_start=wall_start, wall_perf=wall_perf, stream=False,
            read_timeout=self.settings.upstream_non_streaming_timeout,
        )
        if route is None:
            return None

        resp = route.resp
        upstream_name = route.upstream_name
        start = route.start

        # Read body, racing against client disconnect and read deadline.
        read_task = asyncio.ensure_future(resp.aread())
        disc_task = asyncio.ensure_future(disconnect.wait())
        await asyncio.wait(
            {read_task, disc_task},
            timeout=self.settings.upstream_non_streaming_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        await cancel_and_await(disc_task)

        if disconnect.is_set() or not read_task.done():
            await cancel_and_await(read_task)
            await self._close_upstream(resp, ctx, upstream_name, cooldown=True)
            if disconnect.is_set():
                self._cancel_telemetry(
                    wall_start, start, ctx.model, ctx.key_hash,
                    upstream_name, resp.status, stream=False,
                )
                return None
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                upstream_name, 502, stream=False,
            )
            raise UpstreamUnavailable(
                f"upstream read timed out after "
                f"{self.settings.upstream_non_streaming_timeout}s",
                upstream=upstream_name,
            )

        try:
            content = read_task.result()
        except (httpcore.NetworkError, httpcore.ProtocolError, httpcore.TimeoutException) as exc:
            await self._close_upstream(resp, ctx, upstream_name, cooldown=True)
            self._error_telemetry(
                wall_start, wall_perf, ctx.model, ctx.key_hash,
                upstream_name, 502, stream=False,
            )
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

        await self._close_upstream(resp, ctx, upstream_name)
        return resp.status, filter_headers(resp.headers), content
