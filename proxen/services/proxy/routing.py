"""Route resolution and provider queuing.

The Router owns the single-route attempt logic (TTFT gate, disconnect
racing, health recording) and the multi-route resolution flow (health
ordering, phase-1 non-blocking acquire, phase-2 provider queue race).
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import suppress
from typing import Any

import aiohttp
import msgspec

from ...core.config import ModelRoute, Upstream
from ...core.body import (
    merge_extra_body,
    patch_field,
)
from ...core.headers import filter_headers, protocol_from_path
from ...core.asgi import race_disconnect
from .context import ModelNotFound, NoRoutes, RequestContext, UpstreamUnavailable
from ..upstream import UpstreamManager
from ..management import Management

log = logging.getLogger(__name__)

_VERSION_RE = re.compile(r"/v\d+$")
_RETRY_STATUSES = frozenset({401, 403, 404, 408, 410, 429})


class _RouteResult(msgspec.Struct):
    """Outcome of a single upstream route attempt."""

    ok: bool = False
    disconnect: bool = False
    held: bool = False
    resp: object = None
    upstream_name: str = ""
    upstream_model_id: str = ""
    first_chunk: bytes = b""
    start: float = 0.0


class Router:
    """Resolves and attempts routes for a request.

    Holds the catalog and upstream manager; receives a `cancel_telemetry`
    callback for recording client disconnects during the TTFT gate.
    """

    def __init__(
        self,
        management: Management,
        upstream_mgr: UpstreamManager,
        cancel_telemetry=None,
    ) -> None:
        self.management = management
        self.upstream_mgr = upstream_mgr
        self._cancel_telemetry = cancel_telemetry

    # ── Helpers ───────────────────────────────────────────────────────

    def prepare_request(self, ctx: RequestContext) -> Any:
        """Model lookup + body preparation. Sets `ctx.routes` and
        `ctx.protocol`.  Returns `body_or_payload` (raw bytes or a merged
        dict when the model has `extra_body`)."""
        if not ctx.model:
            raise ModelNotFound("no model specified")
        model_cfg = self.management.get_model(ctx.model)
        if model_cfg is None or not model_cfg.enabled:
            raise ModelNotFound(f"model '{ctx.model}' is not available")
        ctx.routes = self.management.get_routes_by_name(ctx.model)
        if not ctx.routes:
            raise NoRoutes(f"no routes configured for model '{ctx.model}'")

        ctx.protocol = protocol_from_path(ctx.path)

        if model_cfg is not None and model_cfg.extra_body:
            try:
                payload = msgspec.json.decode(ctx.body) if ctx.body else {}
            except msgspec.DecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            merge_extra_body(payload, model_cfg.extra_body)
            return payload

        return ctx.body

    @staticmethod
    def route_body(body_or_payload: bytes | dict, client_model: str, route: ModelRoute) -> bytes:
        if isinstance(body_or_payload, dict):
            body_or_payload["model"] = route.upstream_model_id
            return msgspec.json.encode(body_or_payload)
        if route.upstream_model_id != client_model:
            return patch_field(body_or_payload, "model", route.upstream_model_id)
        return body_or_payload

    @staticmethod
    def upstream_url(upstream: Upstream, path: str, query: str) -> str:
        base = upstream.base_url.rstrip("/")
        if _VERSION_RE.search(base) and path.startswith("/v1/"):
            path = path[3:]
        url = base + path
        if query:
            url += "?" + query
        return url

    def release_held(self, held: _RouteResult | None) -> None:
        if held is not None and held.resp is not None:
            with suppress(Exception):
                held.resp.release()
            self.upstream_mgr.gate.release_provider(held.upstream_name)

    def route_fail(
        self, upstream: Upstream, route: ModelRoute, weight: int,
        resp: aiohttp.ClientResponse | None = None,
    ) -> _RouteResult:
        if resp is not None:
            with suppress(Exception):
                resp.release()
        self.upstream_mgr.gate.release_provider(upstream.name)
        self.upstream_mgr.health.record_failure((upstream.name, route.upstream_model_id), weight=weight)
        return _RouteResult(upstream_name=upstream.name)

    # ── Single route attempt ──────────────────────────────────────────

    async def attempt_route(
        self,
        ctx: RequestContext,
        route: ModelRoute,
        upstream: Upstream,
        body_or_payload,
        disconnect: asyncio.Event,
        *,
        ttft_timeout: float | None = None,
        simple_timeout: aiohttp.ClientTimeout | None = None,
    ) -> _RouteResult:
        """Try a single route. The provider slot is assumed already acquired
        (by `try_routes`).  Handles health recording, disconnect racing,
        and the TTFT gate.  On failure the provider slot is released via
        `route_fail` or inline."""
        slot = ctx.slot
        if slot:
            slot.upstream = upstream.name

        start = time.perf_counter()
        route_body = self.route_body(body_or_payload, ctx.model, route)
        headers = filter_headers(
            ctx.raw_headers, upstream.api_key.get_secret_value(), ctx.protocol,
        )
        url = self.upstream_url(upstream, ctx.path, ctx.query)
        kw: dict = {"headers": headers, "data": route_body}
        if simple_timeout is not None:
            kw["timeout"] = simple_timeout

        if ttft_timeout:
            remaining = start + ttft_timeout - time.perf_counter()
            post_task = asyncio.ensure_future(
                asyncio.wait_for(
                    self.upstream_mgr.post(url, **kw), max(0.001, remaining),
                )
            )
        else:
            post_task = asyncio.ensure_future(self.upstream_mgr.post(url, **kw))

        if await race_disconnect(post_task, disconnect):
            if post_task.done():
                with suppress(Exception):
                    post_task.result().close()
            else:
                post_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await post_task
            self.upstream_mgr.gate.release_provider(upstream.name)
            return _RouteResult(disconnect=True, upstream_name=upstream.name)

        try:
            resp = post_task.result()
        except asyncio.TimeoutError:
            return self.route_fail(upstream, route, 2)
        except aiohttp.ClientError as exc:
            log.warning("upstream %s connect failed: %s", upstream.name, exc)
            return self.route_fail(upstream, route, 2)
        except BaseException:
            self.upstream_mgr.gate.release_provider(upstream.name)
            ctx.provider = ""
            raise

        if resp.status >= 500:
            return self.route_fail(upstream, route, 1, resp)

        if resp.status in _RETRY_STATUSES:
            return _RouteResult(
                held=True, resp=resp,
                upstream_name=upstream.name,
                upstream_model_id=route.upstream_model_id,
            )

        first_chunk = b""
        if ttft_timeout:
            remaining = start + ttft_timeout - time.perf_counter()
            read_task = asyncio.ensure_future(
                asyncio.wait_for(
                    resp.content.readany(), max(0.001, remaining),
                )
            )

            if await race_disconnect(read_task, disconnect):
                with suppress(Exception):
                    resp.close()
                if not read_task.done():
                    read_task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await read_task
                self.upstream_mgr.gate.release_provider(upstream.name)
                if slot is not None and self._cancel_telemetry is not None:
                    self._cancel_telemetry(
                        slot.wall_start, start, slot.model, slot.key_id,
                        upstream.name, resp.status, stream=True,
                    )
                return _RouteResult(disconnect=True, upstream_name=upstream.name)

            try:
                first_chunk = read_task.result()
            except asyncio.TimeoutError:
                log.warning(
                    "upstream %s TTFT timeout (%.1fs), falling back",
                    upstream.name, ttft_timeout,
                )
                return self.route_fail(upstream, route, 2, resp)
            except aiohttp.ClientError as exc:
                log.warning(
                    "upstream %s read failed during TTFT wait: %s",
                    upstream.name, exc,
                )
                return self.route_fail(upstream, route, 1, resp)

        self.upstream_mgr.health.record_success((upstream.name, route.upstream_model_id))
        return _RouteResult(
            ok=True, resp=resp,
            upstream_name=upstream.name,
            upstream_model_id=route.upstream_model_id,
            first_chunk=first_chunk,
            start=start,
        )

    # ── Multi-route resolution ────────────────────────────────────────

    async def try_routes(
        self,
        ctx: RequestContext,
        body_or_payload,
        disconnect: asyncio.Event,
        *,
        ttft_timeout: float | None = None,
        simple_timeout: aiohttp.ClientTimeout | None = None,
    ) -> tuple[aiohttp.ClientResponse, str, str, float, bytes] | None:
        """Try routes with health-aware ordering and provider concurrency
        queuing.

        Routes are filtered and ordered inline: healthy first
        (`should_try`), then probing (`should_retry`).  Single-route
        models bypass health checking (no alternative to try).

        Phase 1 (non-blocking): iterate routes in order.  For each,
        `acquire_provider` (non-blocking).  The first with capacity is
        attempted.  Full routes are collected for phase 2.

        Phase 2 (queue): when *all* routes are full,
        `wait_acquire_provider` races a waiter on every eligible
        provider; the first to free a slot wins.

        Returns (resp, upstream_name, upstream_model_id, start, first_chunk)
        on success, or None on client disconnect.
        """
        slot = ctx.slot
        held: _RouteResult | None = None
        upstream_name = ""
        full: list[tuple[ModelRoute, Upstream]] = []

        healthy: list[tuple[ModelRoute, Upstream]] = []
        probing: list[tuple[ModelRoute, Upstream]] = []
        single = len(ctx.routes) <= 1
        for r in ctx.routes:
            if not r.enabled:
                continue
            u = self.management.get_upstream(r.upstream_name)
            if u is None or not u.enabled:
                continue
            if single or self.upstream_mgr.health.should_try((u.name, r.upstream_model_id)):
                healthy.append((r, u))
            elif self.upstream_mgr.health.should_retry((u.name, r.upstream_model_id)):
                probing.append((r, u))
        eligible = healthy + probing

        kw = dict(
            disconnect=disconnect,
            ttft_timeout=ttft_timeout, simple_timeout=simple_timeout,
        )

        for route, upstream in eligible:
            if not self.upstream_mgr.gate.try_provider(route.upstream_name):
                full.append((route, upstream))
                continue
            ctx.provider = route.upstream_name

            r = await self.attempt_route(ctx, route, upstream, body_or_payload, **kw)
            upstream_name = r.upstream_name
            if r.ok:
                self.release_held(held)
                if slot is not None:
                    slot.mark_receiving()
                return r.resp, r.upstream_name, r.upstream_model_id, r.start, r.first_chunk
            if r.disconnect:
                self.release_held(held)
                ctx.provider = ""
                return None
            if r.held:
                self.release_held(held)
                held = r
            else:
                ctx.provider = ""

        if held is None and full:
            names = [r.upstream_name for r, _ in full]
            name = await self.upstream_mgr.gate.wait_provider(names, disconnect)
            if name is None:
                self.release_held(held)
                return None
            ctx.provider = name
            route, upstream = next(
                ((r, u) for r, u in full if r.upstream_name == name), (None, None)
            )
            if route is None:
                self.upstream_mgr.gate.release_provider(name)
                ctx.provider = ""
            else:
                r = await self.attempt_route(ctx, route, upstream, body_or_payload, **kw)
                upstream_name = r.upstream_name
                if r.ok:
                    self.release_held(held)
                    if slot is not None:
                        slot.mark_receiving()
                    return r.resp, r.upstream_name, r.upstream_model_id, r.start, r.first_chunk
                if r.disconnect:
                    self.release_held(held)
                    ctx.provider = ""
                    return None
                if r.held:
                    self.release_held(held)
                    held = r
                else:
                    ctx.provider = ""

        if held is not None:
            if slot is not None:
                slot.mark_receiving()
            ctx.provider = held.upstream_name
            return held.resp, held.upstream_name, held.upstream_model_id, time.perf_counter(), b""

        raise UpstreamUnavailable("upstream unavailable", upstream=upstream_name or "none")
