"""Route resolution and provider queuing.

The Router owns the single-route attempt logic (TTFT gate, disconnect
racing, health recording) and the multi-route resolution flow (health
ordering, phase-1 non-blocking acquire, phase-2 provider queue race).

Uses httpcore with HTTP/2.  RST_STREAM cancellation (via the
http2_cancel monkey-patch) ensures timed-out or disconnected requests
are properly cancelled at the upstream - no zombie drain needed.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import httpcore
import msgspec

from ...core.config import ModelRoute, Upstream
from ...core.body import (
    merge_extra_body,
    patch_field,
)
from ...core.headers import filter_headers, protocol_from_path
from .context import ModelNotFound, NoRoutes, RequestContext, UpstreamUnavailable
from .forwarding import cancel_and_await, safe_aclose
from ..upstream import UpstreamManager
from ..management import Management

log = logging.getLogger(__name__)

_VERSION_RE = re.compile(r"/v\d+$")
_RETRY_STATUSES = frozenset({401, 403, 404, 408, 410, 429})


class RouteResult(msgspec.Struct):
    """Outcome of a single upstream route attempt."""

    ok: bool = False
    disconnect: bool = False
    held: bool = False
    resp: object = None
    upstream_name: str = ""
    upstream_model_id: str = ""
    first_chunk: bytes = b""
    start: float = 0.0
    stream_iter: object = None


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

    async def release_held(self, held: RouteResult | None) -> None:
        if held is not None and held.resp is not None:
            await safe_aclose(held.resp)
            self.upstream_mgr.gate.release_provider(held.upstream_name, cooldown=True)

    async def route_fail(
        self, upstream: Upstream, route: ModelRoute, weight: int,
        resp: httpcore.Response | None = None,
    ) -> RouteResult:
        if resp is not None:
            await safe_aclose(resp)
        self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
        self.upstream_mgr.health.record_failure((upstream.name, route.upstream_model_id), weight=weight)
        return RouteResult(upstream_name=upstream.name)

    # ── Single route attempt ──────────────────────────────────────────

    async def attempt_route(
        self,
        ctx: RequestContext,
        route: ModelRoute,
        upstream: Upstream,
        body_or_payload,
        disconnect: asyncio.Event,
        *,
        read_timeout: float = 90.0,
        ttft_timeout: float = 0.0,
    ) -> RouteResult:
        """Try a single route. The provider slot is assumed already acquired
        (by `try_routes`).  Handles health recording, disconnect racing,
        and the TTFT gate.  On failure the provider slot is released."""
        slot = ctx.slot
        if slot:
            slot.upstream = upstream.name

        start = time.perf_counter()
        route_body = self.route_body(body_or_payload, ctx.model, route)
        headers = filter_headers(
            ctx.raw_headers, upstream.api_key.get_secret_value(), ctx.protocol,
        )
        url = self.upstream_url(upstream, ctx.path, ctx.query)

        # Header phase: race POST against client disconnect and a
        # per-stream timeout.  httpcore's socket-level read timeout is
        # defeated by HTTP/2 multiplexing (other streams' traffic resets
        # the shared socket timer), so a per-stream deadline is enforced
        # here - the same pattern used in `_ttft_wait` and the streaming
        # loop.
        post_task = asyncio.ensure_future(
            self.upstream_mgr.request(
                "POST", url, headers=headers,
                content=route_body, read_timeout=read_timeout,
            )
        )
        disc_task = asyncio.ensure_future(disconnect.wait())
        try:
            await asyncio.wait(
                {post_task, disc_task},
                timeout=read_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            await cancel_and_await(post_task)
            await cancel_and_await(disc_task)
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            ctx.provider = ""
            raise
        await cancel_and_await(disc_task)

        if disconnect.is_set():
            if not post_task.done():
                await cancel_and_await(post_task)
            else:
                await safe_aclose(post_task.result())
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            return RouteResult(disconnect=True, upstream_name=upstream.name)

        if not post_task.done():
            await cancel_and_await(post_task)
            self.upstream_mgr.health.record_failure(
                (upstream.name, route.upstream_model_id), weight=2,
            )
            log.warning(
                "upstream %s header timeout (%.1fs)", upstream.name, read_timeout,
            )
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            return RouteResult(upstream_name=upstream.name)

        try:
            resp = post_task.result()
        except httpcore.ReadTimeout:
            log.warning(
                "upstream %s header timeout (%.1fs)", upstream.name, read_timeout,
            )
            self.upstream_mgr.health.record_failure(
                (upstream.name, route.upstream_model_id), weight=2,
            )
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            return RouteResult(upstream_name=upstream.name)
        except httpcore.ConnectError as exc:
            log.warning("upstream %s connect failed: %s", upstream.name, exc)
            return await self.route_fail(upstream, route, 2)
        except (httpcore.NetworkError, httpcore.ProtocolError) as exc:
            log.warning("upstream %s request failed: %s", upstream.name, exc)
            return await self.route_fail(upstream, route, 2)
        except BaseException:
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            ctx.provider = ""
            raise

        if resp.status >= 500:
            return await self.route_fail(upstream, route, 1, resp)

        if resp.status in _RETRY_STATUSES:
            return RouteResult(
                held=True, resp=resp,
                upstream_name=upstream.name,
                upstream_model_id=route.upstream_model_id,
            )

        # TTFT first-chunk gate (optional).
        first_chunk = b""
        stream_iter = None
        if ttft_timeout:
            r = await self._ttft_wait(
                resp, disconnect, slot, upstream, route, start, ttft_timeout,
            )
            if not r.ok:
                return r
            first_chunk = r.first_chunk
            stream_iter = r.stream_iter

        self.upstream_mgr.health.record_success((upstream.name, route.upstream_model_id))
        return RouteResult(
            ok=True, resp=resp,
            upstream_name=upstream.name,
            upstream_model_id=route.upstream_model_id,
            first_chunk=first_chunk,
            start=start,
            stream_iter=stream_iter,
        )

    async def _ttft_wait(
        self,
        resp: httpcore.Response,
        disconnect: asyncio.Event,
        slot,
        upstream: Upstream,
        route: ModelRoute,
        start: float,
        ttft_timeout: float,
    ) -> RouteResult:
        """Race the first-chunk read against disconnect and TTFT timeout.

        Returns a `RouteResult`: `ok=True` with `first_chunk` and
        `stream_iter` on success, or a failure result to return directly.
        """
        remaining = max(0.001, start + ttft_timeout - time.perf_counter())
        stream_iter = aiter(resp.stream)
        read_task = asyncio.ensure_future(anext(stream_iter))
        disc_task = asyncio.ensure_future(disconnect.wait())
        await asyncio.wait(
            {read_task, disc_task}, timeout=remaining,
            return_when=asyncio.FIRST_COMPLETED,
        )
        await cancel_and_await(disc_task)

        if disconnect.is_set():
            await cancel_and_await(read_task)
            await safe_aclose(resp)
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            if slot is not None and self._cancel_telemetry is not None:
                self._cancel_telemetry(
                    slot.wall_start, start, slot.model, slot.key_id,
                    upstream.name, resp.status, stream=True,
                )
            return RouteResult(disconnect=True, upstream_name=upstream.name)

        if not read_task.done():
            await cancel_and_await(read_task)
            await safe_aclose(resp)
            self.upstream_mgr.health.record_failure(
                (upstream.name, route.upstream_model_id), weight=2,
            )
            log.warning(
                "upstream %s TTFT timeout (%.1fs)", upstream.name, ttft_timeout,
            )
            self.upstream_mgr.gate.release_provider(upstream.name, cooldown=True)
            return RouteResult(upstream_name=upstream.name)

        try:
            first_chunk = read_task.result()
        except StopAsyncIteration:
            first_chunk = b""
        except (httpcore.ReadError, httpcore.ReadTimeout) as exc:
            log.warning(
                "upstream %s read failed during TTFT wait: %s",
                upstream.name, exc,
            )
            return await self.route_fail(upstream, route, 1, resp)

        return RouteResult(
            ok=True, first_chunk=first_chunk, stream_iter=stream_iter,
        )

    # ── Multi-route resolution ────────────────────────────────────────

    async def _attempt_and_handle(
        self,
        ctx: RequestContext,
        route: ModelRoute,
        upstream: Upstream,
        body_or_payload,
        held: RouteResult | None,
        slot,
        **kw,
    ) -> tuple[str, RouteResult]:
        """Attempt a route and process the result.

        Returns `(action, result)`:
        - `("ok", r)`: success, caller returns `r`
        - `("disconnect", r)`: client disconnect, caller returns `None`
        - `("held", r)`: retryable status, caller sets `held = r`
        - `("fail", r)`: route failed, caller continues to next
        """
        ctx.provider = route.upstream_name
        r = await self.attempt_route(ctx, route, upstream, body_or_payload, **kw)
        if r.ok:
            await self.release_held(held)
            return "ok", r
        if r.disconnect:
            await self.release_held(held)
            ctx.provider = ""
            return "disconnect", r
        if r.held:
            await self.release_held(held)
            return "held", r
        ctx.provider = ""
        return "fail", r

    async def try_routes(
        self,
        ctx: RequestContext,
        body_or_payload,
        disconnect: asyncio.Event,
        *,
        read_timeout: float = 90.0,
        ttft_timeout: float = 0.0,
    ) -> RouteResult | None:
        """Try routes with health-aware ordering and provider concurrency
        queuing.

        Routes are filtered and ordered inline: healthy first
        (`should_try`), then probing (`should_retry`).  Models with a
        single usable route bypass health checking - there is no
        alternative to fall back to, so blocking would only reject
        requests that have nowhere else to go.  Disabled routes and
        routes whose upstream is disabled do not count toward usable.

        Phase 1 (non-blocking): iterate routes in order.  For each,
        `acquire_provider` (non-blocking).  The first with capacity is
        attempted.  Full routes are collected for phase 2.

        Phase 2 (queue): when *all* routes are full,
        `wait_acquire_provider` races a waiter on every eligible
        provider; the first to free a slot wins.

        Returns the ok `RouteResult` on success, or None on client disconnect.
        """
        slot = ctx.slot
        held: RouteResult | None = None
        upstream_name = ""
        full: list[tuple[ModelRoute, Upstream]] = []

        healthy: list[tuple[ModelRoute, Upstream]] = []
        probing: list[tuple[ModelRoute, Upstream]] = []
        usable: list[tuple[ModelRoute, Upstream]] = []
        for r in ctx.routes:
            if not r.enabled:
                continue
            u = self.management.get_upstream(r.upstream_name)
            if u is None or not u.enabled:
                continue
            usable.append((r, u))
        # With only one usable route there is no alternative to fall back
        # to, so the health guard is bypassed - blocking it would only
        # reject requests that have nowhere else to go.
        single = len(usable) <= 1
        for r, u in usable:
            if single or self.upstream_mgr.health.should_try((u.name, r.upstream_model_id)):
                healthy.append((r, u))
            elif self.upstream_mgr.health.should_retry((u.name, r.upstream_model_id)):
                probing.append((r, u))
        eligible = healthy + probing

        kw = dict(
            disconnect=disconnect,
            read_timeout=read_timeout, ttft_timeout=ttft_timeout,
        )

        # Phase 1: non-blocking acquire.
        for route, upstream in eligible:
            if not self.upstream_mgr.gate.try_provider(route.upstream_name):
                full.append((route, upstream))
                continue
            action, result = await self._attempt_and_handle(
                ctx, route, upstream, body_or_payload, held, slot, **kw,
            )
            upstream_name = result.upstream_name
            if action == "ok":
                return result
            if action == "disconnect":
                return None
            if action == "held":
                held = result

        # Phase 2: queue on full providers.
        if held is None and full:
            names = [r.upstream_name for r, _ in full]
            if slot is not None:
                slot.mark_queued()
            name = await self.upstream_mgr.gate.wait_provider(names, disconnect)
            if name is None:
                await self.release_held(held)
                return None
            ctx.provider = name
            route, upstream = next(
                ((r, u) for r, u in full if r.upstream_name == name), (None, None)
            )
            if route is None:
                self.upstream_mgr.gate.release_provider(name)
                ctx.provider = ""
            else:
                if slot is not None:
                    slot.mark_requesting()
                action, result = await self._attempt_and_handle(
                    ctx, route, upstream, body_or_payload, held, slot, **kw,
                )
                upstream_name = result.upstream_name
                if action == "ok":
                    return result
                if action == "disconnect":
                    return None
                if action == "held":
                    held = result

        # Fall back to a held (retryable-status) response.
        if held is not None:
            ctx.provider = held.upstream_name
            held.start = time.perf_counter()
            return held

        raise UpstreamUnavailable("upstream unavailable", upstream=upstream_name or "none")
