from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

import aiohttp
import msgspec

from ..core.config import ModelRoute, Settings
from ..core.contracts import TelemetrySink, UpstreamCatalog
from ..core.gate import InflightSlot
from ..core.models import RequestRecord
from ..core.sse import UsageStats, parse_json_usage
from .upstream import UpstreamManager

if TYPE_CHECKING:
    from ..core.gate import ConcurrencyGate

log = logging.getLogger("proxen.proxy")

# ─── Exceptions ──────────────────────────────────────────────────────


@dataclass
class ProxyError(Exception):
    """Base for errors that produce an HTTP error response.

    Carries only what the HTTP layer needs (message + status). Telemetry
    for the error path is recorded by `Proxy.handle` itself, which
    already holds the request context (model, key_id, slot, gate) as
    locals, so the exception does not need to courier that data out.
    `upstream` is kept because `_try_routes` is the only producer
    that knows which upstream was last attempted.
    """

    message: str = ""
    status: ClassVar[int] = 502
    upstream: str = "none"

    def __post_init__(self) -> None:
        super().__init__(self.message)


class ModelNotFound(ProxyError):
    status: ClassVar[int] = 404


class NoRoutes(ProxyError):
    status: ClassVar[int] = 503


class UpstreamUnavailable(ProxyError):
    status: ClassVar[int] = 502


# ─── Proxy response (resource ownership) ─────────────────────────────


@dataclass
class ProxyResponse:
    """Owns the upstream response + provider slot + gate slot.

    For non-streaming: body is set, resp is None (already cleaned up).
    For streaming: resp is live, body is None (cleanup deferred to generator).
    """

    status: int
    headers: dict[str, str]
    body: bytes | None = None
    resp: aiohttp.ClientResponse | None = None
    upstream_name: str = ""
    proxy_start: float = 0.0
    wall_start: float = 0.0
    # Resource ownership (None for simple/error responses):
    slot: InflightSlot | None = None
    gate: "ConcurrencyGate | None" = None
    upstream_mgr: UpstreamManager | None = None
    proxy: "Proxy | None" = None
    model: str = ""
    key_id: str = ""
    stream: bool = False
    protocol: str = "openai"
    # Pre-read first chunk from the TTFT check in _try_routes. The
    # streaming provider yields this before continuing iteration so the
    # byte is not lost. None for non-streaming / error responses.
    first_chunk: bytes | None = None
    _cleaned: bool = field(default=False, repr=False)

    def cleanup(self) -> None:
        """Release resp + provider + gate. Idempotent. Never raises."""
        if self._cleaned:
            return
        self._cleaned = True
        if self.resp is not None:
            try:
                self.resp.release()
            except Exception:
                log.debug("failed to release upstream response", exc_info=True)
        if self.upstream_mgr is not None and self.upstream_name:
            self.upstream_mgr.release_provider(self.upstream_name)
        if self.gate is not None and self.slot is not None:
            self.gate.release(self.slot)

    def record_telemetry(
        self, *, usage, ttft, tps, status, duration,
        disconnected=False, completed=True,
    ) -> None:
        """Record telemetry via the owning Proxy. Never raises."""
        if self.proxy is None:
            return
        self.proxy.record_telemetry(
            wall_start=self.wall_start, model=self.model,
            upstream=self.upstream_name, key_id=self.key_id,
            ttft=ttft, tps=tps, usage=usage,
            status=status, duration=duration, stream=self.stream,
            disconnected=disconnected, completed=completed,
        )


# ─── Header forwarding ───────────────────────────────────────────────


_HOP_BY_HOP = {
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "authorization",
    "x-api-key",
    "content-encoding",
    "accept-encoding",
}


def protocol_from_path(path) -> str:
    """Derive the wire protocol from the request path.

    Anthropic endpoints live under `/v1/messages*`; every other path is
    treated as OpenAI-compatible. The path is forwarded to the upstream
    unchanged, so a dual-protocol upstream is served correctly on both.
    """
    if isinstance(path, (bytes, bytearray)):
        path = path.decode("utf-8", "replace")
    return "anthropic" if path.startswith("/v1/messages") else "openai"


def _filter_headers(
    src, provider_key: str | None = None, protocol: str = "openai"
) -> dict[str, str]:
    out: dict[str, str] = {}
    if hasattr(src, "items"):
        pairs = src.items()
    else:
        pairs = src
    for key, value in pairs:
        k = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        v = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        if k.lower() in _HOP_BY_HOP:
            continue
        out[k] = v
    if provider_key:
        if protocol == "anthropic":
            out["x-api-key"] = provider_key
            if not any(k.lower() == "anthropic-version" for k in out):
                out["anthropic-version"] = "2023-06-01"
        else:
            out["Authorization"] = f"Bearer {provider_key}"
        out["Accept-Encoding"] = "gzip, deflate"
    return out


# Streaming generation phase (seconds) below this has no trustworthy rate:
# a buffered flush (tokens arriving in a few ms after a long TTFT) has no
# real generation-phase duration to measure, and the raw gen_time would yield
# an absurd rate. Such rows record NULL tps instead of a floored estimate.
# Non-streaming requests have gen_time == 0 by construction (ttft == duration,
# set in _handle_simple) and are handled separately -- they use the whole
# end-to-end duration as the denominator. The aggregate mirrors both in
# _WEIGHTED_TPS (telemetry.py): non-streaming rows use duration_ms, streaming
# rows below the min are excluded (NULL tps_centi), and the rest use a 1s
# rounding guard on gen_time.
_GEN_TIME_MIN = 1.0


def speed_metrics(
    status: int, ttft: float, duration: float, output_tokens: int
) -> tuple[float, float | None]:
    """Throughput (output_tokens per second), excluding time-to-first-token.

    Three cases:
      * Non-streaming (gen_time == 0, i.e. ttft == duration): no separate
        generation phase exists, so the whole end-to-end `duration` is the
        denominator -- a real measurement, never floored.
      * Streaming with gen_time below `_GEN_TIME_MIN`: a buffered flush /
        short burst whose raw gen_time is too small to trust, so the rate is
        not measurable and returned as None (persisted as NULL tps_centi).
      * Streaming otherwise: output_tokens / gen_time, TTFT excluded.

    TTFT may include queue wait rather than generation, so the whole-duration
    rate is only ever used for non-streaming (where there is no alternative).
    Errors and zero-output rows log 0.0."""
    if status >= 400:
        return 0.0, 0.0
    gen_time = duration - ttft
    if output_tokens <= 0:
        return ttft, 0.0
    if gen_time <= 0:
        # Non-streaming: ttft was set to duration, so there is no generation
        # phase to exclude. Use the real end-to-end duration (always > 0).
        return ttft, output_tokens / duration
    if gen_time < _GEN_TIME_MIN:
        # Streaming burst: gen_time is too small to yield a trustworthy rate.
        return ttft, None
    return ttft, output_tokens / gen_time


_EXTRA_BODY_RESERVED = frozenset({"model", "stream"})


def _merge_extra_body(payload: dict, extra_body: dict) -> None:
    """Merge model-level extra_body defaults into the request payload.

    Client-sent values take precedence (only fills missing keys).
    `model` and `stream` are reserved (controlled by the proxy).
    Values are deep-copied to avoid mutating the shared config dict.
    """
    for key, value in extra_body.items():
        if key in _EXTRA_BODY_RESERVED or key in payload:
            continue
        payload[key] = deepcopy(value)


# ─── Proxy ───────────────────────────────────────────────────────────

_SIMPLE_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=10)

_RETRY_STATUSES = frozenset({401, 403, 404, 408, 410, 429})


class Proxy:
    """Routes client requests to upstreams with fallback and telemetry.

    Depends on the :class:`~proxen.core.contracts.UpstreamCatalog` and
    :class:`~proxen.core.contracts.TelemetrySink` contracts rather than
    the concrete management/telemetry types, so the routing core is
    testable with lightweight fakes.
    """

    def __init__(
        self,
        settings: Settings,
        upstream_mgr: UpstreamManager,
        sink: TelemetrySink,
        catalog: UpstreamCatalog,
    ) -> None:
        self.settings = settings
        self.upstream_mgr = upstream_mgr
        self._sink = sink
        self._catalog = catalog

    def record_telemetry(
        self, *, wall_start, model, upstream, key_id, ttft, tps,
        usage, status, duration, stream, disconnected=False, completed=True,
    ) -> None:
        """Build a RequestRecord and enqueue it. Never raises,
        the sink must handle a full queue internally."""
        record = Proxy._record(
            wall_start=wall_start, model=model, upstream=upstream,
            key_id=key_id, ttft=ttft, tps=tps, usage=usage,
            status=status, duration=duration, stream=stream,
            disconnected=disconnected, completed=completed,
        )
        self._sink.enqueue(record)

    @staticmethod
    def _record(
        *, wall_start, model, upstream, key_id, ttft, tps,
        usage, status, duration, stream, disconnected, completed,
    ) -> RequestRecord:
        no_usage = not (usage.input_tokens or usage.output_tokens)
        return RequestRecord(
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
            # A disconnect is a cancel unless the stream finished with usage
            # (a late disconnect after all data was delivered).
            client_disconnect=disconnected and (not completed or no_usage),
            upstream_dropped=not completed and not disconnected,
            needs_review=Proxy._needs_review(
                status, usage, completed=completed, disconnected=disconnected,
            ),
        )

    async def handle(
        self,
        raw_headers: list[tuple[bytes, bytes]],
        path: str,
        query: str,
        body: bytes,
        key_id: str,
        slot: InflightSlot | None = None,
        model: str = "",
        stream: bool = False,
        gate: ConcurrencyGate | None = None,
    ) -> ProxyResponse:
        wall_start = time.time()
        wall_perf = time.perf_counter()
        try:
            if not model:
                raise ModelNotFound("no model specified")

            if slot:
                slot.model = model

            if not self._catalog.is_model_enabled(model):
                raise ModelNotFound(f"model '{model}' is not available")

            routes = self._catalog.get_routes_by_name(model)
            if not routes:
                raise NoRoutes(f"no routes configured for model '{model}'")

            try:
                payload = msgspec.json.decode(body) if body else {}
            except msgspec.DecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            if stream and protocol_from_path(path) != "anthropic":
                so = payload.get("stream_options")
                if not isinstance(so, dict):
                    so = {}
                if "include_usage" not in so:
                    so["include_usage"] = True
                payload["stream_options"] = so

            model_cfg = self._catalog.get_model(model)
            if model_cfg is not None and model_cfg.extra_body:
                _merge_extra_body(payload, model_cfg.extra_body)

            start = time.perf_counter()

            if stream:
                return await self._handle_stream(
                    raw_headers, path, query, payload, routes,
                    model, key_id, start, wall_start, slot, gate,
                )
            return await self._handle_simple(
                raw_headers, path, query, payload, routes,
                model, key_id, start, wall_start, slot, gate,
            )
        except ProxyError as exc:
            # Proxy owns the error path: release the gate slot and record
            # telemetry here so the endpoint only translates the exception
            # into an HTTP response. gate.release is idempotent, so this is
            # safe even if a partial cleanup already released it (e.g. an
            # upstream-read failure inside _handle_simple).
            if slot is not None and gate is not None:
                gate.release(slot)
            self.record_telemetry(
                wall_start=wall_start, model=model or "unknown",
                upstream=exc.upstream, key_id=key_id, ttft=0.0, tps=0.0,
                usage=UsageStats(), status=exc.status,
                duration=time.perf_counter() - wall_perf,
                stream=stream, completed=True,
            )
            raise

    def _upstream_url(self, upstream, path: str, query: str) -> str:
        base = upstream.base_url.rstrip("/")
        if re.search(r"/v\d+$", base) and path.startswith("/v1/"):
            path = path[3:]
        url = base + path
        if query:
            url += "?" + query
        return url

    def _release_held(self, resp: aiohttp.ClientResponse | None, name: str) -> None:
        """Release a held retryable response + its provider slot (if any)."""
        if resp is not None:
            with suppress(Exception):
                resp.release()
            self.upstream_mgr.release_provider(name)

    async def _try_routes(
        self,
        raw_headers,
        path,
        query,
        payload,
        routes: list[ModelRoute],
        start: float,
        slot=None,
        timeout=None,
        ttft_timeout: float | None = None,
    ) -> tuple[aiohttp.ClientResponse, str, float, bytes | None]:
        """Try each route in order. Returns (resp, upstream_name, start, first_chunk).

        For streaming requests with a `ttft_timeout`, the first chunk is
        pre-read before committing to a route: if it doesn't arrive within the
        timeout the route is abandoned (failure recorded) and the next fallback
        route is tried. `record_upstream_success` is deferred until the first
        byte arrives, so a slow-but-alive upstream that never delivers data
        can't reset the health guard.

        Raises UpstreamUnavailable if all routes fail."""
        last_exc: Exception | None = None
        last_upstream: str = ""
        last_retryable_resp: aiohttp.ClientResponse | None = None
        last_retryable_upstream: str = ""

        for route in routes:
            upstream = self._catalog.get_upstream(route.upstream_name)
            if upstream is None or not upstream.enabled:
                continue
            if not self.upstream_mgr.is_healthy(upstream.name):
                log.info("health guard: skipping failing upstream %s", upstream.name)
                continue
            if not self.upstream_mgr.acquire_provider(upstream.name):
                log.info("provider limit: skipping upstream %s", upstream.name)
                continue
            if slot:
                slot.upstream = upstream.name

            payload["model"] = route.upstream_model_id
            route_body = msgspec.json.encode(payload)

            headers = _filter_headers(
                raw_headers, upstream.api_key.get_secret_value(),
                protocol_from_path(path),
            )
            url = self._upstream_url(upstream, path, query)
            try:
                kw: dict = {"headers": headers, "data": route_body}
                if timeout is not None:
                    kw["timeout"] = timeout
                # TTFT budget spans headers + first byte: some upstreams
                # stall before responding, not just after it.
                if ttft_timeout:
                    resp = await asyncio.wait_for(
                        self.upstream_mgr.post(url, **kw),
                        start + ttft_timeout - time.perf_counter(),
                    )
                else:
                    resp = await self.upstream_mgr.post(url, **kw)
            except asyncio.TimeoutError:
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                last_upstream = upstream.name
                log.warning(
                    "upstream %s TTFT timeout (%.1fs), falling back",
                    upstream.name, ttft_timeout,
                )
                start = time.perf_counter()
                continue
            except aiohttp.ClientError as exc:
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                last_exc = exc
                last_upstream = upstream.name
                log.warning("upstream %s connect failed: %s", upstream.name, exc)
                continue
            except BaseException:
                self.upstream_mgr.release_provider(upstream.name)
                self._release_held(last_retryable_resp, last_retryable_upstream)
                raise

            if resp.status >= 500:
                try:
                    resp.release()
                except Exception:
                    log.debug("failed to release fallback response", exc_info=True)
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                start = time.perf_counter()
                last_upstream = upstream.name
                continue

            if resp.status in _RETRY_STATUSES:
                self._release_held(last_retryable_resp, last_retryable_upstream)
                last_retryable_resp = resp
                last_retryable_upstream = upstream.name
                start = time.perf_counter()
                last_upstream = upstream.name
                continue

            # ── TTFT gate (streaming only) ────────────────────────────
            # Shares the budget above; defers record_upstream_success so a
            # 200 that never delivers data can't reset the health guard.
            if ttft_timeout:
                try:
                    first_chunk = await asyncio.wait_for(
                        resp.content.readany(),
                        start + ttft_timeout - time.perf_counter(),
                    )
                except asyncio.TimeoutError:
                    log.warning(
                        "upstream %s TTFT timeout (%.1fs), falling back",
                        upstream.name, ttft_timeout,
                    )
                    first_chunk = None
                except aiohttp.ClientError as exc:
                    log.warning(
                        "upstream %s read failed during TTFT wait: %s",
                        upstream.name, exc,
                    )
                    first_chunk = None
                if first_chunk is None:
                    try:
                        resp.release()
                    except Exception:
                        log.debug("failed to release TTFT response", exc_info=True)
                    self.upstream_mgr.release_provider(upstream.name)
                    self.upstream_mgr.record_upstream_failure(upstream.name)
                    start = time.perf_counter()
                    last_upstream = upstream.name
                    continue
            else:
                first_chunk = b""

            self.upstream_mgr.record_upstream_success(upstream.name)
            self._release_held(last_retryable_resp, last_retryable_upstream)
            return resp, upstream.name, start, first_chunk

        if last_retryable_resp is not None:
            return last_retryable_resp, last_retryable_upstream, start, None

        message = (
            f"upstream unavailable: {last_exc}" if last_exc
            else "upstream unavailable"
        )
        raise UpstreamUnavailable(message, upstream=last_upstream or "none")

    async def _handle_stream(
        self, raw_headers, path, query, payload, routes, model, key_id,
        start, wall_start, slot=None, gate=None,
    ) -> ProxyResponse:
        ttft = self.settings.upstream_ttft_timeout or None
        resp, upstream_name, start, first_chunk = await self._try_routes(
            raw_headers, path, query, payload, routes, start, slot,
            ttft_timeout=ttft,
        )
        return ProxyResponse(
            status=resp.status,
            headers=_filter_headers(resp.headers),
            body=None,
            resp=resp,
            upstream_name=upstream_name,
            proxy_start=start,
            wall_start=wall_start,
            slot=slot,
            gate=gate,
            upstream_mgr=self.upstream_mgr,
            proxy=self,
            model=model,
            key_id=key_id,
            stream=True,
            protocol=protocol_from_path(path),
            first_chunk=first_chunk,
        )

    async def _handle_simple(
        self, raw_headers, path, query, payload, routes, model, key_id,
        start, wall_start, slot=None, gate=None,
    ) -> ProxyResponse:
        resp, upstream_name, start, _ = await self._try_routes(
            raw_headers, path, query, payload, routes, start, slot,
            timeout=_SIMPLE_TIMEOUT,
        )

        result = ProxyResponse(
            status=resp.status,
            headers=_filter_headers(resp.headers),
            resp=resp,
            upstream_name=upstream_name,
            proxy_start=start,
            wall_start=wall_start,
            slot=slot,
            gate=gate,
            upstream_mgr=self.upstream_mgr,
            proxy=self,
            model=model,
            key_id=key_id,
            stream=False,
            protocol=protocol_from_path(path),
        )

        try:
            content = await resp.read()
        except Exception as exc:
            result.cleanup()
            raise UpstreamUnavailable(
                f"upstream read failed: {exc}", upstream=upstream_name,
            ) from exc

        usage = parse_json_usage(content, result.protocol)
        if slot:
            slot.input_tokens = usage.input_tokens
            slot.output_tokens = usage.output_tokens
        duration = time.perf_counter() - start
        ttft, tps = speed_metrics(resp.status, duration, duration, usage.output_tokens)
        result.cleanup()
        result.record_telemetry(
            usage=usage, ttft=ttft, tps=tps,
            status=resp.status, duration=duration,
            completed=True,
        )
        result.body = content
        result.resp = None
        return result

    @staticmethod
    def _needs_review(
        status: int, usage: UsageStats, *, completed: bool = True, disconnected: bool = False
    ) -> bool:
        """A cleanly completed 200 with zero usage — likely a parser issue.

        Cancels and upstream drops legitimately have zero usage, so only a
        stream that finished normally (completed, not disconnected) is flagged.
        """
        if status >= 400 or not completed or disconnected:
            return False
        return not (usage.input_tokens or usage.output_tokens)
