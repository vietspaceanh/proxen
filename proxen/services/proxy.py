from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import suppress
from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar

import aiohttp
import msgspec

from ..core.config import ModelRoute, Settings
from ..core.contracts import TelemetrySink, UpstreamCatalog
from ..core.gate import InflightSlot
from ..core.models import RequestRecord
from ..core.sse import UsageStats, parse_json_usage, SSEUsageParser
from .upstream import UpstreamManager

if TYPE_CHECKING:
    from ..core.gate import ConcurrencyGate

log = logging.getLogger("proxen.proxy")

# ─── Exceptions ──────────────────────────────────────────────────────


class ProxyError(Exception):
    """Base for errors that produce an HTTP error response."""

    message: str = ""
    status: ClassVar[int] = 502
    upstream: str = "none"

    def __init__(self, message: str = "", *, upstream: str = "none") -> None:
        self.message = message
        self.upstream = upstream
        super().__init__(message)


class ModelNotFound(ProxyError):
    status: ClassVar[int] = 404


class NoRoutes(ProxyError):
    status: ClassVar[int] = 503


class UpstreamUnavailable(ProxyError):
    status: ClassVar[int] = 502


# ─── Header forwarding ───────────────────────────────────────────────

_HOP_BY_HOP = {
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "authorization",
    "x-api-key",
}

_RESP_STRIP = _HOP_BY_HOP | {"content-encoding", "accept-encoding"}


def protocol_from_path(path) -> str:
    if isinstance(path, (bytes, bytearray)):
        path = path.decode("utf-8", "replace")
    return "anthropic" if path.startswith("/v1/messages") else "openai"


def _filter_headers(
    src, provider_key: str | None = None, protocol: str = "openai"
) -> dict[str, str]:
    """Filter headers for forwarding."""
    out: dict[str, str] = {}
    if hasattr(src, "items"):
        pairs = src.items()
    else:
        pairs = src
    strip = _HOP_BY_HOP if provider_key is not None else _RESP_STRIP
    for key, value in pairs:
        k = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        v = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        if k.lower() in strip:
            continue
        out[k] = v
    if provider_key:
        if protocol == "anthropic":
            out["x-api-key"] = provider_key
            if not any(k.lower() == "anthropic-version" for k in out):
                out["anthropic-version"] = "2023-06-01"
        else:
            out["Authorization"] = f"Bearer {provider_key}"
    return out


# ─── Speed metrics ───────────────────────────────────────────────────

_GEN_TIME_MIN = 1.0


def speed_metrics(
    status: int, ttft: float, duration: float, output_tokens: int
) -> tuple[float, float | None]:
    if status >= 400:
        return 0.0, 0.0
    gen_time = duration - ttft
    if output_tokens <= 0:
        return ttft, 0.0
    if gen_time <= 0:
        return ttft, output_tokens / duration
    if gen_time < _GEN_TIME_MIN:
        return ttft, None
    return ttft, output_tokens / gen_time


# ─── Byte-level model field patching ─────────────────────────────────

_WS = b" \t\n\r"
_Q = 0x22    # "
_BS = 0x5C   # \
_LB = 0x7B   # {
_RB = 0x7D   # }
_LK = 0x5B   # [
_RK = 0x5D   # ]
_C = 0x3A    # :
_CM = 0x2C   # ,


def _str_end(body: bytes, i: int, n: int) -> int:
    i += 1
    while i < n:
        if body[i] == _BS:
            i += 2
            continue
        if body[i] == _Q:
            return i + 1
        i += 1
    return i


def _skip_val(body: bytes, i: int, n: int) -> int:
    if i >= n:
        return i
    c = body[i]
    if c == _Q:
        return _str_end(body, i, n)
    if c in (_LB, _LK):
        depth = 1
        in_str = esc = False
        i += 1
        while i < n and depth > 0:
            ch = body[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == _BS:
                    esc = True
                elif ch == _Q:
                    in_str = False
            elif ch == _Q:
                in_str = True
            elif ch in (_LB, _LK):
                depth += 1
            elif ch in (_RB, _RK):
                depth -= 1
            i += 1
        return i
    while i < n and body[i] not in _WS and body[i] not in (_CM, _RB, _RK):
        i += 1
    return i


def patch_field(body: bytes, field: str, new_value: str) -> bytes:
    """Replace a top-level field's value in JSON bytes. Preserves all other bytes."""
    n = len(body)
    i = 0
    while i < n and body[i] in _WS:
        i += 1
    if i >= n or body[i] != _LB:
        return body
    i += 1
    target = msgspec.json.encode(field)
    while i < n:
        while i < n and body[i] in _WS:
            i += 1
        if i >= n or body[i] == _RB:
            break
        if body[i] == _CM:
            i += 1
            continue
        if body[i] != _Q:
            i = _skip_val(body, i, n)
            continue
        ke = _str_end(body, i, n)
        if body[i:ke] == target:
            i = ke
            while i < n and body[i] in _WS:
                i += 1
            if i < n and body[i] == _C:
                i += 1
            while i < n and body[i] in _WS:
                i += 1
            vs = i
            ve = _skip_val(body, i, n)
            return body[:vs] + msgspec.json.encode(new_value) + body[ve:]
        i = ke
        while i < n and body[i] in _WS:
            i += 1
        if i < n and body[i] == _C:
            i += 1
        while i < n and body[i] in _WS:
            i += 1
        i = _skip_val(body, i, n)
    return body


# ─── Extra body merge ────────────────────────────────────────────────

_EXTRA_BODY_RESERVED = frozenset({"model", "stream"})


def _merge_extra_body(payload: dict, extra_body: dict) -> None:
    for key, value in extra_body.items():
        if key in _EXTRA_BODY_RESERVED or key in payload:
            continue
        payload[key] = deepcopy(value)


# ─── Disconnect helpers ──────────────────────────────────────────────


async def watch_disconnect(receive, event: asyncio.Event) -> None:
    """Own receive() and set event on http.disconnect."""
    try:
        while True:
            msg = await receive()
            if msg.get("type") == "http.disconnect":
                event.set()
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        event.set()


async def _race(task: asyncio.Task, disconnect: asyncio.Event) -> bool:
    """Race task against disconnect.wait(). Returns True if disconnect won."""
    disc = asyncio.ensure_future(disconnect.wait())
    await asyncio.wait([task, disc], return_when=asyncio.FIRST_COMPLETED)
    if not disc.done():
        disc.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await disc
    return disconnect.is_set()


# ─── Proxy ───────────────────────────────────────────────────────────

_SIMPLE_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=10)
_RETRY_STATUSES = frozenset({401, 403, 404, 408, 410, 429})


class Proxy:
    """Routes client requests to upstreams with fallback and telemetry."""

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

    # ── Telemetry ────────────────────────────────────────────────────

    def record_telemetry(
        self, *, wall_start, model, upstream, key_id, ttft, tps,
        usage, status, duration, stream, disconnected=False, completed=True,
    ) -> None:
        self._sink.enqueue(self._record(
            wall_start=wall_start, model=model, upstream=upstream,
            key_id=key_id, ttft=ttft, tps=tps, usage=usage,
            status=status, duration=duration, stream=stream,
            disconnected=disconnected, completed=completed,
        ))

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
            client_disconnect=disconnected and (not completed or no_usage),
            upstream_dropped=not completed and not disconnected,
            needs_review=Proxy._needs_review(
                status, usage, completed=completed, disconnected=disconnected,
            ),
        )

    @staticmethod
    def _needs_review(
        status: int, usage: UsageStats, *, completed: bool = True, disconnected: bool = False
    ) -> bool:
        if status >= 400 or not completed or disconnected:
            return False
        return not (usage.input_tokens or usage.output_tokens)

    def _error_telemetry(
        self, wall_start: float, wall_perf: float, model: str,
        key_id: str, upstream: str, status: int, *, stream: bool,
    ) -> None:
        self.record_telemetry(
            wall_start=wall_start, model=model or "unknown",
            upstream=upstream, key_id=key_id, ttft=0.0, tps=0.0,
            usage=UsageStats(), status=status,
            duration=time.perf_counter() - wall_perf,
            stream=stream, completed=True,
        )

    # ── Shared helpers ────────────────────────────────────────────────

    def _upstream_url(self, upstream, path: str, query: str) -> str:
        base = upstream.base_url.rstrip("/")
        if re.search(r"/v\d+$", base) and path.startswith("/v1/"):
            path = path[3:]
        url = base + path
        if query:
            url += "?" + query
        return url

    def _release_held(self, resp: aiohttp.ClientResponse | None, name: str) -> None:
        if resp is not None:
            with suppress(Exception):
                resp.release()
            self.upstream_mgr.release_provider(name)

    def _prepare_request(
        self, body: bytes, model: str, path: str,
    ) -> tuple[list[ModelRoute], bytes | dict, str]:
        """Model lookup + body preparation. No disconnect/gate/resp."""
        if not model:
            raise ModelNotFound("no model specified")
        if not self._catalog.is_model_enabled(model):
            raise ModelNotFound(f"model '{model}' is not available")
        routes = self._catalog.get_routes_by_name(model)
        if not routes:
            raise NoRoutes(f"no routes configured for model '{model}'")

        model_cfg = self._catalog.get_model(model)
        protocol = protocol_from_path(path)

        if model_cfg is not None and model_cfg.extra_body:
            try:
                payload = msgspec.json.decode(body) if body else {}
            except msgspec.DecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            _merge_extra_body(payload, model_cfg.extra_body)
            return routes, payload, protocol

        return routes, body, protocol

    def _route_body(self, body_or_payload: bytes | dict, client_model: str, route: ModelRoute) -> bytes:
        if isinstance(body_or_payload, dict):
            body_or_payload["model"] = route.upstream_model_id
            return msgspec.json.encode(body_or_payload)
        if route.upstream_model_id != client_model:
            return patch_field(body_or_payload, "model", route.upstream_model_id)
        return body_or_payload

    # ── Routing (shared by forward_stream + forward_simple) ───────────

    async def _try_routes(
        self,
        raw_headers,
        path: str,
        query: str,
        body_or_payload,
        routes: list[ModelRoute],
        model: str,
        protocol: str,
        slot: InflightSlot | None,
        disconnect: asyncio.Event,
        *,
        ttft_timeout: float | None = None,
        simple_timeout: aiohttp.ClientTimeout | None = None,
    ) -> tuple[aiohttp.ClientResponse, str, float, bytes] | None:
        """Try each route. Returns (resp, upstream, start, first_chunk) or None."""
        last_exc: Exception | None = None
        last_retryable_resp: aiohttp.ClientResponse | None = None
        last_retryable_upstream = ""
        upstream_name = ""
        start = time.perf_counter()

        for route in routes:
            if not route.enabled:
                continue
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

            route_body = self._route_body(body_or_payload, model, route)
            headers = _filter_headers(
                raw_headers, upstream.api_key.get_secret_value(), protocol,
            )
            url = self._upstream_url(upstream, path, query)
            kw: dict = {"headers": headers, "data": route_body}
            if simple_timeout is not None:
                kw["timeout"] = simple_timeout

            # POST - race against disconnect
            if ttft_timeout:
                remaining = start + ttft_timeout - time.perf_counter()
                post_task = asyncio.ensure_future(
                    asyncio.wait_for(
                        self.upstream_mgr.post(url, **kw), max(0.001, remaining),
                    )
                )
            else:
                post_task = asyncio.ensure_future(self.upstream_mgr.post(url, **kw))

            if await _race(post_task, disconnect):
                if post_task.done():
                    with suppress(Exception):
                        post_task.result().close()
                else:
                    post_task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await post_task
                self.upstream_mgr.release_provider(upstream.name)
                self._release_held(last_retryable_resp, last_retryable_upstream)
                return None

            try:
                resp = post_task.result()
            except asyncio.TimeoutError:
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                upstream_name = upstream.name
                start = time.perf_counter()
                continue
            except aiohttp.ClientError as exc:
                last_exc = exc
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                upstream_name = upstream.name
                log.warning("upstream %s connect failed: %s", upstream.name, exc)
                start = time.perf_counter()
                continue
            except BaseException:
                self.upstream_mgr.release_provider(upstream.name)
                self._release_held(last_retryable_resp, last_retryable_upstream)
                raise

            # ── Status check ──────────────────────────────────────
            if resp.status >= 500:
                with suppress(Exception):
                    resp.release()
                self.upstream_mgr.release_provider(upstream.name)
                self.upstream_mgr.record_upstream_failure(upstream.name)
                upstream_name = upstream.name
                start = time.perf_counter()
                continue

            if resp.status in _RETRY_STATUSES:
                self._release_held(last_retryable_resp, last_retryable_upstream)
                last_retryable_resp = resp
                last_retryable_upstream = upstream.name
                upstream_name = upstream.name
                start = time.perf_counter()
                continue

            # ── TTFT gate (streaming only) ─────────────────────────
            first_chunk = b""
            if ttft_timeout:
                remaining = start + ttft_timeout - time.perf_counter()
                read_task = asyncio.ensure_future(
                    asyncio.wait_for(
                        resp.content.readany(), max(0.001, remaining),
                    )
                )

                if await _race(read_task, disconnect):
                    with suppress(Exception):
                        resp.close()
                    if not read_task.done():
                        read_task.cancel()
                        with suppress(asyncio.CancelledError, Exception):
                            await read_task
                    self.upstream_mgr.release_provider(upstream.name)
                    self._release_held(last_retryable_resp, last_retryable_upstream)
                    return None

                try:
                    first_chunk = read_task.result()
                except asyncio.TimeoutError:
                    log.warning(
                        "upstream %s TTFT timeout (%.1fs), falling back",
                        upstream.name, ttft_timeout,
                    )
                    with suppress(Exception):
                        resp.release()
                    self.upstream_mgr.release_provider(upstream.name)
                    self.upstream_mgr.record_upstream_failure(upstream.name)
                    upstream_name = upstream.name
                    start = time.perf_counter()
                    continue
                except aiohttp.ClientError as exc:
                    last_exc = exc
                    log.warning(
                        "upstream %s read failed during TTFT wait: %s",
                        upstream.name, exc,
                    )
                    with suppress(Exception):
                        resp.release()
                    self.upstream_mgr.release_provider(upstream.name)
                    self.upstream_mgr.record_upstream_failure(upstream.name)
                    upstream_name = upstream.name
                    start = time.perf_counter()
                    continue

            # ── Route succeeded ────────────────────────────────────
            self.upstream_mgr.record_upstream_success(upstream.name)
            self._release_held(last_retryable_resp, last_retryable_upstream)
            return resp, upstream.name, start, first_chunk

        # All routes exhausted
        if last_retryable_resp is not None:
            return last_retryable_resp, last_retryable_upstream, start, b""

        message = (
            f"upstream unavailable: {last_exc}" if last_exc
            else "upstream unavailable"
        )
        raise UpstreamUnavailable(message, upstream=upstream_name or "none")

    # ── Forward: streaming ────────────────────────────────────────────

    async def forward_stream(
        self,
        raw_headers: list[tuple[bytes, bytes]],
        path: str,
        query: str,
        body: bytes,
        model: str,
        key_id: str,
        slot: InflightSlot | None,
        gate: ConcurrencyGate | None,
        disconnect: asyncio.Event,
        watcher: asyncio.Task,
    ) -> tuple[int, dict[str, str], Any] | None:
        """Forward a streaming request. Returns (status, headers, gen) or None."""
        wall_start = time.time()
        wall_perf = time.perf_counter()

        try:
            routes, body_or_payload, protocol = self._prepare_request(
                body, model, path,
            )
        except ProxyError as exc:
            self._error_telemetry(
                wall_start, wall_perf, model, key_id,
                exc.upstream, exc.status, stream=True,
            )
            raise
        if slot:
            slot.model = model

        ttft_timeout = self.settings.upstream_ttft_timeout or None
        result = await self._try_routes(
            raw_headers, path, query, body_or_payload, routes, model,
            protocol, slot, disconnect, ttft_timeout=ttft_timeout,
        )
        if result is None:
            return None

        resp, upstream_name, start, first_chunk = result

        # ── Build streaming generator (closure captures all state) ─────
        _self = self
        _resp = resp
        _disconnect = disconnect
        _watcher = watcher
        _gate = gate
        _slot = slot
        _upstream_name = upstream_name
        _model = model
        _key_id = key_id
        _start = start
        _wall_start = wall_start
        _protocol = protocol
        _first_chunk = first_chunk
        _released = False
        _gen_started = False

        def _release(*, force: bool = True) -> None:
            """Idempotent: close resp + release provider + gate + watcher."""
            nonlocal _released
            if _released:
                return
            _released = True
            with suppress(Exception):
                _resp.close() if force else _resp.release()
            _self.upstream_mgr.release_provider(_upstream_name)
            if _gate is not None and _slot is not None:
                _gate.release(_slot)
            if not _watcher.done():
                _watcher.cancel()

        async def _watch_and_release() -> None:
            """Await disconnect then release. Safety timeout if generator never starts."""
            try:
                await asyncio.wait_for(_disconnect.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                if _gen_started:
                    await _disconnect.wait()
                else:
                    log.warning(
                        "stream generator never started - releasing resources"
                    )
            _release()

        _watch_task = asyncio.ensure_future(_watch_and_release())

        async def stream_gen():
            nonlocal _gen_started
            _gen_started = True
            parser = SSEUsageParser(_protocol)
            completed = False
            upstream_error = False
            ttft: float | None = None
            try:
                if _first_chunk:
                    if ttft is None:
                        ttft = time.perf_counter() - _start
                    parser.feed(_first_chunk)
                    if _slot:
                        _slot.last_byte_time = time.monotonic()
                        if _gate:
                            _gate.reset_idle(_slot)
                    if not _disconnect.is_set():
                        yield _first_chunk
                async for chunk in _resp.content.iter_any():
                    if ttft is None:
                        ttft = time.perf_counter() - _start
                    parser.feed(chunk)
                    if _slot:
                        _slot.last_byte_time = time.monotonic()
                        if _gate:
                            _gate.reset_idle(_slot)
                    if _disconnect.is_set():
                        break
                    yield chunk
                else:
                    completed = True
            except Exception:
                if not _disconnect.is_set():
                    upstream_error = True
                    raise
            finally:
                _watch_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await _watch_task
                disconnected = _disconnect.is_set()
                usage, found_usage = parser.finalize()
                completed_final = completed or found_usage
                if _slot:
                    _slot.input_tokens = usage.input_tokens
                    _slot.output_tokens = usage.output_tokens
                duration = time.perf_counter() - _start
                if ttft is None:
                    ttft = duration
                ttft_val, tps = speed_metrics(
                    _resp.status, ttft, duration, usage.output_tokens
                )
                if upstream_error and _upstream_name:
                    _self.upstream_mgr.record_upstream_failure(_upstream_name)
                _release(force=not completed)
                if not _watcher.done():
                    with suppress(asyncio.CancelledError, Exception):
                        await _watcher
                _self.record_telemetry(
                    wall_start=_wall_start, model=_model,
                    upstream=_upstream_name, key_id=_key_id,
                    ttft=ttft_val, tps=tps, usage=usage,
                    status=_resp.status, duration=duration,
                    stream=True, disconnected=disconnected,
                    completed=completed_final,
                )
                log.info(
                    "stream ended completed=%s disconnected=%s model=%s duration=%.3f",
                    completed_final, disconnected, _model, duration,
                )

        return resp.status, _filter_headers(resp.headers), stream_gen

    # ── Forward: non-streaming ────────────────────────────────────────

    async def forward_simple(
        self,
        raw_headers: list[tuple[bytes, bytes]],
        path: str,
        query: str,
        body: bytes,
        model: str,
        key_id: str,
        slot: InflightSlot | None,
        disconnect: asyncio.Event,
    ) -> tuple[int, dict[str, str], bytes] | None:
        """Forward a non-streaming request. Returns (status, headers, body) or None."""
        wall_start = time.time()
        wall_perf = time.perf_counter()

        try:
            routes, body_or_payload, protocol = self._prepare_request(
                body, model, path,
            )
        except ProxyError as exc:
            self._error_telemetry(
                wall_start, wall_perf, model, key_id,
                exc.upstream, exc.status, stream=False,
            )
            raise
        if slot:
            slot.model = model

        result = await self._try_routes(
            raw_headers, path, query, body_or_payload, routes, model,
            protocol, slot, disconnect, simple_timeout=_SIMPLE_TIMEOUT,
        )
        if result is None:
            return None

        resp, upstream_name, start, _ = result

        # Read response - race against disconnect
        read_task = asyncio.ensure_future(resp.read())
        if await _race(read_task, disconnect):
            if not read_task.done():
                read_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await read_task
            with suppress(Exception):
                resp.close()
            self.upstream_mgr.release_provider(upstream_name)
            return None

        try:
            content = read_task.result()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            with suppress(Exception):
                resp.close()
            self.upstream_mgr.release_provider(upstream_name)
            self._error_telemetry(
                wall_start, wall_perf, model, key_id,
                upstream_name, 502, stream=False,
            )
            raise UpstreamUnavailable(
                f"upstream read failed: {exc}", upstream=upstream_name,
            ) from exc

        # ── Telemetry + cleanup ───────────────────────────────────
        usage = parse_json_usage(content, protocol)
        if slot:
            slot.input_tokens = usage.input_tokens
            slot.output_tokens = usage.output_tokens
        duration = time.perf_counter() - start
        ttft, tps = speed_metrics(resp.status, duration, duration, usage.output_tokens)

        with suppress(Exception):
            resp.release()
        self.upstream_mgr.release_provider(upstream_name)

        self.record_telemetry(
            wall_start=wall_start, model=model, upstream=upstream_name,
            key_id=key_id, ttft=ttft, tps=tps, usage=usage,
            status=resp.status, duration=duration,
            stream=False, completed=True,
        )

        return resp.status, _filter_headers(resp.headers), content
