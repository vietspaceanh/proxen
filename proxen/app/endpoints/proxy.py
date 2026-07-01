from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress

import aiohttp
import msgspec
from blacksheep import Content, Request, Response, StreamedContent

from ...core.gate import ConcurrencyGate, QueueOverflow, QueueTimeout, RateLimitExceeded
from ...core.security import AuthRateLimiter
from ...core.sse import SSEUsageParser
from ...services.management import Management
from ...services.proxy import Proxy, ProxyError, ProxyResponse, speed_metrics
from ..auth import authenticate
from ..http import error_json, json_response
from . import get, post

log = logging.getLogger("proxen.endpoints.proxy")


@get("/health")
async def health() -> Response:
    return json_response({"status": "ok"})


async def _handle(
    request: Request,
    proxy: Proxy,
    gate: ConcurrencyGate,
    management: Management,
    auth_limiter: AuthRateLimiter,
) -> Response:
    req_start = time.perf_counter()

    auth_result = authenticate(request, management, auth_limiter)
    if isinstance(auth_result, Response):
        return auth_result
    key_id = auth_result

    body = await request.read()
    stream, model = _peek_stream(body)

    try:
        slot = await gate.acquire(key_id)
    except QueueOverflow:
        return error_json(429, "Queue is full (too many waiting requests)")
    except QueueTimeout:
        return error_json(408, "Request timed out waiting in queue")
    except RateLimitExceeded as exc:
        return _rate_limit_response(exc)

    result: ProxyResponse | None = None
    try:
        raw_headers = list(request.headers)
        path = request.path
        query = request.url.query.decode() if isinstance(request.url.query, bytes) else (request.url.query or "")
        result = await proxy.handle(
            raw_headers, path, query, body,
            key_id=key_id, slot=slot, model=model, stream=stream, gate=gate,
        )
        overhead_ms = (time.perf_counter() - req_start) * 1000
        return _build_response(request, result, overhead_ms)
    except ProxyError as exc:
        # Telemetry + slot release are handled inside Proxy.handle.
        return error_json(exc.status, str(exc))
    except Exception:
        log.exception("unexpected error in proxy")
        if result is not None:
            result.cleanup()
        elif slot is not None:
            gate.release(slot)
        return error_json(500, "internal error")
    except BaseException:
        if result is not None:
            result.cleanup()
        elif slot is not None:
            gate.release(slot)
        raise


for _path in (
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/messages",
    "/v1/messages/count_tokens",
):
    post(_path)(_handle)


@get("/v1/models")
async def list_models(management: Management) -> Response:
    models = []
    for pm in management.proxen_models.values():
        if not pm.enabled:
            continue
        m = {"id": pm.id, "object": "model", "owned_by": "proxen"}
        if pm.max_input_tokens is not None:
            m["max_input_tokens"] = pm.max_input_tokens
        if pm.max_output_tokens is not None:
            m["max_output_tokens"] = pm.max_output_tokens
        models.append(m)
    if not models:
        return error_json(503, "model catalog not yet configured")
    return json_response({"object": "list", "data": models})


# ─── Helpers ─────────────────────────────────────────────────────────


async def _disconnect_watcher(
    request: Request, event: asyncio.Event, resp: aiohttp.ClientResponse,
    result: ProxyResponse,
) -> None:
    try:
        while True:
            msg = await request.content.receive()
            if msg.get("type") == "http.disconnect":
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        log.debug("disconnect watcher receive failed", exc_info=True)

    # The client is gone: stop the upstream read and release the gate slot +
    # provider inflight immediately, instead of waiting for the streaming
    # generator's `finally` (which only runs once blacksheep resumes it).
    event.set()
    with suppress(Exception):
        resp.close()
    result.cleanup()


def _peek_stream(body: bytes) -> tuple[bool, str]:
    if not body:
        return False, ""
    try:
        payload = msgspec.json.decode(body)
    except (msgspec.DecodeError, UnicodeDecodeError, TypeError, ValueError):
        return False, ""
    if not isinstance(payload, dict):
        return False, ""
    return bool(payload.get("stream")), payload.get("model", "")


def _encode_headers(headers: dict[str, str]) -> list[tuple[bytes, bytes]]:
    return [(k.encode(), v.encode()) for k, v in headers.items()]


def _rate_limit_response(exc: RateLimitExceeded) -> Response:
    return json_response({
        "error": {
            "message": f"Rate limit exceeded: {exc.limit_type} (limit: {exc.limit})",
            "type": "rate_limit_exceeded",
            "limit_type": exc.limit_type,
            "limit": exc.limit,
        }
    }, status=429)


def _get_header(headers: dict[str, str], name: str, default: str = "") -> str:
    lower = name.lower()
    for k, v in headers.items():
        if k.lower() == lower:
            return v
    return default


def _headers_with_overhead(result: ProxyResponse, overhead_ms: float) -> dict[str, str]:
    headers = dict(result.headers)
    headers["x-proxen-overhead-ms"] = f"{overhead_ms:.1f}"
    return headers


def _build_response(
    request: Request,
    result: ProxyResponse,
    overhead_ms: float,
) -> Response:
    if result.resp is not None:
        return _build_streamed(request, result, overhead_ms)

    headers = _headers_with_overhead(result, overhead_ms)
    content_type = _get_header(result.headers, "content-type", "application/json").encode()
    return Response(
        result.status,
        _encode_headers(headers),
        Content(content_type, result.body or b""),
    )


def _build_streamed(
    request: Request,
    result: ProxyResponse,
    overhead_ms: float,
) -> Response:
    resp = result.resp

    async def provider():
        parser = SSEUsageParser(result.protocol)
        completed = False
        disconnected = False
        ttft: float | None = None
        disconnect_event = asyncio.Event()
        watcher = None
        upstream_error = False
        try:
            watcher = asyncio.create_task(
                _disconnect_watcher(request, disconnect_event, resp, result)
            )
            # Yield the pre-read first chunk (from the TTFT gate in
            # _try_routes) before continuing with the upstream iterator.
            if result.first_chunk:
                chunk = result.first_chunk
                if ttft is None:
                    ttft = time.perf_counter() - result.proxy_start
                parser.feed(chunk)
                if result.slot:
                    result.slot.last_byte_time = time.monotonic()
                    if result.gate:
                        result.gate.reset_idle(result.slot)
                if not disconnect_event.is_set():
                    yield chunk
            async for chunk in resp.content.iter_any():
                if ttft is None:
                    ttft = time.perf_counter() - result.proxy_start
                parser.feed(chunk)
                if result.slot:
                    result.slot.last_byte_time = time.monotonic()
                    if result.gate:
                        result.gate.reset_idle(result.slot)
                if disconnect_event.is_set():
                    break
                yield chunk
            else:
                completed = True
        except Exception:
            if not disconnect_event.is_set():
                upstream_error = True
                raise
        finally:
            if watcher is not None:
                watcher.cancel()
            disconnected = disconnect_event.is_set()
            usage, found_usage = parser.finalize()
            completed = completed or found_usage
            if result.slot:
                result.slot.input_tokens = usage.input_tokens
                result.slot.output_tokens = usage.output_tokens
            duration = time.perf_counter() - result.proxy_start
            if ttft is None:
                ttft = duration
            ttft_val, tps = speed_metrics(
                result.status, ttft, duration, usage.output_tokens
            )
            # Feed the health guard only when the upstream itself terminated
            # the stream abnormally (read error / sock_read timeout / reset).
            # A client disconnect is a user action, not an upstream-health
            # signal: poisoning on it forces spurious fallback, which
            # discards the upstream's prompt cache. The cancel is still
            # recorded as telemetry below for dashboard visibility.
            if (upstream_error
                    and result.upstream_mgr is not None
                    and result.upstream_name):
                result.upstream_mgr.record_upstream_failure(result.upstream_name)
            result.cleanup()
            result.record_telemetry(
                usage=usage, ttft=ttft_val, tps=tps,
                status=result.status, duration=duration,
                disconnected=disconnected,
                completed=completed,
            )
            log.info(
                "stream ended completed=%s disconnected=%s model=%s duration=%.3f",
                completed, disconnected, result.model, duration,
            )

    headers = _headers_with_overhead(result, overhead_ms)
    headers["X-Accel-Buffering"] = "no"
    headers["Cache-Control"] = "no-cache"
    content_type = _get_header(result.headers, "content-type", "text/event-stream").encode()

    return Response(
        result.status,
        _encode_headers(headers),
        StreamedContent(content_type, provider),
    )
