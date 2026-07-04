from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress

from blacksheep import Content, Request, Response, StreamedContent
import msgspec

from ...core.gate import ConcurrencyGate, QueueOverflow, QueueTimeout, RateLimitExceeded
from ...core.security import AuthRateLimiter
from ...services.management import Management
from ...services.proxy import Proxy, ProxyError, watch_disconnect
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

    # Disconnect detection: asyncio.Event + watcher task.
    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(request.content.receive, disconnect)
    )

    async def stop_watcher() -> None:
        if not watcher.done():
            watcher.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await watcher

    # Extract model/stream (throwaway decode; body bytes forwarded as-is).
    try:
        payload = msgspec.json.decode(body) if body else {}
    except (msgspec.DecodeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    model = str(payload.get("model", "") or "")
    stream = bool(payload.get("stream", False))

    try:
        slot = await gate.acquire(key_id)
    except QueueOverflow:
        await stop_watcher()
        return error_json(429, "Queue is full (too many waiting requests)")
    except QueueTimeout:
        await stop_watcher()
        return error_json(408, "Request timed out waiting in queue")
    except RateLimitExceeded as exc:
        await stop_watcher()
        return _rate_limit_response(exc)

    # Check if client disconnected during queue wait.
    if disconnect.is_set():
        gate.release(slot)
        await stop_watcher()
        return error_json(499, "client disconnected")

    raw_headers = list(request.headers)
    path = request.path
    query = (
        request.url.query.decode()
        if isinstance(request.url.query, bytes)
        else (request.url.query or "")
    )

    gate_released = False
    try:
        if stream:
            result = await proxy.forward_stream(
                raw_headers, path, query, body, model, key_id,
                slot, gate, disconnect, watcher,
            )
            if result is None:
                return error_json(499, "client disconnected")

            status, headers, stream_gen = result
            gate_released = True  # gate released in stream_gen's finally

            overhead_ms = (time.perf_counter() - req_start) * 1000
            headers["x-proxen-overhead-ms"] = f"{overhead_ms:.1f}"
            headers["X-Accel-Buffering"] = "no"
            headers["Cache-Control"] = "no-cache"
            content_type = _get_header(headers, "content-type", "text/event-stream").encode()

            return Response(
                status,
                _encode_headers(headers),
                StreamedContent(content_type, stream_gen),
            )
        else:
            result = await proxy.forward_simple(
                raw_headers, path, query, body, model, key_id,
                slot, disconnect,
            )
            if result is None:
                return error_json(499, "client disconnected")

            status, headers, body_content = result
            overhead_ms = (time.perf_counter() - req_start) * 1000
            headers["x-proxen-overhead-ms"] = f"{overhead_ms:.1f}"
            content_type = _get_header(headers, "content-type", "application/json").encode()

            return Response(
                status,
                _encode_headers(headers),
                Content(content_type, body_content),
            )

    except ProxyError as exc:
        return error_json(exc.status, str(exc))
    except Exception:
        log.exception("unexpected error in proxy")
        return error_json(500, "internal error")
    finally:
        if not gate_released:
            gate.release(slot)
            await stop_watcher()


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
