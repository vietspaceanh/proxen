from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress

from blacksheep import Content, Request, Response, StreamedContent

from ...core.concurrency import QueueOverflow, QueueTimeout, RateLimitExceeded
from ...core.asgi import watch_disconnect
from ...core.body import peek_model_stream
from ...core.security import AuthRateLimiter
from ...services.management import Management
from ...services.proxy import AdmissionError, Proxy, ProxyError, RequestContext
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
    management: Management,
    auth_limiter: AuthRateLimiter,
) -> Response:
    req_start = time.perf_counter()

    auth_result = authenticate(request, management, auth_limiter)
    if isinstance(auth_result, Response):
        return auth_result
    key_hash = auth_result

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

    # Fast model/stream extraction (no full body decode).
    model, stream = peek_model_stream(body)

    raw_headers = list(request.headers)
    path = request.path
    url_query = request.url.query
    query = (
        url_query.decode()
        if isinstance(url_query, bytes)
        else (url_query or "")
    )
    if isinstance(path, (bytes, bytearray)):
        path = path.decode("utf-8", "replace")

    ctx = RequestContext(
        key_hash=key_hash,
        model=model,
        stream=stream,
        path=path,
        query=query,
        body=body,
        raw_headers=raw_headers,
    )

    # ── Admission hooks (before any resource is acquired) ──────────
    try:
        proxy.admit(ctx)
    except AdmissionError as exc:
        await stop_watcher()
        return error_json(exc.status, exc.message)

    # ── Global concurrency gate ────────────────────────────────────
    try:
        await proxy.acquire(ctx)
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
        proxy.release(ctx)
        await stop_watcher()
        return error_json(499, "client disconnected")

    gate_released = False
    try:
        if stream:
            result = await proxy.forward_stream(
                ctx, disconnect, watcher,
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
                ctx, disconnect,
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

    except QueueOverflow:
        return error_json(429, "Provider queue is full")
    except QueueTimeout:
        return error_json(408, "Request timed out waiting for upstream")
    except ProxyError as exc:
        return error_json(exc.status, str(exc))
    except Exception:
        log.exception("unexpected error in proxy")
        return error_json(500, "internal error")
    finally:
        if not gate_released:
            proxy.release(ctx)
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
