"""ASGI-level utilities: client-disconnect detection and body-size middleware."""
from __future__ import annotations

import asyncio
from contextlib import suppress

import msgspec


# ─── Disconnect helpers ─────────────────────────────────────────────


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


async def race_disconnect(task: asyncio.Task, disconnect: asyncio.Event) -> bool:
    """Race task against disconnect.wait(). Returns True if disconnect won."""
    disc = asyncio.ensure_future(disconnect.wait())
    await asyncio.wait([task, disc], return_when=asyncio.FIRST_COMPLETED)
    if not disc.done():
        disc.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await disc
    return disconnect.is_set()


# ─── Body-size middleware ───────────────────────────────────────────


class BodyTooLargeError(Exception):
    """Raised internally when an inbound request body exceeds the limit."""


class BodySizeMiddleware:
    """Pure-ASGI middleware that rejects request bodies over `max_bytes`.

    Defends against memory-exhaustion from oversized payloads.  A declared
    `Content-Length` is rejected up-front (without reading the body); a
    streamed/chunked body that grows past the limit is truncated mid-read and
    the request is aborted with 413.
    """

    def __init__(self, app, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        for k, v in scope.get("headers", []):
            if k == b"content-length":
                try:
                    declared = int(v)
                except ValueError:
                    await _send_json(send, 413, "Invalid Content-Length")
                    return
                if declared > self.max_bytes:
                    await _send_json(send, 413, "Request body too large")
                    return
                break

        total = 0

        async def guarded_receive():
            nonlocal total
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.max_bytes:
                    raise BodyTooLargeError()
            return message

        try:
            await self.app(scope, guarded_receive, send)
        except BodyTooLargeError:
            await _send_json(send, 413, "Request body too large")


async def _send_json(send, status: int, message: str) -> None:
    body = msgspec.json.encode({"error": {"message": message, "type": "proxen_error"}})
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
