"""Security primitives shared across all layers.

A leaf module: no dependencies on other proxen code, so anything can import
it without creating a circular dependency.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from collections import deque

import msgspec


def hash_key(key: str) -> str:
    """Stable, non-reversible identifier for a proxen API key."""
    return hashlib.sha256(key.encode()).hexdigest()[:12]


def secure_in(token: str, candidates: set[str]) -> bool:
    """Constant-time membership test of `token` against `candidates`.

    Iterates over *every* candidate (no short-circuit) and ORs the
    `hmac.compare_digest` results, so timing does not reveal which key (or
    whether any key) matched.  Returns `False` for an empty token or empty
    candidate set.
    """
    if not token or not candidates:
        return False
    found = False
    token_b = token.encode("utf-8")
    for c in candidates:
        # compare_digest returns False (without raising) for unequal lengths
        # and does not short-circuit on a prefix mismatch.
        if hmac.compare_digest(token_b, c.encode("utf-8")):
            found = True
    return found


def mask_key(key: str) -> str:
    """Return a non-reversible preview of a secret (`"sk-…wxyz"`)."""
    if not key:
        return ""
    if len(key) <= 8:
        return "…"
    return key[:3] + "…" + key[-4:]


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


class AuthRateLimiter:
    """In-memory per-IP throttle for failed authentication attempts.

    After `max_failures` failures within `window` seconds, the IP is locked
    out for `cooldown` seconds.  All state is in-process and best-effort;
    behind a reverse proxy the client IP is taken from the ASGI connection.
    """

    def __init__(
        self,
        window: float = 60.0,
        max_failures: int = 10,
        cooldown: float = 60.0,
    ) -> None:
        self.window = window
        self.max_failures = max_failures
        self.cooldown = cooldown
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    def allow(self, ip: str) -> bool:
        """Return True if the IP may attempt authentication."""
        until = self._locked_until.get(ip)
        if until and time.monotonic() < until:
            return False
        if until:
            self._locked_until.pop(ip, None)
        return True

    def record_failure(self, ip: str) -> None:
        now = time.monotonic()
        lst = self._failures.setdefault(ip, [])
        lst.append(now)
        cutoff = now - self.window
        lst[:] = [t for t in lst if t >= cutoff]
        if len(lst) >= self.max_failures:
            self._locked_until[ip] = now + self.cooldown
            lst.clear()

    def reset(self, ip: str) -> None:
        self._failures.pop(ip, None)
        self._locked_until.pop(ip, None)


class SlidingWindowLimiter:
    """Per-key sliding-window rate limiter.

    After `max_requests` within `window_s` seconds, subsequent requests
    from the same key are rejected until old entries expire.  Each entry is
    appended and evicted exactly once, so admission is amortised O(1).
    """

    def __init__(self, max_requests: int = 100, window_s: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window_s = window_s
        self._entries: dict[str, deque[float]] = {}

    def allow(self, key: str) -> bool:
        """Return True and record the request if under the limit."""
        now = time.monotonic()
        cutoff = now - self.window_s
        entries = self._entries.get(key)
        if entries is None:
            entries = deque()
            self._entries[key] = entries
        while entries and entries[0] < cutoff:
            entries.popleft()
        if len(entries) >= self.max_requests:
            return False
        entries.append(now)
        return True
