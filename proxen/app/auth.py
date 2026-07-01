"""Authentication: client API-key validation + admin-key gating.

A boundary adapter that translates the management service's key store into
HTTP request authentication. Lives in the interface layer because it is
concerned with HTTP requests (headers, query params, client IP).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable

from blacksheep import Request, Response

from ..core.security import AuthRateLimiter, SlidingWindowLimiter, hash_key, secure_in
from ..services.management import Management
from .http import error_json

log = logging.getLogger("proxen.auth")

_bg_tasks: set[asyncio.Task] = set()


def track_background_task(coro: Awaitable[Any]) -> asyncio.Task:
    """Schedule a fire-and-forget coroutine on the module task set.

    The task is tracked so it is not GC'd mid-run, and auto-discards
    on completion. Exceptions are the coroutine's responsibility.
    """
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


def _extract_token(request, query_param: str = "api_key") -> str | None:
    auth = request.headers.get_first(b"authorization")
    if auth:
        auth_str = auth.decode()
        if auth_str.lower().startswith("bearer "):
            return auth_str[7:].strip()
    # Anthropic clients (e.g. Claude Code) send the key as x-api-key.
    api_key = request.headers.get_first(b"x-api-key")
    if api_key:
        return api_key.decode().strip()
    vals = request.query.get(query_param)
    if vals:
        return vals[0]
    return None


async def _safe_touch_key(management: Management, token: str) -> None:
    try:
        await management.touch_key(token)
    except Exception:
        log.debug("touch_key failed", exc_info=True)


def authenticate(request, management, limiter: AuthRateLimiter) -> str | Response:
    """Validate the client's proxen API key.

    Returns a `key_id` string on success, or a `Response` (error) on failure.
    """
    ip = request.client_ip or ""
    if not limiter.allow(ip):
        return error_json(429, "Too many failed auth attempts")

    if not management.keys:
        return "anonymous"

    active_keys = management.active_key_set()
    if not active_keys:
        return error_json(403, "All API keys are deactivated")

    token = _extract_token(request, "api_key")
    if not token or not secure_in(token, active_keys):
        limiter.record_failure(ip)
        return error_json(401, "Invalid API key")
    limiter.reset(ip)
    track_background_task(_safe_touch_key(management, token))
    return hash_key(token)


def authenticate_admin_key(
    request, management, *, allow_open: bool = False
) -> str | Response:
    admin_keys = management.admin_keys()
    if not admin_keys:
        if allow_open:
            return "dashboard"
        return error_json(
            403, "Management API disabled: set admin_api_keys to enable."
        )
    token = _extract_token(request, "admin_key")
    if not token or not secure_in(token, admin_keys):
        return error_json(401, "Invalid admin API key")
    return "admin"


async def admin_auth_middleware(
    request: Request, handler, management: Management, admin_limiter: SlidingWindowLimiter
) -> Response:
    path = request.path
    is_management = path.startswith("/api/management/")
    is_stats = path in ("/api/stats", "/api/analysis")
    if is_management or is_stats:
        ip = request.client_ip or ""
        if not admin_limiter.allow(ip):
            return error_json(429, "Rate limit exceeded")
        result = authenticate_admin_key(request, management, allow_open=is_stats)
        if isinstance(result, Response):
            return result
    return await handler(request)
