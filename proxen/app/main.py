from __future__ import annotations

import asyncio
import logging
from ipaddress import ip_address
from pathlib import Path

from blacksheep import Application, Content, Response
from blacksheep.server.remotes.forwarding import XForwardedHeadersMiddleware
from blacksheep.server.routing import Router

from ..core.config import Settings, load_settings
from ..core.concurrency import ConcurrencyGate, KeyLimits
from ..core.asgi import BodySizeMiddleware
from ..core.security import AuthRateLimiter, SlidingWindowLimiter
from ..services.management import Management
from ..services.proxy import AdmissionError, Proxy
from ..services.telemetry import Database, TelemetryWriter
from ..services.upstream import UpstreamManager
from .auth import admin_auth_middleware
from .broadcaster import StatsBroadcaster
from .endpoints import registry
from .http import error_json

log = logging.getLogger("proxen")

# Static dashboard assets served alongside the package.
DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"


def create_app(settings: Settings | None = None) -> BodySizeMiddleware:
    """Build the ASGI application: construct the service graph, wire the
    Blacksheep app, and register lifecycle hooks. Returns the wrapped app
    (the outermost middleware)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)

    settings = settings or load_settings()

    # ── Service graph ────────────────────────────────────────────────
    # Construction order follows dependencies.
    db = Database(settings.db_path)
    writer = TelemetryWriter(db)
    management = Management(settings, db)
    gate = ConcurrencyGate(
        settings.max_inflight,
        settings.max_waiting,
        settings.queue_timeout,
    )
    upstream_mgr = UpstreamManager(settings, db, management, gate)

    # Allowlist admit hook - deny requests whose model is not in the
    # presenting key's allowlist (absent allowlist = allow all).
    def _allowlist_hook(ctx):
        if not management.is_model_allowed(ctx.key_hash, ctx.model):
            raise AdmissionError(
                403,
                f"model '{ctx.model}' is not available for this key",
                type="model_access_denied",
            )

    proxy = Proxy(
        settings, upstream_mgr, management, writer, gate,
        admit_hooks=[_allowlist_hook],
    )
    broadcaster = StatsBroadcaster(gate, db, writer, management, upstream_mgr)
    writer.on_flush = broadcaster.mark_dirty
    gate.set_on_change(broadcaster.mark_dirty)
    upstream_mgr.set_on_change(broadcaster.mark_dirty)
    auth_limiter = AuthRateLimiter()
    admin_limiter = SlidingWindowLimiter(
        settings.admin_rate_limit,
        settings.admin_rate_limit_window,
    )
    bg_tasks: list[asyncio.Task] = []

    # ── Blacksheep application ───────────────────────────────────────
    # Fresh router per app, populated from the module-level `registry`
    # (populated when the `proxen.app.endpoints` package is imported).
    # This keeps each test app isolated, unlike blacksheep's default singleton
    # router which is shared across Application instances.
    router = Router()
    for route in registry:
        router.add(route.method, route.pattern, route.handler)

    app = Application(router=router, show_error_details=False)
    app.exceptions_handlers[Exception] = _unhandled
    app.exceptions_handlers[ValueError] = _bad_request

    for svc in [db, writer, management, upstream_mgr,
                proxy, gate, broadcaster, auth_limiter, admin_limiter]:
        app.services.add_instance(svc)

    if settings.trusted_hosts:
        proxies = [ip_address(h.strip()) for h in settings.trusted_hosts.split(",") if h.strip()]
        app.middlewares.append(
            XForwardedHeadersMiddleware(
                known_proxies=proxies,
                accept_only_proxied_requests=False,
            )
        )

    app.middlewares.append(admin_auth_middleware)

    app.on_start(lambda _: _start(settings, db, management, upstream_mgr, writer, gate, broadcaster, bg_tasks))
    app.on_stop(lambda _: _stop(bg_tasks, upstream_mgr, db))
    app.serve_files(DASHBOARD_DIR, root_path="static", allow_anonymous=True)
    app.router.fallback = _spa_fallback

    return BodySizeMiddleware(app, max_bytes=settings.max_body_bytes)


async def _start(settings, db, management, upstream_mgr, writer, gate, broadcaster, bg_tasks):
    await db.init()
    await management.init()
    await upstream_mgr.init()
    await upstream_mgr.sync_models()
    await writer.init_totals()

    db_limits = await management.load_gate_limits()
    max_inflight, max_waiting = db_limits or (
        settings.max_inflight,
        settings.max_waiting,
    )
    gate.set_limits(max_inflight, max_waiting)

    all_key_limits = await management.load_all_key_limits()
    for kid, limits_data in all_key_limits.items():
        key_hash = management.key_hash_by_id(kid)
        if key_hash:
            gate.set_key_limits(key_hash, KeyLimits.from_dict(limits_data))

    for u in management.upstreams:
        upstream_mgr.gate.set_provider_limit(u.name, u.max_inflight)

    log.info(
        "proxen starting -- inflight=%d waiting=%d queue_timeout=%ss db=%s "
        "management=%s",
        max_inflight,
        max_waiting,
        settings.queue_timeout,
        settings.db_path,
        "enabled" if management.management_enabled else "disabled",
    )
    log.info(
        "loaded %d upstreams, %d proxen keys, %d proxen models",
        len(management.upstreams),
        len(management.keys),
        len(management.proxen_models),
    )

    bg_tasks.extend(
        [
            asyncio.create_task(writer.run()),
            asyncio.create_task(upstream_mgr.start_sync_loop()),
            asyncio.create_task(broadcaster.run()),
        ]
    )
    for t, name in zip(
        bg_tasks,
        ("telemetry-writer", "upstream-sync", "stats-broadcaster"),
    ):
        t.add_done_callback(_bg_done(name))


async def _stop(bg_tasks, upstream_mgr, db):
    for task in bg_tasks:
        task.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)
    try:
        await upstream_mgr.aclose()
    finally:
        await db.close()
    log.info("proxen stopped")


def _spa_fallback(request):
    path = request.path
    if path.startswith(("/api/", "/v1/", "/static/")):
        return error_json(404, "Not found")
    return Response(
        200,
        [(b"content-type", b"text/html")],
        Content(b"text/html", (DASHBOARD_DIR / "index.html").read_bytes()),
    )


def _bg_done(name: str):
    def cb(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        if exc := task.exception():
            log.error("%s crashed: %r", name, exc)
        else:
            log.warning("%s exited unexpectedly", name)
    return cb


async def _unhandled(app, request, exc) -> Response:
    log.exception("unhandled exception in request handler")
    return error_json(500, "Internal server error")


async def _bad_request(app, request, exc) -> Response:
    return error_json(400, str(exc))
