from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

import msgspec
from blacksheep import Response
from blacksheep.server.websocket import WebSocket

from ...core.security import secure_in
from ...services.management import Management
from ...services.telemetry import Database
from ..broadcaster import StatsBroadcaster
from ..http import json_response
from . import get, ws

log = logging.getLogger("proxen.endpoints.dashboard")


def _origin_ok(ws: WebSocket) -> bool:
    origin = ws.headers.get_first(b"origin")
    host = ws.headers.get_first(b"host")
    if not origin or not host:
        return True
    return urlparse(origin.decode()).netloc == host.decode()


@get("/api/stats")
async def stats(broadcaster: StatsBroadcaster) -> Response:
    return json_response(await broadcaster.get_stats())


@get("/api/analysis")
async def analysis(management: Management, db: Database) -> Response:
    model_breakdown, key_breakdown, error_stats, daily_errors, daily_cost = (
        await asyncio.gather(
            db.model_breakdown(),
            db.key_breakdown(),
            db.error_stats(),
            db.daily_errors(),
            db.daily_cost(),
        )
    )
    return json_response({
        "model_breakdown": model_breakdown,
        "key_breakdown": key_breakdown,
        "error_stats": error_stats,
        "daily_errors": daily_errors,
        "daily_cost": daily_cost,
        "key_map": management.key_label_map(),
    })


@ws("/ws")
async def ws_endpoint(
    ws: WebSocket,
    management: Management,
    broadcaster: StatsBroadcaster,
) -> None:
    if not _origin_ok(ws):
        await ws.close(code=1008)
        return

    admin_keys = management.admin_keys()
    if admin_keys:
        token = ws.query.get("admin_key", [None])[0]
        auth = ws.headers.get_first(b"authorization")
        if auth:
            auth_str = auth.decode()
            if auth_str.lower().startswith("bearer "):
                token = auth_str[7:].strip()
        if not token or not secure_in(token, admin_keys):
            await ws.close(code=1008)
            return

    await ws.accept()
    q = broadcaster.subscribe()
    try:
        stats_data = await broadcaster.get_stats()
        await ws.send_text(msgspec.json.encode(stats_data).decode())
        while True:
            stats_data = await q.get()
            await ws.send_text(msgspec.json.encode(stats_data).decode())
    except Exception:
        log.debug("websocket error", exc_info=True)
    finally:
        broadcaster.unsubscribe(q)
        try:
            await ws.close()
        except Exception:
            pass
