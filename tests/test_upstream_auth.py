from __future__ import annotations

import asyncio
import json
import time

import pytest

from proxen.core.config import Settings, Upstream
from proxen.services.management import Management
from proxen.services.telemetry import Database
from proxen.services.upstream_auth import BrowserAuthService


class _Response:
    def __init__(self, status: int, body: dict):
        self.status = status
        self._body = json.dumps(body).encode()

    async def aread(self) -> bytes:
        return self._body

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_database_schema_has_current_upstream_fields(tmp_path):
    db = Database(str(tmp_path / "schema.db"))
    await db.init()
    async with await db.execute("PRAGMA table_info(upstreams)") as cur:
        columns = {row[1] for row in await cur.fetchall()}
    assert "profile" in columns
    async with await db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'upstream_auth_tokens'"
    ) as cur:
        assert await cur.fetchone() is not None
    await db.close()


@pytest.mark.asyncio
async def test_browser_auth_refresh_is_single_flight(tmp_path):
    db = Database(str(tmp_path / "auth.db"))
    await db.init()
    await db.execute_commit(
        """INSERT INTO upstreams
           (name, base_url, api_key, enabled, created_at, updated_at)
           VALUES (?, ?, ?, 1, ?, ?)""",
        ("chatgpt", "https://example.test", "", time.time(), time.time()),
    )
    calls = 0

    async def requester(method, url, **kwargs):
        nonlocal calls
        calls += 1
        assert method == "POST"
        assert url.endswith("/oauth/token")
        return _Response(200, {
            "access_token": "fresh-access",
            "refresh_token": "fresh-refresh",
            "expires_in": 3600,
        })

    service = BrowserAuthService(db, requester)
    await service._save_tokens("chatgpt", {
        "access_token": "old-access",
        "refresh_token": "old-refresh",
        "expires_in": 1,
    })
    await db.execute_commit(
        "UPDATE upstream_auth_tokens SET expires_at = ? WHERE upstream_name = ?",
        (time.time() - 1, "chatgpt"),
    )
    upstream = Upstream(
        name="chatgpt",
        profile="openai-responses",
    )

    headers = await asyncio.gather(
        service.headers(upstream),
        service.headers(upstream),
    )
    assert calls == 1
    assert headers[0]["Authorization"] == "Bearer fresh-access"
    assert headers[1]["Authorization"] == "Bearer fresh-access"

    await service.aclose()
    await db.close()


@pytest.mark.asyncio
async def test_browser_flow_returns_url_without_credentials(tmp_path):
    db = Database(str(tmp_path / "auth-flow.db"))
    await db.init()
    await db.execute_commit(
        """INSERT INTO upstreams
           (name, base_url, api_key, enabled, created_at, updated_at)
           VALUES (?, ?, ?, 1, ?, ?)""",
        ("chatgpt", "https://example.test", "", time.time(), time.time()),
    )

    async def requester(*args, **kwargs):
        raise AssertionError("browser flow should not call token endpoints yet")

    service = BrowserAuthService(db, requester)
    upstream = Upstream(
        name="chatgpt",
        profile="openai-responses",
    )
    flow = await service.start(upstream)
    assert flow["status"] == "pending"
    assert flow["authorization_url"].startswith("https://auth.openai.com/oauth/authorize?")
    assert "access_token" not in flow
    assert "refresh_token" not in flow

    await service.aclose()
    await db.close()


@pytest.mark.asyncio
async def test_rename_moves_routes_cache_and_auth_tokens_atomically(tmp_path):
    db = Database(str(tmp_path / "rename.db"))
    await db.init()
    management = Management(Settings(), db)
    await management.init()
    await management.add_upstream({
        "name": "chatgpt-old",
        "profile": "openai-responses",
    })
    await management.add_proxen_model({
        "id": "gpt-test",
        "routes": [{
            "upstream_name": "chatgpt-old",
            "upstream_model_id": "gpt-test",
        }],
    })
    await db.execute_commit(
        "INSERT INTO models_cache (upstream, id, object, updated_at) VALUES (?, ?, ?, ?)",
        ("chatgpt-old", "gpt-test", "model", time.time()),
    )
    await db.execute_commit(
        "INSERT INTO upstream_auth_tokens "
        "(upstream_name, access_token, refresh_token, expires_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("chatgpt-old", "access", "refresh", time.time() + 3600, time.time()),
    )

    await management.update_upstream("chatgpt-old", {"name": "chatgpt-new"})

    async with await db.execute(
        "SELECT upstream_name FROM model_routes"
    ) as cur:
        assert (await cur.fetchone())[0] == "chatgpt-new"
    async with await db.execute(
        "SELECT upstream FROM models_cache"
    ) as cur:
        assert (await cur.fetchone())[0] == "chatgpt-new"
    async with await db.execute(
        "SELECT upstream_name FROM upstream_auth_tokens"
    ) as cur:
        assert (await cur.fetchone())[0] == "chatgpt-new"
    assert management.get_upstream("chatgpt-old") is None
    assert management.get_upstream("chatgpt-new") is not None
    await db.close()
