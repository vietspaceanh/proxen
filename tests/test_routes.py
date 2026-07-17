from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import httpcore
import pytest

from proxen.core.config import ModelRoute, SecretStr, Upstream
from proxen.services.proxy import Proxy, RequestContext, Router

KEY = {"Authorization": "Bearer gw-secret"}
ADM = {"Authorization": "Bearer admin-secret"}


# ─── Health ─────────────────────────────────────────────────────────


def test_health_open(app_client):
    r = app_client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ─── Auth gating ────────────────────────────────────────────────────


def test_proxy_requires_key(app_client):
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 401


def test_proxy_rejects_bad_key(app_client):
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers={"Authorization": "Bearer nope"},
    )
    assert r.status_code == 401


def test_management_requires_admin(app_client):
    r = app_client.get("/api/management/status")
    assert r.status_code == 401


# ─── End-to-end proxy paths ─────────────────────────────────────────


def test_non_stream_completion(app_client):
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "Hi"
    assert "x-proxen-overhead-ms" in r.headers


def test_finished_request_appears_as_record(app_client):
    """A completed request appears as a permanent record row via the
    telemetry writer."""
    app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    time.sleep(0.3)
    stats = app_client.get("/api/stats", headers=ADM).json()
    assert stats["gate"]["active"] == 0
    assert stats["gate"]["inflight"] == []
    assert "key_map" in stats, "stats response must include key_map for dashboard polling"
    recent = stats["recent"]
    assert recent, "expected at least one recent record"
    rec = recent[0]
    assert rec["model"] == "gpt-test"
    assert rec["status"] == 200


def test_streaming_completion(app_client):
    with app_client.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
        headers=KEY,
    ) as r:
        assert r.status_code == 200
        assert r.headers.get("X-Accel-Buffering") == "no"
        body = b"".join(r.iter_raw())
    assert b"[DONE]" in body
    assert b"Hello" in body and b"world" in body


def test_models_list(app_client):
    r = app_client.get("/v1/models", headers=KEY)
    assert r.status_code == 200
    assert "gpt-test" in [m["id"] for m in r.json()["data"]]


def test_models_list_omits_token_limits_when_unset(app_client):
    """Models created from pricing config have no token limits set."""
    r = app_client.get("/v1/models", headers=KEY)
    assert r.status_code == 200
    model = next(m for m in r.json()["data"] if m["id"] == "gpt-test")
    assert "max_input_tokens" not in model
    assert "max_output_tokens" not in model


def test_import_extracts_and_surfaces_token_limits(app_client):
    """Importing from an upstream captures token limits and surfaces them in /v1/models."""
    r = app_client.post(
        "/api/management/upstreams/mock/import-models",
        json={"models": ["gpt-test"], "overwrite": ["gpt-test"]},
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    assert r.json()["imported"] == ["gpt-test"]

    r = app_client.get("/v1/models", headers=KEY)
    assert r.status_code == 200
    model = next(m for m in r.json()["data"] if m["id"] == "gpt-test")
    assert model["max_input_tokens"] == 128000
    assert model["max_output_tokens"] == 16384


def test_extract_token_limit_handles_alias_names():
    """Upstreams use varying field names (e.g. context_length, nested max_completion_tokens)."""
    from proxen.services.management import _TOKEN_LIMIT_KEYS, _extract_token_limit

    meta = {"context_length": 200000, "top_provider": {"max_completion_tokens": 8192}}
    assert _extract_token_limit(meta, _TOKEN_LIMIT_KEYS["input"]) == 200000
    assert _extract_token_limit(meta, _TOKEN_LIMIT_KEYS["output"]) == 8192

    # Falls back across aliases and ignores non-integer / out-of-range values.
    meta = {"max_model_len": "131072", "max_tokens": 4096}
    assert _extract_token_limit(meta, _TOKEN_LIMIT_KEYS["input"]) is None
    assert _extract_token_limit(meta, _TOKEN_LIMIT_KEYS["output"]) == 4096


# ─── Model gating ───────────────────────────────────────────────────


def test_disabled_model_returns_404(app_client):
    app_client.put(
        "/api/management/models/gpt-test",
        json={"enabled": False},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 404


# ─── Fallback across a dead upstream ────────────────────────────────


def test_fallback_skips_dead_upstream(app_client):
    """A dead upstream in route order must be skipped in favor of the live one."""
    r = app_client.post(
        "/api/management/upstreams",
        json={
            "name": "dead",
            "base_url": "http://127.0.0.1:1/v1",
            "api_key": "x",
            "enabled": True,
        },
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    r = app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "dead", "upstream_model_id": "gpt-test", "sort_order": 0},
                {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
            ],
        },
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "Hi"


# ─── 429 fallback ──────────────────────────────────────────────────


def test_429_fallback_to_next_upstream(app_client, mock_upstream):
    """A 429 from the first route triggers fallback to the next one."""
    app_client.post(
        "/api/management/upstreams",
        json={
            "name": "rate-limited",
            "base_url": mock_upstream + "/v1",
            "api_key": "rate-limited-key",
            "enabled": True,
        },
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "rate-limited", "upstream_model_id": "gpt-test", "sort_order": 0},
                {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
            ],
        },
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "Hi"


def test_429_only_route_returns_429(app_client, mock_upstream):
    """A 429 from the only route is returned to the client."""
    app_client.post(
        "/api/management/upstreams",
        json={
            "name": "rate-only",
            "base_url": mock_upstream + "/v1",
            "api_key": "rate-limited-key",
            "enabled": True,
        },
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "rate-only", "upstream_model_id": "gpt-test", "sort_order": 0},
            ],
        },
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 429


def test_429_does_not_affect_health_guard(app_client, mock_upstream):
    """A 429 does not mark the upstream as failing in the health guard."""
    app_client.post(
        "/api/management/upstreams",
        json={
            "name": "rate-limited",
            "base_url": mock_upstream + "/v1",
            "api_key": "rate-limited-key",
            "enabled": True,
        },
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "rate-limited", "upstream_model_id": "gpt-test", "sort_order": 0},
            ],
        },
        headers=ADM,
    )
    for _ in range(6):
        r = app_client.post(
            "/v1/chat/completions",
            json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
            headers=KEY,
        )
        assert r.status_code == 429
    stats = app_client.get("/api/stats", headers=ADM).json()
    assert stats["providers"]["rate-limited"]["routes"] == {}


# ─── All upstreams down -> 502 ──────────────────────────────────────


def test_all_upstreams_unavailable_returns_502(app_client):
    app_client.post(
        "/api/management/upstreams",
        json={
            "name": "deadonly",
            "base_url": "http://127.0.0.1:1/v1",
            "api_key": "x",
            "enabled": True,
        },
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "deadonly", "upstream_model_id": "gpt-test", "sort_order": 0},
            ],
        },
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 502


# ─── Disabled route skipped ─────────────────────────────────────────


def test_disabled_route_is_skipped(app_client):
    """A route with enabled=false is skipped at routing time (not attempted),
    so a model whose only route is disabled returns 502 rather than 200."""
    app_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 0, "enabled": False},
            ],
        },
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 502


# ─── WebSocket auth + origin ───────────────────────────────


def test_ws_open_in_dev_mode(dev_client):
    with dev_client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
        assert "gate" in msg and "totals" in msg


def test_ws_rejects_missing_admin_key(app_client):
    """When admin keys are configured, a WS without a key is closed (1008)."""
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect) as exc:
        with app_client.websocket_connect("/ws") as ws:
            ws.receive_json()
    assert exc.value.code == 1008


def test_ws_accepts_admin_key(app_client):
    with app_client.websocket_connect("/ws?admin_key=admin-secret") as ws:
        msg = ws.receive_json()
        assert "gate" in msg


def test_ws_rejects_cross_origin(app_client):
    """A browser Origin that doesn't match Host is rejected."""
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect) as exc:
        with app_client.websocket_connect(
            "/ws?admin_key=admin-secret",
            headers={"origin": "http://evil.example"},
        ) as ws:
            ws.receive_json()
    assert exc.value.code == 1008


# ─── Per-key windowed rate limits ───────────────────────────────────


def _gw_key_id(app_client) -> int:
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    return next(k["id"] for k in keys if k["key"] == "gw-secret")


def test_windowed_limits_set_get_clear(app_client):
    """Windowed per-key limits round-trip through the management API."""
    kid = _gw_key_id(app_client)
    r = app_client.put(
        f"/api/management/keys/{kid}/limits",
        json={
            "max_inflight": 3,
            "max_requests": 200,
            "max_requests_window_s": 5 * 3600,
            "max_tokens": 10_000_000,
            "max_tokens_window_s": 5 * 3600,
        },
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["max_requests"] == 200
    assert body["max_requests_window_s"] == 5 * 3600
    assert body["max_tokens"] == 10_000_000
    assert body["max_tokens_window_s"] == 5 * 3600

    # Read back.
    r = app_client.get(f"/api/management/keys/{kid}/limits", headers=ADM)
    assert r.json()["max_requests"] == 200
    assert r.json()["max_tokens_window_s"] == 5 * 3600

    # Appears in list_keys.
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    gw = next(k for k in keys if k["key"] == "gw-secret")
    assert gw["limits"]["max_requests"] == 200

    # Clear.
    app_client.delete(f"/api/management/keys/{kid}/limits", headers=ADM)
    r = app_client.get(f"/api/management/keys/{kid}/limits", headers=ADM)
    assert r.json()["max_requests"] is None
    assert r.json()["max_tokens"] is None


def test_windowed_limits_reject_half_pair(app_client):
    """A max without a window (and vice-versa) is rejected with 400."""
    kid = _gw_key_id(app_client)
    r = app_client.put(
        f"/api/management/keys/{kid}/limits",
        json={"max_requests": 200},
        headers=ADM,
    )
    assert r.status_code == 400
    assert "together" in r.text

    r = app_client.put(
        f"/api/management/keys/{kid}/limits",
        json={"max_tokens_window_s": 3600},
        headers=ADM,
    )
    assert r.status_code == 400

    # Non-positive window is rejected.
    r = app_client.put(
        f"/api/management/keys/{kid}/limits",
        json={"max_requests": 5, "max_requests_window_s": 0},
        headers=ADM,
    )
    assert r.status_code == 400


def test_request_window_enforced_end_to_end(app_client):
    """A max_requests window actually 429s once the cap is exceeded."""
    kid = _gw_key_id(app_client)
    app_client.put(
        f"/api/management/keys/{kid}/limits",
        json={"max_requests": 2, "max_requests_window_s": 3600},
        headers=ADM,
    )
    # Two requests succeed.
    for _ in range(2):
        r = app_client.post(
            "/v1/chat/completions",
            json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
            headers=KEY,
        )
        assert r.status_code == 200, r.text
    # Third is rejected with limit_type="requests".
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 429
    assert r.json()["error"]["type"] == "rate_limit_exceeded"
    assert r.json()["error"]["limit_type"] == "requests"


# ─── Upstream URL construction ─────────────────────────────────────


def _proxy():
    return Proxy(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())


def test_upstream_url_strips_v1_for_versioned_bases():
    """The client's /v1 prefix is stripped when the base already carries a
    version segment (/v1, /v4, ...); kept for versionless bases."""
    from proxen.services.proxy.routing import Router
    v1 = Upstream(name="a", base_url="https://api.openai.com/v1")
    v4 = Upstream(name="b", base_url="https://api.z.ai/api/coding/paas/v4")
    bare = Upstream(name="c", base_url="https://gateway.example")
    assert Router.upstream_url(v1, "/v1/chat/completions", "") == "https://api.openai.com/v1/chat/completions"
    assert Router.upstream_url(v4, "/v1/chat/completions", "") == "https://api.z.ai/api/coding/paas/v4/chat/completions"
    assert Router.upstream_url(bare, "/v1/chat/completions", "") == "https://gateway.example/v1/chat/completions"
    assert Router.upstream_url(v4, "/v1/chat/completions", "stream=true") == "https://api.z.ai/api/coding/paas/v4/chat/completions?stream=true"


# ─── 4xx fallback ───────────────────────────────────────────────────


def test_404_fallback_to_next_upstream(app_client, mock_upstream):
    """A 404 from the first route (unknown path via a /v9 base) falls back."""
    app_client.post(
        "/api/management/upstreams",
        json={"name": "v9", "base_url": mock_upstream + "/v9", "api_key": "x", "enabled": True},
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={"routes": [
            {"upstream_name": "v9", "upstream_model_id": "gpt-test", "sort_order": 0},
            {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
        ]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["choices"][0]["message"]["content"] == "Hi"


def test_all_routes_404_returns_last_404(app_client, mock_upstream):
    """When every route 404s, the last 404 is returned (not a synthetic 502)."""
    app_client.post(
        "/api/management/upstreams",
        json={"name": "v9only", "base_url": mock_upstream + "/v9", "api_key": "x", "enabled": True},
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={"routes": [
            {"upstream_name": "v9only", "upstream_model_id": "gpt-test", "sort_order": 0},
        ]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 404, r.text


def test_400_returned_directly_without_fallback(app_client, mock_upstream):
    """A 400 (malformed request) is returned directly; the next route is not tried."""
    app_client.post(
        "/api/management/upstreams",
        json={"name": "bad", "base_url": mock_upstream + "/v1", "api_key": "bad-request", "enabled": True},
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={"routes": [
            {"upstream_name": "bad", "upstream_model_id": "gpt-test", "sort_order": 0},
            {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
        ]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 400, r.text


def test_404_does_not_trip_health_guard(app_client, mock_upstream):
    """A 404 is a per-route config issue, not an upstream health signal."""
    app_client.post(
        "/api/management/upstreams",
        json={"name": "v9h", "base_url": mock_upstream + "/v9", "api_key": "x", "enabled": True},
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={"routes": [
            {"upstream_name": "v9h", "upstream_model_id": "gpt-test", "sort_order": 0},
        ]},
        headers=ADM,
    )
    for _ in range(6):
        r = app_client.post(
            "/v1/chat/completions",
            json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
            headers=KEY,
        )
        assert r.status_code == 404
    stats = app_client.get("/api/stats", headers=ADM).json()
    assert stats["providers"]["v9h"]["routes"] == {}


@pytest.mark.asyncio
async def test_single_usable_route_bypasses_poisoned_health_guard():
    """A model with multiple routes but only one usable (the rest disabled)
    must bypass the health guard for that lone route - there is no
    alternative to fall back to, so blocking it only rejects requests that
    have nowhere else to go.

    Regression: ``single`` was derived from ``len(ctx.routes)`` which counts
    disabled routes, so the only working provider could be circuit-broken
    and subsequent requests short-circuited to a synthetic 502 without ever
    contacting the upstream.
    """
    upstream = Upstream(name="mock", base_url="http://mock/v1", api_key=SecretStr("k"))

    management = MagicMock()
    management.get_upstream.return_value = upstream

    upstream_mgr = MagicMock()
    # Guard fully tripped: neither pass would allow a try.
    upstream_mgr.health.should_try.return_value = False
    upstream_mgr.health.should_retry.return_value = False
    upstream_mgr.gate.try_provider.return_value = True

    fake_resp = MagicMock()
    fake_resp.status = 200
    fake_resp.headers = []
    upstream_mgr.request = AsyncMock(return_value=fake_resp)

    router = Router(management, upstream_mgr)
    ctx = RequestContext(
        model="gpt-test",
        path="/v1/chat/completions",
        routes=[
            ModelRoute(upstream_name="mock", upstream_model_id="gpt-test", enabled=True),
            ModelRoute(upstream_name="mock", upstream_model_id="gpt-test", enabled=False),
        ],
    )

    await router.try_routes(
        ctx, b'{"model":"gpt-test"}', asyncio.Event(),
        read_timeout=5.0,
    )

    # The lone usable route was attempted despite the poisoned guard.
    upstream_mgr.request.assert_called_once()


def test_fallback_uses_each_route_model_id(app_client, mock_upstream):
    """With the primary skipped (max_inflight=0), the fallback is called with
    its OWN upstream_model_id, not the primary's."""
    app_client.post(
        "/api/management/upstreams",
        json={"name": "v9p", "base_url": mock_upstream + "/v9", "api_key": "x",
              "enabled": True, "max_inflight": 0},
        headers=ADM,
    )
    app_client.put(
        "/api/management/models/gpt-test",
        json={"routes": [
            {"upstream_name": "v9p", "upstream_model_id": "primary-only-id", "sort_order": 0},
            {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
        ]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["choices"][0]["message"]["content"] == "Hi"


# ─── Anthropic (Claude Code) end-to-end ────────────────────────────


ANTHROPIC_KEY = {"x-api-key": "gw-secret"}


def test_anthropic_non_stream_completion(app_client):
    r = app_client.post(
        "/v1/messages",
        json={"model": "claude-test", "max_tokens": 64, "messages": [{"role": "user", "content": "hi"}]},
        headers=ANTHROPIC_KEY,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["content"][0]["text"] == "Hi"
    assert body["usage"]["input_tokens"] == 30


def test_anthropic_auth_via_x_api_key(app_client):
    # Claude Code sends x-api-key; the same proxen client keys work.
    r = app_client.post(
        "/v1/messages",
        json={"model": "claude-test", "max_tokens": 64, "messages": [{"role": "user", "content": "hi"}]},
        headers={"x-api-key": "gw-secret", "anthropic-version": "2023-06-01"},
    )
    assert r.status_code == 200


def test_anthropic_rejects_bad_key(app_client):
    r = app_client.post(
        "/v1/messages",
        json={"model": "claude-test", "max_tokens": 64, "messages": [{"role": "user", "content": "hi"}]},
        headers={"x-api-key": "nope"},
    )
    assert r.status_code == 401


def test_anthropic_streaming_completion(app_client):
    with app_client.stream(
        "POST",
        "/v1/messages",
        json={
            "model": "claude-test",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
        headers=ANTHROPIC_KEY,
    ) as r:
        assert r.status_code == 200
        assert r.headers.get("X-Accel-Buffering") == "no"
        body = b"".join(r.iter_raw())
    assert b"message_stop" in body
    assert b"Hello" in body and b"world" in body


def test_anthropic_count_tokens(app_client):
    r = app_client.post(
        "/v1/messages/count_tokens",
        json={"model": "claude-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=ANTHROPIC_KEY,
    )
    assert r.status_code == 200
    assert r.json()["input_tokens"] == 42


def test_anthropic_and_openai_coexist(app_client):
    """One proxen instance serves both protocols on the same upstream."""
    r_oai = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r_oai.status_code == 200
    r_ant = app_client.post(
        "/v1/messages",
        json={"model": "claude-test", "max_tokens": 64, "messages": [{"role": "user", "content": "hi"}]},
        headers=ANTHROPIC_KEY,
    )
    assert r_ant.status_code == 200


# ─── TTFT fallback (slow first-byte → next route) ───────────────────


def test_ttft_timeout_falls_back_to_next_upstream(ttft_client, mock_upstream):
    """A streaming upstream that returns 200 but stalls before the first
    chunk must be abandoned within the TTFT timeout and the next route
    tried, instead of hanging until the client gives up."""
    ttft_client.post(
        "/api/management/upstreams",
        json={
            "name": "slow",
            "base_url": mock_upstream + "/v1",
            "api_key": "slow-key",
            "enabled": True,
        },
        headers=ADM,
    )
    ttft_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "slow", "upstream_model_id": "gpt-test", "sort_order": 0},
                {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
            ],
        },
        headers=ADM,
    )
    with ttft_client.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
        headers=KEY,
    ) as r:
        assert r.status_code == 200
        body = b"".join(r.iter_raw())
    assert b"Hello" in body
    assert b"slow" not in body

    # After enough TTFT timeouts the slow upstream's guard must trip.
    for _ in range(4):
        with ttft_client.stream(
            "POST",
            "/v1/chat/completions",
            json={
                "model": "gpt-test",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
            headers=KEY,
        ) as r:
            assert r.status_code == 200
    stats = ttft_client.get("/api/stats", headers=ADM).json()
    assert stats["providers"]["slow"]["routes"]["gpt-test"] == "failing"


def test_ttft_timeout_only_route_returns_502(ttft_client, mock_upstream):
    """When the only route stalls past the TTFT timeout, a 502 is returned
    instead of hanging indefinitely."""
    ttft_client.post(
        "/api/management/upstreams",
        json={
            "name": "slow-only",
            "base_url": mock_upstream + "/v1",
            "api_key": "slow-key",
            "enabled": True,
        },
        headers=ADM,
    )
    ttft_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "slow-only", "upstream_model_id": "gpt-test", "sort_order": 0},
            ],
        },
        headers=ADM,
    )
    r = ttft_client.post(
        "/v1/chat/completions",
        json={
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
        headers=KEY,
    )
    assert r.status_code == 502


def test_ttft_timeout_bounds_header_phase(ttft_client, mock_upstream):
    """A streaming upstream that stalls before sending response headers must
    be abandoned within the TTFT timeout (not the longer socket read
    timeout) and fall back to the next route."""
    ttft_client.post(
        "/api/management/upstreams",
        json={
            "name": "stall-headers",
            "base_url": mock_upstream + "/v1",
            "api_key": "stall-headers-key",
            "enabled": True,
        },
        headers=ADM,
    )
    ttft_client.put(
        "/api/management/models/gpt-test",
        json={
            "routes": [
                {"upstream_name": "stall-headers", "upstream_model_id": "gpt-test", "sort_order": 0},
                {"upstream_name": "mock", "upstream_model_id": "gpt-test", "sort_order": 1},
            ],
        },
        headers=ADM,
    )
    t0 = time.monotonic()
    with ttft_client.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
        headers=KEY,
    ) as r:
        assert r.status_code == 200
        body = b"".join(r.iter_raw())
    elapsed = time.monotonic() - t0
    assert b"Hello" in body
    # TTFT (0.5s) bounds the header phase, not upstream_sock_read (90s).
    assert elapsed < 30
