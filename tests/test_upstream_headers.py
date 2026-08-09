"""Tests for per-upstream extra_headers (config, management API, forwarding)."""
from __future__ import annotations

from proxen.core.config import _build_settings
from proxen.core.headers import resolve_extra_headers

KEY = {"Authorization": "Bearer gw-secret"}
ADM = {"Authorization": "Bearer admin-secret"}


# ─── Template resolution unit tests ──────────────────────────────────


def test_resolve_header_template_case_insensitive():
    src = [(b"X-Client-Session", b"sess-1")]
    out = resolve_extra_headers({"x-session-id": "$x-client-session"}, src)
    assert out == {"x-session-id": "sess-1"}


def test_resolve_context_value_wins_over_header():
    src = [(b"model", b"header-model")]
    out = resolve_extra_headers({"x-m": "$model"}, src, {"model": "ctx-model"})
    assert out == {"x-m": "ctx-model"}


def test_resolve_missing_template_omitted():
    out = resolve_extra_headers({"a": "$nope"}, [])
    assert out == {}


def test_resolve_mixed_static_and_template():
    out = resolve_extra_headers({"a": "static", "b": "$nope"}, [])
    assert out == {"a": "static"}


def test_resolve_none_returns_empty():
    assert resolve_extra_headers(None, []) == {}


# ─── Config loading ──────────────────────────────────────────────────


def test_upstream_extra_headers_from_config():
    settings = _build_settings({
        "upstreams": [{
            "name": "u",
            "base_url": "http://example.com/v1",
            "api_key": "k",
            "extra_headers": {"x-test-extra": "configured"},
        }],
    })
    assert settings.upstreams[0].extra_headers == {"x-test-extra": "configured"}


# ─── Management API round-trip ───────────────────────────────────────


def test_extra_headers_roundtrip_via_api(app_client):
    r = app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "configured"}},
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    assert r.json()["extra_headers"] == {"x-test-extra": "configured"}

    r = app_client.get("/api/management/upstreams", headers=ADM)
    upstream = next(u for u in r.json()["data"] if u["name"] == "mock")
    assert upstream["extra_headers"] == {"x-test-extra": "configured"}


def test_extra_headers_cleared_with_empty_dict(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "configured"}},
        headers=ADM,
    )
    r = app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {}},
        headers=ADM,
    )
    assert r.status_code == 200
    assert r.json()["extra_headers"] is None


def test_extra_headers_none_by_default(app_client):
    r = app_client.get("/api/management/upstreams", headers=ADM)
    upstream = next(u for u in r.json()["data"] if u["name"] == "mock")
    assert upstream["extra_headers"] is None


# ─── End-to-end: extra_headers forwarded to upstream ─────────────────


def test_extra_headers_forwarded_to_upstream(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "configured"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == "configured"


def test_extra_headers_upstream_wins_over_client(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "configured"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers={**KEY, "x-test-extra": "client-value"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == "configured"


def test_extra_headers_not_sent_when_unset(app_client):
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == ""


# ─── End-to-end: template passthrough ────────────────────────────────


def test_extra_headers_template_from_request_header(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "$x-session-affinity"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers={**KEY, "x-session-affinity": "sess-abc"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == "sess-abc"


def test_extra_headers_template_omitted_when_header_missing(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "$x-session-affinity"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == ""


def test_extra_headers_template_from_model_context(app_client):
    app_client.put(
        "/api/management/upstreams/mock",
        json={"extra_headers": {"x-test-extra": "$model"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    assert r.json()["_headers_echo"]["x-test-extra"] == "gpt-test"
