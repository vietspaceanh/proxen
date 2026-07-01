from __future__ import annotations

import pytest

from proxen.services.proxy import _merge_extra_body


# ─── _merge_extra_body unit tests ────────────────────────────────────


def test_merge_fills_missing_keys():
    payload = {"model": "m", "messages": []}
    _merge_extra_body(payload, {"reasoning_effort": "high", "temperature": 0.7})
    assert payload["reasoning_effort"] == "high"
    assert payload["temperature"] == 0.7


def test_merge_client_value_wins():
    payload = {"model": "m", "reasoning_effort": "low"}
    _merge_extra_body(payload, {"reasoning_effort": "high"})
    assert payload["reasoning_effort"] == "low"


def test_merge_reserved_keys_skipped():
    payload = {"model": "client-model", "stream": False}
    _merge_extra_body(payload, {"model": "override", "stream": True, "temperature": 0.5})
    assert payload["model"] == "client-model"
    assert payload["stream"] is False
    assert payload["temperature"] == 0.5


def test_merge_deep_copies_values():
    extra = {"metadata": {"trace_id": "abc"}}
    payload: dict = {}
    _merge_extra_body(payload, extra)
    payload["metadata"]["trace_id"] = "xyz"
    assert extra["metadata"]["trace_id"] == "abc"


def test_merge_empty_extra_body_is_noop():
    payload = {"model": "m"}
    _merge_extra_body(payload, {})
    assert payload == {"model": "m"}


def test_merge_none_payload_key_not_overwritten():
    payload = {"temperature": None}
    _merge_extra_body(payload, {"temperature": 0.7})
    assert payload["temperature"] is None


# ─── Management API round-trip ──────────────────────────────────────


KEY = {"Authorization": "Bearer gw-secret"}
ADM = {"Authorization": "Bearer admin-secret"}


def test_extra_body_roundtrip_via_api(app_client):
    """extra_body set via PUT is persisted and returned by GET."""
    r = app_client.put(
        "/api/management/models/gpt-test",
        json={"extra_body": {"reasoning_effort": "high"}},
        headers=ADM,
    )
    assert r.status_code == 200, r.text
    assert r.json()["extra_body"] == {"reasoning_effort": "high"}

    r = app_client.get("/api/management/models", headers=ADM)
    model = next(m for m in r.json()["data"] if m["id"] == "gpt-test")
    assert model["extra_body"] == {"reasoning_effort": "high"}


def test_extra_body_cleared_with_empty_dict(app_client):
    """Sending extra_body: {} clears the field (stored as None)."""
    app_client.put(
        "/api/management/models/gpt-test",
        json={"extra_body": {"reasoning_effort": "high"}},
        headers=ADM,
    )
    r = app_client.put(
        "/api/management/models/gpt-test",
        json={"extra_body": {}},
        headers=ADM,
    )
    assert r.status_code == 200
    assert r.json()["extra_body"] is None


def test_extra_body_none_by_default(app_client):
    r = app_client.get("/api/management/models", headers=ADM)
    model = next(m for m in r.json()["data"] if m["id"] == "gpt-test")
    assert model["extra_body"] is None


# ─── End-to-end: extra_body forwarded to upstream ──────────────────


def test_extra_body_forwarded_to_upstream(app_client):
    """Configured extra_body is merged into the request sent upstream."""
    app_client.put(
        "/api/management/models/gpt-test",
        json={"extra_body": {"reasoning_effort": "high"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    echo = r.json().get("_echo", {})
    assert echo.get("reasoning_effort") == "high"


def test_extra_body_client_value_wins_end_to_end(app_client):
    """When the client sends the same key, the client value is forwarded."""
    app_client.put(
        "/api/management/models/gpt-test",
        json={"extra_body": {"reasoning_effort": "high"}},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "low",
        },
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    echo = r.json().get("_echo", {})
    assert echo["reasoning_effort"] == "low"


def test_extra_body_not_sent_when_unset(app_client):
    """No extra_body configured → no extra fields forwarded."""
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200, r.text
    echo = r.json().get("_echo", {})
    assert "reasoning_effort" not in echo
