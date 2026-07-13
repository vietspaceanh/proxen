"""Tests for per-key model allowlist (admission + management API)."""
from __future__ import annotations

KEY = {"Authorization": "Bearer gw-secret"}
ADM = {"Authorization": "Bearer admin-secret"}


def test_empty_allowlist_allows_all(app_client):
    """By default (no allowlist set), all models are accessible."""
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200


def test_allowlist_permits_listed_model(app_client):
    """A model in the allowlist is accessible."""
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    key_id = keys[0]["id"]
    app_client.put(
        f"/api/management/keys/{key_id}/models",
        json={"models": ["gpt-test"]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200


def test_allowlist_denies_unlisted_model(app_client):
    """A model NOT in the allowlist returns 403."""
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    key_id = keys[0]["id"]
    app_client.put(
        f"/api/management/keys/{key_id}/models",
        json={"models": ["claude-test"]},
        headers=ADM,
    )
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 403


def test_allowlist_clear_restores_access(app_client):
    """Clearing the allowlist restores full access."""
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    key_id = keys[0]["id"]
    app_client.put(
        f"/api/management/keys/{key_id}/models",
        json={"models": ["claude-test"]},
        headers=ADM,
    )
    # gpt-test is denied
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 403
    # clear allowlist
    app_client.delete(f"/api/management/keys/{key_id}/models", headers=ADM)
    # now gpt-test is allowed
    r = app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )
    assert r.status_code == 200


def test_allowlist_get(app_client):
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    key_id = keys[0]["id"]
    app_client.put(
        f"/api/management/keys/{key_id}/models",
        json={"models": ["gpt-test", "claude-test"]},
        headers=ADM,
    )
    r = app_client.get(f"/api/management/keys/{key_id}/models", headers=ADM)
    assert r.status_code == 200
    assert set(r.json()["models"]) == {"gpt-test", "claude-test"}


def test_allowlist_denial_happens_before_gate(app_client):
    """A denied request must not consume a global gate slot."""
    keys = app_client.get("/api/management/keys", headers=ADM).json()["data"]
    key_id = keys[0]["id"]
    app_client.put(
        f"/api/management/keys/{key_id}/models",
        json={"models": ["claude-test"]},
        headers=ADM,
    )
    stats_before = app_client.get("/api/stats", headers=ADM).json()
    active_before = stats_before["gate"]["active"]

    app_client.post(
        "/v1/chat/completions",
        json={"model": "gpt-test", "messages": [{"role": "user", "content": "hi"}]},
        headers=KEY,
    )

    stats_after = app_client.get("/api/stats", headers=ADM).json()
    assert stats_after["gate"]["active"] == active_before  # no slot consumed
