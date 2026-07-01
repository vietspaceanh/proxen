from __future__ import annotations

import os

import blacksheep.server.routing
from proxen.core.security import AuthRateLimiter, SlidingWindowLimiter, mask_key, secure_in


# ─── Constant-time comparison ──────────────────────────────────


def test_secure_in_match():
    assert secure_in("abc", {"abc", "def"}) is True


def test_secure_in_no_match():
    assert secure_in("nope", {"abc", "def"}) is False


def test_secure_in_empty():
    assert secure_in("", {"abc"}) is False
    assert secure_in("abc", set()) is False


def test_secure_in_length_mismatch_safe():
    # Unequal length must not raise and must return False.
    assert secure_in("a", {"abcd"}) is False


# ─── Key masking ───────────────────────────────────────────────


def test_mask_key_long():
    assert mask_key("sk-abcdefghijklmno") == "sk-…lmno"


def test_mask_key_short():
    assert mask_key("short") == "…"
    assert mask_key("") == ""


# ─── Auth rate limiter ────────────────────────────────────────


def test_rate_limiter_allows_under_threshold():
    rl = AuthRateLimiter(window=60.0, max_failures=3, cooldown=60.0)
    for _ in range(2):
        rl.record_failure("1.2.3.4")
    assert rl.allow("1.2.3.4") is True


def test_rate_limiter_locks_out():
    rl = AuthRateLimiter(window=60.0, max_failures=3, cooldown=60.0)
    for _ in range(3):
        rl.record_failure("1.2.3.4")
    assert rl.allow("1.2.3.4") is False


def test_rate_limiter_reset_clears():
    rl = AuthRateLimiter(window=60.0, max_failures=2, cooldown=60.0)
    rl.record_failure("1.2.3.4")
    rl.reset("1.2.3.4")
    rl.record_failure("1.2.3.4")
    assert rl.allow("1.2.3.4") is True


# ─── Body size limit ───────────────────────────────────────────


def test_body_size_rejects_oversized_content_length():
    # Use a tiny cap (16B) and a small body to exercise the 413 path quickly.
    from blacksheep import Application, json as bs_json
    from starlette.testclient import TestClient

    from proxen.core.security import BodySizeMiddleware

    app = Application(router=blacksheep.server.routing.Router())

    @app.router.post("/echo")
    async def echo():
        return bs_json({"ok": True})

    wrapped = BodySizeMiddleware(app, max_bytes=16)

    with TestClient(wrapped) as c:
        r = c.post("/echo", content=b"x" * 64)
        assert r.status_code == 413
        assert r.json()["error"]["type"] == "proxen_error"


def test_body_size_allows_small_bodies(app_client):
    # Default app allows normal completions (tested end-to-end in test_routes).
    # Here we just confirm a small body to /health-ish path is fine.
    r = app_client.get("/health")
    assert r.status_code == 200


# ─── Upstream scheme allowlist ────────────────────────────────


def test_add_upstream_rejects_bad_scheme(app_client):
    ADM = {"Authorization": "Bearer admin-secret"}
    r = app_client.post(
        "/api/management/upstreams",
        json={"name": "bad", "base_url": "file:///etc/passwd"},
        headers=ADM,
    )
    assert r.status_code == 400


def test_add_upstream_rejects_no_host(app_client):
    ADM = {"Authorization": "Bearer admin-secret"}
    r = app_client.post(
        "/api/management/upstreams",
        json={"name": "bad2", "base_url": "http://"},
        headers=ADM,
    )
    assert r.status_code == 400


# ─── Dashboard auth ────────────────────────────────────────────


def test_dashboard_stats_requires_admin_when_configured(app_client):
    # app_client has admin_api_keys=["admin-secret"] -> stats needs auth.
    r = app_client.get("/api/stats")
    assert r.status_code == 401
    r = app_client.get("/api/stats", headers={"Authorization": "Bearer admin-secret"})
    assert r.status_code == 200


def test_dashboard_open_in_dev_mode(dev_client):
    # dev_client has no admin keys -> dashboard endpoints are open.
    r = dev_client.get("/api/stats")
    assert r.status_code == 200


def test_dashboard_html_shell_is_public(app_client):
    # The HTML shell has no data and stays reachable without auth.
    r = app_client.get("/")
    assert r.status_code == 200


def test_provider_keys_masked_in_list(app_client):
    ADM = {"Authorization": "Bearer admin-secret"}
    r = app_client.get("/api/management/upstreams", headers=ADM)
    assert r.status_code == 200
    keys = [u["api_key"] for u in r.json()["data"]]
    # The seeded key is "provider-secret"; must not appear verbatim.
    assert "provider-secret" not in keys
    assert all(k == mask_key("provider-secret") for k in keys)


# ─── File permissions ──────────────────────────────────────────


def test_db_file_permissions_restrictive(tmp_path):
    import asyncio

    from proxen.services.telemetry import Database

    db_path = str(tmp_path / "perm.db")
    db = Database(db_path)
    asyncio.run(db.init())
    mode = os.stat(db_path).st_mode & 0o777
    asyncio.run(db.close())
    assert mode == 0o600


# ─── Sliding-window rate limiter ────────────────────────────────────


def test_sliding_window_allows_under_limit():
    rl = SlidingWindowLimiter(max_requests=5, window_s=60.0)
    for _ in range(5):
        assert rl.allow("1.2.3.4") is True


def test_sliding_window_rejects_over_limit():
    rl = SlidingWindowLimiter(max_requests=3, window_s=60.0)
    for _ in range(3):
        rl.allow("1.2.3.4")
    assert rl.allow("1.2.3.4") is False


def test_sliding_window_keys_are_independent():
    rl = SlidingWindowLimiter(max_requests=2, window_s=60.0)
    assert rl.allow("a") is True
    assert rl.allow("a") is True
    assert rl.allow("a") is False
    assert rl.allow("b") is True
