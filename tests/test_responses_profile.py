from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from proxen.core.config import Settings, Upstream
from proxen.core.concurrency import ConcurrencyGate
from proxen.core.headers import protocol_from_path
from proxen.core.sse import SSEUsageParser, parse_json_usage
from proxen.core.upstream_profiles import (
    normalize_profile_models,
    prepare_responses_body,
    profile_models_url,
    upstream_url,
)
from proxen.services.telemetry import Database
from proxen.services.upstream import ModelSyncError, UpstreamManager


def test_responses_protocol_and_fixed_endpoint():
    upstream = Upstream(
        name="chatgpt",
        profile="openai-responses",
    )
    assert protocol_from_path("/v1/responses") == "responses"
    assert upstream_url(upstream, "/v1/responses", "") == (
        "https://chatgpt.com/backend-api/codex/responses"
    )
    assert profile_models_url("openai-responses").startswith(
        "https://chatgpt.com/backend-api/codex/models?client_version="
    )
    assert profile_models_url("openai-responses").endswith("client_version=0.147.0")


def test_responses_model_catalog_normalizes_and_filters():
    models = normalize_profile_models("openai-responses", {
        "models": [
            {"slug": "gpt-visible", "display_name": "Visible", "visibility": "list", "context_window": 128000},
            {"slug": "gpt-hidden", "visibility": "hidden"},
            {"slug": "gpt-unsupported", "supported_in_api": False},
        ],
    })
    assert [model["id"] for model in models] == ["gpt-visible"]
    assert models[0]["owned_by"] == "openai"
    assert models[0]["context_window"] == 128000


def test_responses_model_catalog_rejects_malformed_success_response():
    with pytest.raises(ValueError, match="missing a models list"):
        normalize_profile_models("openai-responses", {"object": "list"})


def test_responses_usage_json():
    body = json.dumps({
        "id": "resp_1",
        "usage": {
            "input_tokens": 12,
            "output_tokens": 4,
            "input_tokens_details": {"cached_tokens": 3},
        },
    }).encode()
    usage = parse_json_usage(body, "responses")
    assert usage.input_tokens == 12
    assert usage.cached_input_tokens == 3
    assert usage.output_tokens == 4


def test_responses_usage_no_cache_info():
    """Responses usage without input_tokens_details reports None for cached."""
    body = json.dumps({
        "usage": {"input_tokens": 6, "output_tokens": 2},
    }).encode()
    usage = parse_json_usage(body, "responses")
    assert usage.input_tokens == 6
    assert usage.cached_input_tokens is None
    assert usage.output_tokens == 2


def test_responses_usage_stream_event():
    parser = SSEUsageParser("responses")
    parser.feed(
        b'data: {"type":"response.output_text.delta","delta":"Hi"}\n\n'
        b'data: {"type":"response.completed","response":{"usage":'
        b'{"input_tokens":8,"output_tokens":2,"input_tokens_details":'
        b'{"cached_tokens":1}}}}\n\n'
    )
    usage, found = parser.finalize()
    assert found is True
    assert usage.input_tokens == 8
    assert usage.cached_input_tokens == 1
    assert usage.output_tokens == 2


def test_responses_usage_stream_event_no_cache():
    parser = SSEUsageParser("responses")
    parser.feed(
        b'data: {"type":"response.completed","response":{"usage":'
        b'{"input_tokens":4,"output_tokens":1}}}\n\n'
    )
    usage, found = parser.finalize()
    assert found is True
    assert usage.input_tokens == 4
    assert usage.cached_input_tokens is None


def test_responses_usage_stream_event_accepts_crlf_and_split_boundaries():
    parser = SSEUsageParser("responses")
    event = (
        b'data: {"type":"response.completed","response":{"usage":'
        b'{"input_tokens":11,"output_tokens":6}}}\r\n\r\n'
    )
    for index in range(0, len(event), 3):
        parser.feed(event[index : index + 3])
    usage, found = parser.finalize()
    assert found is True
    assert usage.input_tokens == 11
    assert usage.output_tokens == 6


@pytest.mark.parametrize("path", ["/v1/chat/completions", "/v1/messages"])
def test_responses_profile_rejects_other_protocols(path):
    from proxen.core.upstream_profiles import profile_accepts_path

    assert profile_accepts_path("openai-responses", path) is False


def test_prepare_responses_body_forces_store_false_and_instructions():
    body = json.dumps({
        "model": "gpt-5.6-luna",
        "input": [{"role": "user", "content": "hi"}],
        "store": True,
        "max_output_tokens": 512,
        "max_tokens": 256,
    }).encode()
    patched = json.loads(prepare_responses_body(body))
    assert patched["store"] is False
    assert patched["instructions"]
    assert patched["model"] == "gpt-5.6-luna"
    assert "max_output_tokens" not in patched
    assert "max_tokens" not in patched


def test_prepare_responses_body_preserves_conforming_request():
    body = json.dumps({
        "model": "gpt-5.6-luna",
        "instructions": "Be brief.",
        "store": False,
        "input": [],
    }).encode()
    assert prepare_responses_body(body) == body


def test_prepare_responses_body_preserves_nested_bytes():
    body = (
        b'{"input":[{"text":"literal , } and [ ]"}],'
        b'"instructions":"","store":true,"max_tokens":42}'
    )
    patched = prepare_responses_body(body)
    decoded = json.loads(patched)
    assert decoded["input"] == [{"text": "literal , } and [ ]"}]
    assert decoded["store"] is False
    assert decoded["instructions"]
    assert "max_tokens" not in decoded
    assert b'literal , } and [ ]' in patched


@pytest.mark.parametrize(
    "body",
    [b"", b"not json", b"[1,2]", b'{"store":true,}', b'{"store":oops}'],
)
def test_prepare_responses_body_passes_through_malformed(body):
    assert prepare_responses_body(body) == body


@pytest.mark.asyncio
async def test_empty_model_refresh_clears_stale_cache(tmp_path):
    db = Database(str(tmp_path / "models.db"))
    await db.init()
    manager = UpstreamManager(
        Settings(), db, MagicMock(), ConcurrencyGate(2, 2, 1),
    )
    await manager.replace_cached_models("chatgpt", [{"id": "old-model"}])
    await manager.replace_cached_models("chatgpt", [])
    async with await db.execute(
        "SELECT COUNT(*) FROM models_cache WHERE upstream = ?", ("chatgpt",)
    ) as cur:
        assert (await cur.fetchone())[0] == 0
    await manager.aclose()
    await db.close()


@pytest.mark.asyncio
async def test_responses_model_sync_uses_browser_headers(tmp_path):
    class Response:
        status = 200

        async def aread(self):
            return json.dumps({
                "models": [{"slug": "gpt-subscription", "visibility": "list"}],
            }).encode()

        async def aclose(self):
            pass

    upstream = Upstream(
        name="chatgpt",
        profile="openai-responses",
    )
    db = Database(str(tmp_path / "models.db"))
    await db.init()
    management = MagicMock()
    management.enabled_upstreams.return_value = [upstream]
    manager = UpstreamManager(
        Settings(), db, management, ConcurrencyGate(2, 2, 1),
    )
    manager.auth.headers = AsyncMock(return_value={
        "Authorization": "Bearer access-token",
        "ChatGPT-Account-Id": "account-id",
    })
    manager.request = AsyncMock(return_value=Response())

    models = await manager.sync_models("chatgpt")

    manager.auth.headers.assert_awaited_once_with(upstream)
    manager.request.assert_awaited_once()
    method, url = manager.request.await_args.args[:2]
    assert method == "GET"
    assert url == profile_models_url("openai-responses")
    assert manager.request.await_args.kwargs["headers"] == {
        "Authorization": "Bearer access-token",
        "ChatGPT-Account-Id": "account-id",
    }
    assert [model["id"] for model in models] == ["gpt-subscription"]
    await manager.aclose()
    await db.close()


@pytest.mark.asyncio
async def test_explicit_model_sync_reports_provider_error(tmp_path):
    class Response:
        status = 401

        async def aclose(self):
            pass

    upstream = Upstream(
        name="chatgpt",
        profile="openai-responses",
    )
    db = Database(str(tmp_path / "models.db"))
    await db.init()
    management = MagicMock()
    management.enabled_upstreams.return_value = [upstream]
    manager = UpstreamManager(
        Settings(), db, management, ConcurrencyGate(2, 2, 1),
    )
    manager.auth.headers = AsyncMock(return_value={"Authorization": "Bearer token"})
    manager.request = AsyncMock(return_value=Response())

    with pytest.raises(ModelSyncError, match="HTTP 401"):
        await manager.sync_models("chatgpt")

    await manager.aclose()
    await db.close()
