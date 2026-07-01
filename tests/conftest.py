from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import pytest
from aiohttp import web
from starlette.testclient import TestClient

from proxen.app import create_app
from proxen.core.config import Pricing, Settings, Upstream

TEST_TMP = Path(__file__).parent / "tmp"
TEST_TMP.mkdir(exist_ok=True)


# ─── Mock upstream server (threaded, shares the smoke-test contract) ─────


async def mock_handler(request: web.Request) -> web.Response:
    path = request.path
    if path == "/v1/models":
        return web.json_response(
            {
                "object": "list",
                "data": [
                    {
                        "id": "gpt-test",
                        "object": "model",
                        "owned_by": "test",
                        "max_input_tokens": 128000,
                        "max_output_tokens": 16384,
                    }
                ],
            }
        )
    if path == "/v1/chat/completions":
        auth = request.headers.get("Authorization", "")
        if "rate-limited" in auth:
            return web.json_response(
                {"error": {"message": "Rate limit exceeded", "type": "rate_limit_error"}},
                status=429,
            )
        if "bad-request" in auth:
            return web.json_response(
                {"error": {"message": "Bad request", "type": "invalid_request_error"}},
                status=400,
            )
        body = await request.json()
        if body.get("model") != "gpt-test":
            return web.json_response(
                {"error": {"message": "model not found", "type": "not_found"}},
                status=404,
            )
        if body.get("stream"):
            if "slow" in auth:
                resp = web.StreamResponse(
                    status=200, headers={"Content-Type": "text/event-stream"}
                )
                await resp.prepare(request)
                await asyncio.sleep(2)
                await resp.write(b'data: {"choices":[{"delta":{"content":"slow"}}]}\n\n')
                await resp.write_eof()
                return resp
            chunks = [
                b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
                b'data: {"choices":[{"delta":{"content":" world"}}]}\n\n',
                b'data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1}}}\n\n',
                b'data: [DONE]\n\n',
            ]
            resp = web.StreamResponse(
                status=200, headers={"Content-Type": "text/event-stream"}
            )
            await resp.prepare(request)
            for chunk in chunks:
                await resp.write(chunk)
            await resp.write_eof()
            return resp
        return web.json_response(
            {
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hi"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 3,
                    "prompt_tokens_details": {"cached_tokens": 2},
                },
                "_echo": {k: v for k, v in body.items() if k not in ("model", "messages", "stream", "stream_options")},
            }
        )
    if path == "/v1/messages":
        # Anthropic-format passthrough. Verifies the upstream key was
        # injected as x-api-key (NOT the client's proxen key).
        if request.headers.get("x-api-key", "") != "provider-secret":
            return web.json_response(
                {"type": "error", "error": {"type": "authentication_error", "message": "invalid x-api-key"}},
                status=401,
            )
        body = await request.json()
        if body.get("model") != "claude-test":
            return web.json_response(
                {"type": "error", "error": {"type": "not_found_error", "message": "model not found"}},
                status=404,
            )
        if body.get("stream"):
            chunks = [
                b'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":9,"output_tokens":1,"cache_read_input_tokens":2}}}\n\n',
                b'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
                b'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}\n\n',
                b'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}\n\n',
                b'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
                b'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4}}\n\n',
                b'event: message_stop\ndata: {"type":"message_stop"}\n\n',
            ]
            resp = web.StreamResponse(
                status=200, headers={"Content-Type": "text/event-stream"}
            )
            await resp.prepare(request)
            for chunk in chunks:
                await resp.write(chunk)
            await resp.write_eof()
            return resp
        return web.json_response({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hi"}],
            "model": "claude-test",
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 30, "output_tokens": 7, "cache_read_input_tokens": 3},
        })
    if path == "/v1/messages/count_tokens":
        return web.json_response({"input_tokens": 42})
    return web.json_response({"error": f"unexpected path: {path}"}, status=404)


def _start_mock() -> tuple[str, asyncio.AbstractEventLoop, web.AppRunner]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", mock_handler)
    loop = asyncio.new_event_loop()

    async def _start():
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        return port, runner

    port, runner = loop.run_until_complete(_start())
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", loop, runner


def _stop_mock(loop: asyncio.AbstractEventLoop, runner: web.AppRunner) -> None:
    async def _cleanup():
        await runner.cleanup()

    fut = asyncio.run_coroutine_threadsafe(_cleanup(), loop)
    fut.result(timeout=5)
    loop.call_soon_threadsafe(loop.stop)


@pytest.fixture(scope="session")
def mock_upstream():
    base_url, loop, runner = _start_mock()
    yield base_url
    _stop_mock(loop, runner)


def _build_settings(base_url: str, db_path: str, **overrides) -> Settings:
    return Settings(
        api_keys=["gw-secret"],
        admin_api_keys=["admin-secret"],
        upstreams=[Upstream(name="mock", base_url=base_url + "/v1", api_key="provider-secret")],
        max_inflight=4,
        max_waiting=8,
        queue_timeout=10.0,
        model_sync_interval=999999.0,
        db_path=db_path,
        pricing={
            "gpt-test": Pricing(input_per_1m=1.0, cached_input_per_1m=0.5, output_per_1m=2.0),
            "claude-test": Pricing(input_per_1m=3.0, cached_input_per_1m=1.5, output_per_1m=15.0),
        },
        **overrides,
    )


@pytest.fixture
def app_client(mock_upstream):
    """A fresh TestClient + isolated DB per test."""
    db_path = str(TEST_TMP / f"test-{os.getpid()}-{threading.get_ident()}.db")
    settings = _build_settings(mock_upstream, db_path)
    app = create_app(settings)
    with TestClient(app) as client:
        yield client
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass
        # WAL/shm sidecars may also exist; ignore cleanup failures.


@pytest.fixture
def dev_client(mock_upstream):
    """TestClient with NO api/admin keys (dev mode, auth disabled)."""
    db_path = str(TEST_TMP / f"dev-{os.getpid()}-{threading.get_ident()}.db")
    settings = Settings(
        api_keys=[],
        admin_api_keys=[],
        upstreams=[Upstream(name="mock", base_url=mock_upstream + "/v1", api_key="provider-secret")],
        max_inflight=4,
        max_waiting=8,
        queue_timeout=10.0,
        model_sync_interval=999999.0,
        db_path=db_path,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        yield client
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def ttft_client(mock_upstream):
    """TestClient with a short TTFT timeout for fallback testing."""
    db_path = str(TEST_TMP / f"ttft-{os.getpid()}-{threading.get_ident()}.db")
    settings = _build_settings(mock_upstream, db_path, upstream_ttft_timeout=0.5)
    app = create_app(settings)
    with TestClient(app) as client:
        yield client
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass
