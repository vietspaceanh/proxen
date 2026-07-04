"""Tests for streaming disconnect behavior via Proxy.forward_stream.

Verifies that client disconnect during streaming:
- Interrupts iter_any() via resp.close()
- Records correct telemetry (disconnected=True)
- Does not poison the health guard
- Releases provider slot and gate
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from proxen.core.config import ModelRoute, SecretStr, Settings, Upstream
from proxen.core.gate import InflightSlot
from proxen.services.proxy import Proxy, watch_disconnect


# ─── Fakes ─────────────────────────────────────────────────────────


class _BlockingReceive:
    """ASGI receive that blocks forever (no disconnect)."""

    async def receive(self):
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}


class _FakeUpstreamContent:
    """Fake aiohttp resp.content - supports readany() (TTFT) + iter_any()."""

    def __init__(self, chunks: list[bytes] | None = None, block_after: int = -1):
        self._chunks = list(chunks or [])
        self._block_after = block_after
        self._close_event = asyncio.Event()
        self._readany_done = False

    def _close(self) -> None:
        self._close_event.set()

    async def readany(self):
        if not self._readany_done and self._chunks:
            self._readany_done = True
            return self._chunks[0]
        return b""

    def iter_any(self):
        chunks = self._chunks
        block_after = self._block_after
        close_event = self._close_event
        readany_done = self._readany_done

        async def _gen():
            start = 1 if readany_done else 0
            for i in range(start, len(chunks)):
                if block_after == i:
                    await close_event.wait()
                    raise aiohttp.ClientConnectionError("Connection closed")
                yield chunks[i]
            if block_after >= 0:
                await close_event.wait()
                raise aiohttp.ClientConnectionError("Connection closed")

        return _gen()


class _FakeUpstreamResponse:
    def __init__(self, chunks: list[bytes] | None = None, block_after: int = -1):
        self.status = 200
        self.headers = {"content-type": "text/event-stream"}
        self._content = _FakeUpstreamContent(chunks, block_after)

    @property
    def content(self):
        return self._content

    def close(self) -> None:
        self._content._close()

    def release(self) -> None:
        pass


def _make_proxy(resp, *, ttft_timeout: float = 30.0) -> tuple[Proxy, MagicMock, MagicMock, MagicMock]:
    settings = Settings(upstream_ttft_timeout=ttft_timeout)
    upstream = Upstream(name="mock", base_url="http://mock/v1", api_key=SecretStr("key"))

    catalog = MagicMock()
    catalog.is_model_enabled.return_value = True
    catalog.get_routes_by_name.return_value = [
        ModelRoute(upstream_name="mock", upstream_model_id="gpt-test")
    ]
    catalog.get_upstream.return_value = upstream
    catalog.get_model.return_value = None

    upstream_mgr = MagicMock()
    upstream_mgr.is_healthy.return_value = True
    upstream_mgr.acquire_provider.return_value = True
    upstream_mgr.post = AsyncMock(return_value=resp)

    sink = MagicMock()
    proxy = Proxy(settings, upstream_mgr, sink, catalog)
    return proxy, upstream_mgr, sink, catalog


def _call_forward_stream(proxy, disconnect, watcher, *, body=b'{"model":"gpt-test","stream":true}'):
    slot = InflightSlot(key_id="key-1")
    gate = MagicMock()
    return proxy.forward_stream(
        raw_headers=[],
        path="/v1/chat/completions",
        query="",
        body=body,
        model="gpt-test",
        key_id="key-1",
        slot=slot,
        gate=gate,
        disconnect=disconnect,
        watcher=watcher,
    )


async def _delayed_disconnect(event: asyncio.Event, delay: float = 0.1):
    await asyncio.sleep(delay)
    event.set()


async def _consume(gen, chunks):
    async for chunk in gen:
        chunks.append(chunk)


# ─── Tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_disconnect_interrupts_stalled_upstream():
    """When the client disconnects while the upstream is stalled, the
    provider exits promptly with disconnected=True."""
    resp = _FakeUpstreamResponse(chunks=[b"data: chunk1\n\n"], block_after=1)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    asyncio.create_task(_delayed_disconnect(disconnect, 0.1))
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is not None
    status, headers, stream_gen = result

    chunks = []
    gen = stream_gen()

    done, _ = await asyncio.wait(
        [asyncio.create_task(_consume(gen, chunks))], timeout=2.0,
    )
    assert done, "provider should exit within 2s after disconnect, not hang"
    assert len(chunks) == 1  # first_chunk only (iter_any blocked)

    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is True
    upstream_mgr.release_provider.assert_called_with("mock")


@pytest.mark.asyncio
async def test_normal_completion_disconnected_false():
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is not None
    _, _, stream_gen = result

    received = []
    async for chunk in stream_gen():
        received.append(chunk)

    assert b"[DONE]" in b"".join(received)
    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is False


@pytest.mark.asyncio
async def test_disconnect_does_not_poison_health_guard():
    """Client disconnect is a user action, not an upstream health signal."""
    resp = _FakeUpstreamResponse(chunks=[b"data: chunk1\n\n"], block_after=1)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    asyncio.create_task(_delayed_disconnect(disconnect, 0.1))
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is not None
    _, _, stream_gen = result

    chunks = []
    async for chunk in stream_gen():
        chunks.append(chunk)

    upstream_mgr.record_upstream_failure.assert_not_called()
    sink.enqueue.assert_called_once()
    assert sink.enqueue.call_args.args[0].client_disconnect is True


@pytest.mark.asyncio
async def test_normal_completion_does_not_record_failure():
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    _, _, stream_gen = result

    async for _ in stream_gen():
        pass

    upstream_mgr.record_upstream_failure.assert_not_called()


@pytest.mark.asyncio
async def test_upstream_error_not_disconnected():
    """When the upstream raises an error (not a disconnect), it should
    propagate and the health guard should be poisoned."""

    class _ErrorContent:
        def __init__(self):
            self._readany_done = False

        async def readany(self):
            if not self._readany_done:
                self._readany_done = True
                return b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
            return b""

        def iter_any(self):
            async def _gen():
                raise aiohttp.ClientConnectionError("upstream died")
                yield  # pragma: no cover - makes _gen an async generator
            return _gen()

    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "text/event-stream"}
    resp.content = _ErrorContent()
    resp.release = MagicMock()
    resp.close = MagicMock()

    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    _, _, stream_gen = result

    with pytest.raises(aiohttp.ClientConnectionError):
        async for _ in stream_gen():
            pass

    upstream_mgr.record_upstream_failure.assert_called_once_with("mock")
