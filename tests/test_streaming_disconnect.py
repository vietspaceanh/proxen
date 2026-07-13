"""Tests for streaming disconnect behavior via Proxy.forward_stream.

Verifies that client disconnect during streaming:
- Interrupts iter_any() via resp.close()
- Records correct telemetry (disconnected=True)
- Does not poison the health guard
- Releases provider slot and gate
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from proxen.core.config import ModelRoute, ProxenModel, SecretStr, Settings, Upstream
from proxen.core.concurrency import InflightSlot
from proxen.core.asgi import watch_disconnect
from proxen.services.proxy import Proxy, RequestContext


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


def _make_proxy(resp, *, ttft_timeout: float = 30.0, gate=None) -> tuple[Proxy, MagicMock, MagicMock, MagicMock]:
    settings = Settings(upstream_ttft_timeout=ttft_timeout)
    upstream = Upstream(name="mock", base_url="http://mock/v1", api_key=SecretStr("key"))

    catalog = MagicMock()
    catalog.get_model.return_value = ProxenModel(id="gpt-test", enabled=True)
    catalog.get_routes_by_name.return_value = [
        ModelRoute(upstream_name="mock", upstream_model_id="gpt-test")
    ]
    catalog.get_upstream.return_value = upstream

    upstream_mgr = MagicMock()
    upstream_mgr.health.should_try.return_value = True
    upstream_mgr.gate.try_provider.return_value = True
    upstream_mgr.post = AsyncMock(return_value=resp)

    sink = MagicMock()
    if gate is None:
        gate = MagicMock()
    proxy = Proxy(settings, upstream_mgr, catalog, sink, gate)
    return proxy, upstream_mgr, sink, catalog


def _call_forward_stream(proxy, disconnect, watcher, *, body=b'{"model":"gpt-test","stream":true}'):
    ctx = RequestContext(
        key_hash="key-1",
        model="gpt-test",
        stream=True,
        path="/v1/chat/completions",
        body=body,
    )
    ctx.slot = InflightSlot(key_id="key-1")
    return proxy.forward_stream(ctx, disconnect, watcher)


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
    upstream_mgr.gate.release_provider.assert_called_with("mock")


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

    upstream_mgr.health.record_failure.assert_not_called()
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

    upstream_mgr.health.record_failure.assert_not_called()


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

    upstream_mgr.health.record_failure.assert_called_once_with(("mock", "gpt-test"), weight=1)


# ─── Receiving-phase disconnect recording ───────────────────────────


class _StalledContent:
    """readany() blocks forever (upstream never sends the first byte)."""

    async def readany(self):
        await asyncio.Event().wait()
        return b""  # pragma: no cover

    def iter_any(self):
        async def _g():
            return
            yield  # pragma: no cover

        return _g()


def _call_forward_simple(proxy, disconnect, *, body=b'{"model":"gpt-test"}'):
    ctx = RequestContext(
        key_hash="key-1",
        model="gpt-test",
        path="/v1/chat/completions",
        body=body,
    )
    ctx.slot = InflightSlot(key_id="key-1")
    return proxy.forward_simple(ctx, disconnect)


@pytest.mark.asyncio
async def test_ttft_race_disconnect_records_cancelled():
    """Receiving-phase disconnect during the TTFT wait (headers received,
    no first byte yet) is recorded as a client disconnect, not dropped."""
    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "text/event-stream"}
    resp.content = _StalledContent()
    resp.release = MagicMock()
    resp.close = MagicMock()

    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    asyncio.create_task(_delayed_disconnect(disconnect, 0.1))
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is None

    watcher.cancel()
    with suppress(asyncio.CancelledError, Exception):
        await watcher

    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is True
    assert record.status == 200
    assert record.stream is True
    upstream_mgr.gate.release_provider.assert_called_with("mock")


@pytest.mark.asyncio
async def test_post_race_disconnect_not_recorded():
    """Requesting-phase disconnect (POST not yet resolved) is NOT recorded."""

    async def _blocking_post(*args, **kwargs):
        await asyncio.Event().wait()
        return MagicMock(spec=aiohttp.ClientResponse)  # pragma: no cover

    proxy, upstream_mgr, sink, _ = _make_proxy(MagicMock(spec=aiohttp.ClientResponse))
    upstream_mgr.post = AsyncMock(side_effect=_blocking_post)

    disconnect = asyncio.Event()
    asyncio.create_task(_delayed_disconnect(disconnect, 0.1))
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is None

    watcher.cancel()
    with suppress(asyncio.CancelledError, Exception):
        await watcher

    sink.enqueue.assert_not_called()
    upstream_mgr.gate.release_provider.assert_called_with("mock")


@pytest.mark.asyncio
async def test_simple_read_race_disconnect_records_cancelled():
    """Non-streaming receiving-phase disconnect (during body read) is recorded."""

    async def _blocking_read():
        await asyncio.Event().wait()
        return b""  # pragma: no cover

    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "application/json"}
    resp.read = _blocking_read
    resp.release = MagicMock()
    resp.close = MagicMock()

    proxy, upstream_mgr, sink, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    asyncio.create_task(_delayed_disconnect(disconnect, 0.1))

    result = await _call_forward_simple(proxy, disconnect)
    assert result is None

    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is True
    assert record.status == 200
    assert record.stream is False
    upstream_mgr.gate.release_provider.assert_called_with("mock")


# ─── In-flight TTFT surfacing ───────────────────────────────────────


@pytest.mark.asyncio
async def test_streaming_sets_slot_ttft():
    """The slot's ttft is populated once the first byte streams."""
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data)
    proxy, _, _, _ = _make_proxy(resp)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    ctx = RequestContext(
        key_hash="key-1", model="gpt-test", stream=True,
        path="/v1/chat/completions", body=b'{"model":"gpt-test","stream":true}',
    )
    ctx.slot = InflightSlot(key_id="key-1")
    slot = ctx.slot

    result = await proxy.forward_stream(ctx, disconnect, watcher)
    _, _, stream_gen = result

    async for _ in stream_gen():
        pass

    assert slot.ttft > 0.0
    assert slot.phase == "receiving"


@pytest.mark.asyncio
async def test_inflight_snapshot_includes_ttft():
    from proxen.core.concurrency import ConcurrencyGate

    gate = ConcurrencyGate(max_inflight=5, max_waiting=10, timeout=10.0)
    slot = await gate.acquire("key-1")
    slot.model = "gpt-test"
    slot.upstream = "mock"
    slot.ttft = 0.456
    slot.phase = "receiving"

    snap = gate.snapshot()
    assert len(snap.inflight) == 1
    row = snap.inflight[0]
    assert row["ttft"] == 0.456
    assert row["phase"] == "receiving"

    gate.release(slot)
    assert len(gate.snapshot().inflight) == 0


@pytest.mark.asyncio
async def test_phase_change_notifies_gate():
    """The requesting->receiving transition and the first-byte TTFT each
    trigger a dashboard push via the slot's notify callback."""
    from proxen.core.concurrency import ConcurrencyGate

    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data)
    gate = ConcurrencyGate(max_inflight=5, max_waiting=10, timeout=10.0)
    notifies: list[int] = []
    gate.set_on_change(lambda: notifies.append(1))
    slot = await gate.acquire("key-1")

    proxy, _, _, _ = _make_proxy(resp, gate=gate)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    ctx = RequestContext(
        key_hash="key-1", model="gpt-test", stream=True,
        path="/v1/chat/completions", body=b'{"model":"gpt-test","stream":true}',
    )
    ctx.slot = slot

    result = await proxy.forward_stream(ctx, disconnect, watcher)
    _, _, stream_gen = result
    async for _ in stream_gen():
        pass

    assert slot.phase == "receiving"
    assert slot.ttft > 0.0
    # acquire + phase-transition + first-byte TTFT (release may add one more)
    assert len(notifies) >= 3
