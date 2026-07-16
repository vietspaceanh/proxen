"""Tests for streaming disconnect behavior via Proxy.forward_stream.

Verifies that client disconnect during streaming:
- Interrupts the stream
- Records correct telemetry (disconnected=True)
- Does not poison the health guard
- Releases provider slot and gate
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import httpcore
import pytest

from proxen.core.config import ModelRoute, ProxenModel, SecretStr, Settings, Upstream
from proxen.core.concurrency import InflightSlot
from proxen.core.asgi import watch_disconnect
from proxen.services.proxy import Proxy, RequestContext, UpstreamUnavailable


# ─── Fakes ─────────────────────────────────────────────────────────


class _BlockingReceive:
    """ASGI receive that blocks forever (no disconnect)."""

    async def receive(self):
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}


class _FakeStream:
    """Async iterable that yields chunks, optionally blocking after N chunks."""

    def __init__(self, chunks: list[bytes] | None = None, block_after: int = -1):
        self._chunks = list(chunks or [])
        self._block_after = block_after
        self._close_event = asyncio.Event()

    def _close(self) -> None:
        self._close_event.set()

    async def __aiter__(self):
        for i, chunk in enumerate(self._chunks):
            if self._block_after == i:
                await self._close_event.wait()
                raise httpcore.RemoteProtocolError("Connection closed")
            yield chunk
        if self._block_after >= 0:
            await self._close_event.wait()
            raise httpcore.RemoteProtocolError("Connection closed")


class _FakeResponse:
    def __init__(self, chunks: list[bytes] | None = None, block_after: int = -1,
                 status: int = 200, headers=None):
        self.status = status
        self.headers = headers or [(b"content-type", b"text/event-stream")]
        self._stream = _FakeStream(chunks, block_after)

    @property
    def stream(self):
        return self._stream

    async def aread(self) -> bytes:
        return b"".join([c async for c in self._stream])

    async def aclose(self) -> None:
        self._stream._close()


def _make_proxy(resp, *, ttft_timeout: float = 30.0, upstream_sock_read: float = 90.0, upstream_non_streaming_timeout: float = 300.0, gate=None) -> tuple[Proxy, MagicMock, MagicMock, MagicMock]:
    settings = Settings(
        upstream_ttft_timeout=ttft_timeout,
        upstream_sock_read=upstream_sock_read,
        upstream_non_streaming_timeout=upstream_non_streaming_timeout,
    )
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
    upstream_mgr.request = AsyncMock(return_value=resp)

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
    resp = _FakeResponse(chunks=[b"data: chunk1\n\n"], block_after=1)
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
    assert len(chunks) == 1  # first_chunk only (stream blocked)

    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is True
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)


@pytest.mark.asyncio
async def test_normal_completion_disconnected_false():
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeResponse(chunks=chunks_data)
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
    resp = _FakeResponse(chunks=[b"data: chunk1\n\n"], block_after=1)
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
    resp = _FakeResponse(chunks=chunks_data)
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

    class _ErrorStream:
        async def __aiter__(self):
            raise httpcore.RemoteProtocolError("upstream died")
            yield  # pragma: no cover

    resp = MagicMock(spec=httpcore.Response)
    resp.status = 200
    resp.headers = [(b"content-type", b"text/event-stream")]
    resp.stream = _ErrorStream()
    resp.aread = AsyncMock(return_value=b"")
    resp.aclose = AsyncMock()

    proxy, upstream_mgr, sink, _ = _make_proxy(resp, ttft_timeout=0.0)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(watch_disconnect(_BlockingReceive().receive, disconnect))

    result = await _call_forward_stream(proxy, disconnect, watcher)
    _, _, stream_gen = result

    with pytest.raises(httpcore.RemoteProtocolError):
        async for _ in stream_gen():
            pass

    upstream_mgr.health.record_failure.assert_called_once_with(("mock", "gpt-test"), weight=1)


@pytest.mark.asyncio
async def test_stalled_upstream_releases_on_stream_idle_timeout():
    """When the upstream stalls mid-stream (no more data, no client disconnect),
    the per-stream idle timeout releases the slot and records a drop.

    HTTP/2 multiplexing can defeat httpcore's connection-level read timeout
    (other streams' traffic keeps the shared socket alive, resetting the
    per-read timer), so a per-stream deadline is enforced in the loop.
    """
    resp = _FakeResponse(chunks=[b"data: chunk1\n\n"], block_after=1)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp, upstream_sock_read=0.2)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is not None
    _, _, stream_gen = result

    chunks = []
    done, _ = await asyncio.wait(
        [asyncio.create_task(_consume(stream_gen(), chunks))], timeout=2.0,
    )
    assert done, "stream should end within 2s after stall timeout, not hang"
    assert len(chunks) == 1  # first_chunk only, then stalled

    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.client_disconnect is False
    assert record.upstream_dropped is True
    upstream_mgr.health.record_failure.assert_called_once_with(("mock", "gpt-test"), weight=1)

    if not watcher.done():
        watcher.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await watcher


@pytest.mark.asyncio
async def test_backpressure_stall_releases_slot():
    """When the client stops pulling the generator (backpressure) while the
    upstream has gone idle, the watcher reclaims the slot via the last-byte
    stall deadline - the per-chunk loop timeout doesn't run while the
    generator is suspended at a yield."""
    from proxen.core.concurrency import ConcurrencyGate

    gate = ConcurrencyGate(max_inflight=5, max_waiting=10, timeout=10.0)
    # Two chunks, then the upstream stalls (blocks on the third read).
    resp = _FakeResponse(chunks=[b"data: c1\n\n", b"data: c2\n\n"], block_after=2)
    proxy, upstream_mgr, _, _ = _make_proxy(
        resp, ttft_timeout=0.0, upstream_sock_read=0.3, gate=gate,
    )
    upstream_mgr.gate = gate  # route provider through the real gate

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    ctx = RequestContext(
        key_hash="key-1", model="gpt-test", stream=True,
        path="/v1/chat/completions", body=b'{"model":"gpt-test","stream":true}',
    )
    ctx.slot = await gate.acquire("key-1")

    result = await proxy.forward_stream(ctx, disconnect, watcher)
    _, _, stream_gen = result

    # Pull exactly two chunks, then stop - the generator suspends at the
    # second yield (backpressure) instead of awaiting the next chunk.
    pulled: list[bytes] = []

    async def _pull_two() -> None:
        async for chunk in stream_gen():
            pulled.append(chunk)
            if len(pulled) >= 2:
                return

    await asyncio.wait_for(_pull_two(), timeout=2.0)
    assert len(pulled) == 2
    assert gate.snapshot().active == 1  # still held while suspended

    # The watcher must reclaim the slot within the stall window.
    await asyncio.sleep(1.0)
    assert gate.snapshot().active == 0

    if not watcher.done():
        watcher.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await watcher


# ─── Receiving-phase disconnect recording ───────────────────────────


class _StalledStream:
    """__aiter__ blocks forever (upstream never sends the first byte)."""

    async def __aiter__(self):
        await asyncio.Event().wait()
        yield b""  # pragma: no cover


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
    resp = MagicMock(spec=httpcore.Response)
    resp.status = 200
    resp.headers = [(b"content-type", b"text/event-stream")]
    resp.stream = _StalledStream()
    resp.aread = AsyncMock(return_value=b"")
    resp.aclose = AsyncMock()

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
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)


@pytest.mark.asyncio
async def test_post_race_disconnect_not_recorded():
    """Requesting-phase disconnect (POST not yet resolved) is NOT recorded."""

    async def _blocking_request(*args, **kwargs):
        await asyncio.Event().wait()
        return MagicMock(spec=httpcore.Response)  # pragma: no cover

    proxy, upstream_mgr, sink, _ = _make_proxy(MagicMock(spec=httpcore.Response))
    upstream_mgr.request = AsyncMock(side_effect=_blocking_request)

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
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)


@pytest.mark.asyncio
async def test_simple_read_race_disconnect_records_cancelled():
    """Non-streaming receiving-phase disconnect (during body read) is recorded."""

    async def _blocking_aread():
        await asyncio.Event().wait()
        return b""  # pragma: no cover

    resp = MagicMock(spec=httpcore.Response)
    resp.status = 200
    resp.headers = [(b"content-type", b"application/json")]
    resp.aread = _blocking_aread
    resp.aclose = AsyncMock()

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
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)


@pytest.mark.asyncio
async def test_simple_stall_releases_on_timeout():
    """A non-streaming read that stalls (no body, no disconnect) is released
    by the per-stream deadline and surfaced as 502 UpstreamUnavailable."""

    async def _blocking_aread():
        await asyncio.Event().wait()
        return b""  # pragma: no cover

    resp = MagicMock(spec=httpcore.Response)
    resp.status = 200
    resp.headers = [(b"content-type", b"application/json")]
    resp.aread = _blocking_aread
    resp.aclose = AsyncMock()

    proxy, upstream_mgr, sink, _ = _make_proxy(
        resp, upstream_non_streaming_timeout=0.2,
    )

    disconnect = asyncio.Event()

    with pytest.raises(UpstreamUnavailable) as exc_info:
        await _call_forward_simple(proxy, disconnect)

    assert exc_info.value.upstream == "mock"
    sink.enqueue.assert_called_once()
    record = sink.enqueue.call_args.args[0]
    assert record.status == 502
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)


# ─── In-flight TTFT surfacing ───────────────────────────────────────


@pytest.mark.asyncio
async def test_streaming_sets_slot_ttft():
    """The slot's ttft is populated once the first byte streams."""
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeResponse(chunks=chunks_data)
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
    resp = _FakeResponse(chunks=chunks_data)
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


@pytest.mark.asyncio
async def test_provider_queue_shows_as_waiting_not_inflight():
    """When the provider is full, a queued request is reported as waiting
    (phase=queued), excluded from the inflight table. After a slot frees
    and the waiter is promoted, it transitions to inflight."""
    from proxen.core.concurrency import ConcurrencyGate

    gate = ConcurrencyGate(max_inflight=5, max_waiting=5, timeout=10.0)
    gate.set_provider_limit("mock", 1)
    resp = _FakeResponse(chunks=[b"data: chunk1\n\n", b"data: [DONE]\n\n"])
    proxy, upstream_mgr, _, _ = _make_proxy(resp, gate=gate)
    # Route provider concurrency through the real gate (not the MagicMock).
    upstream_mgr.gate = gate

    # Pre-fill the sole provider slot so the next request must queue.
    assert gate.try_provider("mock") is True

    slot = await gate.acquire("key-1")
    slot.model = "gpt-test"

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )
    ctx = RequestContext(
        key_hash="key-1", model="gpt-test", stream=True,
        path="/v1/chat/completions", body=b'{"model":"gpt-test","stream":true}',
    )
    ctx.slot = slot

    fwd_task = asyncio.create_task(proxy.forward_stream(ctx, disconnect, watcher))
    # Let the request reach wait_provider and mark itself queued.
    await asyncio.sleep(0.05)

    snap = gate.snapshot()
    assert len(snap.inflight) == 0
    assert snap.active == 0
    assert snap.waiting == 1
    assert slot.phase == "queued"

    # Free the provider slot -> waiter is promoted and the route proceeds.
    gate.release_provider("mock")
    result = await asyncio.wait_for(fwd_task, 2.0)
    assert result is not None

    snap = gate.snapshot()
    assert len(snap.inflight) == 1
    assert snap.active == 1
    assert snap.waiting == 0
    assert slot.phase == "receiving"

    # Drain the stream so resources are released.
    _, _, stream_gen = result
    async for _ in stream_gen():
        pass

    assert gate.snapshot().active == 0
    if not watcher.done():
        watcher.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await watcher


@pytest.mark.asyncio
async def test_release_survives_watch_cancel_race():
    """When watch() calls release() and the generator's finally cancels
    watch() mid-aclose(), proxy.release() must still have been called.

    Reproduces the race that caused stuck slots after the httpcore migration:

    1. Client disconnects near stream end
    2. watch() detects disconnect, calls release(), starts aclose()
    3. stream()'s finally cancels watch_task mid-aclose()
    4. CancelledError interrupts aclose() - but proxy.release() already ran

    Before the fix, proxy.release() was AFTER aclose() in release(), so
    CancelledError prevented it from ever running - orphaning the slot.
    """
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]

    aclose_started = asyncio.Event()

    class _SlowCloseResponse(_FakeResponse):
        async def aclose(self):
            aclose_started.set()
            await asyncio.Event().wait()

    resp = _SlowCloseResponse(chunks=chunks_data)
    proxy, upstream_mgr, sink, _ = _make_proxy(resp, upstream_sock_read=0.5)

    disconnect = asyncio.Event()
    watcher = asyncio.create_task(
        watch_disconnect(_BlockingReceive().receive, disconnect)
    )

    result = await _call_forward_stream(proxy, disconnect, watcher)
    assert result is not None
    _, _, stream_gen = result

    gen = stream_gen()

    first = await gen.__anext__()
    assert first

    disconnect.set()
    await asyncio.wait_for(aclose_started.wait(), timeout=2.0)

    received = [first]
    async for chunk in gen:
        received.append(chunk)

    proxy._gate.release.assert_called_once()
    upstream_mgr.gate.release_provider.assert_called_with("mock", cooldown=True)
