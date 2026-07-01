from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from proxen.app.endpoints.proxy import _disconnect_watcher, _build_response
from proxen.services.proxy import ProxyResponse


# ─── _disconnect_watcher unit tests ─────────────────────────────────


class _FakeContent:
    """Minimal stand-in for BlackSheep's ASGIContent, just the receive
    callable that the watcher calls via `request.content.receive()`."""

    def __init__(self, messages: list[dict]):
        self._messages = list(messages)
        self.receive = self._make_receive()

    def _make_receive(self):
        async def receive():
            if self._messages:
                return self._messages.pop(0)
            # Block forever once exhausted (simulates waiting for disconnect)
            await asyncio.Event().wait()
            return {"type": "http.disconnect"}

        return receive


def _fake_request(messages: list[dict]):
    """Build a request-like object with a `content` attribute whose
    `receive` callable is controllable."""
    request = MagicMock()
    request.content = _FakeContent(messages)
    return request


def _fake_resp():
    """Build a fake aiohttp.ClientResponse whose `close` is trackable."""
    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.close = MagicMock()
    resp.release = AsyncMock()
    return resp


@pytest.mark.asyncio
async def test_watcher_sets_event_on_disconnect():
    """The watcher sets the event and closes resp when http.disconnect arrives."""
    request = _fake_request([{"type": "http.disconnect"}])
    event = asyncio.Event()
    resp = _fake_resp()

    await _disconnect_watcher(request, event, resp, MagicMock())

    assert event.is_set()
    resp.close.assert_called_once()


@pytest.mark.asyncio
async def test_watcher_loops_past_non_disconnect_messages():
    """The watcher loops until http.disconnect, ignoring other messages."""
    request = _fake_request([
        {"type": "http.request", "body": b"", "more_body": False},
        {"type": "http.request", "body": b"", "more_body": False},
        {"type": "http.disconnect"},
    ])
    event = asyncio.Event()
    resp = _fake_resp()

    await _disconnect_watcher(request, event, resp, MagicMock())

    assert event.is_set()
    resp.close.assert_called_once()


@pytest.mark.asyncio
async def test_watcher_treats_receive_error_as_disconnect():
    """If receive() raises (channel closed), the watcher treats it as a
    disconnect and tears down the upstream."""
    request = MagicMock()
    request.content = MagicMock()
    request.content.receive = AsyncMock(side_effect=RuntimeError("channel closed"))
    event = asyncio.Event()
    resp = _fake_resp()

    await _disconnect_watcher(request, event, resp, MagicMock())

    assert event.is_set()
    resp.close.assert_called_once()


@pytest.mark.asyncio
async def test_watcher_cancelled_does_not_set_event():
    """When the watcher is cancelled (normal stream completion), it must
    NOT set the event or close resp."""
    request = _fake_request([])  # will block forever
    event = asyncio.Event()
    resp = _fake_resp()

    task = asyncio.create_task(_disconnect_watcher(request, event, resp, MagicMock()))
    await asyncio.sleep(0.05)  # let it block on receive()

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert not event.is_set()
    resp.close.assert_not_called()


# ─── Provider integration tests ─────────────────────────────────────


class _FakeUpstreamContent:
    """Fake for aiohttp's resp.content, yields pre-set chunks, then
    raises ClientConnectionError when close() is called (mirrors real
    aiohttp behavior where resp.close() interrupts a pending read)."""

    def __init__(self, chunks: list[bytes] | None = None, block_after: int = -1):
        self._chunks = list(chunks or [])
        self._block_after = block_after
        self._close_event = asyncio.Event()

    def _close(self) -> None:
        self._close_event.set()

    def iter_any(self):
        chunks = self._chunks
        block_after = self._block_after
        close_event = self._close_event

        async def _gen():
            for i, c in enumerate(chunks):
                if block_after == i:
                    await close_event.wait()
                    raise aiohttp.ClientConnectionError("Connection closed")
                yield c
            if block_after >= 0:
                await close_event.wait()
                raise aiohttp.ClientConnectionError("Connection closed")

        return _gen()


class _FakeUpstreamResponse:
    """Fake aiohttp.ClientResponse whose close() interrupts pending reads."""

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


def _build_provider_test_env(resp, request, result=None, slot=None):
    """Build the minimal objects needed to invoke _build_response's
    provider() directly."""
    if slot is None:
        slot = MagicMock()
        slot.model = "test-model"
        slot.input_tokens = 0
        slot.output_tokens = 0

    proxy = MagicMock()
    proxy.record_telemetry = MagicMock()

    gate = MagicMock()
    upstream_mgr = MagicMock()

    if result is None:
        result = ProxyResponse(
            status=200,
            headers={"content-type": "text/event-stream"},
            body=None,
            resp=resp,
            upstream_name="test-upstream",
            proxy_start=time.perf_counter(),
            wall_start=time.time(),
            slot=slot,
            gate=gate,
            upstream_mgr=upstream_mgr,
            proxy=proxy,
            model="test-model",
            key_id="key-1",
            stream=True,
        )
    else:
        result.proxy = proxy
        result.gate = gate
        result.upstream_mgr = upstream_mgr
        result.slot = slot

    response = _build_response(request, result, 1.0)
    return response, proxy, slot, gate, upstream_mgr


@pytest.mark.asyncio
async def test_disconnect_interrupts_stalled_upstream():
    """When the client disconnects while the upstream is stalled (no chunks),
    the provider exits promptly with disconnected=True."""
    resp = _FakeUpstreamResponse(
        chunks=[b"data: chunk1\n\n"], block_after=1,
    )

    request = _fake_request([{"type": "http.disconnect"}])

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(resp, request)

    # Consume the provider, it should yield the first chunk, then exit
    # when the disconnect is detected (within ~2s, not 60s).
    chunks = []
    gen = response.content.generator()

    async def consume():
        async for chunk in gen:
            chunks.append(chunk)

    done, pending = await asyncio.wait(
        [asyncio.create_task(consume())], timeout=2.0,
    )
    assert done, "provider should exit within 2s after disconnect, not hang"

    assert len(chunks) == 1
    assert chunks[0] == b"data: chunk1\n\n"

    # Verify telemetry
    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["disconnected"] is True
    assert call_kwargs["completed"] is False

    # Verify slots were released
    upstream_mgr.release_provider.assert_called_once()
    gate.release.assert_called_once()


@pytest.mark.asyncio
async def test_upstream_error_not_recorded_as_disconnect():
    """When the upstream raises an error (not a disconnect), disconnected
    should be False and the exception should propagate."""
    class _ErrorContent:
        def iter_any(self):
            async def _gen():
                yield b"data: chunk1\n\n"
                raise aiohttp.ClientConnectionError("upstream died")
            return _gen()

    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "text/event-stream"}
    resp.content = _ErrorContent()
    resp.release = MagicMock()
    resp.close = MagicMock()

    # No disconnect, client is still connected
    request = _fake_request([])

    result = ProxyResponse(
        status=200,
        headers={"content-type": "text/event-stream"},
        body=None,
        resp=resp,
        upstream_name="test-upstream",
        proxy_start=time.perf_counter(),
        wall_start=time.time(),
        proxy=MagicMock(),
        model="test-model",
        key_id="key-1",
        stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )

    gen = response.content.generator()

    with pytest.raises(aiohttp.ClientConnectionError):
        async for _ in gen:
            pass

    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["disconnected"] is False
    assert call_kwargs["completed"] is False


@pytest.mark.asyncio
async def test_upstream_drop_records_completed_false():
    """When the upstream raises mid-stream (not a client disconnect),
    completed=False and disconnected=False → upstream_dropped=True."""
    class _DropContent:
        def iter_any(self):
            async def _gen():
                yield b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
                raise aiohttp.ClientConnectionError("upstream dropped")
            return _gen()

    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "text/event-stream"}
    resp.content = _DropContent()
    resp.release = MagicMock()
    resp.close = MagicMock()

    request = _fake_request([])  # client connected, no disconnect

    result = ProxyResponse(
        status=200, headers={"content-type": "text/event-stream"},
        body=None, resp=resp, upstream_name="test-upstream",
        proxy_start=time.perf_counter(), wall_start=time.time(),
        proxy=MagicMock(), model="test-model", key_id="key-1", stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(resp, request, result)
    gen = response.content.generator()

    with pytest.raises(aiohttp.ClientConnectionError):
        async for _ in gen:
            pass

    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["disconnected"] is False
    assert call_kwargs["completed"] is False
    upstream_mgr.record_upstream_failure.assert_called_once_with("test-upstream")


@pytest.mark.asyncio
async def test_normal_completion_disconnected_false():
    """A stream that completes naturally should record disconnected=False."""
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]

    resp = _FakeUpstreamResponse(chunks=chunks_data)

    request = _fake_request([])

    result = ProxyResponse(
        status=200,
        headers={"content-type": "text/event-stream"},
        body=None,
        resp=resp,
        upstream_name="test-upstream",
        proxy_start=time.perf_counter(),
        wall_start=time.time(),
        proxy=MagicMock(),
        model="test-model",
        key_id="key-1",
        stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )

    gen = response.content.generator()
    received = []
    async for chunk in gen:
        received.append(chunk)

    assert b"[DONE]" in b"".join(received)
    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["disconnected"] is False
    assert call_kwargs["completed"] is True


@pytest.mark.asyncio
async def test_completed_stream_with_late_disconnect_not_cancelled():
    """When the stream completes fully and the client disconnects right
    after, completed=True and disconnected=True, but the request must
    NOT be recorded as cancelled (the client received all data)."""
    class _Content:
        def iter_any(self):
            async def _gen():
                yield b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
                yield b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n'
                yield b'data: [DONE]\n\n'
                await asyncio.sleep(0.05)
            return _gen()

    resp = MagicMock(spec=aiohttp.ClientResponse)
    resp.status = 200
    resp.headers = {"content-type": "text/event-stream"}
    resp.content = _Content()
    resp.release = MagicMock()
    resp.close = MagicMock()

    request = _fake_request([{"type": "http.disconnect"}])

    result = ProxyResponse(
        status=200, headers={"content-type": "text/event-stream"},
        body=None, resp=resp, upstream_name="test-upstream",
        proxy_start=time.perf_counter(), wall_start=time.time(),
        proxy=MagicMock(), model="test-model", key_id="key-1", stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )
    gen = response.content.generator()

    received = []
    async for chunk in gen:
        received.append(chunk)

    assert b"[DONE]" in b"".join(received)
    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["completed"] is True
    assert call_kwargs["disconnected"] is True


@pytest.mark.asyncio
async def test_usage_rescues_completed_when_resp_closed():
    """When the agent closes the connection right after the stream finishes,
    the watcher's resp.close() interrupts iter_any() before it can raise
    StopAsyncIteration, so the else clause never runs. The presence of
    usage data in the parser buffer should still mark the stream as completed."""
    chunks = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks, block_after=3)
    request = _fake_request([{"type": "http.disconnect"}])

    result = ProxyResponse(
        status=200, headers={"content-type": "text/event-stream"},
        body=None, resp=resp, upstream_name="test-upstream",
        proxy_start=time.perf_counter(), wall_start=time.time(),
        proxy=MagicMock(), model="test-model", key_id="key-1", stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )
    gen = response.content.generator()

    received = []
    async for chunk in gen:
        received.append(chunk)

    assert b"[DONE]" in b"".join(received)
    proxy.record_telemetry.assert_called_once()
    call_kwargs = proxy.record_telemetry.call_args.kwargs
    assert call_kwargs["completed"] is True
    assert call_kwargs["disconnected"] is True


# ─── Fix 3: client disconnect with 0 tokens trips health guard ──────


@pytest.mark.asyncio
async def test_disconnect_with_zero_tokens_does_not_poison_health_guard():
    """A client disconnect before any tokens were produced is a user cancel,
    not an upstream-health signal: the guard must NOT be poisoned (poisoning
    would force spurious fallback and discard the upstream's prompt cache).
    The cancel is still recorded as telemetry so it stays visible in the
    dashboard."""
    resp = _FakeUpstreamResponse(
        chunks=[b"data: chunk1\n\n"], block_after=1,
    )
    request = _fake_request([{"type": "http.disconnect"}])

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(resp, request)

    gen = response.content.generator()
    chunks = []
    async for chunk in gen:
        chunks.append(chunk)

    upstream_mgr.record_upstream_failure.assert_not_called()
    proxy.record_telemetry.assert_called_once()
    assert proxy.record_telemetry.call_args.kwargs["disconnected"] is True


@pytest.mark.asyncio
async def test_normal_completion_does_not_record_failure():
    """A stream that completes normally must NOT record a failure."""
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data)
    request = _fake_request([])

    result = ProxyResponse(
        status=200,
        headers={"content-type": "text/event-stream"},
        body=None,
        resp=resp,
        upstream_name="test-upstream",
        proxy_start=time.perf_counter(),
        wall_start=time.time(),
        proxy=MagicMock(),
        model="test-model",
        key_id="key-1",
        stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )

    gen = response.content.generator()
    async for _ in gen:
        pass

    upstream_mgr.record_upstream_failure.assert_not_called()


@pytest.mark.asyncio
async def test_disconnect_with_tokens_does_not_record_failure():
    """A client disconnect after tokens were delivered is a user cancel,
    not an upstream health signal."""
    chunks_data = [
        b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        b'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
        b'data: [DONE]\n\n',
    ]
    resp = _FakeUpstreamResponse(chunks=chunks_data, block_after=3)
    request = _fake_request([{"type": "http.disconnect"}])

    result = ProxyResponse(
        status=200, headers={"content-type": "text/event-stream"},
        body=None, resp=resp, upstream_name="test-upstream",
        proxy_start=time.perf_counter(), wall_start=time.time(),
        proxy=MagicMock(), model="test-model", key_id="key-1", stream=True,
    )

    response, proxy, slot, gate, upstream_mgr = _build_provider_test_env(
        resp, request, result,
    )
    gen = response.content.generator()
    async for _ in gen:
        pass

    upstream_mgr.record_upstream_failure.assert_not_called()
