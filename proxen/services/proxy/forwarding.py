"""Streaming response forwarder.

Manages a streaming response lifecycle: yields chunks, tracks disconnect,
parses SSE usage, records telemetry, releases resources.

Extracted from `Proxy.forward_stream` to eliminate the closure-capture
pattern.  State lives on the struct; methods operate on `self`.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress

import httpcore
import msgspec

from ...core.sse import SSEUsageParser
from .context import RequestContext

log = logging.getLogger("proxen.streaming")

_CLOSE_TIMEOUT = 5.0


async def safe_aclose(resp: httpcore.Response, *, timeout: float = _CLOSE_TIMEOUT) -> None:
    """Best-effort `aclose()` with timeout and CancelledError suppression."""
    with suppress(asyncio.CancelledError, Exception):
        await asyncio.wait_for(resp.aclose(), timeout=timeout)


async def cancel_and_await(task: asyncio.Task) -> None:
    """Cancel *task* and swallow the resulting exception (if any)."""
    if not task.done():
        task.cancel()
    with suppress(asyncio.CancelledError, Exception):
        await task


class StreamForwarder(msgspec.Struct):
    """Manages a streaming response: yields chunks, tracks disconnect,
    parses SSE usage, records telemetry, releases resources."""

    proxy: object  # Proxy, typed as object to avoid circular import
    resp: httpcore.Response
    ctx: RequestContext
    wall_start: float
    start: float
    first_chunk: bytes
    disconnect: asyncio.Event
    watcher: asyncio.Task
    upstream_name: str
    upstream_model_id: str
    stall_timeout: float
    stream_iter: object = None
    released: bool = False
    gen_started: bool = False
    watch_task: asyncio.Task | None = None

    @classmethod
    def from_route(
        cls,
        proxy: object,
        ctx: RequestContext,
        route: object,
        wall_start: float,
        disconnect: asyncio.Event,
        watcher: asyncio.Task,
        stall_timeout: float,
    ) -> StreamForwarder:
        """Build a `StreamForwarder` from a `RouteResult`."""
        return cls(
            proxy=proxy, resp=route.resp, ctx=ctx,
            wall_start=wall_start, start=route.start,
            first_chunk=route.first_chunk,
            disconnect=disconnect, watcher=watcher,
            upstream_name=route.upstream_name,
            upstream_model_id=route.upstream_model_id,
            stream_iter=route.stream_iter,
            stall_timeout=stall_timeout,
        )

    def start_watch(self) -> None:
        """Start the background disconnect-watcher task."""
        self.watch_task = asyncio.ensure_future(self.watch())

    async def release(self) -> None:
        """Idempotent: release slot + close resp + cancel watcher.

        Slot is released before `aclose()` so cancellation during close
        cannot orphan it.
        """
        if self.released:
            return
        self.released = True
        self.proxy.release(self.ctx, cooldown=self.disconnect.is_set())
        await safe_aclose(self.resp)
        if not self.watcher.done():
            self.watcher.cancel()

    async def watch(self) -> None:
        """Release on disconnect or stall while the generator is suspended."""
        while not self.disconnect.is_set():
            slot = self.ctx.slot
            now = time.monotonic()
            if slot is not None and slot.last_byte_time:
                # Wake at the stall deadline (last byte + stall_timeout).
                timeout = max(0.1, slot.last_byte_time + self.stall_timeout - now)
            else:
                # No byte yet: poll briefly to pick up gen-start / first byte.
                timeout = min(self.stall_timeout, 1.0)
            try:
                await asyncio.wait_for(self.disconnect.wait(), timeout=timeout)
                break  # client disconnected
            except asyncio.TimeoutError:
                pass
            if not self.gen_started:
                # gen-never-started safety.
                if time.monotonic() - self.start >= 60.0:
                    log.warning("stream generator never started - releasing resources")
                    break
                continue
            slot = self.ctx.slot
            if slot is not None and slot.last_byte_time and \
                    time.monotonic() - slot.last_byte_time >= self.stall_timeout:
                log.warning(
                    "upstream %s stream stalled - no data for %.0fs",
                    self.upstream_name, self.stall_timeout,
                )
                break
        await self.release()

    async def stream(self):
        """The async generator yielded to the HTTP framework."""
        self.gen_started = True
        slot = self.ctx.slot
        parser = SSEUsageParser(self.ctx.protocol)
        completed = False
        upstream_error = False
        ttft: float | None = None
        _disc_wait: asyncio.Task | None = None
        try:
            if self.first_chunk:
                if ttft is None:
                    ttft = time.perf_counter() - self.start
                    if slot:
                        slot.record_ttft(ttft)
                parser.feed(self.first_chunk)
                if slot:
                    slot.last_byte_time = time.monotonic()
                    slot.reset_idle()
                if not self.disconnect.is_set():
                    yield self.first_chunk
            _stream = self.stream_iter if self.stream_iter is not None else aiter(self.resp.stream)
            _disc_wait = asyncio.ensure_future(self.disconnect.wait())
            while True:
                if self.disconnect.is_set():
                    break
                read_task = asyncio.ensure_future(anext(_stream, None))
                done, _pending = await asyncio.wait(
                    {read_task, _disc_wait},
                    timeout=self.stall_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                stalled = not done
                if stalled or read_task not in done:
                    # No chunk or client disconnect.
                    await cancel_and_await(read_task)
                    if stalled:
                        log.warning(
                            "upstream %s stream stalled - no data for %.0fs",
                            self.upstream_name, self.stall_timeout,
                        )
                        upstream_error = True
                    break
                chunk = read_task.result()
                if chunk is None:
                    completed = True
                    break
                if ttft is None:
                    ttft = time.perf_counter() - self.start
                    if slot:
                        slot.record_ttft(ttft)
                parser.feed(chunk)
                if slot:
                    slot.last_byte_time = time.monotonic()
                    slot.reset_idle()
                yield chunk
        except Exception:
            if not self.disconnect.is_set():
                upstream_error = True
                raise
        finally:
            if self.watch_task is not None:
                await cancel_and_await(self.watch_task)
            if _disc_wait is not None:
                await cancel_and_await(_disc_wait)
            disconnected = self.disconnect.is_set()
            usage, found_usage = parser.finalize()
            completed_final = completed or found_usage
            if slot:
                slot.input_tokens = usage.input_tokens
                slot.output_tokens = usage.output_tokens
            duration = time.perf_counter() - self.start
            if ttft is None:
                ttft = duration
            ttft_val, tps = speed_metrics(
                self.resp.status, ttft, duration, usage.output_tokens,
            )
            try:
                if upstream_error and self.upstream_name:
                    self.proxy.upstream_mgr.health.record_failure(
                        (self.upstream_name, self.upstream_model_id), weight=1,
                    )
                self.proxy._record(
                    wall_start=self.wall_start, model=self.ctx.model,
                    upstream=self.upstream_name, key_id=self.ctx.key_hash,
                    ttft=ttft_val, tps=tps, usage=usage,
                    status=self.resp.status, duration=duration,
                    stream=True, disconnected=disconnected,
                    completed=completed_final,
                )
            except Exception:
                # Telemetry must never block slot release.
                log.exception("telemetry recording failed")
            await self.release()
            await cancel_and_await(self.watcher)
            log.info(
                "stream ended completed=%s disconnected=%s model=%s duration=%.3f",
                completed_final, disconnected, self.ctx.model, duration,
            )


# ─── Speed metrics ─────────────────────────────────────────────────

_GEN_TIME_MIN = 1.0


def speed_metrics(
    status: int, ttft: float, duration: float, output_tokens: int
) -> tuple[float, float | None]:
    """Compute (ttft, tps) from raw timing.  Returns tps=None for short
    streams where the rate would be unreliable."""
    if status >= 400:
        return 0.0, 0.0
    gen_time = duration - ttft
    if output_tokens <= 0:
        return ttft, 0.0
    if gen_time <= 0:
        return ttft, output_tokens / duration if duration > 0 else 0.0
    if gen_time < _GEN_TIME_MIN:
        return ttft, None
    return ttft, output_tokens / gen_time
