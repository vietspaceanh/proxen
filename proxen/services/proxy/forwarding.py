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
    stream_iter: object = None
    released: bool = False
    gen_started: bool = False
    watch_task: asyncio.Task | None = None

    def start_watch(self) -> None:
        """Start the background disconnect-watcher task."""
        self.watch_task = asyncio.ensure_future(self.watch())

    async def release(self) -> None:
        """Idempotent: close resp + release provider + global gate + watcher."""
        if self.released:
            return
        self.released = True
        with suppress(Exception):
            await self.resp.aclose()
        self.proxy.release(self.ctx, cooldown=self.disconnect.is_set())
        if not self.watcher.done():
            self.watcher.cancel()

    async def watch(self) -> None:
        """Await disconnect then release. Safety timeout if generator never starts."""
        try:
            await asyncio.wait_for(self.disconnect.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            if self.gen_started:
                await self.disconnect.wait()
            else:
                log.warning("stream generator never started - releasing resources")
        await self.release()

    async def stream(self):
        """The async generator yielded to the HTTP framework."""
        self.gen_started = True
        slot = self.ctx.slot
        parser = SSEUsageParser(self.ctx.protocol)
        completed = False
        upstream_error = False
        ttft: float | None = None
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
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if read_task not in done:
                    if not read_task.done():
                        read_task.cancel()
                        with suppress(asyncio.CancelledError, Exception):
                            await read_task
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
            if not _disc_wait.done():
                _disc_wait.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await _disc_wait
        except Exception:
            if not self.disconnect.is_set():
                upstream_error = True
                raise
        finally:
            if self.watch_task is not None:
                self.watch_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await self.watch_task
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
            await self.release()
            if not self.watcher.done():
                with suppress(asyncio.CancelledError, Exception):
                    await self.watcher
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
