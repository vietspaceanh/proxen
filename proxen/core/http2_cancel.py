"""Monkey-patch httpcore's HTTP/2 connection to send RST_STREAM on close.

When a response is closed or cancelled, this patch sends an HTTP/2
RST_STREAM frame with error code CANCEL to the upstream.  This tells
the upstream to stop processing immediately, preventing zombie requests
from consuming concurrency slots.

For already-completed streams (where the server sent END_STREAM), h2
raises ProtocolError which is caught and ignored - no RST_STREAM is
sent, so normal completions are unaffected.

Importing this module applies the patch.  It should be imported once at
startup (before any HTTP/2 connections are created).  If the patch fails
to apply (httpcore internals changed), a RuntimeError is raised at import
time.
"""
from __future__ import annotations

import logging

import h2.errors

log = logging.getLogger("proxen.http2")

# Cap RST_STREAM write so a stalled upstream cannot hang cleanup.
_RST_WRITE_TIMEOUT = 5.0

from httpcore._async.http2 import AsyncHTTP2Connection

_original_response_closed = AsyncHTTP2Connection._response_closed


async def _rst_stream_response_closed(self, stream_id: int) -> None:
    """Send RST_STREAM(CANCEL) before the original cleanup."""
    try:
        self._h2_state.reset_stream(
            stream_id, error_code=h2.errors.ErrorCodes.CANCEL,
        )
        data = self._h2_state.data_to_send()
        if data:
            await self._network_stream.write(data, timeout=_RST_WRITE_TIMEOUT)
    except Exception:
        pass
    await _original_response_closed(self, stream_id)


AsyncHTTP2Connection._response_closed = _rst_stream_response_closed

if AsyncHTTP2Connection._response_closed is not _rst_stream_response_closed:
    raise RuntimeError(
        "RST_STREAM patch failed to apply - httpcore internals may have changed. "
        "Cancelling requests will NOT send RST_STREAM to the upstream."
    )

log.debug("RST_STREAM patch applied to AsyncHTTP2Connection")
