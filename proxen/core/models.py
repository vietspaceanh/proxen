from __future__ import annotations

import msgspec


class RequestRecord(msgspec.Struct):
    """A single completed (or aborted) request, persisted to telemetry."""

    timestamp: float
    model: str
    upstream: str
    key_id: str
    ttft: float = 0.0
    tps: float | None = 0.0
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    status: int = 0
    duration: float = 0.0
    stream: bool = False
    client_disconnect: bool = False
    upstream_dropped: bool = False
    needs_review: bool = False
