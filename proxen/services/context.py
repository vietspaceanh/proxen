"""Proxy domain types: exceptions and request context.

No internal dependencies — imported by routing, streaming, and proxy modules.
"""
from __future__ import annotations

from typing import ClassVar

import msgspec

from ..core.gate import InflightSlot


class ProxyError(Exception):
    """Base for errors that produce an HTTP error response."""

    message: str = ""
    status: ClassVar[int] = 502
    upstream: str = "none"

    def __init__(self, message: str = "", *, upstream: str = "none") -> None:
        self.message = message
        self.upstream = upstream
        super().__init__(message)


class ModelNotFound(ProxyError):
    status: ClassVar[int] = 404


class NoRoutes(ProxyError):
    status: ClassVar[int] = 503


class UpstreamUnavailable(ProxyError):
    status: ClassVar[int] = 502


class AdmissionError(Exception):
    """Raised by an admit hook to deny a request before any resource is acquired."""

    def __init__(self, status: int, message: str, *, type: str = "") -> None:
        self.status = status
        self.message = message
        self.type = type
        super().__init__(message)


class RequestContext(msgspec.Struct):
    """Mutable per-request state threaded through the pipeline.

    Request identity fields are set by the endpoint.  ``slot`` and
    ``provider`` are set during concurrency acquisition and routing.
    No telemetry result fields - those go directly to ``_record()``.
    """

    key_hash: str = ""
    model: str = ""
    stream: bool = False
    path: str = ""
    query: str = ""
    body: bytes = b""
    raw_headers: list = msgspec.field(default_factory=list)
    routes: list = msgspec.field(default_factory=list)
    protocol: str = "openai"
    slot: InflightSlot | None = None
    provider: str = ""
