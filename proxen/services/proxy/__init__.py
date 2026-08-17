"""Proxy pipeline sub-package.

Public API:
    Proxy              - orchestrator (from .pipeline)
    Router             - route resolution (from .routing)
    StreamForwarder    - streaming forwarder (from .forwarding)
    RequestContext     - per-request state (from .context)
    ProxyError, ...    - error hierarchy (from .context)
    speed_metrics      - TTFT/TPS calculation (from .forwarding)
"""
from .context import (
    AdmissionError,
    ModelNotFound,
    NoRoutes,
    ProxyError,
    RequestContext,
    UpstreamAuthUnavailable,
    UpstreamProtocolError,
    UpstreamUnavailable,
)
from .forwarding import StreamForwarder, speed_metrics
from .pipeline import Proxy
from .routing import Router, RouteResult

__all__ = [
    "Proxy",
    "Router",
    "RouteResult",
    "StreamForwarder",
    "RequestContext",
    "ProxyError",
    "ModelNotFound",
    "NoRoutes",
    "UpstreamUnavailable",
    "UpstreamAuthUnavailable",
    "UpstreamProtocolError",
    "AdmissionError",
    "speed_metrics",
]
