"""Inter-service contracts (Protocols).

Services depend on these narrow interfaces rather than each other's concrete
types, so each service is constructible and testable with lightweight fakes.
The concrete implementations live in :mod:`proxen.services`.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from .config import ModelRoute, ProxenModel, Upstream
from .models import RequestRecord


@runtime_checkable
class UpstreamCatalog(Protocol):
    """Read-only view of the model/upstream routing store.

    Consumed by :class:`~proxen.services.proxy.Proxy` (to resolve routes)
    and :class:`~proxen.services.upstream.UpstreamManager` (to enumerate
    enabled upstreams for model sync). Mutating the catalog is the
    management service's own concern and stays off this interface.
    """

    def is_model_enabled(self, model_id: str) -> bool: ...

    def get_model(self, model_id: str) -> ProxenModel | None: ...

    def get_routes_by_name(self, model_id: str) -> list[ModelRoute]: ...

    def get_upstream(self, name: str) -> Upstream | None: ...

    def enabled_upstreams(self) -> list[Upstream]: ...


@runtime_checkable
class TelemetrySink(Protocol):
    """Where the proxy drops completed request records.

    Implementations must be non-blocking: a full sink drops the record and
    increments a counter rather than back-pressuring the hot path.
    """

    def enqueue(self, record: RequestRecord) -> None: ...
