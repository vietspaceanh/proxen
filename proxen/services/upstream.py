from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from contextlib import suppress

import aiohttp
import msgspec

from ..core.config import Settings, Upstream
from ..core.concurrency import ConcurrencyGate
from ..core.health import HealthCheck
from .telemetry import Database
from .management import Management

log = logging.getLogger("proxen.upstream")


class UpstreamManager:
    """Owns the shared HTTP session and keeps the model catalog in sync."""

    def __init__(
        self,
        settings: Settings,
        db: Database,
        management: Management,
        gate: ConcurrencyGate,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._settings = settings
        self._db = db
        self.management = management
        self.gate = gate
        self._session = session
        self._models_cache: dict[str, list[dict]] = {}
        self._on_change: Callable[[], None] | None = None
        self.health = HealthCheck(
            failure_threshold=settings.health_guard_failures,
            backoff_base=settings.health_guard_retry_delay,
        )

    # ── Provider helpers (with extra logic beyond gate delegation) ───

    def set_on_change(self, cb: Callable[[], None]) -> None:
        self._on_change = cb

    def rename_provider(self, old_name: str, new_name: str) -> None:
        self.gate.rename_provider(old_name, new_name)
        if old_name in self._models_cache:
            self._models_cache[new_name] = self._models_cache.pop(old_name)

    def remove_provider(self, name: str) -> None:
        self.gate.remove_provider(name)
        self._models_cache.pop(name, None)

    def provider_status(self) -> dict[str, dict]:
        out = self.gate.provider_status()
        for (name, model_id), state in self.health.failing_states().items():
            out.setdefault(name, {"inflight": 0, "waiting": 0, "max_inflight": None, "max_waiting": 0, "routes": {}})["routes"][model_id] = state
        return out

    async def init(self) -> None:
        """Create the aiohttp session if one was not injected."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                # No total cap: a streaming response (including reasoning
                # tokens) must run as long as data keeps flowing, a fixed
                # total timeout was cutting off long deep-research streams.
                # `sock_read` instead kills only connections that receive
                # zero bytes for that long (genuinely stalled/dead), since
                # each received chunk resets it.
                timeout=aiohttp.ClientTimeout(
                    total=None,
                    connect=10,
                    sock_read=self._settings.upstream_sock_read,
                ),
                connector=aiohttp.TCPConnector(
                    limit=100,
                    limit_per_host=100,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True,
                    ttl_dns_cache=300,
                ),
                # auto_decompress lets aiohttp transparently decode gzip/deflate
                # on non-streaming JSON so the upstream→proxen hop stays
                # compressed. SSE streams carry no Content-Encoding, so this is
                # a no-op for streaming. Response headers still strip
                # content-encoding/content-length before forwarding.
                auto_decompress=True,
            )
        # Load manual models from DB so they survive restarts.
        self._models_cache["manual"] = await self.load_cached_models("manual")

    @property
    def session(self) -> aiohttp.ClientSession:
        return self._session  # type: ignore[return-value]

    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        try:
            return await self.session.post(url, **kwargs)
        except aiohttp.ClientConnectorError:
            log.info("upstream POST connection failed, retrying on fresh connection")
            await asyncio.sleep(0.05)
            return await self.session.post(url, **kwargs)
        except asyncio.TimeoutError:
            # `sock_read` timeout: the upstream received the request but
            # sent no data within the read window. aiohttp closes the
            # connection, but the TCP close may not have reached the
            # upstream yet. Brief pause so the upstream detects the close
            # before the caller releases the provider slot and sends a
            # new request - otherwise the upstream briefly sees an extra
            # concurrent connection.
            log.info("upstream POST timed out (sock_read), pausing before release")
            await asyncio.sleep(0.5)
            raise

    def all_enabled(self) -> list[Upstream]:
        return self.management.enabled_upstreams()

    def get_models(self) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for upstream in self.all_enabled():
            for model in self._models_cache.get(upstream.name, []):
                mid = model.get("id")
                if mid and mid not in seen:
                    seen.add(mid)
                    out.append(model)
        for model in self._models_cache.get("manual", []):
            mid = model.get("id")
            if mid and mid not in seen:
                seen.add(mid)
                out.append(model)
        return out

    async def sync_models(self, upstream_name: str | None = None) -> list[dict]:
        """Sync the model catalog for one upstream (by name) or all enabled."""
        targets = self.all_enabled()
        if upstream_name is not None:
            targets = [u for u in targets if u.name == upstream_name]
            if not targets:
                raise KeyError(upstream_name)
        for upstream in targets:
            if not self.gate.try_provider(upstream.name):
                log.info(
                    "skipping model sync for %s: provider at capacity",
                    upstream.name,
                )
                continue
            try:
                url = f"{upstream.base_url.rstrip('/')}/models"
                headers = {
                    "Authorization": f"Bearer {upstream.api_key.get_secret_value()}"
                }
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        log.warning(
                            "model sync from %s returned %s", upstream.name, resp.status
                        )
                        continue
                    data = await resp.json()
                models = data.get("data", []) if isinstance(data, dict) else []
                self._models_cache[upstream.name] = models
                await self.replace_cached_models(upstream.name, models)
                log.info("synced %d models from %s", len(models), upstream.name)
            except Exception:
                log.exception("model sync failed for %s", upstream.name)
            finally:
                self.gate.release_provider(upstream.name)
        return self.get_models()

    async def start_sync_loop(self) -> None:
        interval = self._settings.model_sync_interval
        while True:
            await asyncio.sleep(interval)
            try:
                await self.sync_models()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("model sync loop crashed, will retry next cycle")

    async def aclose(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def load_cached_models(self, upstream: str) -> list[dict]:
        async with await self._db.execute(
            "SELECT id, object, created, owned_by FROM models_cache WHERE upstream = ?",
            (upstream,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def replace_cached_models(self, upstream: str, models: list[dict]) -> None:
        if not models:
            return
        now = time.time()
        await self._db.execute("DELETE FROM models_cache WHERE upstream = ?", (upstream,))
        rows = [
            (
                upstream, model.get("id", ""), model.get("object", "model"),
                model.get("created"), model.get("owned_by"),
                msgspec.json.encode(extra).decode() if (extra := {k: v for k, v in model.items() if k not in {"id", "object", "created", "owned_by"}}) else None,
                now, now,
            )
            for model in models
        ]
        await self._db.executemany_commit(
            """INSERT OR REPLACE INTO models_cache
               (upstream, id, object, created, owned_by, fetched_meta, fetched_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            rows,
        )
