from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass, fields
from functools import wraps
from urllib.parse import urlparse

import msgspec

from ..core.config import ModelRoute, ProxenModel, Settings, Upstream
from ..core.security import hash_key, mask_key
from .telemetry import Database


def _validate_base_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"invalid base_url: {url!r} (must be an http(s) URL)")


def _from_row(cls, row, *, bool_fields=frozenset(), json_fields=frozenset()):
    def _convert(name, val):
        if val is None:
            return None
        if name in bool_fields:
            return bool(val)
        if name in json_fields:
            return msgspec.json.decode(val)
        return val

    return cls(**{
        f.name: _convert(f.name, row[f.name])
        for f in fields(cls) if f.name in row.keys()
    })


# Field names upstreams use for context/input and output token limits.
# Providers vary (context_length, max_model_len, max_input_tokens, ...), so
# several aliases are accepted.
_TOKEN_LIMIT_KEYS = {
    "input": (
        "context_length", "context_window", "context_size",
        "max_context_length", "max_position_embeddings", "max_model_len",
        "max_input_tokens", "max_sequence_length", "max_seq_len",
        "n_ctx_train", "n_ctx", "ctx_size",
    ),
    "output": ("max_completion_tokens", "max_output_tokens", "max_tokens"),
}


def _extract_token_limit(meta, keys) -> int | None:
    """First int (>= 1024) under any of *keys* in *meta* (nested, case-insensitive)."""
    wanted = {k.lower() for k in keys}

    def walk(obj):
        if not isinstance(obj, dict):
            return None
        for k, v in obj.items():
            if str(k).lower() in wanted and isinstance(v, int) and v >= 1024:
                return v
        for v in obj.values():
            if (r := walk(v)) is not None:
                return r
        return None

    return walk(meta)


def _upsert_sql(table, pk, cols, update_cols=None):
    if update_cols is None:
        update_cols = [c for c in cols if c != pk]
    sets = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    return (
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
        f"ON CONFLICT({pk}) DO UPDATE SET {sets}"
    )


async def _upsert(db, table: str, pk: str, row: dict, *, exclude_update=(), commit=True) -> None:
    """Generic upsert from a {column: value} dict.

    The column list and parameter tuple are derived from a single source
    (the dict keys/values), so they can never drift out of sync.
    """
    cols = list(row)
    update_cols = [c for c in cols if c != pk and c not in exclude_update]
    fn = db.execute_commit if commit else db.execute
    await fn(_upsert_sql(table, pk, cols, update_cols), tuple(row.values()))


def crud(reload: str):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(self, *a, **kw):
            async with self._lock:
                result = await fn(self, *a, **kw)
                await getattr(self, reload)()
                return result
        return wrapper
    return decorator


def _serialize_upstream(u: Upstream) -> dict:
    d = u.to_dict()
    d["api_key"] = mask_key(d["api_key"])
    return d


_LIMIT_COLS = ("max_inflight", "max_requests", "max_requests_window_s", "max_tokens", "max_tokens_window_s")
_NO_LIMITS = dict.fromkeys(_LIMIT_COLS)


@dataclass
class ProxenKey:
    id: int
    key: str
    label: str
    active: bool
    created_at: float
    last_used_at: float | None


class Management:
    """Runtime source of truth for upstreams, proxen user keys, and proxen
    model definitions (pricing + routing + on/off toggle).

    The proxy and upstream manager depend on the read methods below
    (`is_model_enabled`, `get_model`, `get_routes_by_name`,
    `get_upstream`, `enabled_upstreams`) rather than the full
    mutable store.
    """

    def __init__(self, settings: Settings, db: Database) -> None:
        self._settings = settings
        self._db = db
        self._lock = asyncio.Lock()
        self.upstreams: list[Upstream] = []
        self._upstream_index: dict[str, Upstream] = {}
        self.keys: list[ProxenKey] = []
        self._active_key_bytes: set[bytes] = set()
        self._key_label_map: dict[str, str] = {}
        self._admin_key_bytes: set[bytes] = {
            k.encode("utf-8") for k in settings.admin_api_keys
        }
        self.proxen_models: dict[str, ProxenModel] = {}
        self.model_routes: dict[str, list[ModelRoute]] = {}
        self._key_models: dict[str, set[str]] = {}
        self._touch_cache: dict[str, float] = {}

    @property
    def management_enabled(self) -> bool:
        return bool(self._settings.admin_api_keys)

    def admin_keys(self) -> set[bytes]:
        return self._admin_key_bytes

    # ── Read interface (used by proxy + upstream manager) ────────────

    def is_model_enabled(self, model_id: str) -> bool:
        pm = self.proxen_models.get(model_id)
        return pm is not None and pm.enabled

    def get_model(self, model_id: str) -> ProxenModel | None:
        return self.proxen_models.get(model_id)

    def get_routes_by_name(self, model_id: str) -> list[ModelRoute]:
        return self.model_routes.get(model_id, [])

    def get_upstream(self, name: str) -> Upstream | None:
        return self._upstream_index.get(name)

    def enabled_upstreams(self) -> list[Upstream]:
        return [u for u in self.upstreams if u.enabled]

    # ── Lifecycle ────────────────────────────────────────────────────

    async def init(self) -> None:
        async with self._lock:
            await self._load_or_seed()

    async def _load_or_seed(self) -> None:
        await self._load_upstreams()
        if not self.upstreams and self._settings.upstreams:
            for u in self._settings.upstreams:
                await self._db_upsert_upstream(u)
            await self._load_upstreams()

        await self._load_keys()
        if not self.keys and self._settings.api_keys:
            for k in self._settings.api_keys:
                await self._db_add_key(k, "seeded")
            await self._load_keys()

        await self._load_key_models()

        await self._load_proxen_models()
        if not self.proxen_models and self._settings.pricing:
            for model_name, p in self._settings.pricing.items():
                pm = ProxenModel(id=model_name, **asdict(p))
                await self._db_upsert_proxen_model(pm)
                for i, u in enumerate(self.upstreams):
                    await self._db.execute_commit(
                        "INSERT OR IGNORE INTO model_routes (model_id, upstream_name, upstream_model_id, sort_order, enabled) VALUES (?,?,?,?,1)",
                        (model_name, u.name, model_name, i),
                    )
            await self._load_proxen_models()

    # ---- Data loading from DB --------------------------------------------

    async def _load_upstreams(self) -> None:
        cur = await self._db.execute("SELECT * FROM upstreams ORDER BY id ASC")
        self.upstreams = [_from_row(Upstream, r, bool_fields={"enabled"}) for r in await cur.fetchall()]
        self._upstream_index = {u.name: u for u in self.upstreams}

    async def _load_keys(self) -> None:
        cur = await self._db.execute("SELECT * FROM keys ORDER BY id ASC")
        self.keys = [_from_row(ProxenKey, r, bool_fields={"active"}) for r in await cur.fetchall()]
        self._active_key_bytes = {k.key.encode("utf-8") for k in self.keys if k.active}
        self._key_label_map = {
            hash_key(k.key): k.label or f"key-{k.id}" for k in self.keys
        }

    async def _load_proxen_models(self) -> None:
        cur = await self._db.execute("SELECT * FROM models ORDER BY id ASC")
        self.proxen_models = {
            r["id"]: _from_row(ProxenModel, r, bool_fields={"enabled"}, json_fields={"extra_body"}) for r in await cur.fetchall()
        }
        cur = await self._db.execute("SELECT * FROM model_routes ORDER BY model_id ASC, sort_order ASC")
        self.model_routes = {}
        for r in await cur.fetchall():
            self.model_routes.setdefault(r["model_id"], []).append(
                ModelRoute(upstream_name=r["upstream_name"], upstream_model_id=r["upstream_model_id"], sort_order=r["sort_order"], enabled=bool(r["enabled"]))
            )

    # ---- Upstreams ------------------------------------------------------

    async def list_upstreams(self) -> list[dict]:
        return [_serialize_upstream(u) for u in self.upstreams]

    @crud("_load_upstreams")
    async def add_upstream(self, data: dict) -> dict:
        base_url = data.get("base_url", "https://api.openai.com/v1")
        _validate_base_url(base_url)
        upstream = Upstream(
            name=data["name"],
            base_url=base_url,
            api_key=data.get("api_key", ""),
            enabled=data.get("enabled", True),
            max_inflight=data.get("max_inflight"),
        )
        await self._db_upsert_upstream(upstream)
        return _serialize_upstream(upstream)

    @crud("_load_upstreams")
    async def update_upstream(self, name: str, data: dict) -> dict | None:
        existing = next((u for u in self.upstreams if u.name == name), None)
        if existing is None:
            return None
        merged = existing.to_dict()
        new_name = data.get("name")
        for k in ("name", "base_url", "api_key", "enabled", "max_inflight"):
            if k in data:
                merged[k] = data[k]
        if "base_url" in merged:
            _validate_base_url(merged["base_url"])
        upstream = Upstream.from_dict(merged)
        if new_name and new_name != name:
            await self._db.execute("UPDATE model_routes SET upstream_name = ? WHERE upstream_name = ?", (new_name, name))
            await self._db.execute("UPDATE models_cache SET upstream = ? WHERE upstream = ?", (new_name, name))
            await self._db.execute("DELETE FROM upstreams WHERE name = ?", (name,))
        await self._db_upsert_upstream(upstream)
        return _serialize_upstream(upstream)

    @crud("_load_upstreams")
    async def delete_upstream(self, name: str) -> bool:
        await self._db.execute("DELETE FROM model_routes WHERE upstream_name = ?", (name,))
        await self._db.execute("DELETE FROM models_cache WHERE upstream = ?", (name,))
        cur = await self._db.execute_commit("DELETE FROM upstreams WHERE name = ?", (name,))
        return cur.rowcount > 0

    async def _db_upsert_upstream(self, upstream: Upstream) -> None:
        now = time.time()
        await _upsert(self._db, "upstreams", "name", {
            **upstream.to_dict(),
            "enabled": int(upstream.enabled),
            "created_at": now,
            "updated_at": now,
        }, exclude_update=("created_at",))

    # ---- Proxen user keys ----------------------------------------------

    async def list_keys(self) -> list[dict]:
        all_limits = await self.load_all_key_limits()
        return [
            {**asdict(k), "limits": all_limits.get(k.id, _NO_LIMITS)}
            for k in self.keys
        ]

    async def add_key(self, key: str, label: str = "") -> dict:
        async with self._lock:
            await self._db_add_key(key, label)
            await self._load_keys()
            entry = next((k for k in self.keys if k.key == key), None)
            return {
                "id": entry.id if entry else None,
                "key": key,
                "label": label,
                "active": True,
            }

    @crud("_load_keys")
    async def delete_key(self, key_id: int) -> bool:
        await self._db.execute_commit("DELETE FROM key_limits WHERE key_id = ?", (key_id,))
        cur = await self._db.execute_commit("DELETE FROM keys WHERE id = ?", (key_id,))
        return cur.rowcount > 0

    @crud("_load_keys")
    async def update_key(
        self, key_id: int, *, active: bool | None = None, label: str | None = None
    ) -> bool:
        sets: list[str] = []
        args: list = []
        if active is not None:
            sets.append("active = ?")
            args.append(int(active))
        if label is not None:
            sets.append("label = ?")
            args.append(label)
        if not sets:
            return any(k.id == key_id for k in self.keys)
        args.append(key_id)
        cur = await self._db.execute_commit(
            f"UPDATE keys SET {', '.join(sets)} WHERE id = ?", args,
        )
        return cur.rowcount > 0

    async def touch_key(self, key: str) -> None:
        now = time.time()
        last = self._touch_cache.get(key, 0)
        if now - last < 60:
            return
        self._touch_cache[key] = now
        async with self._lock:
            await self._db.execute_commit(
                "UPDATE keys SET last_used_at = ? WHERE key = ?",
                (now, key),
            )
            for k in self.keys:
                if k.key == key:
                    k.last_used_at = now
                    break

    async def _db_add_key(self, key: str, label: str) -> None:
        await self._db.execute_commit(
            "INSERT OR IGNORE INTO keys (key, label, active, created_at) VALUES (?,?,?,?)",
            (key, label, 1, time.time()),
        )

    def active_key_set(self) -> set[bytes]:
        return self._active_key_bytes

    def key_label_map(self) -> dict[str, str]:
        return self._key_label_map

    def key_hash_by_id(self, key_id: int) -> str | None:
        k = next((k for k in self.keys if k.id == key_id), None)
        return hash_key(k.key) if k else None

    # ---- Per-key limits ------------------------------------------------

    async def load_all_key_limits(self) -> dict[int, dict]:
        cur = await self._db.execute("SELECT * FROM key_limits")
        return {r["key_id"]: {c: r[c] for c in _LIMIT_COLS} for r in await cur.fetchall()}

    async def save_key_limits(self, key_id: int, data: dict) -> dict:
        await _upsert(self._db, "key_limits", "key_id", {
            "key_id": key_id,
            **{c: data.get(c) for c in _LIMIT_COLS},
            "updated_at": time.time(),
        })
        return {"key_id": key_id, **data}

    async def delete_key_limits(self, key_id: int) -> bool:
        cur = await self._db.execute_commit(
            "DELETE FROM key_limits WHERE key_id = ?", (key_id,)
        )
        return cur.rowcount > 0

    # ---- Per-key model allowlists -------------------------------------

    async def _load_key_models(self) -> None:
        """Load allowlists into memory keyed by key hash for O(1) lookup."""
        cur = await self._db.execute("SELECT key_id, model_id FROM key_models")
        rows = await cur.fetchall()
        by_key_id: dict[int, set[str]] = {}
        for r in rows:
            by_key_id.setdefault(r["key_id"], set()).add(r["model_id"])
        self._key_models = {
            self.key_hash_by_id(kid): models
            for kid, models in by_key_id.items()
            if self.key_hash_by_id(kid) is not None
        }

    def is_model_allowed(self, key_hash: str, model_id: str) -> bool:
        """True if the key may use the model.  Absent allowlist = allow all."""
        allowed = self._key_models.get(key_hash)
        return allowed is None or model_id in allowed

    async def get_key_models(self, key_id: int) -> list[str]:
        cur = await self._db.execute(
            "SELECT model_id FROM key_models WHERE key_id = ? ORDER BY model_id", (key_id,)
        )
        return [r["model_id"] for r in await cur.fetchall()]

    async def set_key_models(self, key_id: int, models: list[str]) -> list[str]:
        await self._db.execute(
            "DELETE FROM key_models WHERE key_id = ?", (key_id,)
        )
        if models:
            await self._db.executemany_commit(
                "INSERT INTO key_models (key_id, model_id) VALUES (?, ?)",
                [(key_id, m) for m in models],
            )
        else:
            await self._db.commit()
        await self._load_key_models()
        return models

    # ---- Proxen models -------------------------------------------------

    async def list_proxen_models(self) -> list[dict]:
        return [
            {**pm.to_dict(), "routes": [r.to_dict() for r in self.model_routes.get(pm.id, [])]}
            for pm in self.proxen_models.values()
        ]

    @crud("_load_proxen_models")
    async def add_proxen_model(self, data: dict) -> dict:
        model_id = data["id"]
        if model_id in self.proxen_models:
            raise ValueError(f"model '{model_id}' already exists")
        routes = data.get("routes", [])
        if not routes:
            raise ValueError("at least one route is required")
        self._validate_routes(model_id, routes)
        pm = ProxenModel(
            id=model_id,
            enabled=data.get("enabled", True),
            input_per_1m=float(data.get("input_per_1m", 0.0)),
            cached_input_per_1m=float(data.get("cached_input_per_1m", 0.0)),
            output_per_1m=float(data.get("output_per_1m", 0.0)),
            max_input_tokens=data.get("max_input_tokens"),
            max_output_tokens=data.get("max_output_tokens"),
            fallback_strategy=data.get("fallback_strategy", "manual"),
            extra_body=data.get("extra_body"),
        )
        await self._db_upsert_proxen_model(pm)
        await self._db_upsert_routes(model_id, routes)
        return {**pm.to_dict(), "routes": routes}

    @crud("_load_proxen_models")
    async def update_proxen_model(self, model_id: str, data: dict) -> dict | None:
        pm = self.proxen_models.get(model_id)
        if pm is None:
            return None

        new_id = data.get("id")
        if new_id and new_id != model_id:
            if new_id in self.proxen_models:
                raise ValueError(f"model '{new_id}' already exists")
            await self._db.execute("UPDATE models SET id = ? WHERE id = ?", (new_id, model_id))
            await self._db.execute("UPDATE request_tags SET name = ? WHERE name = ?", (new_id, model_id))
            pm.id = new_id

        if "enabled" in data:
            pm.enabled = bool(data["enabled"])
        for k in ("input_per_1m", "cached_input_per_1m", "output_per_1m"):
            if k in data and data[k] is not None:
                setattr(pm, k, float(data[k]))
        for k in ("max_input_tokens", "max_output_tokens"):
            if k in data:
                setattr(pm, k, data[k])
        if "fallback_strategy" in data:
            pm.fallback_strategy = data["fallback_strategy"]
        if "extra_body" in data:
            pm.extra_body = data["extra_body"] or None
        routes = data.get("routes")
        if routes is not None:
            if not routes:
                raise ValueError("at least one route is required")
            self._validate_routes(pm.id, routes)
            await self._db_upsert_routes(pm.id, routes)
        await self._db_upsert_proxen_model(pm)
        return {**pm.to_dict(), "routes": [r.to_dict() for r in self.model_routes.get(pm.id, [])]}

    @crud("_load_proxen_models")
    async def delete_proxen_model(self, model_id: str) -> bool:
        await self._db.execute_commit("DELETE FROM model_routes WHERE model_id = ?", (model_id,))
        cur = await self._db.execute_commit("DELETE FROM models WHERE id = ?", (model_id,))
        return cur.rowcount > 0

    def _validate_routes(self, model_id: str, routes: list[dict]) -> None:
        upstream_names = {u.name for u in self.upstreams}
        seen_upstreams: set[str] = set()
        for r in routes:
            name = r.get("upstream_name", "")
            if name not in upstream_names:
                raise ValueError(f"upstream '{name}' not found")
            if name in seen_upstreams:
                raise ValueError(f"duplicate upstream '{name}' in routes")
            seen_upstreams.add(name)
            if not r.get("upstream_model_id"):
                raise ValueError(f"upstream_model_id required for upstream '{name}'")

    async def _db_upsert_proxen_model(self, pm: ProxenModel, *, commit=True) -> None:
        await _upsert(self._db, "models", "id", {
            **asdict(pm),
            "enabled": int(pm.enabled),
            "extra_body": msgspec.json.encode(pm.extra_body) if pm.extra_body else None,
            "updated_at": time.time(),
        }, commit=commit)

    async def _db_upsert_routes(self, model_id: str, routes: list[dict]) -> None:
        await self._db.execute(
            "DELETE FROM model_routes WHERE model_id = ?", (model_id,),
        )
        rows = [
            (model_id, r["upstream_name"], r["upstream_model_id"], r.get("sort_order", 0), int(r.get("enabled", True)))
            for r in routes
        ]
        await self._db.executemany_commit(
            "INSERT INTO model_routes (model_id, upstream_name, upstream_model_id, sort_order, enabled) VALUES (?,?,?,?,?)",
            rows,
        )

    async def list_available_models(self, upstream_name: str) -> list[dict]:
        cur = await self._db.execute(
            "SELECT id, object, created, owned_by, fetched_meta FROM models_cache WHERE upstream = ? ORDER BY id ASC",
            (upstream_name,),
        )
        out = []
        for r in await cur.fetchall():
            meta = None
            if r["fetched_meta"]:
                try:
                    meta = msgspec.json.decode(r["fetched_meta"])
                except (msgspec.DecodeError, TypeError):
                    meta = None
            out.append({"id": r["id"], "object": r["object"], "created": r["created"], "owned_by": r["owned_by"], "fetched_meta": meta})
        return out

    async def import_models(
        self,
        upstream_name: str,
        models: list[str] | None = None,
        overrides: dict[str, str] | None = None,
        overwrite: list[str] | None = None,
    ) -> dict:
        async with self._lock:
            return await self._import_models_locked(upstream_name, models, overrides, overwrite)

    async def _import_models_locked(
        self,
        upstream_name: str,
        models: list[str] | None,
        overrides: dict[str, str] | None,
        overwrite: list[str] | None,
    ) -> dict:
        overrides = overrides or {}
        overwrite_set = set(overwrite or [])

        if models is not None:
            placeholders = ",".join("?" for _ in models)
            cur = await self._db.execute(
                f"SELECT id, fetched_meta FROM models_cache WHERE upstream = ? AND id IN ({placeholders}) ORDER BY id ASC",
                (upstream_name, *models),
            )
        else:
            cur = await self._db.execute(
                "SELECT id, fetched_meta FROM models_cache WHERE upstream = ? ORDER BY id ASC",
                (upstream_name,),
            )

        rows = await cur.fetchall()
        if not rows:
            raise ValueError(f"no models found for upstream '{upstream_name}'")

        config_pricing = self._settings.pricing
        imported = []
        skipped = []
        conflicts = []

        for r in rows:
            mid = r["id"]
            proxen_id = overrides.get(mid, mid)

            if proxen_id in self.proxen_models:
                if mid in overwrite_set:
                    await self._db.execute("DELETE FROM model_routes WHERE model_id = ?", (proxen_id,))
                    await self._db.execute("DELETE FROM models WHERE id = ?", (proxen_id,))
                elif proxen_id == mid:
                    conflicts.append(mid)
                    continue
                else:
                    skipped.append(mid)
                    continue

            p = config_pricing.get(mid)

            try:
                meta = msgspec.json.decode(r["fetched_meta"])
            except (msgspec.DecodeError, TypeError):
                meta = None

            pm = ProxenModel(
                id=proxen_id,
                input_per_1m=p.input_per_1m if p else 0.0,
                cached_input_per_1m=p.cached_input_per_1m if p else 0.0,
                output_per_1m=p.output_per_1m if p else 0.0,
                max_input_tokens=_extract_token_limit(meta, _TOKEN_LIMIT_KEYS["input"]),
                max_output_tokens=_extract_token_limit(meta, _TOKEN_LIMIT_KEYS["output"]),
            )
            await self._db_upsert_proxen_model(pm, commit=False)
            await self._db.execute(
                "INSERT INTO model_routes (model_id, upstream_name, upstream_model_id, sort_order, enabled) VALUES (?,?,?,?,1)",
                (proxen_id, upstream_name, mid, 0),
            )
            imported.append(proxen_id)

        await self._db.commit()
        await self._load_proxen_models()
        return {"imported": imported, "skipped": skipped, "conflicts": conflicts}

    # ---- Gate limits ---------------------------------------------------

    async def load_gate_limits(self) -> tuple[int, int] | None:
        cur = await self._db.execute(
            "SELECT max_inflight, max_waiting FROM gate_limits WHERE key = 'default'"
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return int(row["max_inflight"]), int(row["max_waiting"])

    async def set_gate_limits(self, max_inflight: int, max_waiting: int) -> None:
        await _upsert(self._db, "gate_limits", "key", {
            "key": "default", "max_inflight": max_inflight, "max_waiting": max_waiting,
            "updated_at": time.time(),
        })
