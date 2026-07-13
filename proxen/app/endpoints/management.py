from __future__ import annotations

import logging

import msgspec
from blacksheep import FromBytes, Response

from ...core.gate import ConcurrencyGate, KeyLimits
from ...services.management import Management
from ...services.upstream import UpstreamManager
from ..broadcaster import StatsBroadcaster
from ..http import error_json, json_response
from . import delete, get, patch, post, put

log = logging.getLogger("proxen.endpoints.management")


class UpstreamIn(msgspec.Struct):
    name: str
    base_url: str = "https://api.openai.com/v1"
    api_key: str = ""
    enabled: bool = True
    max_inflight: int | None = None


class UpstreamUpdateIn(msgspec.Struct):
    name: str | None | msgspec.UnsetType = msgspec.UNSET
    base_url: str | None | msgspec.UnsetType = msgspec.UNSET
    api_key: str | None | msgspec.UnsetType = msgspec.UNSET
    enabled: bool | None | msgspec.UnsetType = msgspec.UNSET
    max_inflight: int | None | msgspec.UnsetType = msgspec.UNSET


class KeyIn(msgspec.Struct):
    key: str
    label: str = ""


class KeyUpdateIn(msgspec.Struct):
    active: bool | None = None
    label: str | None = None


class ProxenModelIn(msgspec.Struct):
    id: str
    enabled: bool = True
    input_per_1m: float = 0.0
    cached_input_per_1m: float = 0.0
    output_per_1m: float = 0.0
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    fallback_strategy: str = "manual"
    extra_body: dict | None = None
    routes: list[RouteIn] = []


class ProxenModelUpdateIn(msgspec.Struct):
    id: str | None = None
    enabled: bool | None = None
    input_per_1m: float | None = None
    cached_input_per_1m: float | None = None
    output_per_1m: float | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    fallback_strategy: str | None = None
    extra_body: dict | None = None
    routes: list[RouteIn] | None = None


class RouteIn(msgspec.Struct):
    upstream_name: str
    upstream_model_id: str
    sort_order: int = 0
    enabled: bool = True


class ImportModelsIn(msgspec.Struct):
    models: list[str] | None = None
    overrides: dict[str, str] | None = None
    overwrite: list[str] | None = None


class BulkDeleteIn(msgspec.Struct):
    ids: list[str]


class GateLimitsIn(msgspec.Struct):
    max_inflight: int | None = None
    max_waiting: int | None = None


class KeyLimitsIn(msgspec.Struct):
    max_inflight: int | None = None
    max_requests: int | None = None
    max_requests_window_s: float | None = None
    max_tokens: int | None = None
    max_tokens_window_s: float | None = None


def _decode_body(body: bytes, cls):
    try:
        return msgspec.json.decode(body, type=cls)
    except msgspec.DecodeError as exc:
        raise ValueError(str(exc)) from None


def _struct_to_dict(obj) -> dict:
    return msgspec.to_builtins(obj)


# ─── Route handlers ─────────────────────────────────────────────────


@get("/api/management/status")
async def status(management: Management) -> Response:
    return json_response({"enabled": management.management_enabled})


@get("/api/management/gate")
async def get_gate(management: Management, gate: ConcurrencyGate) -> Response:
    snapshot = gate.snapshot()
    return json_response({
        "max_inflight": snapshot.max_inflight,
        "max_waiting": snapshot.max_waiting,
    })


@patch("/api/management/gate")
async def update_gate(
    body: FromBytes,
    management: Management,
    gate: ConcurrencyGate,
) -> Response:
    data = _decode_body(body.value, GateLimitsIn)
    if data.max_inflight is None and data.max_waiting is None:
        return error_json(400, "nothing to update")
    snapshot = gate.snapshot()
    new_mi = data.max_inflight if data.max_inflight is not None else snapshot.max_inflight
    new_mw = data.max_waiting if data.max_waiting is not None else snapshot.max_waiting
    await management.set_gate_limits(new_mi, new_mw)
    gate.set_limits(new_mi, new_mw)
    return json_response({"max_inflight": new_mi, "max_waiting": new_mw})


@get("/api/management/upstreams")
async def list_upstreams(management: Management) -> Response:
    return json_response({"data": await management.list_upstreams()})


@post("/api/management/upstreams")
async def add_upstream(
    body: FromBytes,
    management: Management,
    upstream_mgr: UpstreamManager,
) -> Response:
    data = _decode_body(body.value, UpstreamIn)
    result = await management.add_upstream(_struct_to_dict(data))
    upstream_mgr.set_provider_limit(result["name"], result.get("max_inflight"))
    return json_response(result)


@put("/api/management/upstreams/{name}")
async def update_upstream(
    body: FromBytes,
    name: str,
    management: Management,
    upstream_mgr: UpstreamManager,
) -> Response:
    data = _decode_body(body.value, UpstreamUpdateIn)
    upd = _struct_to_dict(data)
    result = await management.update_upstream(name, upd)
    if result is None:
        return error_json(404, f"upstream '{name}' not found")
    new_name = result["name"]
    if new_name != name:
        upstream_mgr.rename_provider(name, new_name)
    upstream_mgr.set_provider_limit(new_name, result.get("max_inflight"))
    return json_response(result)


@delete("/api/management/upstreams/{name}")
async def delete_upstream(name: str, management: Management) -> Response:
    if not await management.delete_upstream(name):
        return error_json(404, f"upstream '{name}' not found")
    return json_response({"deleted": name})


@post("/api/management/upstreams/{name}/fetch-models")
async def fetch_models(
    name: str,
    management: Management,
    upstream_mgr: UpstreamManager,
) -> Response:
    try:
        await upstream_mgr.sync_models(name)
    except KeyError:
        return error_json(404, f"upstream '{name}' not found")
    return json_response({"data": await management.list_available_models(name)})


@get("/api/management/upstreams/{name}/available-models")
async def available_models(name: str, management: Management) -> Response:
    models = await management.list_available_models(name)
    return json_response({"data": models})


@post("/api/management/upstreams/{name}/import-models")
async def import_models(
    body: FromBytes,
    name: str,
    management: Management,
    upstream_mgr: UpstreamManager,
) -> Response:
    try:
        data = _decode_body(body.value, ImportModelsIn)
    except ValueError:
        data = ImportModelsIn()
    try:
        result = await management.import_models(name, data.models, data.overrides, data.overwrite)
    except KeyError:
        return error_json(404, f"upstream '{name}' not found")
    return json_response(result)


@get("/api/management/keys")
async def list_keys(management: Management) -> Response:
    return json_response({"data": await management.list_keys()})


@post("/api/management/keys")
async def add_key(body: FromBytes, management: Management) -> Response:
    data = _decode_body(body.value, KeyIn)
    if not data.key:
        return error_json(400, "key is required")
    return json_response(await management.add_key(data.key, data.label))


@patch("/api/management/keys/{key_id}")
async def update_key(
    key_id: int,
    body: FromBytes,
    management: Management,
) -> Response:
    data = _decode_body(body.value, KeyUpdateIn)
    updates = {k: v for k, v in _struct_to_dict(data).items() if v is not None}
    if not await management.update_key(key_id, **updates):
        return error_json(404, f"key {key_id} not found")
    return json_response({"updated": key_id})


@delete("/api/management/keys/{key_id}")
async def delete_key(
    key_id: int,
    management: Management,
    gate: ConcurrencyGate,
) -> Response:
    key_hash = management.key_hash_by_id(key_id)
    if not await management.delete_key(key_id):
        return error_json(404, f"key {key_id} not found")
    if key_hash:
        gate.remove_key_limits(key_hash)
    return json_response({"deleted": key_id})


@get("/api/management/keys/{key_id}/limits")
async def get_key_limits(
    key_id: int,
    management: Management,
    gate: ConcurrencyGate,
) -> Response:
    key_hash = management.key_hash_by_id(key_id)
    if key_hash is None:
        return error_json(404, f"key {key_id} not found")
    limits = gate.get_key_limits(key_hash)
    return json_response({"key_id": key_id, **limits.to_dict()})


@put("/api/management/keys/{key_id}/limits")
async def set_key_limits(
    body: FromBytes,
    key_id: int,
    management: Management,
    gate: ConcurrencyGate,
) -> Response:
    key_hash = management.key_hash_by_id(key_id)
    if key_hash is None:
        return error_json(404, f"key {key_id} not found")
    data = _decode_body(body.value, KeyLimitsIn)
    limits_data = _struct_to_dict(data)
    limits = KeyLimits.from_dict(limits_data)
    limits.validate()
    await management.save_key_limits(key_id, limits_data)
    gate.set_key_limits(key_hash, limits)
    return json_response({"key_id": key_id, **limits.to_dict()})


@delete("/api/management/keys/{key_id}/limits")
async def clear_key_limits(
    key_id: int,
    management: Management,
    gate: ConcurrencyGate,
) -> Response:
    key_hash = management.key_hash_by_id(key_id)
    if key_hash is None:
        return error_json(404, f"key {key_id} not found")
    await management.delete_key_limits(key_id)
    gate.remove_key_limits(key_hash)
    return json_response({"key_id": key_id, "cleared": True})


# ─── Per-key model allowlists ───────────────────────────────────────


class KeyModelsIn(msgspec.Struct):
    models: list[str]


@get("/api/management/keys/{key_id}/models")
async def get_key_models(
    key_id: int,
    management: Management,
) -> Response:
    if management.key_hash_by_id(key_id) is None:
        return error_json(404, f"key {key_id} not found")
    models = await management.get_key_models(key_id)
    return json_response({"key_id": key_id, "models": models})


@put("/api/management/keys/{key_id}/models")
async def set_key_models(
    body: FromBytes,
    key_id: int,
    management: Management,
) -> Response:
    if management.key_hash_by_id(key_id) is None:
        return error_json(404, f"key {key_id} not found")
    data = _decode_body(body.value, KeyModelsIn)
    models = await management.set_key_models(key_id, data.models)
    return json_response({"key_id": key_id, "models": models})


@delete("/api/management/keys/{key_id}/models")
async def clear_key_models(
    key_id: int,
    management: Management,
) -> Response:
    if management.key_hash_by_id(key_id) is None:
        return error_json(404, f"key {key_id} not found")
    await management.set_key_models(key_id, [])
    return json_response({"key_id": key_id, "cleared": True})


@get("/api/management/models")
async def list_models(management: Management) -> Response:
    return json_response({"data": await management.list_proxen_models()})


@post("/api/management/models")
async def add_model(
    body: FromBytes,
    management: Management,
    broadcaster: StatsBroadcaster,
) -> Response:
    data = _decode_body(body.value, ProxenModelIn)
    result = await management.add_proxen_model(_struct_to_dict(data))
    broadcaster.invalidate_chart_cache()
    return json_response(result)


@put("/api/management/models/{model_id}")
async def update_model(
    body: FromBytes,
    model_id: str,
    management: Management,
    broadcaster: StatsBroadcaster,
) -> Response:
    data = _decode_body(body.value, ProxenModelUpdateIn)
    data = {k: v for k, v in _struct_to_dict(data).items() if v is not None}
    result = await management.update_proxen_model(model_id, data)
    if result is None:
        return error_json(404, f"model '{model_id}' not found")
    broadcaster.invalidate_chart_cache()
    return json_response(result)


@delete("/api/management/models/{model_id}")
async def delete_model(
    model_id: str,
    management: Management,
    broadcaster: StatsBroadcaster,
) -> Response:
    if not await management.delete_proxen_model(model_id):
        return error_json(404, f"model '{model_id}' not found")
    broadcaster.invalidate_chart_cache()
    return json_response({"deleted": model_id})


@post("/api/management/models/bulk-delete")
async def bulk_delete_models(
    body: FromBytes,
    management: Management,
    broadcaster: StatsBroadcaster,
) -> Response:
    data = _decode_body(body.value, BulkDeleteIn)
    if not data.ids:
        return error_json(400, "no model ids provided")
    for model_id in data.ids:
        await management.delete_proxen_model(model_id)
    broadcaster.invalidate_chart_cache()
    return json_response({"deleted": data.ids})
