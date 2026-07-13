"""HTTP utilities for the proxy: header filtering, byte-level JSON patching,
disconnect racing, and speed-metrics computation.

These are stateless helpers with no dependency on the Proxy class.  Extracted
from `services.proxy` to keep the proxy module focused on orchestration.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
from copy import deepcopy

import msgspec

# ─── Header forwarding ───────────────────────────────────────────────

_HOP_BY_HOP = {
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "authorization",
    "x-api-key",
}

_RESP_STRIP = _HOP_BY_HOP | {"content-encoding", "accept-encoding"}


def protocol_from_path(path) -> str:
    if isinstance(path, (bytes, bytearray)):
        path = path.decode("utf-8", "replace")
    return "anthropic" if path.startswith("/v1/messages") else "openai"


def filter_headers(
    src, provider_key: str | None = None, protocol: str = "openai"
) -> dict[str, str]:
    """Filter headers for forwarding."""
    out: dict[str, str] = {}
    if hasattr(src, "items"):
        pairs = src.items()
    else:
        pairs = src
    strip = _HOP_BY_HOP if provider_key is not None else _RESP_STRIP
    for key, value in pairs:
        k = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        v = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        if k.lower() in strip:
            continue
        out[k] = v
    if provider_key:
        if protocol == "anthropic":
            out["x-api-key"] = provider_key
            if not any(k.lower() == "anthropic-version" for k in out):
                out["anthropic-version"] = "2023-06-01"
        else:
            out["Authorization"] = f"Bearer {provider_key}"
    return out


# ─── Speed metrics ──────────────────────────────────────────────────

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


# ─── Byte-level model field patching ─────────────────────────────────

_WS = b" \t\n\r"
_Q = 0x22    # "
_BS = 0x5C   # \
_LB = 0x7B   # {
_RB = 0x7D   # }
_LK = 0x5B   # [
_RK = 0x5D   # ]
_C = 0x3A    # :
_CM = 0x2C   # ,


def _str_end(body: bytes, i: int, n: int) -> int:
    i += 1
    while i < n:
        if body[i] == _BS:
            i += 2
            continue
        if body[i] == _Q:
            return i + 1
        i += 1
    return i


def _skip_val(body: bytes, i: int, n: int) -> int:
    if i >= n:
        return i
    c = body[i]
    if c == _Q:
        return _str_end(body, i, n)
    if c in (_LB, _LK):
        depth = 1
        in_str = esc = False
        i += 1
        while i < n and depth > 0:
            ch = body[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == _BS:
                    esc = True
                elif ch == _Q:
                    in_str = False
            elif ch == _Q:
                in_str = True
            elif ch in (_LB, _LK):
                depth += 1
            elif ch in (_RB, _RK):
                depth -= 1
            i += 1
        return i
    while i < n and body[i] not in _WS and body[i] not in (_CM, _RB, _RK):
        i += 1
    return i


def patch_field(body: bytes, field: str, new_value: str) -> bytes:
    """Replace a top-level field's value in JSON bytes. Preserves all other bytes."""
    n = len(body)
    i = 0
    while i < n and body[i] in _WS:
        i += 1
    if i >= n or body[i] != _LB:
        return body
    i += 1
    target = msgspec.json.encode(field)
    while i < n:
        while i < n and body[i] in _WS:
            i += 1
        if i >= n or body[i] == _RB:
            break
        if body[i] == _CM:
            i += 1
            continue
        if body[i] != _Q:
            i = _skip_val(body, i, n)
            continue
        ke = _str_end(body, i, n)
        if body[i:ke] == target:
            i = ke
            while i < n and body[i] in _WS:
                i += 1
            if i < n and body[i] == _C:
                i += 1
            while i < n and body[i] in _WS:
                i += 1
            vs = i
            ve = _skip_val(body, i, n)
            return body[:vs] + msgspec.json.encode(new_value) + body[ve:]
        i = ke
        while i < n and body[i] in _WS:
            i += 1
        if i < n and body[i] == _C:
            i += 1
        while i < n and body[i] in _WS:
            i += 1
        i = _skip_val(body, i, n)
    return body


# ─── Extra body merge ────────────────────────────────────────────────

_EXTRA_BODY_RESERVED = frozenset({"model", "stream"})


def merge_extra_body(payload: dict, extra_body: dict) -> None:
    for key, value in extra_body.items():
        if key in _EXTRA_BODY_RESERVED or key in payload:
            continue
        payload[key] = deepcopy(value)


# ─── Disconnect helpers ──────────────────────────────────────────────


async def watch_disconnect(receive, event: asyncio.Event) -> None:
    """Own receive() and set event on http.disconnect."""
    try:
        while True:
            msg = await receive()
            if msg.get("type") == "http.disconnect":
                event.set()
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        event.set()


async def race_disconnect(task: asyncio.Task, disconnect: asyncio.Event) -> bool:
    """Race task against disconnect.wait(). Returns True if disconnect won."""
    disc = asyncio.ensure_future(disconnect.wait())
    await asyncio.wait([task, disc], return_when=asyncio.FIRST_COMPLETED)
    if not disc.done():
        disc.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await disc
    return disconnect.is_set()
