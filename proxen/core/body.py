"""Byte-level JSON body scanning, field patching, and extra-body merging.

The proxy needs to peek at `model` and `stream` from a request body
without paying for a full `msgspec.json.decode` of a large conversation
history.  The scanner reads only top-level object members and skips nested
structures (the `messages` array) by byte-counting, never parsing them.

`patch_field` replaces a top-level field's value in-place, preserving all
other bytes.  `merge_extra_body` merges model-config overrides into a
payload dict.

Safety contract: on *any* structural uncertainty the scanner falls back
to `msgspec.json.decode`.  The forwarded bytes are never touched - this
is read-only introspection, so it is cache-safe.
"""
from __future__ import annotations

from copy import deepcopy

import msgspec

# ─── Shared byte constants ───────────────────────────────────────────

_WS = b" \t\n\r"
_Q = 0x22    # "
_BS = 0x5C   # \
_LB = 0x7B   # {
_RB = 0x7D   # }
_LK = 0x5B   # [
_RK = 0x5D   # ]
_C = 0x3A    # :
_CM = 0x2C   # ,


# ─── Byte-level JSON scanners (shared) ──────────────────────────────


def _str_end(body: bytes, i: int, n: int) -> int:
    """Index just past the closing quote of the string starting at `i`."""
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
    """Index just past the JSON value starting at `i` (string, object,
    array, or bare literal).  Nested structures are skipped by depth-counting
    - no parsing, no allocation."""
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


# ─── peek_model_stream ──────────────────────────────────────────────


def peek_model_stream(body: bytes) -> tuple[str, bool]:
    """Return `(model, stream)` from the top level of a JSON body.

    Scans only top-level members; stops as soon as both fields are found so
    the `messages` array (the expensive part) is never entered.  Falls back
    to a full decode on any structural anomaly.
    """
    n = len(body)
    i = 0
    while i < n and body[i] in _WS:
        i += 1
    if i >= n or body[i] != _LB:
        return _full_decode(body)
    i += 1

    model = ""
    stream = False
    found_model = False
    found_stream = False

    while i < n:
        while i < n and body[i] in _WS:
            i += 1
        if i >= n or body[i] == _RB:
            break
        if body[i] == _CM:
            i += 1
            continue
        if body[i] != _Q:
            return _full_decode(body)
        ke = _str_end(body, i, n)
        key_bytes = body[i:ke]

        i = ke
        while i < n and body[i] in _WS:
            i += 1
        if i >= n or body[i] != _C:
            return _full_decode(body)
        i += 1
        while i < n and body[i] in _WS:
            i += 1

        vs = i
        ve = _skip_val(body, i, n)

        if key_bytes == b'"model"' and not found_model:
            if vs >= n or body[vs] != _Q:
                return _full_decode(body)
            se = _str_end(body, vs, n)
            try:
                model = msgspec.json.decode(body[vs:se])
            except Exception:
                return _full_decode(body)
            found_model = True
        elif key_bytes == b'"stream"' and not found_stream:
            token = body[vs:ve]
            if token == b"true":
                stream = True
            elif token == b"false":
                stream = False
            else:
                return _full_decode(body)
            found_stream = True

        i = ve
        if found_model and found_stream:
            break

    return model, stream


def _full_decode(body: bytes) -> tuple[str, bool]:
    """Fallback: full decode."""
    try:
        payload = msgspec.json.decode(body) if body else {}
    except (msgspec.DecodeError, ValueError):
        return "", False
    if not isinstance(payload, dict):
        return "", False
    model = str(payload.get("model", "") or "")
    stream = bool(payload.get("stream", False))
    return model, stream


# ─── patch_field ────────────────────────────────────────────────────


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


# ─── merge_extra_body ──────────────────────────────────────────────

_EXTRA_BODY_RESERVED = frozenset({"model", "stream"})


def merge_extra_body(payload: dict, extra_body: dict) -> None:
    for key, value in extra_body.items():
        if key in _EXTRA_BODY_RESERVED or key in payload:
            continue
        payload[key] = deepcopy(value)
