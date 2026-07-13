"""Fast extraction of `model` and `stream` from a JSON request body.

The proxy needs only these two top-level fields for routing (model) and path
selection (stream).  A full `msgspec.json.decode` of a large conversation
history is the single largest CPU cost the proxy adds - this scanner reads
only the top-level object members and skips nested structures (the `messages`
array) by byte-counting, never parsing them.

Safety contract: on *any* structural uncertainty (body is not a top-level
object, a key is not a string, `model` is not a string, `stream` is not a
bool literal) the function falls back to `msgspec.json.decode`.  Worst case
is therefore identical to the previous behaviour; best case avoids parsing the
body entirely.  The forwarded bytes are never touched - this is read-only
introspection, so it is cache-safe (byte-identical forwarding).
"""
from __future__ import annotations

import msgspec

_WS = b" \t\n\r"
_Q = 0x22   # "
_BS = 0x5C  # backslash
_LB = 0x7B  # {
_RB = 0x7D  # }
_LK = 0x5B  # [
_RK = 0x5D  # ]
_C = 0x3A   # :
_CM = 0x2C  # ,


def _str_end(b: bytes, i: int, n: int) -> int:
    """Index just past the closing quote of the string starting at `i`."""
    i += 1
    while i < n:
        if b[i] == _BS:
            i += 2
            continue
        if b[i] == _Q:
            return i + 1
        i += 1
    return i


def _skip_val(b: bytes, i: int, n: int) -> int:
    """Index just past the JSON value starting at `i` (string, object,
    array, or bare literal).  Nested structures are skipped by depth-counting
    - no parsing, no allocation."""
    if i >= n:
        return i
    c = b[i]
    if c == _Q:
        return _str_end(b, i, n)
    if c in (_LB, _LK):
        depth = 1
        in_str = esc = False
        i += 1
        while i < n and depth > 0:
            ch = b[i]
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
    while i < n and b[i] not in _WS and b[i] not in (_CM, _RB, _RK):
        i += 1
    return i


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
    """Fallback: full decode, mirroring the original throwaway decode."""
    try:
        payload = msgspec.json.decode(body) if body else {}
    except (msgspec.DecodeError, ValueError):
        return "", False
    if not isinstance(payload, dict):
        return "", False
    model = str(payload.get("model", "") or "")
    stream = bool(payload.get("stream", False))
    return model, stream
