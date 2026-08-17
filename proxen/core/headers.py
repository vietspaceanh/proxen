"""Header filtering and protocol detection for the proxy."""
from __future__ import annotations

_HOP_BY_HOP = {
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "authorization",
    "x-api-key",
    "accept-encoding",
}

_RESP_STRIP = _HOP_BY_HOP | {"content-encoding"}

# Reserved template names resolved from request context, checked before
# falling back to a request-header lookup.
_TEMPLATE_CONTEXT = frozenset({"model", "key", "path", "query", "stream", "protocol"})
_TEMPLATE_PREFIX = "$"


def protocol_from_path(path) -> str:
    if isinstance(path, (bytes, bytearray)):
        path = path.decode("utf-8", "replace")
    if path == "/v1/responses":
        return "responses"
    return "anthropic" if path.startswith("/v1/messages") else "openai"


def resolve_extra_headers(
    extra_headers: dict | None,
    src=(),
    template_values: dict | None = None,
) -> dict:
    """Resolve `$<name>` templates in an extra-headers dict.

    Reserved context names (`model`, `key`, `path`, `query`, `stream`,
    `protocol`) are resolved from `template_values` first, then any other
    name is looked up in the request headers `src` (case-insensitive).
    Templates that cannot be resolved are omitted; static values pass
    through unchanged.
    """
    if not extra_headers:
        return {}
    lookup: dict[str, str] = {}
    pairs = src.items() if hasattr(src, "items") else src
    for key, value in pairs:
        k = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        v = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
        lookup[k.lower()] = v
    values = dict(template_values or {})
    out: dict[str, str] = {}
    for name, value in extra_headers.items():
        if isinstance(value, str) and value.startswith(_TEMPLATE_PREFIX):
            tname = value[len(_TEMPLATE_PREFIX):].strip()
            if not tname:
                continue
            if tname in values:
                out[name] = str(values[tname])
            elif tname.lower() in lookup:
                out[name] = lookup[tname.lower()]
            continue
        out[name] = value
    return out


def filter_headers(
    src, provider_key: str | None = None, protocol: str = "openai",
    extra_headers: dict | None = None, template_values: dict | None = None,
) -> dict[str, str]:
    """Filter headers for forwarding.

    `extra_headers` are merged last, so configured upstream headers
    override client-sent headers with the same name (including the
    injected auth header).  `$<name>` values in `extra_headers` are
    resolved from the request (see `resolve_extra_headers`).
    """
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
    if extra_headers:
        out.update(resolve_extra_headers(extra_headers, src, template_values))
    return out
