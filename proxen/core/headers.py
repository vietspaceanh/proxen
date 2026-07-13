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
