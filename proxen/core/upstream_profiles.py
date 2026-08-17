"""Small built-in upstream profiles.

Profiles only describe wire-level differences. Authentication is handled by
the upstream auth service so the proxy routing flow stays shared.
"""
from __future__ import annotations

from re import compile
from urllib.parse import urlencode, urlsplit

import msgspec

from .body import rewrite_top_level_object, top_level_value


OPENAI_RESPONSES_PROFILE = "openai-responses"
COMPATIBLE_PROFILE = "compatible"
OPENAI_RESPONSES_ENDPOINT = "https://chatgpt.com/backend-api/codex/responses"
OPENAI_RESPONSES_MODELS_ENDPOINT = "https://chatgpt.com/backend-api/codex/models"
# The ChatGPT catalog gates models by Codex client version, not the Proxen
# package version. Keep this aligned with the current Codex release.
CODEX_CLIENT_VERSION = "0.147.0"
# The Codex backend rejects requests without instructions outright.
RESPONSES_FALLBACK_INSTRUCTIONS = "You are a helpful assistant."

_VERSION_RE = compile(r"/v\d+$")


def is_responses_profile(profile: str) -> bool:
    return profile == OPENAI_RESPONSES_PROFILE


def profile_accepts_path(profile: str, path: str) -> bool:
    if is_responses_profile(profile):
        return path == "/v1/responses"
    return True


def profile_models_url(profile: str) -> str | None:
    if not is_responses_profile(profile):
        return None
    return f"{OPENAI_RESPONSES_MODELS_ENDPOINT}?{urlencode({'client_version': CODEX_CLIENT_VERSION})}"


def normalize_profile_models(profile: str, data: dict) -> list[dict]:
    """Convert a profile catalog into Proxen's cached model shape."""
    if not isinstance(data, dict):
        raise ValueError("model catalog response must be an object")
    key = "models" if is_responses_profile(profile) else "data"
    models = data.get(key)
    if not isinstance(models, list):
        raise ValueError(f"model catalog response is missing a {key} list")
    if not is_responses_profile(profile):
        return models

    normalized = []
    for model in models:
        if not isinstance(model, dict):
            continue
        if model.get("supported_in_api") is False:
            continue
        if model.get("visibility") not in (None, "list"):
            continue
        slug = model.get("slug")
        if not slug:
            continue
        normalized.append({
            **model,
            "id": str(slug),
            "object": "model",
            "owned_by": "openai",
        })
    return normalized


def prepare_responses_body(body: bytes) -> bytes:
    """Adapt a Responses request for the ChatGPT Codex backend.

    The backend requires `store: false` and non-empty `instructions`, and
    rejects output-token limits, which public-API clients may send.
    Malformed or non-object bodies pass through untouched so the upstream
    rejection surfaces unchanged.
    """
    if not body:
        return body
    if not body.lstrip().startswith(b"{"):
        return body
    replacements = {}
    additions = {}
    store = top_level_value(body, "store")
    if store is None:
        additions["store"] = b"false"
    else:
        try:
            if msgspec.json.decode(store) is not False:
                replacements["store"] = b"false"
        except (msgspec.DecodeError, ValueError):
            return body
    instructions = top_level_value(body, "instructions")
    if instructions is None:
        additions["instructions"] = msgspec.json.encode(RESPONSES_FALLBACK_INSTRUCTIONS)
    else:
        try:
            if not msgspec.json.decode(instructions):
                replacements["instructions"] = msgspec.json.encode(RESPONSES_FALLBACK_INSTRUCTIONS)
        except (msgspec.DecodeError, ValueError):
            return body
    return rewrite_top_level_object(
        body,
        replacements=replacements,
        remove={"max_output_tokens", "max_tokens"},
        additions=additions,
    )


def upstream_url(upstream, path: str, query: str) -> str:
    if is_responses_profile(upstream.profile):
        url = OPENAI_RESPONSES_ENDPOINT
    else:
        base = upstream.base_url.rstrip("/")
        if _VERSION_RE.search(base) and path.startswith("/v1/"):
            path = path[3:]
        url = base + path
    if query:
        url += "?" + query
    return url


def validate_profile_url(profile: str, base_url: str) -> None:
    """Validate URLs used by configurable compatible profiles."""
    if profile not in {COMPATIBLE_PROFILE, OPENAI_RESPONSES_PROFILE}:
        raise ValueError(f"unsupported upstream profile: {profile}")
    if is_responses_profile(profile):
        return
    parsed = urlsplit(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(
            f"invalid base_url: {base_url!r} (must be an http(s) URL)"
        )
