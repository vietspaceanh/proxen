from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONFIG_DIR = Path.home() / ".config" / "proxen"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "config.toml"
DEFAULT_DB_NAME = "data.db"

_DEFAULT_CONFIG_TOML = """\
# proxen configuration file.
# Generated on first run, edit this file to configure proxen.
# Path: ~/.config/proxen/config.toml

host = "127.0.0.1"
port = 1212

# Database file location. Relative paths resolve to this config directory.
db_path = "data.db"

# Concurrency limits. max_inflight caps total active requests; max_waiting
# caps queued requests (both global and per-provider); queue_timeout is the
# max wait in seconds. Per-key limits can be set in the dashboard Manage tab.
# Set each provider's max_inflight in the dashboard or [[upstreams]].
max_inflight = 5
max_waiting = 50
queue_timeout = 120

# Keys your clients present to proxen. Leave empty to disable auth (dev only).
# Generate one with: openssl rand -hex 24
api_keys = []

# Admin keys for the management API + dashboard Manage tab.
admin_api_keys = []

# How often (seconds) to refresh the upstream model catalog.
model_sync_interval = 3600

# Hard cap on inbound request body size in bytes (default 10 MB).
max_body_bytes = 10485760

# Upstream idle/read timeout: kill a connection only after this many seconds
# with ZERO bytes received. A streaming response (including reasoning tokens)
# resets this continuously and is never cut off. Only genuinely stalled
# connections time out. There is no total cap, so a long-running stream stays
# alive as long as data keeps flowing.
upstream_sock_read = 90

# Time-to-first-token timeout for streaming requests (seconds). If the
# upstream accepts the connection (200 OK) but sends no data within this
# window, the route is abandoned and the next fallback route is tried. This
# prevents a slow-but-alive upstream from monopolising traffic while faster
# fallbacks are available. Set to 0 to disable.
upstream_ttft_timeout = 60

# Trusted reverse proxy IPs for X-Forwarded-For / X-Forwarded-Proto handling.
# Set to the IP(s) of your reverse proxy (nginx, Caddy, etc.).
# Use "*" to trust all (not recommended for internet-facing deployments).
# Leave at "127.0.0.1" if running without a reverse proxy.
trusted_hosts = "127.0.0.1"

# Admin API rate limit: max requests per IP per window.
admin_rate_limit = 100
admin_rate_limit_window = 60

# Health guard: weighted failures before a route is marked "failing" and skipped
# (fallback routes are tried instead).  Connection errors and TTFT timeouts count
# as weight 2; 5xx as weight 1.  After tripping, probes use exponential backoff
# (retry_delay x2 each time). Set failures to 0 to disable.
health_guard_failures = 5
health_guard_retry_delay = 5

# Upstream providers (OpenAI-compatible).
# Uncomment and edit to configure:
#
# [[upstreams]]
# name = "openai"
# base_url = "https://api.openai.com/v1"
# api_key = "sk-..."
# enabled = true
"""


class SecretStr:
    """Lightweight wrapper to avoid accidental logging of secrets."""
    __slots__ = ("_value",)

    def __init__(self, value: str | "SecretStr") -> None:
        self._value = value._value if isinstance(value, SecretStr) else value

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "**********"

    def __str__(self) -> str:
        return "**********"


def _bootstrap_config_dir() -> Path:
    """Create ~/.config/proxen/ with a default config.toml if missing."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    config_path = CONFIG_DIR / "config.toml"
    if not config_path.exists():
        config_path.write_text(_DEFAULT_CONFIG_TOML)
        try:
            os.chmod(config_path, 0o600)
        except OSError:
            pass
    return config_path


@dataclass
class Upstream:
    """A single upstream OpenAI-compatible provider."""

    name: str
    base_url: str = "https://api.openai.com/v1"
    api_key: SecretStr = field(default_factory=lambda: SecretStr(""))
    enabled: bool = True
    max_inflight: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.api_key, str):
            self.api_key = SecretStr(self.api_key)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "api_key": self.api_key.get_secret_value(),
            "enabled": self.enabled,
            "max_inflight": self.max_inflight,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Upstream":
        return cls(
            name=d["name"],
            base_url=d.get("base_url", "https://api.openai.com/v1"),
            api_key=SecretStr(d.get("api_key", "")),
            enabled=d.get("enabled", True),
            max_inflight=d.get("max_inflight"),
        )


@dataclass
class Pricing:
    """Per-million-token pricing for a model (USD)."""

    input_per_1m: float = 0.0
    cached_input_per_1m: float = 0.0
    output_per_1m: float = 0.0


@dataclass
class ProxenModel:
    """A model exposed to proxen clients."""

    id: str = ""
    enabled: bool = True
    input_per_1m: float = 0.0
    cached_input_per_1m: float = 0.0
    output_per_1m: float = 0.0
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    fallback_strategy: str = "manual"
    extra_body: dict | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "enabled": self.enabled,
            "input_per_1m": self.input_per_1m,
            "cached_input_per_1m": self.cached_input_per_1m,
            "output_per_1m": self.output_per_1m,
            "max_input_tokens": self.max_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "fallback_strategy": self.fallback_strategy,
            "extra_body": self.extra_body,
        }


@dataclass
class ModelRoute:
    """One upstream mapping for a proxen model."""

    upstream_name: str
    upstream_model_id: str
    sort_order: int = 0
    enabled: bool = True

    def to_dict(self) -> dict:
        return {
            "upstream_name": self.upstream_name,
            "upstream_model_id": self.upstream_model_id,
            "sort_order": self.sort_order,
            "enabled": self.enabled,
        }


@dataclass
class Settings:
    host: str = "127.0.0.1"
    port: int = 1212
    api_keys: list[str] = field(default_factory=list)
    admin_api_keys: list[str] = field(default_factory=list)
    upstreams: list[Upstream] = field(default_factory=list)
    max_inflight: int = 5
    max_waiting: int = 50
    queue_timeout: float = 120.0
    model_sync_interval: float = 3600.0
    db_path: str = DEFAULT_DB_NAME
    max_body_bytes: int = 10 * 1024 * 1024
    upstream_sock_read: float = 90.0
    upstream_ttft_timeout: float = 60.0
    upstream_non_streaming_timeout: float = 300.0
    trusted_hosts: str = "127.0.0.1"
    admin_rate_limit: int = 100
    admin_rate_limit_window: float = 60.0
    health_guard_failures: int = 5
    health_guard_retry_delay: float = 5.0
    pricing: dict[str, Pricing] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "api_keys": list(self.api_keys),
            "admin_api_keys": list(self.admin_api_keys),
            "upstreams": [u.to_dict() for u in self.upstreams],
            "max_inflight": self.max_inflight,
            "max_waiting": self.max_waiting,
            "queue_timeout": self.queue_timeout,
            "model_sync_interval": self.model_sync_interval,
            "db_path": self.db_path,
            "max_body_bytes": self.max_body_bytes,
            "upstream_sock_read": self.upstream_sock_read,
            "upstream_ttft_timeout": self.upstream_ttft_timeout,
            "upstream_non_streaming_timeout": self.upstream_non_streaming_timeout,
            "trusted_hosts": self.trusted_hosts,
            "admin_rate_limit": self.admin_rate_limit,
            "admin_rate_limit_window": self.admin_rate_limit_window,
            "health_guard_failures": self.health_guard_failures,
            "health_guard_retry_delay": self.health_guard_retry_delay,
            "pricing": {k: v.__dict__ for k, v in self.pricing.items()},
        }

    def copy(self, **overrides: Any) -> "Settings":
        d = self.to_dict()
        d.update(overrides)
        return _build_settings(d)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _env_to_dict() -> dict[str, Any]:
    """Convert `PROXEN_FOO__BAR=baz` into `{"foo": {"bar": baz}}`."""
    result: dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith("PROXEN_"):
            continue
        parts = key[len("PROXEN_"):].lower().split("__")
        node = result
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        leaf = parts[-1]
        if leaf in ("api_keys", "admin_api_keys") and "," in value:
            node[leaf] = [v.strip() for v in value.split(",") if v.strip()]
        else:
            node[leaf] = value
    return result


def _read_config_file(path: Path) -> dict[str, Any]:
    if path.suffix == ".json":
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _build_settings(data: dict[str, Any]) -> Settings:
    """Construct Settings from a merged dict, coercing types."""
    upstreams_data = data.pop("upstreams", [])
    upstreams = [Upstream.from_dict(u) for u in upstreams_data]

    pricing_data = data.pop("pricing", {})
    pricing = {k: Pricing(**v) for k, v in pricing_data.items()}

    return Settings(
        host=str(data.get("host", "127.0.0.1")),
        port=int(data.get("port", 1212)),
        api_keys=list(data.get("api_keys", [])),
        admin_api_keys=list(data.get("admin_api_keys", [])),
        upstreams=upstreams,
        max_inflight=int(data.get("max_inflight", 5)),
        max_waiting=int(data.get("max_waiting", 50)),
        queue_timeout=float(data.get("queue_timeout", 120.0)),
        model_sync_interval=float(data.get("model_sync_interval", 3600.0)),
        db_path=str(data.get("db_path", DEFAULT_DB_NAME)),
        max_body_bytes=int(data.get("max_body_bytes", 10 * 1024 * 1024)),
        upstream_sock_read=float(data.get("upstream_sock_read", 90.0)),
        upstream_ttft_timeout=float(data.get("upstream_ttft_timeout", 60.0)),
        upstream_non_streaming_timeout=float(data.get("upstream_non_streaming_timeout", 300.0)),
        trusted_hosts=str(data.get("trusted_hosts", "127.0.0.1")),
        admin_rate_limit=int(data.get("admin_rate_limit", 100)),
        admin_rate_limit_window=float(data.get("admin_rate_limit_window", 60.0)),
        health_guard_failures=int(data.get("health_guard_failures", 5)),
        health_guard_retry_delay=float(data.get("health_guard_retry_delay", 5.0)),
        pricing=pricing,
    )


def load_settings(config_path: Path | None = None) -> Settings:
    """Load settings from a TOML/JSON file (if any) with env-var overrides on top.

    Resolution order for the file:
      1. Explicit `config_path` argument
      2. `PROXEN_CONFIG` environment variable
      3. `~/.config/proxen/config.toml` (auto-created on first run)

    The `db_path` is resolved relative to the config directory when it is
    a relative path, so the database lives alongside the config file by
    default.
    """
    data: dict[str, Any] = {}
    path = config_path
    if path is None:
        env_config = os.environ.get("PROXEN_CONFIG")
        if env_config:
            path = Path(env_config)
    if path is None:
        path = _bootstrap_config_dir()

    config_dir = Path(path).parent if path else CONFIG_DIR

    if path is not None and Path(path).exists():
        data = _read_config_file(Path(path))
    data = _deep_merge(data, _env_to_dict())
    settings = _build_settings(data)

    # Resolve relative db_path against the config directory.
    if not Path(settings.db_path).is_absolute():
        settings.db_path = str(config_dir / settings.db_path)
    return settings
