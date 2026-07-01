from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import msgspec
import uvicorn

from .. import __version__
from ..core.config import CONFIG_DIR, SecretStr, Upstream, load_settings


def _auto_config_path() -> str | None:
    """Return the default config path (~/.config/proxen/config.toml)."""
    path = CONFIG_DIR / "config.toml"
    return str(path.resolve()) if path.exists() else None


def _dump_settings_json(settings) -> str:
    data = settings.to_dict()
    return msgspec.json.format(msgspec.json.encode(data), indent=2).decode()


# ─── Server ──────────────────────────────────────────────────────────


def _run_server(args, settings) -> None:
    overrides: dict = {}
    if args.host is not None:
        overrides["host"] = args.host
    if args.port is not None:
        overrides["port"] = args.port
    if args.max_inflight is not None:
        overrides["max_inflight"] = args.max_inflight
    if args.max_waiting is not None:
        overrides["max_waiting"] = args.max_waiting
    if args.queue_timeout is not None:
        overrides["queue_timeout"] = args.queue_timeout
    if args.db_path is not None:
        overrides["db_path"] = str(Path(args.db_path).resolve())
    if args.keys is not None:
        overrides["api_keys"] = args.keys

    if args.upstream is not None or args.upstream_key is not None:
        upstreams = list(settings.upstreams)
        if not upstreams:
            upstreams = [
                Upstream(
                    name="default",
                    base_url=args.upstream or "https://api.openai.com/v1",
                    api_key=SecretStr(args.upstream_key or ""),
                )
            ]
        else:
            first = upstreams[0]
            upstreams[0] = Upstream(
                name=first.name,
                base_url=args.upstream or first.base_url,
                api_key=SecretStr(args.upstream_key or first.api_key.get_secret_value()),
                enabled=first.enabled,
                max_inflight=first.max_inflight,
            )
        overrides["upstreams"] = upstreams

    if overrides:
        settings = settings.copy(**overrides)

    fd, config_path = tempfile.mkstemp(suffix=".json", prefix="proxen-config-")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(_dump_settings_json(settings))
        # mkstemp is already 0600 on POSIX; chmod explicitly to be safe, this
        # file holds plaintext API keys.
        os.chmod(config_path, 0o600)
        os.environ["PROXEN_CONFIG"] = config_path

        uvicorn.run(
            "proxen.app:create_app",
            host=settings.host,
            port=settings.port,
            factory=True,
            loop="uvloop",
            log_level="info",
            timeout_graceful_shutdown=5,
        )
    finally:
        try:
            os.unlink(config_path)
        except OSError:
            pass


# ─── systemd user service ───────────────────────────────────────────

SERVICE_NAME = "proxen.service"
SERVICE_DIR = Path.home() / ".config" / "systemd" / "user"


def _systemctl(*args: str) -> tuple[int, str, str]:
    """Run a systemctl --user command and return (returncode, stdout, stderr)."""
    cmd = ["systemctl", "--user", *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def _detect_exec_start() -> str:
    """Build the ExecStart line for the service file."""
    bindir = Path(sys.executable).parent
    proxen_bin = bindir / "proxen"
    if proxen_bin.is_file():
        return str(proxen_bin)

    return f"{sys.executable} -m proxen"


def _service_file_content(config_path: str | None, host: str | None, port: int | None) -> str:
    exec_start = _detect_exec_start()
    parts = [exec_start]
    if config_path:
        parts.append(f"--config {config_path}")
    if host:
        parts.append(f"--host {host}")
    if port:
        parts.append(f"--port {port}")

    return f"""[Unit]
Description=Proxen LLM API Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={' '.join(parts)}
WorkingDirectory={Path.cwd()}
Restart=always
RestartSec=2
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
"""


def _service_install(args) -> None:
    config_path = str(args.config) if args.config else os.environ.get("PROXEN_CONFIG") or _auto_config_path()

    settings = load_settings(args.config if args.config else None)
    host = args.host or settings.host
    port = args.port or settings.port

    SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    service_file = SERVICE_DIR / SERVICE_NAME

    content = _service_file_content(config_path, host, port)
    service_file.write_text(content)
    print(f"Written service file to {service_file}")

    # Reload systemd.
    rc, _, err = _systemctl("daemon-reload")
    if rc != 0:
        print(f"Warning: daemon-reload failed: {err}")

    # Enable linger so the service survives logout.
    user = os.environ.get("USER", "unknown")
    linger_rc = subprocess.run(
        ["loginctl", "enable-linger", user], capture_output=True, text=True
    )
    if linger_rc.returncode != 0:
        print(f"Warning: could not enable linger: {linger_rc.stderr.strip()}")

    # Enable + start.
    _systemctl("enable", SERVICE_NAME)
    rc, _, err = _systemctl("start", SERVICE_NAME)
    if rc != 0:
        print(f"Error starting service: {err}")
    else:
        print("Service installed and started.")

    _service_status(args)


def _service_uninstall(args) -> None:
    _systemctl("stop", SERVICE_NAME)
    _systemctl("disable", SERVICE_NAME)
    service_file = SERVICE_DIR / SERVICE_NAME
    if service_file.exists():
        service_file.unlink()
        print(f"Removed {service_file}")
    _systemctl("daemon-reload")
    print("Service uninstalled.")


def _service_start(args) -> None:
    rc, _, err = _systemctl("start", SERVICE_NAME)
    if rc != 0:
        print(f"Error: {err}")
    else:
        print("Service started.")
    _service_status(args)


def _service_stop(args) -> None:
    rc, _, err = _systemctl("stop", SERVICE_NAME)
    if rc != 0:
        print(f"Error: {err}")
    else:
        print("Service stopped.")


def _service_restart(args) -> None:
    rc, _, err = _systemctl("restart", SERVICE_NAME)
    if rc != 0:
        print(f"Error: {err}")
    else:
        print("Service restarted.")
    _service_status(args)


def _service_status(args) -> None:
    rc, out, err = _systemctl("status", SERVICE_NAME)
    # systemctl status returns non-zero if service is not running, that's OK.
    if out:
        print(out)
    elif err:
        print(err)
    else:
        print("No status available.")


# ─── CLI ────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(prog="proxen", description="LLM API proxy")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    # Proxy options, running the server is the default action.
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--upstream", default=None, help="Upstream base URL (quick config)")
    parser.add_argument("--upstream-api-key", default=None, dest="upstream_key")
    parser.add_argument(
        "--proxen-api-key",
        default=None,
        dest="keys",
        action="append",
        help="API key clients must present to proxen (repeatable)",
    )
    parser.add_argument("--max-inflight", type=int, default=None)
    parser.add_argument("--max-waiting", type=int, default=None)
    parser.add_argument("--queue-timeout", type=float, default=None)
    parser.add_argument("--db-path", default=None)

    # Only subcommand: systemd service management.
    sub = parser.add_subparsers(dest="command")
    svc_p = sub.add_parser("service", help="Manage systemd user service")
    svc_sub = svc_p.add_subparsers(dest="service_command", required=True)

    svc_install = svc_sub.add_parser("install", help="Install and start as systemd user service")
    svc_install.add_argument("--config", type=Path, default=None)
    svc_install.add_argument("--host", default=None)
    svc_install.add_argument("--port", type=int, default=None)

    for cmd_name, cmd_help in [
        ("uninstall", "Stop and remove the systemd user service"),
        ("start", "Start the systemd user service"),
        ("stop", "Stop the systemd user service"),
        ("restart", "Restart the systemd user service"),
        ("status", "Show service status"),
    ]:
        svc_sub.add_parser(cmd_name, help=cmd_help)

    args = parser.parse_args()

    if args.command == "service":
        if args.service_command == "install":
            _service_install(args)
        elif args.service_command == "uninstall":
            _service_uninstall(args)
        elif args.service_command == "start":
            _service_start(args)
        elif args.service_command == "stop":
            _service_stop(args)
        elif args.service_command == "restart":
            _service_restart(args)
        elif args.service_command == "status":
            _service_status(args)
    else:
        settings = load_settings(args.config)
        _run_server(args, settings)


if __name__ == "__main__":
    main()
