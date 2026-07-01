"""Frontend build + dev runner.

Usage:
  make dev      # or: uv run main.py            vite (:1313, HMR) + proxen (:1212, .py auto-restart)
  make build    # or: uv run main.py build      generate minified build into proxen/dashboard/
  make publish  # or: uv run main.py publish    build + uv build + uv publish

Vite dev server on :1313 proxies API/WebSocket to proxen on :1212.
A single `proxen` process (production mode) is restarted when `proxen/**/*.py`
change (via watchfiles, the same engine uvicorn's --reload uses). Children run
in their own session and are killed via the process group; PR_SET_PDEATHSIG
additionally ensures they die if this process is killed abnormally (SIGKILL/OOM).
"""
from __future__ import annotations

import argparse
import atexit
import gzip
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from watchfiles import watch

ROOT = Path(__file__).resolve().parent
DASHBOARD = ROOT / "proxen" / "dashboard"
NODE_BIN = ROOT / "node_modules" / ".bin"
PROXEN_PKG = ROOT / "proxen"

JS_OUT = DASHBOARD / "app.js"
CSS_OUT = DASHBOARD / "app.css"
RELOAD_DEBOUNCE = 0.25
_PDEATH_WRAPPER = (
    "import os,sys\n"
    "try:\n"
    " import ctypes,ctypes.util\n"
    " ctypes.CDLL(ctypes.util.find_library('c') or 'libc.so.6').prctl(1,9)\n"
    "except Exception:\n"
    " pass\n"
    "os.execvp(sys.argv[1],sys.argv[1:])\n"
)


# ─── node / vite helpers ───────────────────────────────────────────


def _ensure_node_deps() -> None:
    """Run `npm install` if the frontend toolchain isn't present."""
    if (NODE_BIN / "vite").exists():
        return
    if shutil.which("npm") is None:
        sys.exit("error: npm not found on PATH — install Node.js first.")
    print("[npm] install (node_modules missing)", flush=True)
    subprocess.run(["npm", "install"], cwd=ROOT, check=True)


def _bin(name: str) -> str:
    local = NODE_BIN / name
    if not local.exists():
        sys.exit(f"error: '{name}' not found at {local}\n       run `npm install` first.")
    return str(local)


def _vite_cmd(*, watch: bool) -> list[str]:
    cmd = [_bin("vite")]
    if not watch:
        cmd.append("build")
    return cmd


# ─── size reporting ────────────────────────────────────────────────


def _gzip_size(path: Path) -> int:
    return len(gzip.compress(path.read_bytes()))


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}MB"


# ─── production build ──────────────────────────────────────────────


def build_prod() -> None:
    DASHBOARD.mkdir(parents=True, exist_ok=True)
    _ensure_node_deps()
    vite_cmd = _vite_cmd(watch=False)
    print(f"[vite] {' '.join(vite_cmd)}", flush=True)
    subprocess.run(vite_cmd, cwd=ROOT, check=True)
    print("\n=== production build ===")
    for f in (JS_OUT, CSS_OUT):
        if not f.exists():
            print(f"  {str(f.relative_to(ROOT)):40} (missing)")
            continue
        raw = f.stat().st_size
        print(f"  {str(f.relative_to(ROOT)):40} {_human(raw):>9}  (gzip {_human(_gzip_size(f))})")


# ─── dev supervisor ───────────────────────────────────────────────


class DevSession:
    """Run vite + proxen, restarting proxen on `proxen/**/*.py` changes."""

    def __init__(self) -> None:
        self._procs: list[subprocess.Popen] = []
        self._proxen: subprocess.Popen | None = None
        self._lock = threading.Lock()
        self._last_restart = 0.0

    def run(self) -> None:
        DASHBOARD.mkdir(parents=True, exist_ok=True)
        _ensure_node_deps()
        self._procs.append(self._spawn(_vite_cmd(watch=True), "vite"))
        self._restart_proxen()
        threading.Thread(target=self._watch_py, daemon=True).start()
        atexit.register(self._kill_all)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._on_signal)
        self._poll()

    def _spawn(self, cmd: list[str], label: str) -> subprocess.Popen:
        full = [sys.executable, "-c", _PDEATH_WRAPPER, *cmd]
        print(f"[{label}] {' '.join(cmd)}", flush=True)
        return subprocess.Popen(full, cwd=ROOT, start_new_session=True)

    @staticmethod
    def _kill_one(p: subprocess.Popen) -> None:
        if p.poll() is None:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass

    def _kill_all(self) -> None:
        for p in self._procs:
            self._kill_one(p)
        if self._proxen is not None:
            self._kill_one(self._proxen)

    def _restart_proxen(self) -> bool:
        with self._lock:
            now = time.monotonic()
            if now - self._last_restart < RELOAD_DEBOUNCE:
                return False
            self._last_restart = now
            if self._proxen is not None:
                self._kill_one(self._proxen)
            self._proxen = self._spawn([sys.executable, "-m", "proxen"], "proxen")
            return True

    def _watch_py(self) -> None:
        for _changes in watch(PROXEN_PKG, watch_filter=lambda _c, path: path.endswith(".py")):
            if self._restart_proxen():
                print("[watchfiles] .py change, restarted proxen", flush=True)

    def _on_signal(self, *_: object) -> None:
        self._kill_all()
        sys.exit(0)

    def _poll(self) -> None:
        while True:
            for p in list(self._procs):
                if p.poll() is not None:
                    print(f"[vite] exited ({p.returncode}), shutting down", flush=True)
                    self._kill_all()
                    sys.exit(p.returncode)
            cur = self._proxen
            if cur is not None and cur.poll() is not None:
                print(f"[proxen] exited ({cur.returncode}), shutting down", flush=True)
                self._kill_all()
                sys.exit(cur.returncode)
            time.sleep(0.2)


# ─── publish ──────────────────────────────────────────────────────


def publish() -> None:
    build_prod()
    print("\n[uv] build", flush=True)
    subprocess.run(["uv", "build"], cwd=ROOT, check=True)
    print("[uv] publish", flush=True)
    subprocess.run(["uv", "publish"], cwd=ROOT, check=True)
    print("\ndone.")


# ─── entrypoint ────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(prog="main.py", description="Frontend build/dev runner.")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("build", help="generate minified production build (no server)")
    sub.add_parser("dev", help="watch + proxen with auto-restart (default)")
    sub.add_parser("publish", help="build + uv build + uv publish")
    args = parser.parse_args()
    if args.cmd == "build":
        build_prod()
    elif args.cmd == "publish":
        publish()
    else:
        DevSession().run()


if __name__ == "__main__":
    main()
