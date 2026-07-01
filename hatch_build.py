from __future__ import annotations
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class CustomHook(BuildHookInterface):
    def initialize(self, version, build_data):
        dashboard = Path(__file__).parent / "proxen" / "dashboard"
        for name in ("app.js", "app.css", "meta.json"):
            path = dashboard / name
            if path.exists():
                build_data["force_include"][str(path)] = f"proxen/dashboard/{name}"
