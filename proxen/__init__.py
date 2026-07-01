from __future__ import annotations

from importlib.metadata import version as _pkg_version

from .app import create_app

__version__ = _pkg_version("proxen")

__all__ = ["create_app", "__version__"]
