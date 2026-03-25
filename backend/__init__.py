"""Application package export for ASGI servers.

This keeps ``from backend import app`` compatibility without eagerly importing
``backend.app`` during package import.
"""

from __future__ import annotations

from typing import Any

__all__ = ["app"]


def __getattr__(name: str) -> Any:
	if name == "app":
		from .app import app as fastapi_app

		return fastapi_app
	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
