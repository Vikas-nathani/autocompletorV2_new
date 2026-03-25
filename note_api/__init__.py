"""Public package exports for the note completion API module.

This package exposes the FastAPI router used to register note completion
endpoints in the main application.
"""

from .router import router

__all__ = ["router"]
