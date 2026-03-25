"""Runtime configuration for the note completion API.

This module centralizes environment-driven settings so deployments can tune
behavior without code changes.
"""

from __future__ import annotations

import os


def _get_env_int(name: str, default: int) -> int:
    """Read a positive integer from env with a safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value


def _get_env_list(name: str, default: list[str]) -> list[str]:
    """Read a comma-separated list from env with trimming and deduplication."""
    raw = os.getenv(name)
    if raw is None:
        return list(default)

    values = []
    seen = set()
    for part in raw.split(","):
        item = part.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        values.append(item)

    return values or list(default)


_DEFAULT_VALID_SECTIONS = [
    "chief_complaint",
    "diagnosis",
    "investigations",
    "medications",
    "procedures",
    "advice",
]

SOLR_URL = os.getenv("SOLR_URL", "http://localhost:8983/solr/umls_core")
NOTE_API_DEFAULT_ROWS = _get_env_int("NOTE_API_DEFAULT_ROWS", 15)
NOTE_API_MAX_ROWS = _get_env_int("NOTE_API_MAX_ROWS", 50)
NOTE_API_VERSION = os.getenv("NOTE_API_VERSION", "1.0.0")
VALID_SECTIONS = _get_env_list("NOTE_API_VALID_SECTIONS", _DEFAULT_VALID_SECTIONS)
