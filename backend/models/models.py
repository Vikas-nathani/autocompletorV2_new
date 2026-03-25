"""Pydantic models for note completion request validation and responses.

The API validates incoming query parameters and returns a stable typed response
shape for frontend integration.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from backend.core.config import NOTE_API_DEFAULT_ROWS, NOTE_API_MAX_ROWS, VALID_SECTIONS


class NoteCompleteRequest(BaseModel):
    """Validated query parameter model for note completion."""

    q: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Partial word being typed",
    )
    section: str = Field(..., description="Clinical note section")
    rows: int = Field(
        default=NOTE_API_DEFAULT_ROWS,
        ge=1,
        le=NOTE_API_MAX_ROWS,
        description="Number of results to return",
    )
    fuzzy: bool = Field(
        default=True,
        description="Enable spell correction fallback for zero results",
    )
    source: Optional[str] = Field(
        default=None,
        description="Filter to specific source vocabulary",
    )
    tty: Optional[str] = Field(
        default=None,
        description="Comma-separated list of TTY codes to filter",
    )

    @field_validator("section")
    @classmethod
    def validate_section(cls, value: str) -> str:
        if value not in VALID_SECTIONS:
            raise ValueError(
                f"Invalid section '{value}'. Valid sections: {', '.join(VALID_SECTIONS)}"
            )
        return value


class NoteCompleteResult(BaseModel):
    """Single note completion suggestion."""

    term: str
    semantic_type: str
    source: str
    tty: str
    concept_id: str
    code: str
    tty_priority: int
    source_priority: int


class NoteCompleteContextRequest(BaseModel):
    """Body model for context-aware note completion POST requests."""

    q: str
    section: str
    rows: int = NOTE_API_DEFAULT_ROWS
    fuzzy: bool = True
    source: Optional[str] = None
    tty: Optional[str] = None
    patient_context: Optional[str] = None
    patient_context_json: Optional[dict[str, Any]] = None


class NoteCompleteContextResult(NoteCompleteResult):
    """Single context-aware completion suggestion."""

    from_patient_history: bool = False


class NoteCompleteResponse(BaseModel):
    """Structured response payload for section-aware note completion."""

    query: str
    section: str
    semantic_types_applied: list[str]
    spell_corrected: bool
    total: int
    results: list[NoteCompleteResult]
    response_time_ms: float
    solr_hits: int


class NoteCompleteContextResponse(NoteCompleteResponse):
    """Response payload for context-boosted note completion."""

    results: list[NoteCompleteContextResult]
    context_boosted_count: int
