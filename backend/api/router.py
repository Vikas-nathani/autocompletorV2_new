"""FastAPI router for section-aware note completion endpoints.

The router exposes a production-facing API for section-filtered term
suggestions and a discovery endpoint for available sections.
"""

from __future__ import annotations

from datetime import date
import importlib
import json
import time

import httpx
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import ValidationError


class _LegacyAppProxy:
    def __getattr__(self, name: str):
        module = importlib.import_module("backend.app")
        return getattr(module, name)


legacy_app = _LegacyAppProxy()

from backend.core.config import NOTE_API_DEFAULT_ROWS, NOTE_API_MAX_ROWS, VALID_SECTIONS
from backend.services.context_parser import find_matching_context_terms, parse_patient_context, parse_patient_context_json
from backend.models.models import (
    NoteCompleteContextRequest,
    NoteCompleteContextResult,
    NoteCompleteContextResponse,
    NoteCompleteRequest,
    NoteCompleteResponse,
    NoteCompleteResult,
)
from backend.services.search import note_complete
from backend.services.section_config import SECTION_SEMANTIC_TYPES

router = APIRouter(prefix="/api/note", tags=["Note Completion"])


def _merge_context_and_umls_results(
    docs: list[dict],
    context_matches: list[dict],
    rows: int,
) -> tuple[list[NoteCompleteContextResult], int]:
    umls_results = [
        {
            "term": str(legacy_app._get_scalar(doc, "term", "")),
            "semantic_type": str(legacy_app._get_scalar(doc, "semantic_type", "")),
            "source": str(legacy_app._get_scalar(doc, "source", "")),
            "tty": str(legacy_app._get_scalar(doc, "tty", "")),
            "concept_id": str(legacy_app._get_scalar(doc, "concept_id", "")),
            "code": str(legacy_app._get_scalar(doc, "code", "")),
            "tty_priority": legacy_app._tty_priority_value(doc),
            "source_priority": legacy_app._source_priority_value(doc),
            "from_patient_history": False,
        }
        for doc in docs
    ]

    by_term_lower = {
        item["term"].strip().lower(): item
        for item in umls_results
        if item["term"].strip()
    }

    boosted_results = []
    seen_boosted = set()
    for match in context_matches:
        term = str(match.get("term", "")).strip()
        term_lower = term.lower()
        if not term or term_lower in seen_boosted:
            continue
        seen_boosted.add(term_lower)

        existing = by_term_lower.get(term_lower)
        if existing is not None:
            boosted = dict(existing)
            boosted["from_patient_history"] = True
            boosted_results.append(boosted)
            continue

        boosted_results.append(
            {
                "term": term,
                "semantic_type": "Patient Context",
                "source": "PATIENT_HISTORY",
                "tty": "",
                "concept_id": "",
                "code": "",
                "tty_priority": 0,
                "source_priority": 0,
                "from_patient_history": True,
            }
        )

    boosted_term_lowers = {item["term"].strip().lower() for item in boosted_results}
    remaining_umls = [
        item for item in umls_results if item["term"].strip().lower() not in boosted_term_lowers
    ]
    merged = (boosted_results + remaining_umls)[: rows]
    results = [NoteCompleteContextResult(**item) for item in merged]
    return results, len(boosted_results)


@router.get(
    "/complete",
    response_model=NoteCompleteResponse,
    summary="Section-aware medical term completion for clinical notes",
    description=(
        "Returns ranked UMLS term suggestions filtered to the semantic types "
        "relevant for the specified clinical note section."
    ),
)
async def note_complete_endpoint(
    q: str = Query(..., description="Partial word being typed"),
    section: str = Query(..., description="Clinical note section"),
    rows: int = Query(NOTE_API_DEFAULT_ROWS, description="Number of results to return"),
    fuzzy: bool = Query(True, description="Enable spell correction fallback for zero results"),
    source: str | None = Query(None, description="Filter to specific source vocabulary"),
    tty: str | None = Query(None, description="Comma-separated list of TTY codes to filter"),
):
    start_ts = time.perf_counter()

    if section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section '{section}'. Valid sections: {', '.join(VALID_SECTIONS)}",
        )

    try:
        validated = NoteCompleteRequest(
            q=q,
            section=section,
            rows=rows,
            fuzzy=fuzzy,
            source=source,
            tty=tty,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail={"message": "Invalid request parameters", "errors": exc.errors()})

    try:
        docs, solr_hits, spell_corrected = await note_complete(
            q=validated.q,
            section=validated.section,
            rows=validated.rows,
            fuzzy=validated.fuzzy,
            source=validated.source,
            tty=validated.tty,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Solr service unavailable: {exc}")
    except Exception as exc:  # pragma: no cover - safety net for API resilience
        return JSONResponse(
            status_code=503,
            content={
                "error": "note_completion_unavailable",
                "message": "Failed to complete note suggestions",
                "detail": str(exc),
            },
        )

    results = [
        NoteCompleteResult(
            term=str(legacy_app._get_scalar(doc, "term", "")),
            semantic_type=str(legacy_app._get_scalar(doc, "semantic_type", "")),
            source=str(legacy_app._get_scalar(doc, "source", "")),
            tty=str(legacy_app._get_scalar(doc, "tty", "")),
            concept_id=str(legacy_app._get_scalar(doc, "concept_id", "")),
            code=str(legacy_app._get_scalar(doc, "code", "")),
            tty_priority=legacy_app._tty_priority_value(doc),
            source_priority=legacy_app._source_priority_value(doc),
        )
        for doc in docs
    ]

    response_time_ms = (time.perf_counter() - start_ts) * 1000.0

    return NoteCompleteResponse(
        query=validated.q,
        section=validated.section,
        semantic_types_applied=SECTION_SEMANTIC_TYPES[validated.section],
        spell_corrected=spell_corrected,
        total=len(results),
        results=results,
        response_time_ms=response_time_ms,
        solr_hits=solr_hits,
    )


@router.get(
    "/complete/context",
    response_model=NoteCompleteContextResponse,
    summary="Context-aware medical term completion for clinical notes",
    description=(
        "Returns ranked UMLS term suggestions with patient-history context "
        "matches boosted to the top."
    ),
)
async def note_complete_context_endpoint(
    q: str = Query(..., description="Partial word being typed"),
    section: str = Query(..., description="Clinical note section"),
    rows: int = Query(NOTE_API_DEFAULT_ROWS, description="Number of results to return"),
    fuzzy: bool = Query(True, description="Enable spell correction fallback for zero results"),
    source: str | None = Query(None, description="Filter to specific source vocabulary"),
    tty: str | None = Query(None, description="Comma-separated list of TTY codes to filter"),
    patient_context: str | None = Query(None, description="Plain text clinical summary for the patient"),
    patient_context_json: str | None = Query(None, description="Clean structured patient context JSON"),
):
    start_ts = time.perf_counter()

    if section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section '{section}'. Valid sections: {', '.join(VALID_SECTIONS)}",
        )

    has_json_context = isinstance(patient_context_json, str) and patient_context_json.strip()
    has_text_context = isinstance(patient_context, str) and patient_context.strip()

    if not has_json_context and not has_text_context:
        raise HTTPException(
            status_code=400,
            detail="Either patient_context_json or patient_context is required",
        )

    try:
        validated = NoteCompleteRequest(
            q=q,
            section=section,
            rows=rows,
            fuzzy=fuzzy,
            source=source,
            tty=tty,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail={"message": "Invalid request parameters", "errors": exc.errors()})

    try:
        if has_json_context:
            parsed_context = parse_patient_context_json(json.loads(patient_context_json))
        else:
            parsed_context = parse_patient_context(patient_context)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="patient_context_json must be valid JSON")
    except Exception:
        detail = "patient_context_json must be valid JSON" if has_json_context else "patient_context must be valid plain text clinical summary"
        raise HTTPException(status_code=400, detail=detail)

    if not isinstance(parsed_context, dict):
        detail = "patient_context_json must be valid JSON" if has_json_context else "patient_context must be valid plain text clinical summary"
        raise HTTPException(
            status_code=400,
            detail=detail,
        )

    try:
        context_matches = find_matching_context_terms(
            query=validated.q,
            section=validated.section,
            parsed=parsed_context,
            today=date.today(),
        )
    except Exception:
        context_matches = []

    try:
        docs, solr_hits, spell_corrected = await note_complete(
            q=validated.q,
            section=validated.section,
            rows=validated.rows,
            fuzzy=validated.fuzzy,
            source=validated.source,
            tty=validated.tty,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Solr service unavailable: {exc}")
    except Exception as exc:  # pragma: no cover - safety net for API resilience
        return JSONResponse(
            status_code=503,
            content={
                "error": "note_completion_unavailable",
                "message": "Failed to complete note suggestions",
                "detail": str(exc),
            },
        )

    umls_results = [
        {
            "term": str(legacy_app._get_scalar(doc, "term", "")),
            "semantic_type": str(legacy_app._get_scalar(doc, "semantic_type", "")),
            "source": str(legacy_app._get_scalar(doc, "source", "")),
            "tty": str(legacy_app._get_scalar(doc, "tty", "")),
            "concept_id": str(legacy_app._get_scalar(doc, "concept_id", "")),
            "code": str(legacy_app._get_scalar(doc, "code", "")),
            "tty_priority": legacy_app._tty_priority_value(doc),
            "source_priority": legacy_app._source_priority_value(doc),
            "from_patient_history": False,
        }
        for doc in docs
    ]

    by_term_lower = {
        item["term"].strip().lower(): item
        for item in umls_results
        if item["term"].strip()
    }

    boosted_results = []
    seen_boosted = set()
    for match in context_matches:
        term = str(match.get("term", "")).strip()
        term_lower = term.lower()
        if not term or term_lower in seen_boosted:
            continue
        seen_boosted.add(term_lower)

        existing = by_term_lower.get(term_lower)
        if existing is not None:
            boosted = dict(existing)
            boosted["from_patient_history"] = True
            boosted_results.append(boosted)
            continue

        boosted_results.append(
            {
                "term": term,
                "semantic_type": "Patient Context",
                "source": "PATIENT_HISTORY",
                "tty": "",
                "concept_id": "",
                "code": "",
                "tty_priority": 0,
                "source_priority": 0,
                "from_patient_history": True,
            }
        )

    boosted_term_lowers = {item["term"].strip().lower() for item in boosted_results}
    remaining_umls = [
        item for item in umls_results if item["term"].strip().lower() not in boosted_term_lowers
    ]
    merged = (boosted_results + remaining_umls)[: validated.rows]

    results = [NoteCompleteContextResult(**item) for item in merged]
    response_time_ms = (time.perf_counter() - start_ts) * 1000.0

    return NoteCompleteContextResponse(
        query=validated.q,
        section=validated.section,
        semantic_types_applied=SECTION_SEMANTIC_TYPES[validated.section],
        spell_corrected=spell_corrected,
        total=len(results),
        results=results,
        response_time_ms=response_time_ms,
        solr_hits=solr_hits,
        context_boosted_count=len(boosted_results),
    )


@router.post(
    "/complete/context",
    response_model=NoteCompleteContextResponse,
    summary="Context-aware medical term completion for clinical notes (POST)",
    description=(
        "Returns ranked UMLS term suggestions with patient-history context "
        "matches boosted to the top, using JSON request body input."
    ),
)
async def note_complete_context_post_endpoint(body: NoteCompleteContextRequest):
    start_ts = time.perf_counter()

    if body.section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section '{body.section}'. Valid sections: {', '.join(VALID_SECTIONS)}",
        )

    try:
        validated = NoteCompleteRequest(
            q=body.q,
            section=body.section,
            rows=body.rows,
            fuzzy=body.fuzzy,
            source=body.source,
            tty=body.tty,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail={"message": "Invalid request parameters", "errors": exc.errors()})

    if body.patient_context_json is not None:
        try:
            parsed_context = parse_patient_context_json(body.patient_context_json)
        except Exception:
            raise HTTPException(status_code=400, detail="patient_context_json must be valid JSON")
    elif isinstance(body.patient_context, str) and body.patient_context.strip():
        try:
            parsed_context = parse_patient_context(body.patient_context)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="patient_context must be valid plain text clinical summary",
            )
    else:
        raise HTTPException(
            status_code=400,
            detail="Either patient_context_json or patient_context is required",
        )

    if not isinstance(parsed_context, dict):
        raise HTTPException(
            status_code=400,
            detail="patient_context_json must be valid JSON" if body.patient_context_json is not None else "patient_context must be valid plain text clinical summary",
        )

    try:
        context_matches = find_matching_context_terms(
            query=validated.q,
            section=validated.section,
            parsed=parsed_context,
            today=date.today(),
        )
    except Exception:
        context_matches = []

    try:
        docs, solr_hits, spell_corrected = await note_complete(
            q=validated.q,
            section=validated.section,
            rows=validated.rows,
            fuzzy=validated.fuzzy,
            source=validated.source,
            tty=validated.tty,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Solr service unavailable: {exc}")
    except Exception as exc:  # pragma: no cover - safety net for API resilience
        return JSONResponse(
            status_code=503,
            content={
                "error": "note_completion_unavailable",
                "message": "Failed to complete note suggestions",
                "detail": str(exc),
            },
        )

    results, boosted_count = _merge_context_and_umls_results(
        docs=docs,
        context_matches=context_matches,
        rows=validated.rows,
    )

    response_time_ms = (time.perf_counter() - start_ts) * 1000.0
    return NoteCompleteContextResponse(
        query=validated.q,
        section=validated.section,
        semantic_types_applied=SECTION_SEMANTIC_TYPES[validated.section],
        spell_corrected=spell_corrected,
        total=len(results),
        results=results,
        response_time_ms=response_time_ms,
        solr_hits=solr_hits,
        context_boosted_count=boosted_count,
    )


@router.post(
    "/complete/context/file",
    response_model=NoteCompleteContextResponse,
    summary="Context-aware medical term completion from uploaded file",
    description=(
        "Accepts multipart/form-data and reads patient context from an uploaded "
        "file (.json for structured context, otherwise plain text)."
    ),
)
async def note_complete_context_file_endpoint(
    q: str = Form(..., description="Partial word being typed"),
    section: str = Form(..., description="Clinical note section"),
    rows: int = Form(NOTE_API_DEFAULT_ROWS, description="Number of results to return"),
    fuzzy: bool = Form(True, description="Enable spell correction fallback for zero results"),
    source: str | None = Form(None, description="Filter to specific source vocabulary"),
    tty: str | None = Form(None, description="Comma-separated list of TTY codes to filter"),
    patient_context_file: UploadFile = File(..., description="Patient context file (.json or text)"),
):
    filename = (patient_context_file.filename or "").strip()
    if not filename:
        raise HTTPException(status_code=400, detail="patient_context_file filename is required")

    try:
        raw_bytes = await patient_context_file.read()
    finally:
        await patient_context_file.close()

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="patient_context_file is empty")

    content_type = (patient_context_file.content_type or "").lower()
    lower_name = filename.lower()
    is_json_file = (
        lower_name.endswith(".json")
        or content_type in {"application/json", "text/json"}
    )

    if is_json_file:
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise HTTPException(status_code=400, detail="patient_context_file must contain valid UTF-8 JSON")

        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="patient_context_file JSON must be an object")

        body = NoteCompleteContextRequest(
            q=q,
            section=section,
            rows=rows,
            fuzzy=fuzzy,
            source=source,
            tty=tty,
            patient_context_json=payload,
        )
        return await note_complete_context_post_endpoint(body)

    try:
        text_payload = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="patient_context_file text must be UTF-8 encoded")

    if not text_payload.strip():
        raise HTTPException(status_code=400, detail="patient_context_file text is empty")

    body = NoteCompleteContextRequest(
        q=q,
        section=section,
        rows=rows,
        fuzzy=fuzzy,
        source=source,
        tty=tty,
        patient_context=text_payload,
    )
    return await note_complete_context_post_endpoint(body)


@router.get(
    "/sections",
    summary="List all valid sections and their semantic type filters",
)
async def list_sections():
    """Expose section metadata for API clients and UI discovery."""
    return {
        "sections": [
            {
                "name": section,
                "semantic_types": SECTION_SEMANTIC_TYPES.get(section, []),
            }
            for section in VALID_SECTIONS
        ],
        "total": len(VALID_SECTIONS),
    }
