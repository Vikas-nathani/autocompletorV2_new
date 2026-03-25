"""Strict parser for machine-generated FHIR preprocessor context text."""

from __future__ import annotations

from datetime import date, datetime
import json
import re
from typing import Any


KNOWN_HEADINGS = {
    "Patient Summary",
    "Patient Information",
    "Allergies",
    "Social History",
    "Vital Signs",
    "Assessment Scores",
    "Conditions",
    "Medications",
    "Procedures",
    "Care Plan",
    "Laboratory Results",
    "Imaging",
    "Immunizations",
    "Reports",
    "Documents",
}

INVESTIGATION_KEYWORDS = (
    "test",
    "culture",
    "screening",
    "measurement",
    "analysis",
    "count",
    "typing",
    "titer",
    "scan",
    "x-ray",
    "ultrasound",
    "urinalysis",
    "hemogram",
    "assay",
    "smear",
)

# Common confusables used in keyboard/layout mixed input.
CONFUSABLE_TRANSLATION = str.maketrans(
    {
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "х": "x",
        "і": "i",
        "А": "a",
        "В": "b",
        "Е": "e",
        "К": "k",
        "М": "m",
        "Н": "h",
        "О": "o",
        "Р": "p",
        "С": "c",
        "Т": "t",
        "Х": "x",
        "І": "i",
    }
)


class _ParserState:
    def __init__(self) -> None:
        self.encounter_id: str = "summary"
        self.encounter_date: str | None = None
        self.section: str | None = None
        self.report_header: str | None = None
        self.in_report_plan: bool = False
        self.pending_care_plan_header: bool = False


def _normalize_spaces(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _normalize_term(text: str | None) -> str:
    return _normalize_spaces(text)


def _safe_lower(text: str | None) -> str:
    return _normalize_spaces(text).translate(CONFUSABLE_TRANSLATION).lower()


def _parse_date(raw: str | None) -> str | None:
    if not raw:
        return None
    text = _normalize_spaces(raw)
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _is_heading_line(line: str) -> bool:
    if not line or line[:1].isspace():
        return False
    stripped = line.strip()
    if stripped in KNOWN_HEADINGS:
        return True
    return re.fullmatch(r"Encounter\s+\d+", stripped) is not None


def _extract_parenthesized_value(meta: str, key: str) -> str | None:
    match = re.search(rf"{re.escape(key)}\s*:\s*([^,\)]+)", meta, flags=re.IGNORECASE)
    if not match:
        return None
    return _normalize_spaces(match.group(1))


def _strip_and_n_more(text: str) -> str:
    return re.sub(r",?\s*and\s+\d+\s+more\s*$", "", text, flags=re.IGNORECASE).strip()


def _split_summary_list(raw: str) -> list[str]:
    cleaned = _strip_and_n_more(_normalize_spaces(raw))
    if not cleaned:
        return []
    items = [_normalize_spaces(part) for part in cleaned.split(",")]
    return [item for item in items if item]


def _extract_summary_segment(summary_line: str, start_pattern: str, stop_patterns: list[str]) -> str:
    flags = re.IGNORECASE
    start_match = re.search(start_pattern, summary_line, flags=flags)
    if not start_match:
        return ""

    start = start_match.end()
    end = len(summary_line)
    for pattern in stop_patterns:
        stop_match = re.search(pattern, summary_line[start:], flags=flags)
        if stop_match:
            end = min(end, start + stop_match.start())
    period_idx = summary_line.find(".", start)
    if period_idx != -1:
        end = min(end, period_idx)

    return _normalize_spaces(summary_line[start:end])


def _extract_patient_summary(lines: list[str]) -> tuple[str, list[str], list[str], list[str]]:
    patient_name = ""
    active_conditions: list[str] = []
    resolved_conditions: list[str] = []
    recent_medications: list[str] = []

    try:
        idx = lines.index("Patient Summary")
    except ValueError:
        return patient_name, active_conditions, resolved_conditions, recent_medications

    summary_line = ""
    for line in lines[idx + 1 :]:
        stripped = line.strip()
        if not stripped:
            if summary_line:
                break
            continue
        if _is_heading_line(line):
            break
        summary_line = stripped
        break

    if not summary_line:
        return patient_name, active_conditions, resolved_conditions, recent_medications

    name_match = re.match(r"^(.*?)\s+is\s+a\s+", summary_line, flags=re.IGNORECASE)
    if name_match:
        patient_name = _normalize_spaces(name_match.group(1))

    active_segment = _extract_summary_segment(
        summary_line,
        r"Active\s+conditions\s*:\s*",
        [r"\bResolved\s+conditions\s+include\s*:", r"\bResolved\s+conditions\s*:", r"\bRecent\s+medications\s*:"],
    )
    resolved_segment = _extract_summary_segment(
        summary_line,
        r"Resolved\s+conditions(?:\s+include)?\s*:\s*",
        [r"\bRecent\s+medications\s*:"] ,
    )
    medications_segment = _extract_summary_segment(summary_line, r"Recent\s+medications\s*:\s*", [])

    active_conditions = _split_summary_list(active_segment)
    resolved_conditions = _split_summary_list(resolved_segment)
    recent_medications = _split_summary_list(medications_segment)

    return patient_name, active_conditions, resolved_conditions, recent_medications


def _is_investigation(term: str) -> bool:
    lowered = _safe_lower(term)
    return any(keyword in lowered for keyword in INVESTIGATION_KEYWORDS)


def _is_investigation_term(term: str) -> bool:
    return _is_investigation(term)


def _clean_display(display: str) -> str:
    if not display:
        return ""
    cleaned = re.sub(
        r"\s*\((finding|disorder|situation|procedure|regime/therapy|person|observable entity|substance|product)\)\s*$",
        "",
        display,
        flags=re.IGNORECASE,
    )
    return _normalize_term(cleaned)


def _parse_condition_line(line: str) -> dict[str, Any] | None:
    stripped = _normalize_spaces(line)
    if not stripped:
        return None

    term = stripped
    onset = None
    resolved = None

    meta_match = re.search(r"\((.*?)\)", stripped)
    if meta_match:
        term = _normalize_spaces(stripped[: meta_match.start()])
        meta = meta_match.group(1)
        onset = _parse_date(_extract_parenthesized_value(meta, "onset"))
        resolved = _parse_date(_extract_parenthesized_value(meta, "resolved"))

    if not term:
        return None

    return {
        "term": term,
        "term_lower": _safe_lower(term),
        "onset": onset,
        "resolved": resolved,
        "status": "resolved" if resolved else "active",
    }


def _parse_medication_term(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None

    lowered = stripped.lower()
    if lowered.startswith("for:") or lowered.startswith("(for:"):
        return None

    for sep in (" — ", " -- "):
        if sep in stripped:
            stripped = stripped.split(sep, 1)[0].strip()
            break

    cleaned = _normalize_spaces(stripped)
    return cleaned or None


def _parse_procedure_term(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None
    if "(" in stripped:
        stripped = stripped.split("(", 1)[0].strip()
    cleaned = _normalize_spaces(stripped)
    return cleaned or None


def _parse_imaging_term(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None

    if "(" in stripped:
        stripped = stripped.split("(", 1)[0].strip()
    if "," in stripped:
        stripped = stripped.split(",", 1)[0].strip()

    cleaned = _normalize_spaces(stripped)
    return cleaned or None


def _parse_lab_result(line: str) -> tuple[str | None, str]:
    stripped = line.strip()
    if ":" not in stripped:
        return None, "active"

    test_name = _normalize_spaces(stripped.split(":", 1)[0])
    if not test_name:
        return None, "active"

    status = "flagged" if re.search(r"\[(HIGH|LOW|CRITICAL)\]", stripped, flags=re.IGNORECASE) else "active"
    return test_name, status


def _is_care_plan_name_line(line: str) -> bool:
    """Detect care-plan title lines with date range/status metadata."""
    stripped = _normalize_spaces(line)
    if not stripped:
        return False

    has_status = re.search(r"\[(completed|active)\]", stripped, flags=re.IGNORECASE) is not None
    has_date_range = (
        re.search(
            r"\([^\)]*\bto\b[^\)]*(?:ongoing|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})[^\)]*\)",
            stripped,
            flags=re.IGNORECASE,
        )
        is not None
    )
    return has_status or has_date_range


def _extract_allergy_term(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None
    if "(" in stripped:
        stripped = stripped.split("(", 1)[0].strip()
    cleaned = _normalize_spaces(stripped)
    return cleaned or None


def _extract_report_presenting_terms(line: str) -> list[str]:
    match = re.search(r"Patient\s+is\s+presenting\s+with\s+(.+)", line, flags=re.IGNORECASE)
    if not match:
        return []

    tail = match.group(1).strip().rstrip(".")
    chunks = [chunk.strip() for chunk in tail.split(",")]
    out: list[str] = []
    for chunk in chunks:
        term = _normalize_spaces(re.sub(r"\([^\)]*\)", "", chunk))
        if term:
            out.append(term)
    return out


def _empty_result() -> dict[str, Any]:
    return {
        "patient_name": "",
        "conditions": [],
        "medications": [],
        "procedures": [],
        "investigations": [],
        "care_plan": [],
        "allergies": [],
        "immunizations": [],
    }


def _make_term_entry(term: str, status: str = "active", onset: str | None = None, resolved: str | None = None) -> dict[str, Any]:
    return {
        "term": term,
        "term_lower": _safe_lower(term),
        "status": status,
        "onset": onset,
        "resolved": resolved,
    }


def parse_patient_context(text: str) -> dict:
    """Parse strict FHIR preprocessor context text into structured buckets."""
    if not isinstance(text, str) or not text.strip():
        return _empty_result()

    lines = text.splitlines()
    result = _empty_result()

    # Per-bucket stores by normalized term.
    condition_store: dict[str, dict[str, Any]] = {}
    medication_store: dict[str, dict[str, Any]] = {}
    procedure_store: dict[str, dict[str, Any]] = {}
    investigation_store: dict[str, dict[str, Any]] = {}
    care_plan_store: dict[str, dict[str, Any]] = {}

    encounters: dict[str, dict[str, set[str]]] = {
        "conditions": {},
        "medications": {},
        "procedures": {},
        "investigations": {},
        "care_plan": {},
    }

    def touch(bucket: str, key: str, encounter_id: str) -> None:
        bucket_map = encounters[bucket]
        if key not in bucket_map:
            bucket_map[key] = set()
        bucket_map[key].add(encounter_id)

    def merge_entry(
        bucket: str,
        store: dict[str, dict[str, Any]],
        entry: dict[str, Any],
        encounter_id: str,
        last_seen: str | None,
    ) -> None:
        key = entry["term_lower"]
        existing = store.get(key)
        if existing is None:
            existing = {
                "term": entry["term"],
                "term_lower": key,
                "encounter_count": 0,
                "status": entry.get("status", "active"),
                "onset": entry.get("onset"),
                "resolved": entry.get("resolved"),
                "last_seen": last_seen,
            }
            store[key] = existing
        else:
            if entry.get("onset") and not existing.get("onset"):
                existing["onset"] = entry.get("onset")
            if entry.get("resolved"):
                existing["resolved"] = entry.get("resolved")
                existing["status"] = "resolved"
            if existing.get("status") != "flagged" and entry.get("status") == "flagged":
                existing["status"] = "flagged"
            if last_seen and (not existing.get("last_seen") or last_seen > str(existing.get("last_seen"))):
                existing["last_seen"] = last_seen

        touch(bucket, key, encounter_id)

    state = _ParserState()

    patient_name, active_conditions, resolved_conditions, recent_medications = _extract_patient_summary(lines)
    result["patient_name"] = patient_name

    for term in active_conditions:
        cond = _parse_condition_line(term)
        if cond:
            merge_entry("conditions", condition_store, cond, "summary", None)

    for term in resolved_conditions:
        cond = _parse_condition_line(term)
        if cond:
            cond["status"] = "resolved"
            merge_entry("conditions", condition_store, cond, "summary", None)

    for term in recent_medications:
        med = _parse_medication_term(term)
        if med:
            merge_entry(
                "medications",
                medication_store,
                _make_term_entry(med, status="active"),
                "summary",
                None,
            )

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped:
            continue

        # Headings are strict: no leading whitespace and known heading names.
        if _is_heading_line(line):
            state.in_report_plan = False
            state.pending_care_plan_header = False
            state.report_header = None

            if re.fullmatch(r"Encounter\s+\d+", stripped):
                state.encounter_id = stripped
                state.encounter_date = None
                state.section = None
                continue

            if stripped == "Documents":
                state.section = "Documents"
                continue

            state.section = stripped
            if stripped == "Care Plan":
                state.pending_care_plan_header = True
            continue

        if state.section is None:
            continue

        if state.section == "Documents":
            continue

        if stripped.lower().startswith("date:") and state.encounter_id != "summary":
            state.encounter_date = _parse_date(stripped.split(":", 1)[1].strip())
            continue

        if state.section == "Allergies":
            allergy = _extract_allergy_term(stripped)
            if allergy and allergy not in result["allergies"]:
                result["allergies"].append(allergy)
            continue

        if state.section == "Immunizations":
            imm = _normalize_spaces(stripped)
            if imm and imm not in result["immunizations"]:
                result["immunizations"].append(imm)
            continue

        if state.section == "Conditions":
            cond = _parse_condition_line(stripped)
            if cond:
                merge_entry("conditions", condition_store, cond, state.encounter_id, state.encounter_date)
            continue

        if state.section == "Medications":
            med = _parse_medication_term(stripped)
            if med:
                merge_entry(
                    "medications",
                    medication_store,
                    _make_term_entry(med, status="active"),
                    state.encounter_id,
                    state.encounter_date,
                )
            continue

        if state.section == "Procedures":
            proc = _parse_procedure_term(stripped)
            if not proc:
                continue
            bucket = "investigations" if _is_investigation(proc) else "procedures"
            store = investigation_store if bucket == "investigations" else procedure_store
            merge_entry(
                bucket,
                store,
                _make_term_entry(proc, status="active"),
                state.encounter_id,
                state.encounter_date,
            )
            continue

        if state.section == "Care Plan":
            if _is_care_plan_name_line(stripped):
                state.pending_care_plan_header = False
                continue
            if re.match(r"^\s{2,}\S", line):
                rec = _normalize_spaces(stripped)
                if rec:
                    merge_entry(
                        "care_plan",
                        care_plan_store,
                        _make_term_entry(rec, status="active"),
                        state.encounter_id,
                        state.encounter_date,
                    )
            continue

        if state.section == "Laboratory Results":
            test_name, status = _parse_lab_result(stripped)
            if test_name:
                merge_entry(
                    "investigations",
                    investigation_store,
                    _make_term_entry(test_name, status=status),
                    state.encounter_id,
                    state.encounter_date,
                )
            continue

        if state.section == "Imaging":
            imaging = _parse_imaging_term(stripped)
            if imaging:
                merge_entry(
                    "investigations",
                    investigation_store,
                    _make_term_entry(imaging, status="active"),
                    state.encounter_id,
                    state.encounter_date,
                )
            continue

        if state.section == "Reports":
            if stripped.startswith("#"):
                state.report_header = stripped.lstrip("#").strip().lower()
                state.in_report_plan = stripped.lower().startswith("## plan")
                continue

            if state.report_header == "medications":
                for chunk in stripped.split(";"):
                    med = _parse_medication_term(chunk)
                    if med:
                        merge_entry(
                            "medications",
                            medication_store,
                            _make_term_entry(med, status="active"),
                            state.encounter_id,
                            state.encounter_date,
                        )
                continue

            if state.report_header == "assessment and plan":
                for term in _extract_report_presenting_terms(stripped):
                    cond = _parse_condition_line(term)
                    if cond:
                        merge_entry("conditions", condition_store, cond, state.encounter_id, state.encounter_date)
                continue

            if state.in_report_plan and stripped.startswith("-"):
                bullet = stripped.lstrip("-").strip()
                med = _parse_medication_term(bullet)
                if med and ("tablet" in med.lower() or "capsule" in med.lower() or "mg" in med.lower()):
                    merge_entry(
                        "medications",
                        medication_store,
                        _make_term_entry(med, status="active"),
                        state.encounter_id,
                        state.encounter_date,
                    )
                else:
                    proc = _parse_procedure_term(bullet)
                    if proc:
                        bucket = "investigations" if _is_investigation(proc) else "procedures"
                        store = investigation_store if bucket == "investigations" else procedure_store
                        merge_entry(
                            bucket,
                            store,
                            _make_term_entry(proc, status="active"),
                            state.encounter_id,
                            state.encounter_date,
                        )
                continue

            continue

    for key, entry in condition_store.items():
        entry["encounter_count"] = len(encounters["conditions"].get(key, set()))
        result["conditions"].append(entry)

    for key, entry in medication_store.items():
        entry["encounter_count"] = len(encounters["medications"].get(key, set()))
        result["medications"].append(entry)

    for key, entry in procedure_store.items():
        entry["encounter_count"] = len(encounters["procedures"].get(key, set()))
        result["procedures"].append(entry)

    for key, entry in investigation_store.items():
        entry["encounter_count"] = len(encounters["investigations"].get(key, set()))
        result["investigations"].append(entry)

    for key, entry in care_plan_store.items():
        entry["encounter_count"] = len(encounters["care_plan"].get(key, set()))
        result["care_plan"].append(entry)

    return result


def parse_patient_context_json(json_data) -> dict:
    """Parse structured clean patient-context JSON into context buckets."""
    if isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except Exception:
            return _empty_result()

    if not isinstance(json_data, dict):
        return _empty_result()

    result = _empty_result()
    result["patient_name"] = _normalize_term(json_data.get("fullName", ""))

    condition_store: dict[str, dict[str, Any]] = {}
    medication_store: dict[str, dict[str, Any]] = {}
    procedure_store: dict[str, dict[str, Any]] = {}
    investigation_store: dict[str, dict[str, Any]] = {}
    care_plan_store: dict[str, dict[str, Any]] = {}

    encounters: dict[str, dict[str, set[str]]] = {
        "conditions": {},
        "medications": {},
        "procedures": {},
        "investigations": {},
        "care_plan": {},
    }

    def touch(bucket: str, key: str, encounter_id: str) -> None:
        bucket_map = encounters[bucket]
        if key not in bucket_map:
            bucket_map[key] = set()
        bucket_map[key].add(encounter_id)

    def merge_entry(
        bucket: str,
        store: dict[str, dict[str, Any]],
        entry: dict[str, Any],
        encounter_id: str,
        last_seen: str | None,
    ) -> None:
        key = entry["term_lower"]
        existing = store.get(key)
        if existing is None:
            existing = {
                "term": entry["term"],
                "term_lower": key,
                "encounter_count": 0,
                "status": entry.get("status", "active"),
                "onset": entry.get("onset"),
                "resolved": entry.get("resolved"),
                "last_seen": last_seen,
            }
            store[key] = existing
        else:
            if entry.get("onset") and not existing.get("onset"):
                existing["onset"] = entry.get("onset")
            if entry.get("resolved"):
                existing["resolved"] = entry.get("resolved")
                existing["status"] = "resolved"
            if existing.get("status") != "flagged" and entry.get("status") == "flagged":
                existing["status"] = "flagged"
            if last_seen and (not existing.get("last_seen") or last_seen > str(existing.get("last_seen"))):
                existing["last_seen"] = last_seen

        touch(bucket, key, encounter_id)

    for allergy in json_data.get("allergies", []) or []:
        display = _normalize_term(((allergy or {}).get("code") or {}).get("display", ""))
        clean = re.sub(r"\s*\([^)]+\)\s*$", "", display).strip()
        if clean and clean not in result["allergies"]:
            result["allergies"].append(clean)

    for encounter in json_data.get("encounters", []) or []:
        encounter = encounter or {}
        encounter_id = _normalize_term(encounter.get("id", "")) or "json"
        encounter_date = str(((encounter.get("period") or {}).get("start") or ""))[:10] or None

        for condition in encounter.get("conditions", []) or []:
            condition = condition or {}
            term = _clean_display(((condition.get("code") or {}).get("display") or ""))
            if not term:
                continue
            onset = str(condition.get("onsetDateTime") or "")[:10] or None
            resolved = str(condition.get("abatementDateTime") or "")[:10] or None
            status = _safe_lower(condition.get("clinicalStatus") or "active") or "active"
            entry = _make_term_entry(
                term,
                status="resolved" if resolved else status,
                onset=onset,
                resolved=resolved,
            )
            merge_entry("conditions", condition_store, entry, encounter_id, encounter_date)

        for medication in encounter.get("medications", []) or []:
            medication = medication or {}
            term = _normalize_term(((medication.get("medication") or {}).get("display") or ""))
            if not term:
                continue
            date_seen = str(medication.get("authoredOn") or "")[:10] or encounter_date
            entry = _make_term_entry(term, status=_safe_lower(medication.get("status") or "active") or "active")
            merge_entry("medications", medication_store, entry, encounter_id, date_seen)

        for procedure in encounter.get("procedures", []) or []:
            procedure = procedure or {}
            term = _clean_display(((procedure.get("code") or {}).get("display") or ""))
            if not term:
                continue
            date_seen = str(((procedure.get("performedPeriod") or {}).get("start") or ""))[:10] or encounter_date
            bucket = "investigations" if _is_investigation_term(term) else "procedures"
            store = investigation_store if bucket == "investigations" else procedure_store
            merge_entry(bucket, store, _make_term_entry(term, status="active"), encounter_id, date_seen)

        for care_plan in encounter.get("carePlans", []) or []:
            care_plan = care_plan or {}
            care_plan_date = str(((care_plan.get("period") or {}).get("start") or ""))[:10] or encounter_date
            for activity in care_plan.get("activity", []) or []:
                activity = activity or {}
                term = _clean_display(((activity.get("code") or {}).get("display") or ""))
                if not term:
                    continue
                status = _safe_lower(activity.get("status") or care_plan.get("status") or "active") or "active"
                merge_entry(
                    "care_plan",
                    care_plan_store,
                    _make_term_entry(term, status=status),
                    encounter_id,
                    care_plan_date,
                )

        for observation in encounter.get("observations", []) or []:
            observation = observation or {}
            if _safe_lower(observation.get("category") or "") != "laboratory":
                continue
            term = _normalize_term(((observation.get("code") or {}).get("display") or ""))
            if not term:
                continue
            interpretation = _safe_lower(str(observation.get("interpretation") or ""))
            flagged = re.search(r"\b(hh|ll|h|l|critical|high)\b", interpretation, flags=re.IGNORECASE) is not None
            status = "flagged" if flagged else "active"
            merge_entry(
                "investigations",
                investigation_store,
                _make_term_entry(term, status=status),
                encounter_id,
                encounter_date,
            )

        for immunization in encounter.get("immunizations", []) or []:
            immunization = immunization or {}
            term = _normalize_term(((immunization.get("vaccineCode") or {}).get("display") or ""))
            if term and term not in result["immunizations"]:
                result["immunizations"].append(term)

    for key, entry in condition_store.items():
        entry["encounter_count"] = len(encounters["conditions"].get(key, set()))
        result["conditions"].append(entry)

    for key, entry in medication_store.items():
        entry["encounter_count"] = len(encounters["medications"].get(key, set()))
        result["medications"].append(entry)

    for key, entry in procedure_store.items():
        entry["encounter_count"] = len(encounters["procedures"].get(key, set()))
        result["procedures"].append(entry)

    for key, entry in investigation_store.items():
        entry["encounter_count"] = len(encounters["investigations"].get(key, set()))
        result["investigations"].append(entry)

    for key, entry in care_plan_store.items():
        entry["encounter_count"] = len(encounters["care_plan"].get(key, set()))
        result["care_plan"].append(entry)

    return result


def calculate_boost_score(term_entry: dict, today: date) -> float:
    """Calculate contextual boost score using encounter count and recency."""
    try:
        encounter_count = int(term_entry.get("encounter_count", 0) or 0)
    except Exception:
        encounter_count = 0

    if encounter_count <= 0:
        return 0.0

    status = _safe_lower(str(term_entry.get("status", "")))
    resolved = term_entry.get("resolved")

    if status in {"active", "flagged"}:
        return float(encounter_count) * 3.0

    if status == "resolved" and resolved:
        resolved_date = _parse_date(str(resolved))
        if resolved_date:
            try:
                age_days = (today - datetime.strptime(resolved_date, "%Y-%m-%d").date()).days
            except ValueError:
                age_days = 365

            if age_days <= 90:
                return float(encounter_count) * 2.0
            if age_days <= 180:
                return float(encounter_count) * 1.5
            if age_days <= 365:
                return float(encounter_count)
            return float(encounter_count) * 0.5

    return float(encounter_count)


def find_matching_context_terms(query: str, section: str, parsed: dict, today: date) -> list[dict]:
    """Find section-aware prefix matches and rank by context boost."""
    if not isinstance(parsed, dict) or not parsed:
        return []

    q = _safe_lower(query)
    if not q:
        return []

    section_map = {
        "chief_complaint": ["conditions"],
        "diagnosis": ["conditions"],
        "medications": ["medications"],
        "procedures": ["procedures"],
        "investigations": ["investigations"],
        "advice": ["conditions", "procedures", "care_plan"],
    }

    buckets = section_map.get(section, [])
    if not buckets:
        return []

    matches: list[dict] = []
    for bucket in buckets:
        for entry in parsed.get(bucket, []) or []:
            term_lower = _safe_lower(entry.get("term_lower") or entry.get("term"))
            if not term_lower:
                continue

            tokens = re.findall(r"[a-z0-9]+", term_lower)
            token_prefix_match = any(token.startswith(q) for token in tokens)
            if not (term_lower.startswith(q) or token_prefix_match):
                continue

            scored = dict(entry)
            scored["boost_score"] = calculate_boost_score(scored, today)
            scored["context_bucket"] = bucket
            matches.append(scored)

    matches.sort(key=lambda item: (-float(item.get("boost_score", 0.0)), str(item.get("term_lower", ""))))
    return matches
