from datetime import date
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "note_api"))

from context_parser import (
    calculate_boost_score,
    find_matching_context_terms,
    parse_patient_context,
)


SAMPLE_CONTEXT = """Patient Summary
Yolanda Delrio is a 29 years 7 months, female patient. Active conditions: Body mass index 30+ - obesity, Full-time employment and 3 more. Resolved conditions: Stress. Recent medications: Meperidine Hydrochloride 50 MG Oral Tablet, Naproxen sodium 220 MG Oral Tablet.

Patient Information
Name: Yolanda Delrio
Gender: female
Date of Birth: 1 Jan 1996

Allergies
Loratadine (criticality: low)

Encounter 24
Encounter Type: Emergency room admission
Date: 11 November 2025
Status: finished

Conditions
Fracture of bone (onset: 11 Nov 2025, resolved: 1 Jan 2026)
Fracture subluxation of wrist (onset: 11 Nov 2025, resolved: 1 Jan 2026)

Medications
Meperidine Hydrochloride 50 MG Oral Tablet -- every 4.0 h
Naproxen sodium 220 MG Oral Tablet -- Take as needed., as needed

Procedures
Plain X-ray of wrist region
Bone immobilization (reason: Fracture subluxation of wrist)
Urine culture (reason: Normal pregnancy)

Reports
History and physical note, Dr. Test
  11 November 2025
  # Medications
  meperidine hydrochloride 50 mg oral tablet; naproxen sodium 220 mg oral tablet
  # Assessment and Plan
  Patient is presenting with fracture of bone (disorder), fracture subluxation of wrist (disorder).

Encounter 25
Encounter Type: Follow up
Date: 1 January 2026
Status: finished

Conditions
Fracture of bone (onset: 11 Nov 2025, resolved: 1 Jan 2026)

Encounter 26
Encounter Type: Encounter for symptom
Date: 9 January 2026
Status: finished

Conditions
Acute viral pharyngitis (onset: 9 Jan 2026, resolved: 20 Jan 2026)
"""


def _find(items, term_lower):
    for item in items:
        if item.get("term_lower") == term_lower:
            return item
    return None


def test_parser_extracts_conditions_from_conditions_section():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    fracture = _find(parsed["conditions"], "fracture of bone")
    assert fracture is not None
    assert fracture["status"] == "resolved"
    assert fracture["onset"] == "2025-11-11"
    assert fracture["resolved"] == "2026-01-01"


def test_parser_extracts_medications_from_encounter_medications_section():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    mep = _find(parsed["medications"], "meperidine hydrochloride 50 mg oral tablet")
    assert mep is not None


def test_parser_extracts_medications_from_reports_medications_subsection():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    naproxen = _find(parsed["medications"], "naproxen sodium 220 mg oral tablet")
    assert naproxen is not None


def test_parser_extracts_procedures_correctly():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    proc = _find(parsed["procedures"], "bone immobilization")
    assert proc is not None


def test_parser_classifies_urine_culture_as_investigation_not_procedure():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    inv = _find(parsed["investigations"], "urine culture")
    proc = _find(parsed["procedures"], "urine culture")
    assert inv is not None
    assert proc is None


def test_encounter_count_increments_across_distinct_encounters():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    fracture = _find(parsed["conditions"], "fracture of bone")
    assert fracture is not None
    assert fracture["encounter_count"] == 2


def test_boost_score_active_higher_than_resolved():
    today = date(2026, 2, 1)
    active = {"encounter_count": 2, "status": "active", "onset": "2025-01-01", "resolved": None}
    resolved = {"encounter_count": 2, "status": "resolved", "onset": "2025-01-01", "resolved": "2025-01-10"}
    assert calculate_boost_score(active, today) > calculate_boost_score(resolved, today)


def test_boost_score_recently_resolved_higher_than_old_resolved():
    today = date(2026, 2, 1)
    recent = {"encounter_count": 2, "status": "resolved", "resolved": "2026-01-20", "onset": "2025-12-01"}
    old = {"encounter_count": 2, "status": "resolved", "resolved": "2024-01-20", "onset": "2023-12-01"}
    assert calculate_boost_score(recent, today) > calculate_boost_score(old, today)


def test_find_matching_context_terms_returns_matches_for_correct_section():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    matches = find_matching_context_terms("frac", "chief_complaint", parsed, date(2026, 2, 1))
    terms = [item["term_lower"] for item in matches]
    assert "fracture of bone" in terms


def test_find_matching_context_terms_does_not_return_medications_for_chief_complaint():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    matches = find_matching_context_terms("mep", "chief_complaint", parsed, date(2026, 2, 1))
    assert matches == []


def test_find_matching_context_terms_returns_empty_for_empty_parsed_context():
    matches = find_matching_context_terms("frac", "chief_complaint", {}, date(2026, 2, 1))
    assert matches == []


def test_parser_does_not_crash_on_empty_string():
    parsed = parse_patient_context("")
    assert isinstance(parsed, dict)
    assert parsed.get("conditions") == []


def test_parser_does_not_crash_on_none_input():
    parsed = parse_patient_context(None)
    assert isinstance(parsed, dict)
    assert parsed.get("conditions") == []


def test_end_to_end_frac_query_contains_fracture_of_bone():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    matches = find_matching_context_terms("frac", "chief_complaint", parsed, date(2026, 2, 1))
    assert any(item.get("term", "").lower() == "fracture of bone" for item in matches)


def test_matching_allows_token_prefix_for_viral_diagnosis():
    parsed = parse_patient_context(SAMPLE_CONTEXT)
    matches = find_matching_context_terms("viral", "diagnosis", parsed, date(2026, 2, 1))
    assert any(item.get("term", "").lower() == "acute viral pharyngitis" for item in matches)
