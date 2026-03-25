"""Semantic type configuration for section-aware clinical note completion.

This module is the single source of truth for mapping each clinical note
section to the semantic types that are valid for that section.
"""

from __future__ import annotations

SECTION_SEMANTIC_TYPES: dict[str, list[str]] = {
    "chief_complaint": [
        "Sign or Symptom",
        "Finding",
        "Disease or Syndrome",
        "Injury or Poisoning",
        "Mental or Behavioral Dysfunction",
    ],
    "diagnosis": [
        "Disease or Syndrome",
        "Neoplastic Process",
        "Mental or Behavioral Dysfunction",
        "Injury or Poisoning",
        "Congenital Abnormality",
        "Finding",
        "Pathologic Function",
    ],
    "investigations": [
        "Laboratory Procedure",
        "Diagnostic Procedure",
        "Laboratory or Test Result",
        "Clinical Attribute",
        "Intellectual Product",
    ],
    "medications": [
        "Pharmacologic Substance",
        "Clinical Drug",
        "Hormone",
        "Antibiotic",
        "Organic Chemical",
        "Amino Acid, Peptide, or Protein",
    ],
    "procedures": [
        "Therapeutic or Preventive Procedure",
        "Diagnostic Procedure",
        "Health Care Activity",
        "Laboratory Procedure",
    ],
    "advice": [
        "Therapeutic or Preventive Procedure",
        "Health Care Activity",
        "Finding",
        "Sign or Symptom",
        "Disease or Syndrome",
    ],
}

CHV_EXCLUDED_SECTIONS = {
    "chief_complaint",
    "diagnosis",
    "investigations",
}

MEDICATION_TRUSTED_SOURCES = [
    "RXNORM",
    "SNOMEDCT_US",
    "NCI",
    "MSH",
    "MMSL",
    "LNC",
]

DIAGNOSIS_CANONICAL_DIABETES = "diabetes mellitus"


def _quote_semantic_type(value: str) -> str:
    escaped = value.replace('"', '\\"')
    if " " in escaped or "," in escaped or "-" in escaped:
        return f'"{escaped}"'
    return escaped


def get_section_fq(section: str) -> str:
    """Build the positive Solr semantic-type filter query for a section."""
    if section not in SECTION_SEMANTIC_TYPES:
        raise ValueError(f"Unknown section: {section}")

    semantic_types = SECTION_SEMANTIC_TYPES[section]
    clauses = [_quote_semantic_type(item) for item in semantic_types]
    return "semantic_type:(" + " OR ".join(clauses) + ")"
