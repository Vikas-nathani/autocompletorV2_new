import urllib.request
import urllib.parse
import json
import time

SOLR_URL = "http://localhost:8983/solr/umls_core"

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def solr_query(query_params):
    base = f"{SOLR_URL}/select?"
    parts = []
    for k, v in query_params.items():
        parts.append(f"{k}={urllib.parse.quote(str(v), safe=':*[]')}")
    url = base + "&".join(parts)
    start = time.time()
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    elapsed = (time.time() - start) * 1000
    return result, elapsed

def get_val(doc, field):
    val = doc.get(field, "")
    if isinstance(val, list):
        val = val[0] if val else ""
    return str(val)

def print_test(title, result, elapsed, show_docs=True, max_docs=5):
    num_found = result["response"]["numFound"]
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"  Found: {num_found:,} | Time: {elapsed:.1f}ms")
    print(f"{'='*65}")
    if show_docs and result["response"]["docs"]:
        print(f"  {'TERM':<45} {'TTY':<8} {'SEMANTIC TYPE':<30} {'SOURCE'}")
        print(f"  {'-'*45} {'-'*8} {'-'*30} {'-'*15}")
        for doc in result["response"]["docs"][:max_docs]:
            term     = get_val(doc, "term")[:44]
            tty      = get_val(doc, "tty")[:7]
            sem_type = get_val(doc, "semantic_type")[:29]
            source   = get_val(doc, "source")[:15]
            print(f"  {term:<45} {tty:<8} {sem_type:<30} {source}")

# ─────────────────────────────────────────
# SECTION 1 — SINGLE LETTER TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 1 — SINGLE LETTER PREFIX TESTS")
print("█"*65)

for letter in ["a", "b", "c", "d", "h", "m", "s", "t"]:
    result, elapsed = solr_query({"q": f"term_lower:{letter}", "rows": "3", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
    print_test(f"Single letter: '{letter}'", result, elapsed)

# ─────────────────────────────────────────
# SECTION 2 — COMMON ABBREVIATIONS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 2 — COMMON MEDICAL ABBREVIATIONS")
print("█"*65)

abbreviations = ["htn", "dm", "cad", "mi", "chf", "copd", "uti", "dvt", "pe", "hiv"]
for abbr in abbreviations:
    result, elapsed = solr_query({"q": f"term_lower:{abbr}", "rows": "3", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
    print_test(f"Abbreviation: '{abbr.upper()}'", result, elapsed)

# ─────────────────────────────────────────
# SECTION 3 — SPECIAL CHARACTER TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 3 — SPECIAL CHARACTER & EDGE CASES")
print("█"*65)

# Numbers
result, elapsed = solr_query({"q": "term_lower:111", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("Numbers: '111'", result, elapsed)

# Hyphenated terms
result, elapsed = solr_query({"q": "term_lower:anti", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("Hyphenated prefix: 'anti'", result, elapsed)

# Terms with parentheses
result, elapsed = solr_query({"q": "term_lower:5azpp", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("Parentheses term: '(5)azpp'", result, elapsed)

# Slash terms HIV/AIDS
result, elapsed = solr_query({"q": "term_lower:hiv", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("Slash term: 'hiv'", result, elapsed)

# Very long prefix
result, elapsed = solr_query({"q": "term_lower:methylenetetrahydrofolate", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("Long prefix: 'methylenetetrahydrofolate'", result, elapsed)

# ─────────────────────────────────────────
# SECTION 4 — WORD TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 4 — COMMON MEDICAL WORD TESTS")
print("█"*65)

words = ["heart", "lung", "brain", "blood", "bone", "nerve", "muscle", "kidney", "liver", "skin"]
for word in words:
    result, elapsed = solr_query({"q": f"term_lower:{word}", "rows": "3", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
    print_test(f"Body part: '{word}'", result, elapsed)

# ─────────────────────────────────────────
# SECTION 5 — ABBREVIATION FILTER TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 5 — ABBREVIATION vs FULL TERM COMPARISON")
print("█"*65)

# Search "htn" — show abbreviations only
result, elapsed = solr_query({"q": "term_lower:htn", "fq": "is_abbreviation:true", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("'htn' — abbreviations only", result, elapsed)

# Search "htn" — show non-abbreviations only
result, elapsed = solr_query({"q": "term_lower:htn", "fq": "is_abbreviation:false", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("'htn' — non-abbreviations only", result, elapsed)

# Search "dm" — abbreviations only
result, elapsed = solr_query({"q": "term_lower:dm", "fq": "is_abbreviation:true", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("'dm' — abbreviations only", result, elapsed)

# Search "dm" — non-abbreviations only
result, elapsed = solr_query({"q": "term_lower:dm", "fq": "is_abbreviation:false", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_test("'dm' — non-abbreviations only", result, elapsed)

# ─────────────────────────────────────────
# SECTION 6 — SOURCE COMPARISON TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 6 — SAME TERM ACROSS DIFFERENT SOURCES")
print("█"*65)

sources = ["SNOMEDCT_US", "ICD10CM", "MSH", "NCI", "RXNORM", "MEDCIN"]
for source in sources:
    result, elapsed = solr_query({"q": "term_lower:diab", "fq": f"source:{source}", "rows": "3", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
    print_test(f"'diab' in {source}", result, elapsed)

# ─────────────────────────────────────────
# SECTION 7 — SEMANTIC TYPE TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 7 — SEARCH WITHIN SPECIFIC SEMANTIC TYPES")
print("█"*65)

sem_types = [
    ("Disease or Syndrome",                "heart"),
    ("Therapeutic or Preventive Procedure","surg"),
    ("Pharmacologic Substance",            "aspir"),
    ("Finding",                            "pain"),
    ("Laboratory Procedure",               "blood"),
]
for sem_type, prefix in sem_types:
    result, elapsed = solr_query({
        "q":   f"term_lower:{prefix}",
        "fq":  f"semantic_type:{urllib.parse.quote(sem_type)}",
        "rows": "3",
        "wt":  "json",
        "fl":  "id,term,tty,semantic_type,source"
    })
    print_test(f"'{prefix}' in {sem_type}", result, elapsed)

# ─────────────────────────────────────────
# SECTION 8 — CONCEPT ID TESTS
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 8 — CONCEPT ID LOOKUP")
print("█"*65)

# Look up by concept_id
result, elapsed = solr_query({"q": "*:*", "fq": "concept_id:C0011849", "rows": "5", "wt": "json", "fl": "id,term,tty,concept_id,semantic_type,source"})
print_test("Concept ID: C0011849 (Diabetes Mellitus)", result, elapsed)

result, elapsed = solr_query({"q": "*:*", "fq": "concept_id:C0020538", "rows": "5", "wt": "json", "fl": "id,term,tty,concept_id,semantic_type,source"})
print_test("Concept ID: C0020538 (Hypertension)", result, elapsed)

# ─────────────────────────────────────────
# SECTION 9 — PERFORMANCE STRESS TEST
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 9 — PERFORMANCE STRESS TEST")
print("█"*65)

test_cases = [
    ("a",        "*:*",           "Single char — all"),
    ("diab",     "*:*",           "4 chars — all"),
    ("diab",     "tty:PT",        "4 chars — PT only"),
    ("diab",     "source:SNOMEDCT_US", "4 chars — SNOMED only"),
    ("diabetes", "ancestor_path:*A1.2.2*", "Full word — Disease"),
    ("heart",    "is_abbreviation:false",  "Word — no abbr"),
    ("mi",       "is_abbreviation:true",   "Abbr — abbr only"),
    ("cancer",   "tty:PT",        "Word — PT only"),
]

print(f"\n  {'TEST':<40} {'RESULTS':>10} {'TIME':>10}")
print(f"  {'-'*40} {'-'*10} {'-'*10}")
for prefix, fq, label in test_cases:
    url = f"{SOLR_URL}/select?q=term_lower:{prefix}&fq={urllib.parse.quote(fq, safe=':*[]')}&rows=0&wt=json"
    start = time.time()
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.loads(resp.read())
    elapsed = (time.time() - start) * 1000
    num_found = result["response"]["numFound"]
    print(f"  {label:<40} {num_found:>10,} {elapsed:>8.1f}ms")

print("\n" + "█"*65)
print("  ALL ADDITIONAL TESTS COMPLETE ✅")
print("█"*65)