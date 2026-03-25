import urllib.request
import urllib.parse
import json
import time

SOLR_URL = "http://localhost:8983/solr/umls_core"

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def solr_query_raw(url):
    start = time.time()
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.loads(resp.read())
    elapsed = (time.time() - start) * 1000
    return result, elapsed

def get_val(doc, field):
    val = doc.get(field, "")
    if isinstance(val, list):
        val = val[0] if val else ""
    return str(val)

def print_test(title, result, elapsed, max_docs=5):
    num_found = result["response"]["numFound"]
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"  Found: {num_found:,} | Time: {elapsed:.1f}ms")
    print(f"{'='*65}")
    if result["response"]["docs"]:
        print(f"  {'TERM':<45} {'TTY':<8} {'SEMANTIC TYPE':<30} {'SOURCE'}")
        print(f"  {'-'*45} {'-'*8} {'-'*30} {'-'*15}")
        for doc in result["response"]["docs"][:max_docs]:
            term     = get_val(doc, "term")[:44]
            tty      = get_val(doc, "tty")[:7]
            sem_type = get_val(doc, "semantic_type")[:29]
            source   = get_val(doc, "source")[:15]
            print(f"  {term:<45} {tty:<8} {sem_type:<30} {source}")

# ─────────────────────────────────────────
# SECTION 7 FIXED — SEMANTIC TYPE FILTER
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SECTION 7 FIXED — SEARCH WITHIN SPECIFIC SEMANTIC TYPES")
print("  Fix: wrap semantic type in quotes for exact phrase match")
print("█"*65)

tests = [
    ("heart",  "Disease or Syndrome"),
    ("surg",   "Therapeutic or Preventive Procedure"),
    ("aspir",  "Pharmacologic Substance"),
    ("pain",   "Finding"),
    ("blood",  "Laboratory Procedure"),
    ("diab",   "Disease or Syndrome"),
    ("cancer", "Disease or Syndrome"),
    ("tumor",  "Neoplastic Process"),
    ("infect", "Disease or Syndrome"),
    ("drug",   "Pharmacologic Substance"),
    ("gene",   "Gene or Genome"),
    ("cell",   "Cell"),
    ("virus",  "Virus"),
    ("bacter", "Bacterium"),
    ("enzyme", "Enzyme"),
]

for prefix, sem_type in tests:
    # Key fix: wrap semantic_type value in %22 (URL encoded quotes)
    url = (f"{SOLR_URL}/select?"
           f"q=term_lower:{urllib.parse.quote(prefix)}"
           f"&fq=semantic_type:%22{urllib.parse.quote(sem_type)}%22"
           f"&rows=5&wt=json&fl=id,term,tty,semantic_type,source")
    result, elapsed = solr_query_raw(url)
    print_test(f"'{prefix}' in [{sem_type}]", result, elapsed)

# ─────────────────────────────────────────
# BONUS — Combined prefix + semantic type + source
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  BONUS — COMBINED: prefix + semantic type + source")
print("█"*65)

combined_tests = [
    ("diab",   "Disease or Syndrome",                "SNOMEDCT_US"),
    ("heart",  "Disease or Syndrome",                "ICD10CM"),
    ("aspir",  "Pharmacologic Substance",             "RXNORM"),
    ("pain",   "Finding",                            "MEDCIN"),
    ("cancer", "Neoplastic Process",                 "NCI"),
    ("blood",  "Laboratory Procedure",               "LNC"),
]

for prefix, sem_type, source in combined_tests:
    url = (f"{SOLR_URL}/select?"
           f"q=term_lower:{urllib.parse.quote(prefix)}"
           f"&fq=semantic_type:%22{urllib.parse.quote(sem_type)}%22"
           f"&fq=source:{source}"
           f"&rows=5&wt=json&fl=id,term,tty,semantic_type,source")
    result, elapsed = solr_query_raw(url)
    print_test(f"'{prefix}' + [{sem_type}] + {source}", result, elapsed)

# ─────────────────────────────────────────
# SUMMARY TABLE
# ─────────────────────────────────────────
print("\n" + "█"*65)
print("  SUMMARY — All Semantic Type Filters")
print("█"*65)
print(f"\n  {'PREFIX':<10} {'SEMANTIC TYPE':<40} {'RESULTS':>10} {'TIME':>10}")
print(f"  {'-'*10} {'-'*40} {'-'*10} {'-'*10}")

summary_tests = [
    ("heart",  "Disease or Syndrome"),
    ("surg",   "Therapeutic or Preventive Procedure"),
    ("aspir",  "Pharmacologic Substance"),
    ("pain",   "Finding"),
    ("blood",  "Laboratory Procedure"),
    ("diab",   "Disease or Syndrome"),
    ("cancer", "Neoplastic Process"),
    ("gene",   "Gene or Genome"),
    ("virus",  "Virus"),
    ("bacter", "Bacterium"),
]

for prefix, sem_type in summary_tests:
    url = (f"{SOLR_URL}/select?"
           f"q=term_lower:{urllib.parse.quote(prefix)}"
           f"&fq=semantic_type:%22{urllib.parse.quote(sem_type)}%22"
           f"&rows=0&wt=json")
    result, elapsed = solr_query_raw(url)
    num_found = result["response"]["numFound"]
    print(f"  {prefix:<10} {sem_type:<40} {num_found:>10,} {elapsed:>8.1f}ms")

print("\n" + "█"*65)
print("  SEMANTIC TYPE FILTER FIX COMPLETE ✅")
print("█"*65)