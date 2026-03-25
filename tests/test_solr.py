import urllib.request
import json
import time

SOLR_URL = "http://localhost:8983/solr/umls_core"

# ─────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────
def solr_query(params):
    url = f"{SOLR_URL}/select?" + "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    start = time.time()
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    elapsed = (time.time() - start) * 1000
    return result, elapsed

import urllib.parse

def print_results(title, result, elapsed, show_docs=True):
    num_found = result["response"]["numFound"]
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"  Found: {num_found:,} results | Time: {elapsed:.1f}ms")
    print(f"{'='*60}")
    if show_docs:
        for doc in result["response"]["docs"]:
            term     = doc.get('term', '')
            tty      = doc.get('tty', '')
            sem_type = doc.get('semantic_type', '')
            source   = doc.get('source', '')
            # Handle list or string
            if isinstance(term, list):     term     = term[0]
            if isinstance(tty, list):      tty      = tty[0]
            if isinstance(sem_type, list): sem_type = sem_type[0]
            if isinstance(source, list):   source   = source[0]
            print(f"  {str(term)[:50]:<50} | {str(tty):<8} | {str(sem_type)[:30]:<30} | {source}")

# ─────────────────────────────────────────
# ROUND 1 — BASIC SANITY TESTS
# ─────────────────────────────────────────
print("\n" + "█"*60)
print("  ROUND 1 — BASIC SANITY TESTS")
print("█"*60)

# Total count
result, elapsed = solr_query({"q": "*:*", "rows": "0", "wt": "json"})
print_results("Total document count", result, elapsed, show_docs=False)

# Exact term search
result, elapsed = solr_query({"q": "term:Diabetes+Mellitus", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source,code"})
print_results("Exact search: 'Diabetes Mellitus'", result, elapsed)

# Prefix search — diab
result, elapsed = solr_query({"q": "term_lower:diab", "rows": "10", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Prefix search: 'diab'", result, elapsed)

# Prefix search — hyp
result, elapsed = solr_query({"q": "term_lower:hyp", "rows": "10", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Prefix search: 'hyp'", result, elapsed)

# Prefix search — card
result, elapsed = solr_query({"q": "term_lower:card", "rows": "10", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Prefix search: 'card'", result, elapsed)

# ─────────────────────────────────────────
# ROUND 2 — FILTER TESTS
# ─────────────────────────────────────────
print("\n" + "█"*60)
print("  ROUND 2 — FILTER TESTS")
print("█"*60)

# Only PT terms
result, elapsed = solr_query({"q": "*:*", "fq": "tty:PT", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Filter: Only PT (Preferred Terms)", result, elapsed)

# Only SNOMEDCT_US
result, elapsed = solr_query({"q": "*:*", "fq": "source:SNOMEDCT_US", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Filter: Only SNOMEDCT_US", result, elapsed)

# Only ICD10CM
result, elapsed = solr_query({"q": "*:*", "fq": "source:ICD10CM", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Filter: Only ICD10CM", result, elapsed)

# Only abbreviations
result, elapsed = solr_query({"q": "*:*", "fq": "is_abbreviation:true", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Filter: Only abbreviations", result, elapsed)

# Only non-abbreviations
result, elapsed = solr_query({"q": "*:*", "fq": "is_abbreviation:false", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,source"})
print_results("Filter: Only non-abbreviations", result, elapsed)

# ─────────────────────────────────────────
# ROUND 3 — HIERARCHY TESTS
# ─────────────────────────────────────────
print("\n" + "█"*60)
print("  ROUND 3 — HIERARCHY TESTS")
print("█"*60)

# All terms under Disease or Syndrome A1.2.2
result, elapsed = solr_query({"q": "*:*", "fq": "ancestor_path:*A1.2.2*", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,stn_path"})
print_results("Hierarchy: All under Disease or Syndrome (A1.2.2)", result, elapsed)

# All terms under B tree (Events)
result, elapsed = solr_query({"q": "*:*", "fq": "ancestor_path:B*", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,stn_path"})
print_results("Hierarchy: All under B tree (Events)", result, elapsed)

# Terms at depth level 7
result, elapsed = solr_query({"q": "*:*", "fq": "depth_level:7", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,stn_path,depth_level"})
print_results("Hierarchy: Terms at depth level 7", result, elapsed)

# Terms at depth level 8 (deepest)
result, elapsed = solr_query({"q": "*:*", "fq": "depth_level:8", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,stn_path,depth_level"})
print_results("Hierarchy: Terms at depth level 8 (deepest)", result, elapsed)

# Only leaf node terms
result, elapsed = solr_query({"q": "*:*", "fq": "is_leaf:true", "rows": "5", "wt": "json", "fl": "id,term,tty,semantic_type,stn_path"})
print_results("Hierarchy: Only leaf node terms", result, elapsed)

# ─────────────────────────────────────────
# ROUND 4 — COMBINED TESTS (Real Doctor Scenarios)
# ─────────────────────────────────────────
print("\n" + "█"*60)
print("  ROUND 4 — COMBINED TESTS (Real Doctor Scenarios)")
print("█"*60)

# Doctor types "diab" + Disease only + no abbreviations
result, elapsed = solr_query({
    "q":   "term_lower:diab",
    "fq":  "ancestor_path:*A1.2.2*",
    "fq2": "is_abbreviation:false",
    "rows": "10",
    "wt":  "json",
    "fl":  "id,term,tty,semantic_type,source"
})
# Manual multi fq
url = f"{SOLR_URL}/select?q=term_lower:diab&fq=ancestor_path:*A1.2.2*&fq=is_abbreviation:false&rows=10&wt=json&fl=id,term,tty,semantic_type,source"
start = time.time()
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
elapsed = (time.time() - start) * 1000
print_results("Combined: 'diab' + Disease or Syndrome + no abbreviations", result, elapsed)

# Doctor types "hyp" + only SNOMEDCT_US + only PT
url = f"{SOLR_URL}/select?q=term_lower:hyp&fq=source:SNOMEDCT_US&fq=tty:PT&rows=10&wt=json&fl=id,term,tty,semantic_type,source"
start = time.time()
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
elapsed = (time.time() - start) * 1000
print_results("Combined: 'hyp' + SNOMEDCT_US + PT only", result, elapsed)

# Doctor types "card" + depth level 4 to 6
url = f"{SOLR_URL}/select?q=term_lower:card&fq=depth_level:[4+TO+6]&rows=10&wt=json&fl=id,term,tty,semantic_type,depth_level"
start = time.time()
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
elapsed = (time.time() - start) * 1000
print_results("Combined: 'card' + depth level 4 to 6", result, elapsed)

# Doctor types "neo" + only PT + Disease or Syndrome
url = f"{SOLR_URL}/select?q=term_lower:neo&fq=tty:PT&fq=ancestor_path:*A1.2.2*&rows=10&wt=json&fl=id,term,tty,semantic_type,source"
start = time.time()
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
elapsed = (time.time() - start) * 1000
print_results("Combined: 'neo' + PT only + Disease or Syndrome", result, elapsed)

# ─────────────────────────────────────────
# ROUND 5 — PERFORMANCE TESTS
# ─────────────────────────────────────────
print("\n" + "█"*60)
print("  ROUND 5 — PERFORMANCE TESTS")
print("█"*60)

prefixes = ["a", "ab", "di", "diab", "hypert", "cardio", "neuro", "infect", "cancer", "tumor"]

print(f"\n{'Prefix':<12} {'Results':>10} {'Time (ms)':>12}")
print("-" * 36)
for prefix in prefixes:
    url = f"{SOLR_URL}/select?q=term_lower:{prefix}&rows=0&wt=json"
    start = time.time()
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.loads(resp.read())
    elapsed = (time.time() - start) * 1000
    num_found = result["response"]["numFound"]
    print(f"  {prefix:<10} {num_found:>10,} {elapsed:>10.1f}ms")

# Stats — terms per source
print("\n" + "="*60)
print("  Stats: Top 10 sources by term count")
print("="*60)
url = f"{SOLR_URL}/select?q=*:*&rows=0&wt=json&facet=true&facet.field=source&facet.limit=10"
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
facets = result["facet_counts"]["facet_fields"]["source"]
for i in range(0, min(20, len(facets)), 2):
    print(f"  {facets[i]:<30} {facets[i+1]:>10,}")

# Stats — terms per semantic type
print("\n" + "="*60)
print("  Stats: Top 10 semantic types by term count")
print("="*60)
url = f"{SOLR_URL}/select?q=*:*&rows=0&wt=json&facet=true&facet.field=semantic_type&facet.limit=10"
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
facets = result["facet_counts"]["facet_fields"]["semantic_type"]
for i in range(0, min(20, len(facets)), 2):
    print(f"  {facets[i]:<40} {facets[i+1]:>10,}")

# Stats — terms per depth level
print("\n" + "="*60)
print("  Stats: Terms per depth level")
print("="*60)
url = f"{SOLR_URL}/select?q=*:*&rows=0&wt=json&facet=true&facet.field=depth_level&facet.limit=10"
with urllib.request.urlopen(url, timeout=30) as resp:
    result = json.loads(resp.read())
facets = result["facet_counts"]["facet_fields"]["depth_level"]
for i in range(0, min(20, len(facets)), 2):
    print(f"  Depth {facets[i]:<5} {facets[i+1]:>10,} terms")

print("\n" + "█"*60)
print("  ALL TESTS COMPLETE ✅")
print("█"*60)