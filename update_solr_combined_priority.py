import mysql.connector
import urllib.request
import json
import time

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "database": "umls_db",
    "user":     "umls_user",
    "password": "umls_password",
}

SOLR_URL   = "http://localhost:8983/solr/umls_core"
BATCH_SIZE = 5000
TOTAL_ROWS = 4271299

# Combined TTY + source priority mapping
# A SNOMEDCT_US PT gets 1, CHV PT gets 11, ensuring source quality is baked
# into the priority itself so only term_length remains as a tiebreaker.
COMBINED_PRIORITY = {
    ("SNOMEDCT_US", "PT"): 1,
    ("ICD10CM",     "PT"): 2,
    ("NCI",         "PT"): 3,
    ("MEDCIN",      "PT"): 4,
    ("RXNORM",      "PT"): 4,
    ("SNOMEDCT_US", "PN"): 5,
    ("SNOMEDCT_US", "SY"): 5,
    ("NCI",         "SY"): 6,
    ("ICD10CM",     "SY"): 6,
    ("MDR",         "PT"): 7,
    ("MSH",         "PT"): 8,
    ("RXNORM",      "SY"): 8,
    ("PDQ",         "PT"): 9,
    ("MMSL",        "PT"): 9,
    ("MTH",         "PN"): 10,
    ("CHV",         "PT"): 11,
    ("CHV",         "SY"): 12,
}
DEFAULT_PRIORITY = 13

# ─────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────
def send_atomic_update(docs):
    url     = f"{SOLR_URL}/update?commit=false"
    payload = json.dumps(docs).encode("utf-8")
    req     = urllib.request.Request(
        url,
        data    = payload,
        headers = {"Content-Type": "application/json"},
        method  = "POST"
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())

def commit_solr():
    url = f"{SOLR_URL}/update?commit=true"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())

def verify_query(query, desc):
    """Run a verification query and print results."""
    url = (f"{SOLR_URL}/select?q={urllib.parse.quote(query)}"
           f"&rows=5&wt=json"
           f"&fl=term,source,tty,tty_priority,term_length"
           f"&sort=term_word_count%20asc,tty_priority%20asc,term_length%20asc")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    
    docs = result.get("response", {}).get("docs", [])
    total = result.get("response", {}).get("numFound", 0)
    
    print(f"\n{desc} (query: {query}, {total:,} results total):")
    print(f"{'TERM':<30} {'SOURCE':<15} {'TTY':<5} {'PRIORITY':<10} {'LENGTH'}")
    print(f"{'-'*30} {'-'*15} {'-'*5} {'-'*10} {'-'*6}")
    for i, doc in enumerate(docs, start=1):
        term = doc.get("term", "N/A")[:28]
        source = doc.get("source", "N/A")[:13]
        tty = doc.get("tty", "N/A")
        priority = doc.get("tty_priority", "N/A")
        length = doc.get("term_length", "N/A")
        print(f"  {term:<30} {source:<15} {tty:<5} {priority:<10} {length}")

# ─────────────────────────────────────────
# STEP 1 — Connect to MySQL
# ─────────────────────────────────────────
print("Connecting to MySQL...")
read_conn = mysql.connector.connect(**MYSQL_CONFIG)
read_cur  = read_conn.cursor(buffered=False)
print("Connected to MySQL ✅")

# ─────────────────────────────────────────
# STEP 2 — Read id + tty + source from solr_preview
# ─────────────────────────────────────────
print("\nFetching id + tty + source from solr_preview...")
read_cur.execute("SELECT id, tty, source FROM solr_preview")
print("Query executed ✅")

# ─────────────────────────────────────────
# STEP 3 — Send atomic updates in batches
# ─────────────────────────────────────────
total_updated = 0
batch_number  = 0
start_time    = time.time()

# Priority distribution tracking
priority_counts = {i: 0 for i in range(1, 14)}

print(f"\nSending combined priority atomic updates to Solr in batches of {BATCH_SIZE}...")

while True:
    rows = read_cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

    batch_number += 1
    docs = []

    for row in rows:
        id_, tty, source = row
        priority = COMBINED_PRIORITY.get((source, tty), DEFAULT_PRIORITY)
        priority_counts[priority] = priority_counts.get(priority, 0) + 1

        doc = {
            "id": str(id_),
            "tty_priority": {"set": priority}
        }
        docs.append(doc)

    send_atomic_update(docs)
    total_updated += len(docs)

    if batch_number % 10 == 0:
        elapsed        = time.time() - start_time
        percentage     = (total_updated / TOTAL_ROWS) * 100
        rows_per_sec   = total_updated / elapsed if elapsed > 0 else 0
        remaining      = (TOTAL_ROWS - total_updated) / rows_per_sec if rows_per_sec > 0 else 0
        mins_remaining = remaining / 60
        print(f"  [{percentage:.1f}%] {total_updated:,} / {TOTAL_ROWS:,} docs | "
              f"{rows_per_sec:,.0f} docs/sec | "
              f"~{mins_remaining:.1f} mins remaining")

print(f"\nAll batches sent! Total updated: {total_updated:,} ✅")

# ─────────────────────────────────────────
# STEP 4 — Final commit
# ─────────────────────────────────────────
print("\nCommitting to Solr...")
commit_solr()
print("Committed ✅")

# ─────────────────────────────────────────
# STEP 5 — Show combined priority distribution
# ─────────────────────────────────────────
print("\nCombined Priority Distribution:")
priority_names = {
    1: "SNOMEDCT_US PT (best)",
    2: "ICD10CM PT",
    3: "NCI PT",
    4: "MEDCIN/RXNORM PT",
    5: "SNOMEDCT_US PN/SY",
    6: "NCI/ICD10CM SY",
    7: "MDR PT",
    8: "MSH PT / RXNORM SY",
    9: "PDQ/MMSL PT",
    10: "MTH PN",
    11: "CHV PT",
    12: "CHV SY",
    13: "Others",
}
for priority in range(1, 14):
    count = priority_counts.get(priority, 0)
    desc = priority_names.get(priority, f"Priority {priority}")
    if count > 0:
        print(f"  {priority:2d}. {desc:<35} {count:>10,}")

# ─────────────────────────────────────────
# STEP 6 — Verify queries with new combined sort
# ─────────────────────────────────────────
import urllib.parse

print("\n" + "="*80)
print("VERIFICATION QUERIES (sort: term_word_count asc, tty_priority asc, term_length asc)")
print("="*80)

verify_query("term_lower:pneum", "Verification: 'pneum' prefix")
verify_query("term_lower:diab", "Verification: 'diab' prefix")
verify_query("term_lower:hypert", "Verification: 'hypert' prefix")

# ─────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────
read_cur.close()
read_conn.close()

print("\n✅ Combined priority update complete!")
