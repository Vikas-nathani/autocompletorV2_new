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

# ─────────────────────────────────────────
# STEP 1 — Connect to MySQL
# ─────────────────────────────────────────
print("Connecting to MySQL...")
read_conn = mysql.connector.connect(**MYSQL_CONFIG)
read_cur  = read_conn.cursor(buffered=False)
print("Connected to MySQL ✅")

# ─────────────────────────────────────────
# STEP 2 — Read id + source_priority from solr_preview
# ─────────────────────────────────────────
print("\nFetching id + source_priority from solr_preview...")
read_cur.execute("SELECT id, source_priority FROM solr_preview")
print("Query executed ✅")

# ─────────────────────────────────────────
# STEP 3 — Send atomic updates in batches
# ─────────────────────────────────────────
total_updated = 0
batch_number  = 0
start_time    = time.time()
priority_counts = {}

print(f"\nSending atomic updates to Solr in batches of {BATCH_SIZE}...")

while True:
    rows = read_cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

    batch_number += 1
    docs = []

    for row in rows:
        id_, source_priority = row
        priority = int(source_priority) if source_priority else 10
        priority_counts[priority] = priority_counts.get(priority, 0) + 1

        doc = {
            "id": str(id_),
            "source_priority": {"set": priority}
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
# STEP 5 — Show distribution
# ─────────────────────────────────────────
source_names = {
    1: "SNOMEDCT_US", 2: "ICD10CM",  3: "NCI",
    4: "RXNORM",      5: "MSH",      6: "MEDCIN",
    7: "LNC",         8: "ICD10PCS", 9: "OMIM",
    10: "Others"
}
print("\nSource Priority Distribution:")
for priority, count in sorted(priority_counts.items()):
    name = source_names.get(priority, "Others")
    print(f"  Priority {priority} ({name}): {count:,}")

# ─────────────────────────────────────────
# STEP 6 — Verify sorting works
# ─────────────────────────────────────────
print("\nVerifying sort by tty_priority + source_priority...")
url = (f"{SOLR_URL}/select?q=term_lower:diab"
       f"&rows=10&wt=json"
       f"&fl=term,tty,tty_priority,source,source_priority"
       f"&sort=tty_priority%20asc,source_priority%20asc")
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read())
    print(f"\nTop 10 results for 'diab' sorted by TTY + Source priority:")
    print(f"{'TERM':<45} {'TTY':<6} {'P':<3} {'SOURCE':<15} {'SP'}")
    print(f"{'-'*45} {'-'*6} {'-'*3} {'-'*15} {'-'*3}")
    for doc in result["response"]["docs"]:
        term     = doc.get("term", [""])[0] if isinstance(doc.get("term"), list) else doc.get("term", "")
        tty      = doc.get("tty", "")
        tp       = doc.get("tty_priority", "")
        source   = doc.get("source", "")
        sp       = doc.get("source_priority", "")
        print(f"  {str(term)[:43]:<45} {tty:<6} {tp:<3} {source:<15} {sp}")

# ─────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────
read_cur.close()
read_conn.close()

print("\n✅ source_priority update complete!")