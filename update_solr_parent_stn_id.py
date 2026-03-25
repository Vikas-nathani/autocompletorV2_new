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
# HELPER — Send atomic update to Solr
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
# STEP 2 — Read id + parent_stn_id from solr_preview
# ─────────────────────────────────────────
print("\nFetching id + parent_stn_id from solr_preview...")
read_cur.execute("""
    SELECT id, parent_stn_id
    FROM solr_preview
""")
print("Query executed ✅")

# ─────────────────────────────────────────
# STEP 3 — Send atomic updates in batches
# ─────────────────────────────────────────
total_updated = 0
batch_number  = 0
start_time    = time.time()

print(f"\nSending atomic updates to Solr in batches of {BATCH_SIZE}...")

while True:
    rows = read_cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

    batch_number += 1
    docs = []

    for row in rows:
        id_, parent_stn_id = row
        doc = {
            "id": str(id_),
            "parent_stn_id": {"set": int(parent_stn_id) if parent_stn_id is not None else None}
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
# STEP 5 — Verify
# ─────────────────────────────────────────
print("\nVerifying...")

# Check a known document
url = f"{SOLR_URL}/select?q=id:1042&wt=json&fl=id,term,stn_path,parent_stn,parent_stn_id"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read())
    docs = result["response"]["docs"]
    if docs:
        doc = docs[0]
        print(f"\nSample document (id=1042):")
        for k, v in doc.items():
            val = v[0] if isinstance(v, list) else v
            print(f"  {k}: {val}")

# Check filter by parent_stn_id
url = f"{SOLR_URL}/select?q=*:*&fq=parent_stn_id:56&rows=0&wt=json"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read())
    print(f"\nTerms with parent_stn_id=56 (A2): {result['response']['numFound']:,}")

# Check null parent_stn_id (root terms)
url = f"{SOLR_URL}/select?q=*:*&fq=-parent_stn_id:*&rows=0&wt=json"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read())
    print(f"Terms with no parent_stn_id (root): {result['response']['numFound']:,}")

# ─────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────
read_cur.close()
read_conn.close()

print("\n✅ parent_stn_id update complete!") 