import mysql.connector
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

BATCH_SIZE = 10000
TOTAL_ROWS = 4271299

# ─────────────────────────────────────────
# STEP 1 — Connect to MySQL
# ─────────────────────────────────────────
print("Connecting to MySQL...")
my_conn = mysql.connector.connect(**MYSQL_CONFIG)
my_cur = my_conn.cursor()
print("Connected to MySQL ✅")

# ─────────────────────────────────────────
# STEP 2 — Read cursor (server side for large data)
# ─────────────────────────────────────────
read_conn = mysql.connector.connect(**MYSQL_CONFIG)
read_cur = read_conn.cursor(buffered=False)

print("\nExecuting join query...")
read_cur.execute("""
    SELECT
        t.id,
        t.term,
        t.term_lower,
        t.is_abbreviation,
        t.tty,
        t.term_id,
        t.concept_id,
        t.semantic_type,
        t.stn,
        t.source,
        t.code,
        s.stn_path,
        s.parent_stn,
        s.ancestor_path,
        s.depth_level,
        s.semantic_type_id,
        s.is_leaf
    FROM terms t
    JOIN stn_tree s ON t.stn_id = s.stn_id
""")
print("Query executed ✅")

# ─────────────────────────────────────────
# STEP 3 — Insert in batches
# ─────────────────────────────────────────
insert_sql = """
    INSERT INTO solr_preview (
        id, term, term_lower, is_abbreviation, tty,
        term_id, concept_id, semantic_type, stn,
        source, code, stn_path, parent_stn,
        ancestor_path, depth_level, semantic_type_id, is_leaf
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

total_inserted = 0
batch_number = 0
start_time = time.time()

print(f"\nInserting into solr_preview in batches of {BATCH_SIZE}...")

while True:
    rows = read_cur.fetchmany(BATCH_SIZE)
    if not rows:
        break

    batch_number += 1
    my_cur.executemany(insert_sql, rows)
    my_conn.commit()
    total_inserted += len(rows)

    if batch_number % 10 == 0:
        elapsed = time.time() - start_time
        percentage = (total_inserted / TOTAL_ROWS) * 100
        rows_per_sec = total_inserted / elapsed if elapsed > 0 else 0
        remaining = (TOTAL_ROWS - total_inserted) / rows_per_sec if rows_per_sec > 0 else 0
        mins_remaining = remaining / 60
        print(f"  [{percentage:.1f}%] {total_inserted:,} / {TOTAL_ROWS:,} rows | "
              f"{rows_per_sec:,.0f} rows/sec | "
              f"~{mins_remaining:.1f} mins remaining")

print(f"\nTotal inserted: {total_inserted:,} rows ✅")

# ─────────────────────────────────────────
# STEP 4 — Verify
# ─────────────────────────────────────────
print("\nVerifying...")

my_cur.execute("SELECT COUNT(*) FROM solr_preview")
total = my_cur.fetchone()[0]
print(f"Total rows in solr_preview: {total:,}")

my_cur.execute("SELECT COUNT(*) FROM solr_preview WHERE ancestor_path IS NULL")
null_ancestor = my_cur.fetchone()[0]
print(f"Null ancestor_path rows: {null_ancestor:,}")

my_cur.execute("SELECT COUNT(*) FROM solr_preview WHERE semantic_type_id IS NULL")
null_tui = my_cur.fetchone()[0]
print(f"Null semantic_type_id rows: {null_tui:,}")

print("\nSample rows:")
my_cur.execute("""
    SELECT id, term, tty, semantic_type, stn_path, 
           ancestor_path, depth_level, semantic_type_id, is_leaf
    FROM solr_preview
    LIMIT 5
""")
for row in my_cur.fetchall():
    print(row)

# ─────────────────────────────────────────
# STEP 5 — Test Disease or Syndrome query
# ─────────────────────────────────────────
print("\nTesting Disease or Syndrome filter...")
my_cur.execute("""
    SELECT COUNT(*) FROM solr_preview
    WHERE ancestor_path LIKE '%A1.2.2%'
""")
disease_count = my_cur.fetchone()[0]
print(f"Terms under Disease or Syndrome (A1.2.2): {disease_count:,}")

# ─────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────
read_cur.close()
read_conn.close()
my_cur.close()
my_conn.close()

print("\n✅ solr_preview population complete!")