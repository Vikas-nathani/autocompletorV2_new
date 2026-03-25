import json
import math
import time
import requests

SOLR = "http://localhost:8983/solr/umls_core"
SELECT_URL = f"{SOLR}/select"
UPDATE_URL = f"{SOLR}/update"

BATCH_SIZE = 2000
COMMIT_EVERY = 20000
MAX_RETRIES = 3
MISSING_ONLY = True


def compute_word_count(term: str) -> int:
    term = (term or "").strip()
    if not term:
        return 0
    return len(term.split())


def compute_term_length(term: str) -> int:
    return len((term or "").strip())


def fetch_total_docs() -> int:
    params = {"q": "*:*", "rows": 0, "wt": "json"}
    if MISSING_ONLY:
        params["fq"] = "-term_word_count:[* TO *]"
    r = requests.get(SELECT_URL, params=params, timeout=30)
    r.raise_for_status()
    return int(r.json()["response"]["numFound"])


def fetch_batch(cursor_mark: str):
    params = {
        "q": "*:*",
        "sort": "id asc",
        "cursorMark": cursor_mark,
        "rows": BATCH_SIZE,
        "fl": "id,term",
        "wt": "json",
    }
    if MISSING_ONLY:
        params["fq"] = "-term_word_count:[* TO *]"
    r = requests.get(SELECT_URL, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    return payload["response"]["docs"], payload.get("nextCursorMark", cursor_mark)


def build_updates(docs):
    updates = []
    for doc in docs:
        doc_id = doc.get("id")
        term_val = doc.get("term", "")
        if isinstance(term_val, list):
            term_val = term_val[0] if term_val else ""

        updates.append(
            {
                "id": doc_id,
                "term_word_count": {"set": compute_word_count(str(term_val))},
                "term_length": {"set": compute_term_length(str(term_val))},
            }
        )
    return updates


def post_updates(updates, commit=False):
    params = {"commit": "true" if commit else "false", "wt": "json"}
    headers = {"Content-Type": "application/json"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(UPDATE_URL, params=params, data=json.dumps(updates), headers=headers, timeout=120)
            r.raise_for_status()
            return
        except Exception:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 * attempt)


def commit_now():
    r = requests.get(UPDATE_URL, params={"commit": "true", "wt": "json"}, timeout=60)
    r.raise_for_status()


def main():
    total = fetch_total_docs()
    scope = "missing docs only" if MISSING_ONLY else "all docs"
    print(f"Total docs ({scope}): {total}", flush=True)

    cursor = "*"
    processed = 0
    since_commit = 0
    start = time.time()

    while True:
        docs, next_cursor = fetch_batch(cursor)
        if not docs:
            break

        updates = build_updates(docs)
        post_updates(updates, commit=False)

        batch_count = len(docs)
        processed += batch_count
        since_commit += batch_count

        elapsed = max(time.time() - start, 1e-6)
        dps = processed / elapsed
        pct = (processed / total) * 100 if total else 100.0
        remaining = max(total - processed, 0)
        eta_sec = int(remaining / dps) if dps > 0 else -1

        eta_text = "unknown" if eta_sec < 0 else f"{eta_sec // 3600:02d}:{(eta_sec % 3600) // 60:02d}:{eta_sec % 60:02d}"
        print(f"Processed {processed}/{total} ({pct:.2f}%) | {dps:.1f} docs/s | ETA {eta_text}", flush=True)

        if since_commit >= COMMIT_EVERY:
            commit_now()
            since_commit = 0
            print("Committed intermediate batch", flush=True)

        if next_cursor == cursor:
            break
        cursor = next_cursor

    commit_now()
    total_elapsed = time.time() - start
    print(f"Done. Updated {processed} docs in {total_elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
