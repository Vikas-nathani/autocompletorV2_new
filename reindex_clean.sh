#!/bin/bash

# ──────────────────────────────────────────────────────────────
# Clean Reindexing Script for term_suggest Fix
# ──────────────────────────────────────────────────────────────

set -e

SOLR_URL="http://localhost:8983/solr/umls_core"

echo "════════════════════════════════════════════════════════════"
echo "  SOLR INDEX CLEANUP & REINDEX"
echo "════════════════════════════════════════════════════════════"

# Step 1: Delete all documents from Solr
echo ""
echo "Step 1: Deleting all documents from Solr..."
curl -s -X POST "$SOLR_URL/update?commit=true" \
  -H "Content-Type: application/json" \
  -d '{"delete":{"query":"*:*"}}' > /dev/null

echo "✅ Index cleared"

# Step 2: Verify index is empty
echo ""
echo "Step 2: Verifying index is empty..."
COUNT=$(curl -s "$SOLR_URL/select?q=*:*&rows=0&wt=json" | grep -o '"numFound":[0-9]*' | cut -d: -f2)
echo "   Documents remaining: $COUNT"

if [ "$COUNT" != "0" ]; then
  echo "⚠️  Warning: Index still has $COUNT documents. Waiting 5 seconds before retry..."
  sleep 5
  echo "   Second attempt..."
  curl -s -X POST "$SOLR_URL/update?commit=true" \
    -H "Content-Type: application/json" \
    -d '{"delete":{"query":"*:*"}}' > /dev/null
fi

# Step 3: Run Python reindexing script
echo ""
echo "Step 3: Re-indexing from MySQL (this may take 10-30 minutes)..."
echo "   Running: python3 index_solr.py"
echo ""

cd "$(dirname "$0")"
python3 index_solr.py

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ REINDEXING COMPLETE!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Verifying..."
FINAL_COUNT=$(curl -s "$SOLR_URL/select?q=*:*&rows=0&wt=json" | grep -o '"numFound":[0-9]*' | cut -d: -f2)
echo "Final document count: $FINAL_COUNT"

# Test a simple prefix query
echo ""
echo "Testing prefix autocomplete (hyperten):"
curl -s "$SOLR_URL/select?q=term_suggest:hyperten&rows=3&fl=term,term_word_count,tty_priority,source_priority&sort=term_word_count+asc,tty_priority+asc,source_priority+asc,term_length+asc&wt=json" | \
  python3 -m json.tool | grep -A 5 '"term"'

echo ""
echo "✅ Test complete. If you see 'Hypertension' or similar above, prefix tokenization is working!"
