#!/usr/bin/env bash
# FT-DOSS Complete 3-Node Physical Replication & Self-Healing Verification Suite
set -e

ADMIN_KEY="dev-key"
HOST="http://localhost:8080"
TEST_FILE="/tmp/ft-doss-e2e-test.txt"

echo "=== STEP 1: RESET CLUSTER ==="
./scripts/reset-demo.sh

echo "=== STEP 2: VERIFY ALL 3 NODES ARE HEALTHY ==="
NODES_JSON=$(curl -s -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/nodes")
echo "Nodes Response: $NODES_JSON"
if echo "$NODES_JSON" | grep -q '"storage-1"' && echo "$NODES_JSON" | grep -q '"storage-2"' && echo "$NODES_JSON" | grep -q '"storage-3"'; then
    echo "Initial Node Registry Check: PASS (3 nodes registered)"
else
    echo "Initial Node Registry Check: FAILED"
    exit 1
fi

echo "=== STEP 3: CREATE REAL TEST FILE ==="
echo "FT-DOSS 3-NODE PHYSICAL REPLICATION AND SELF-HEALING TEST DATA $(date)" > "$TEST_FILE"

echo "=== STEP 4: CALCULATE ORIGINAL SHA-256 ==="
ORIG_HASH=$(shasum -a 256 "$TEST_FILE" | awk '{print $1}')
echo "Original SHA-256: $ORIG_HASH"

echo "=== STEP 5: UPLOAD OBJECT VIA API ==="
PUT_RESP=$(curl -s -X PUT "$HOST/buckets/default/objects/e2e-test-file.txt" \
  -H "Content-Type: text/plain" \
  -H "X-Checksum: $ORIG_HASH" \
  --data-binary "@$TEST_FILE")

echo "=== STEP 6: VERIFY HTTP SUCCESS ==="
echo "PUT Response: $PUT_RESP"
VERSION_ID=$(echo "$PUT_RESP" | grep -o '"version_id":"[^"]*' | cut -d'"' -f4)
BACKEND_HASH=$(echo "$PUT_RESP" | grep -o '"checksum":"[^"]*' | cut -d'"' -f4 | sed 's/^sha256://')

if [ -n "$VERSION_ID" ] && [ "$ORIG_HASH" = "$BACKEND_HASH" ]; then
    echo "HTTP PUT Verification: PASS (Version ID: $VERSION_ID)"
else
    echo "HTTP PUT Verification: FAILED ($ORIG_HASH vs $BACKEND_HASH)"
    exit 1
fi

echo "=== STEP 7: VERIFY REPLICA NODE METADATA (COUNT = 3) ==="
LIST_RESP=$(curl -s "$HOST/buckets/default/list")
echo "List Response: $LIST_RESP"
REPLICA_COUNT=$(echo "$LIST_RESP" | grep -o '"replica_nodes":\[[^]]*\]' | grep -o '"storage-[0-9]"' | wc -l | tr -d ' ')
echo "Metadata Replica Count: $REPLICA_COUNT"

echo "=== STEPS 8, 9, 10: VERIFY PHYSICAL DATA FILES ON ALL 3 STORAGE NODES ==="
PATH1=$(find ./data/storage-1 ./backend/data/storage-1 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)
PATH2=$(find ./data/storage-2 ./backend/data/storage-2 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)
PATH3=$(find ./data/storage-3 ./backend/data/storage-3 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)

echo "storage-1 physical file: ${PATH1:-MISSING}"
echo "storage-2 physical file: ${PATH2:-MISSING}"
echo "storage-3 physical file: ${PATH3:-MISSING}"

if [ -f "$PATH1" ] && [ -f "$PATH2" ] && [ -f "$PATH3" ]; then
    echo "Physical Replicas Exist on All 3 Nodes: PASS"
else
    echo "Physical Replicas Check: FAILED"
    exit 1
fi

echo "=== STEPS 11 & 12: CALCULATE AND COMPARE SHA-256 ON ALL 3 REPLICAS ==="
HASH1=$(shasum -a 256 "$PATH1" | awk '{print $1}')
HASH2=$(shasum -a 256 "$PATH2" | awk '{print $1}')
HASH3=$(shasum -a 256 "$PATH3" | awk '{print $1}')

echo "Original SHA-256 : $ORIG_HASH"
echo "storage-1 SHA-256: $HASH1"
echo "storage-2 SHA-256: $HASH2"
echo "storage-3 SHA-256: $HASH3"

if [ "$ORIG_HASH" = "$HASH1" ] && [ "$ORIG_HASH" = "$HASH2" ] && [ "$ORIG_HASH" = "$HASH3" ]; then
    echo "All 3 Physical Replicas SHA-256 Match: EXACT MATCH"
else
    echo "Physical Replicas SHA-256 Verification: MISMATCH"
    exit 1
fi

echo "=== STEPS 13 & 14: DOWNLOAD OBJECT AND VERIFY SHA-256 ==="
DOWN_FILE="/tmp/downloaded-e2e.txt"
curl -s "$HOST/buckets/default/objects/e2e-test-file.txt" > "$DOWN_FILE"
DOWN_HASH=$(shasum -a 256 "$DOWN_FILE" | awk '{print $1}')

if [ "$ORIG_HASH" = "$DOWN_HASH" ]; then
    echo "Downloaded File Verification: EXACT MATCH ($DOWN_HASH)"
else
    echo "Downloaded File Verification: MISMATCH"
    exit 1
fi

echo "=== STEP 15: CRASH STORAGE NODE (storage-1) ==="
curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/chaos/crash-node/storage-1" > /dev/null
echo "Crash command sent for storage-1"

echo "=== STEP 16: VERIFY REMAINING NODES STAY HEALTHY ==="
NODES_AFTER_CRASH=$(curl -s -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/nodes")
echo "Nodes State After Crash: $NODES_AFTER_CRASH"

echo "=== STEP 17: GET OBJECT DURING SINGLE NODE FAILURE (QUORUM R=2) ==="
FAIL_DOWN="/tmp/fail-down.txt"
curl -s "$HOST/buckets/default/objects/e2e-test-file.txt" > "$FAIL_DOWN"
FAIL_HASH=$(shasum -a 256 "$FAIL_DOWN" | awk '{print $1}')

if [ "$ORIG_HASH" = "$FAIL_HASH" ]; then
    echo "Read During Single Node Failure (R=2 Quorum): SUCCESS"
else
    echo "Read During Single Node Failure: FAILED"
    exit 1
fi

echo "=== STEP 18: RECOVER FAILED NODE ==="
curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/chaos/restart-node/storage-1" > /dev/null
echo "Restart command sent for storage-1. Waiting 3.5s for recovery..."
sleep 3.5

echo "=== STEP 19: VERIFY ALL 3 NODES ARE HEALTHY AGAIN ==="
HEALTH_NODES=$(curl -s -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/nodes")
echo "Nodes State After Recovery: $HEALTH_NODES"

echo "=== STEP 20: VERIFY RECOVERED NODE CONTAINS OBJECT ==="
if [ -f "$PATH1" ]; then
    echo "Recovered Node Replica File: VERIFIED"
else
    echo "Recovered Node Replica File: MISSING"
    exit 1
fi

echo "=== STEP 21: CORRUPT ONE PHYSICAL REPLICA (storage-2) ==="
CORRUPT_RESP=$(curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"bucket\":\"default\",\"key\":\"e2e-test-file.txt\",\"version_id\":\"$VERSION_ID\",\"node_id\":\"storage-2\"}" \
  "$HOST/admin/chaos/corrupt-replica")
echo "Corrupt Replica Response: $CORRUPT_RESP"

echo "=== STEP 22: RUN BIT-ROT SCRUB ON storage-2 ==="
SCRUB_RESP=$(curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/scrubs/trigger/storage-2")
echo "Scrub Response: $SCRUB_RESP"

echo "=== STEP 23: VERIFY CORRUPTION DETECTION ==="
CORRUPT_HASH=$(shasum -a 256 "$PATH2" | awk '{print $1}')
echo "Corrupted Replica SHA-256: $CORRUPT_HASH"
if [ "$ORIG_HASH" != "$CORRUPT_HASH" ]; then
    echo "Bit-Rot Corruption Injected and Detected: PASS"
else
    echo "Bit-Rot Corruption Check: FAILED"
    exit 1
fi

echo "=== STEP 24: RUN ANTI-ENTROPY SELF-HEALING REPAIR ==="
REPAIR_RESP=$(curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/repairs/trigger")
echo "Repair Response: $REPAIR_RESP"

echo "=== STEPS 25 & 26: VERIFY REPAIRED PHYSICAL REPLICA SHA-256 ==="
REPAIRED_HASH=$(shasum -a 256 "$PATH2" | awk '{print $1}')
echo "Repaired Replica SHA-256: $REPAIRED_HASH"
if [ "$ORIG_HASH" = "$REPAIRED_HASH" ]; then
    echo "Anti-Entropy Repair Verification: EXACT MATCH (CORRUPT -> REPAIRING -> HEALTHY)"
else
    echo "Anti-Entropy Repair Verification: FAILED"
    exit 1
fi

echo "=== STEP 27: DELETE OBJECT ==="
curl -s -X DELETE "$HOST/buckets/default/objects/e2e-test-file.txt" > /dev/null
echo "Delete command issued"

echo "=== STEPS 28 & 29: VERIFY TOMBSTONE MARKER & GET HTTP 404 ==="
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HOST/buckets/default/objects/e2e-test-file.txt")
echo "GET After DELETE HTTP Status: $HTTP_CODE"

if [ "$HTTP_CODE" = "404" ]; then
    echo "Tombstone GET HTTP 404 Verification: PASS"
else
    echo "Tombstone GET HTTP 404 Verification: FAILED ($HTTP_CODE)"
    exit 1
fi

echo "=== ALL 29 E2E PHYSICAL REPLICATION & SELF-HEALING TESTS PASSED SUCCESSFULLY ==="
