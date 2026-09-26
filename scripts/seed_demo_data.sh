#!/usr/bin/env bash
# FT-DOSS Real Test Data Seeding Script
set -e

HOST="http://localhost:8080"
BUCKET="default"
ADMIN_KEY="dev-key"
SEED_DIR="./tmp/seed_files"

echo "=== FT-DOSS Seeding Real Test Data ==="

# 1. Ensure backend is running and healthy
HEALTH_RESP=$(curl -s "$HOST/health" || true)
if ! echo "$HEALTH_RESP" | grep -q '"status":"ok"'; then
    echo "Error: Backend gateway is not healthy at $HOST/health"
    exit 1
fi

# 2. Create directory structure for test files
mkdir -p "$SEED_DIR/demo/logs" "$SEED_DIR/demo/datasets" "$SEED_DIR/demo/configs" "$SEED_DIR/demo/documents"

# 3. Create test files
cat << 'EOF' > "$SEED_DIR/demo/ft-doss-test.txt"
FT-DOSS Distributed Object Storage Test

This is a real object stored through the FT-DOSS API.

Purpose:
- Test replication
- Test quorum writes
- Test reads
- Test checksums
- Test downloads
EOF

cat << 'EOF' > "$SEED_DIR/demo/system-status.json"
{
  "system": "FT-DOSS",
  "purpose": "Distributed Object Storage",
  "replication": 3,
  "write_quorum": 2,
  "read_quorum": 2,
  "status": "healthy",
  "created_at": "2026-09-26T05:39:00Z",
  "nodes": ["storage-1", "storage-2", "storage-3"]
}
EOF

cat << 'EOF' > "$SEED_DIR/demo/sample-data.csv"
id,object_name,type,status
1,report-01.pdf,document,stored
2,image-01.png,image,stored
3,data-01.csv,dataset,stored
4,logs-01.txt,log,stored
5,config-01.json,configuration,stored
6,backup-01.tar.gz,archive,stored
7,report-02.pdf,document,stored
8,image-02.png,image,stored
9,data-02.csv,dataset,stored
10,logs-02.txt,log,stored
11,metrics-01.prom,metrics,stored
12,schema-01.sql,database,stored
13,notes-01.md,markdown,stored
14,video-01.mp4,media,stored
15,audio-01.mp3,media,stored
16,artifact-01.bin,binary,stored
17,model-01.onnx,ai_model,stored
18,index-01.db,index,stored
19,key-01.pem,security,stored
20,manifest-01.yaml,manifest,stored
EOF

cat << 'EOF' > "$SEED_DIR/demo/README-test.md"
# FT-DOSS Seeded Demonstration Objects

These files represent real demonstration objects stored in the FT-DOSS cluster.

- **Replication**: 3 physical copies across all storage nodes
- **Consistency**: Write Quorum (W=2), Read Quorum (R=2)
- **Integrity**: Cryptographic SHA-256 digests checked on read, write, and background scrubbing
EOF

cat << 'EOF' > "$SEED_DIR/demo/logs/demo-log.txt"
2026-09-26T05:39:00Z [INFO] Cluster membership initialized: storage-1, storage-2, storage-3
2026-09-26T05:39:01Z [INFO] Quorum configured: N=3, W=2, R=2
2026-09-26T05:39:02Z [INFO] SHA-256 background scrubber active
2026-09-26T05:39:03Z [INFO] EventBus streaming live SSE notifications
EOF

cat << 'EOF' > "$SEED_DIR/demo/datasets/students.csv"
student_id,name,department,gpa,status
101,Alice Smith,Computer Science,3.92,Active
102,Bob Jones,Electrical Engineering,3.78,Active
103,Charlie Brown,Mechanical Engineering,3.65,Active
104,Diana Prince,Data Science,3.95,Active
105,Evan Wright,Cybersecurity,3.81,Active
EOF

cat << 'EOF' > "$SEED_DIR/demo/configs/system.json"
{
  "cluster_name": "ft-doss-production-demo",
  "placement_groups": 1024,
  "fencing_enabled": true,
  "wal_fsync": true,
  "scrub_interval_seconds": 300,
  "alerting_rules": ["stale_node", "checksum_mismatch", "low_quorum"]
}
EOF

cat << 'EOF' > "$SEED_DIR/demo/documents/hackathon-notes.md"
# FT-DOSS Hackathon Architecture Notes

- **Object Storage**: S3-compatible REST API gateway with chunked payload streaming.
- **Metadata Consensus**: Raft term & leader state machine.
- **Anti-Entropy**: Bit-rot detection with automated background repair.
- **Observability**: Prometheus metrics and SSE event streaming.
EOF

FILES=(
  "demo/ft-doss-test.txt"
  "demo/system-status.json"
  "demo/sample-data.csv"
  "demo/README-test.md"
  "demo/logs/demo-log.txt"
  "demo/datasets/students.csv"
  "demo/configs/system.json"
  "demo/documents/hackathon-notes.md"
)

TOTAL_BYTES=0
SUCCESS_COUNT=0

echo "Uploading files through real REST API..."
echo "------------------------------------------------------------"

for REL_PATH in "${FILES[@]}"; do
    FILE_PATH="$SEED_DIR/$REL_PATH"
    KEY="$REL_PATH"
    FILE_SIZE=$(wc -c < "$FILE_PATH" | tr -d ' ')
    ORIG_HASH=$(shasum -a 256 "$FILE_PATH" | awk '{print $1}')
    
    # Determine Content-Type
    CT="text/plain"
    if [[ "$KEY" == *.json ]]; then CT="application/json"; fi
    if [[ "$KEY" == *.csv ]]; then CT="text/csv"; fi
    if [[ "$KEY" == *.md ]]; then CT="text/markdown"; fi

    echo "Uploading: $KEY ($FILE_SIZE bytes, SHA256: ${ORIG_HASH:0:12}...)"

    # PUT through real object API
    PUT_RESP=$(curl -s -X PUT --data-binary "@$FILE_PATH" -H "Content-Type: $CT" "$HOST/buckets/$BUCKET/objects/$KEY")
    
    VERSION_ID=$(echo "$PUT_RESP" | grep -o '"version_id":"[^"]*"' | cut -d'"' -f4)
    if [ -z "$VERSION_ID" ]; then
        echo "Error: PUT failed for $KEY. Response: $PUT_RESP"
        exit 1
    fi

    # Verify Physical Replicas on storage-1, storage-2, storage-3
    PATH1=$(find ./backend/data/storage-1 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)
    PATH2=$(find ./backend/data/storage-2 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)
    PATH3=$(find ./backend/data/storage-3 -name "${VERSION_ID}.dat" 2>/dev/null | head -n 1)

    if [ -z "$PATH1" ] || [ -z "$PATH2" ] || [ -z "$PATH3" ]; then
        echo "Error: Missing physical replica file for $KEY across storage nodes!"
        exit 1
    fi

    H1=$(shasum -a 256 "$PATH1" | awk '{print $1}')
    H2=$(shasum -a 256 "$PATH2" | awk '{print $1}')
    H3=$(shasum -a 256 "$PATH3" | awk '{print $1}')

    if [ "$ORIG_HASH" != "$H1" ] || [ "$ORIG_HASH" != "$H2" ] || [ "$ORIG_HASH" != "$H3" ]; then
        echo "Error: SHA-256 mismatch on physical replicas for $KEY!"
        exit 1
    fi

    # Download & Verify SHA-256
    DL_FILE="/tmp/dl-verify-$SUCCESS_COUNT.tmp"
    curl -s "$HOST/buckets/$BUCKET/objects/$KEY" > "$DL_FILE"
    DL_HASH=$(shasum -a 256 "$DL_FILE" | awk '{print $1}')
    rm -f "$DL_FILE"

    if [ "$ORIG_HASH" != "$DL_HASH" ]; then
        echo "Error: GET Downloaded file SHA-256 mismatch for $KEY!"
        exit 1
    fi

    TOTAL_BYTES=$((TOTAL_BYTES + FILE_SIZE))
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    echo "  └─ PASS: 3/3 physical replicas verified (SHA-256 MATCH)"
done

echo "------------------------------------------------------------"
echo "Seeding Summary:"
echo "  Objects Uploaded   : $SUCCESS_COUNT / ${#FILES[@]}"
echo "  Total Payload Size : $TOTAL_BYTES bytes"
echo "  Physical Replicas  : 3/3 per object ($(expr $SUCCESS_COUNT \* 3) total replicas)"
echo "  Nodes Health State : ALL 3 NODES HEALTHY"
echo "=== SEEDING COMPLETED SUCCESSFULLY ==="
