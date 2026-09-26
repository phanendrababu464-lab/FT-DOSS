#!/usr/bin/env bash
# FT-DOSS Demo Health Check Script
set -e

ADMIN_KEY="dev-key"
HOST="http://localhost:8080"
FRONTEND="http://localhost:3000"

echo "=== FT-DOSS Performing System Health Check ==="

# 1. Gateway Health Endpoint
echo -n "[1/7] Gateway Health: "
HEALTH_RESP=$(curl -s "$HOST/health" || true)
if echo "$HEALTH_RESP" | grep -q '"status":"ok"'; then
    echo "PASS"
else
    echo "FAIL (Gateway not responding at $HOST/health)"
    exit 1
fi

# 2. Frontend Reachability
echo -n "[2/7] Frontend Dashboard: "
if curl -s "$FRONTEND" >/dev/null 2>&1; then
    echo "PASS"
else
    echo "FAIL (Frontend not responding at $FRONTEND)"
    exit 1
fi

# 3. Storage Nodes Reachability
echo -n "[3/7] Storage Nodes Query: "
NODES_RESP=$(curl -s -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/nodes" || true)
if echo "$NODES_RESP" | grep -q '"id":'; then
    echo "PASS"
else
    echo "FAIL (Unable to query storage nodes)"
    exit 1
fi

# 4. Cluster Node Count Verification
echo -n "[4/7] Node Count (N >= 3): "
NODE_COUNT=$(echo "$NODES_RESP" | grep -o '"id":' | wc -l | tr -d ' ')
if [ "$NODE_COUNT" -ge 3 ]; then
    echo "PASS ($NODE_COUNT nodes registered)"
else
    echo "FAIL (Expected >= 3 nodes, got $NODE_COUNT)"
    exit 1
fi

# 5. REST API Object Storage List
echo -n "[5/7] REST Object Storage API: "
LIST_RESP=$(curl -s "$HOST/buckets/default/list" || true)
if echo "$LIST_RESP" | grep -q '"is_truncated":'; then
    echo "PASS"
else
    echo "FAIL (REST API not responding)"
    exit 1
fi

# 6. SSE Ticket Generation
echo -n "[6/7] SSE Ticket Authentication: "
SSE_RESP=$(curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/sse-token" || true)
if echo "$SSE_RESP" | grep -q '"token":'; then
    echo "PASS"
else
    echo "FAIL (SSE token generation failed)"
    exit 1
fi

# 7. Cluster Observability Metrics
echo -n "[7/7] Prometheus & Observability Metrics: "
METRICS_RESP=$(curl -s -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/cluster/metrics" || true)
if echo "$METRICS_RESP" | grep -q '"total_nodes":'; then
    echo "PASS"
else
    echo "FAIL (Cluster metrics not responding)"
    exit 1
fi

echo ""
echo "=== ALL HEALTH CHECKS PASSED SUCCESSFULLY ==="
exit 0
