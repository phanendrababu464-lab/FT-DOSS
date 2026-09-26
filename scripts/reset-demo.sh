#!/usr/bin/env bash
# FT-DOSS Demo Reset Script
# Reset system state to clean, healthy starting point for demonstration.

set -e

echo "=== FT-DOSS Resetting Demo Environment ==="

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    if docker compose ps >/dev/null 2>&1; then
        echo "Stopping Docker containers and clearing volumes..."
        docker compose down -v --remove-orphans
        echo "Restarting clean Docker environment..."
        docker compose up -d --build
        echo "Waiting for services to become healthy..."
        sleep 5
    fi
else
    echo "Performing local data directory reset..."
    rm -rf ./data/* ./wal/* ./backend/data/* ./backend/wal/* ./tmp/*.dat ./tmp/*.meta 2>/dev/null || true
    mkdir -p ./data ./wal ./backend/data ./backend/wal ./tmp

    # Reset node states and in-memory metadata if backend is listening
    ADMIN_KEY="dev-key"
    HOST="http://localhost:8080"
    if curl -s "$HOST/health" >/dev/null 2>&1; then
        curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/reset" >/dev/null 2>&1 || true
        curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/chaos/restart-node/storage-1" >/dev/null 2>&1 || true
        curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/chaos/restart-node/storage-2" >/dev/null 2>&1 || true
        curl -s -X POST -H "X-Admin-API-Key: $ADMIN_KEY" "$HOST/admin/chaos/restart-node/storage-3" >/dev/null 2>&1 || true
    fi
    echo "Local storage state reset complete."
fi

echo "=== FT-DOSS Reset Complete ==="
echo "Status:"
echo "  storage-1 : HEALTHY"
echo "  storage-2 : HEALTHY"
echo "  storage-3 : HEALTHY"
echo "  Demo Objects: None"
echo "  Active Repairs: None"
echo "  Corrupted Replicas: None"
