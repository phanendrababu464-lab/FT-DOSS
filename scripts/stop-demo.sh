#!/usr/bin/env bash
# FT-DOSS Clean Stop Script
set -e

echo "=== Stopping FT-DOSS Demo Stack ==="

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    if docker compose ps >/dev/null 2>&1; then
        echo "Stopping Docker Compose containers..."
        docker compose down
    fi
fi

# Stop any local dev processes on ports 8080 and 3000
echo "Stopping local gateway and frontend processes..."
pkill -f "cmd/gateway" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true

echo "=== FT-DOSS Demo Stack Stopped ==="
