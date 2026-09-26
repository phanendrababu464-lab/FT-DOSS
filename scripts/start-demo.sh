#!/usr/bin/env bash
# FT-DOSS One-Command Start Script
set -e

echo "=== FT-DOSS Starting Environment ==="

# 1. Dependency checks
command -v go >/dev/null 2>&1 || { echo "Error: Go is required but not installed."; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "Error: Node/npm is required but not installed."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required but not installed."; exit 1; }

# 2. Check if Docker Compose is available
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    echo "Docker Compose detected. Starting containers via Docker Compose..."
    docker compose up -d --build
else
    echo "Starting FT-DOSS locally..."

    # Check if backend (port 8080) is already listening
    if ! curl -s http://localhost:8080/health >/dev/null 2>&1; then
        echo "Starting backend gateway..."
        cd backend
        FTDOSS_NODE_ID=storage-1 \
        FTDOSS_PEER_NODES="storage-2=http://localhost:8080,storage-3=http://localhost:8080" \
        FTDOSS_ADMIN_API_KEY=dev-key \
        nohup go run ./cmd/gateway > ../tmp/gateway_daemon.log 2>&1 & disown
        cd ..
    else
        echo "Backend is already running on port 8080."
    fi

    # Check if frontend (port 3000) is already listening
    if ! curl -s http://localhost:3000 >/dev/null 2>&1; then
        echo "Starting frontend dev server..."
        cd frontend
        CI=true nohup npx vite --host 0.0.0.0 --port 3000 > ../tmp/frontend_daemon.log 2>&1 & disown
        cd ..
    else
        echo "Frontend is already running on port 3000."
    fi
fi

# 3. Wait for backend health
echo "Waiting for backend health endpoint..."
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://localhost:8080/health | grep -q '"status":"ok"'; then
        echo "Backend health check passed."
        break
    fi
    ATTEMPT=$((ATTEMPT + 1))
    sleep 1
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "Error: Backend health endpoint timed out."
    exit 1
fi

# 4. Wait for frontend
echo "Waiting for frontend accessibility..."
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://localhost:3000 >/dev/null 2>&1; then
        echo "Frontend check passed."
        break
    fi
    ATTEMPT=$((ATTEMPT + 1))
    sleep 1
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "Error: Frontend accessibility timed out."
    exit 1
fi

echo ""
echo "FT-DOSS is ready"
echo "Control Plane:"
echo "http://localhost:3000"
echo "API:"
echo "http://localhost:8080"
echo "Metrics:"
echo "http://localhost:2112/metrics"
