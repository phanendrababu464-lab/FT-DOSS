# FT-DOSS

## Fault-Tolerant Distributed Object Storage System

> **Quorum Replication • Raft Metadata Consensus • Integrity Verification • Self-Healing • Real-Time Observability**

---

## What is FT-DOSS?

**FT-DOSS** (Fault-Tolerant Distributed Object Storage System) is a high-availability, self-healing object storage prototype engineered to demonstrate quorum replication, Write-Ahead Log (WAL) durability, Raft metadata consensus, cryptographic SHA-256 integrity verification, background scrubbing, anti-entropy repair, and real-time event-driven SSE observability.

---

## Architecture

```text
                                CLIENT / UI
                                     │
                                     ▼
                                API Gateway
                                     │
                 ┌───────────────────┼───────────────────┐
                 │                   │                   │
               Raft             Membership          Replication
                 │                   │                   │
                 └───────────────────┼───────────────────┘
                                     │
                            Placement Groups
                                     │
           ┌─────────────────────────┼─────────────────────────┐
           ▼                         ▼                         ▼
       Storage-1                 Storage-2                 Storage-3
           │                         │                         │
          WAL                       WAL                       WAL
           └─────────────────────────┼─────────────────────────┘
                                     │
                               Scrub + Repair
                                     │
                                     ▼
                                 Event Bus
                                     │
                                     └─────→ SSE Stream
```

- **API Gateway**: Single entry point handling REST operations, payload hashing, and client authentication.
- **Replication Engine**: Enforces $N=3, W=2, R=2$ quorum consensus across storage nodes.
- **Storage Nodes**: Maintain local object payload replicas with Write-Ahead Logging (WAL) and synchronous disk flushes (`fsync`).
- **Scrubbing & Repair**: Background workers scan for silent bit rot via SHA-256 digests and stream valid replicas from surviving quorum nodes to achieve self-healing.
- **Observability**: In-process EventBus publishes system lifecycle events over Server-Sent Events (SSE) to the React control plane.

---

## Features

- **Quorum Replication ($N=3, W=2, R=2$)**: Guarantees read/write consistency and fault tolerance despite single-node outages.
- **Write-Ahead Log (WAL) Durability**: Ensures state recovery and durable object persistence.
- **Raft Metadata Consensus**: Manages term, leader election, and committed metadata updates.
- **Epoch Fencing**: Rejects stale node heartbeats or requests during cluster membership changes.
- **SHA-256 Integrity Verification**: Computes and checks cryptographic digests on writes, reads, and scrubs.
- **Background Scrubbing**: Periodically verifies on-disk byte integrity against stored checksums.
- **Self-Healing Anti-Entropy Repair**: Restores corrupted replicas (`CORRUPT → REPAIRING → HEALTHY`) from healthy quorum members.
- **Tombstone Markers**: Soft-deletes objects consistently across replicas.
- **SSE Observability**: Delivers live events (`NODE_CRASHED`, `CHECKSUM_MISMATCH`, `REPAIR_COMPLETED`) without UI polling.
- **Chaos Engineering Lab**: Provides on-demand node failure, restart, replica corruption, scrub, and repair triggers.
- **Real File Upload & Retrieval**: Full multi-part/chunked client upload support for real files (up to 100 MB).

---

## Quick Start & Demo Commands

### 1. Start the Demo Environment
```bash
./scripts/start-demo.sh
```
Starts the FT-DOSS services, waits for backend API (`http://localhost:8080`), frontend (`http://localhost:3000`), and metrics (`http://localhost:2112/metrics`) to become available, and prints readiness status.

### 2. Run Health Check
```bash
./scripts/health-check.sh
```
Verifies gateway, frontend, node membership ($N \ge 3$), REST API, SSE authentication, and metrics. Returns exit code `0` when healthy.

### 3. Run Automated Real File Smoke Test
```bash
./scripts/e2e_real_file_test.sh
```
Executes a 14-step automated verification: upload, quorum check, read SHA-256 match, node crash, read during failure, node restart, corruption injection, scrubbing, self-healing repair, post-repair download SHA-256 match, and tombstone deletion.

### 4. Reset Demo Environment
```bash
./scripts/reset-demo.sh
```
Restores storage nodes, objects, tombstones, corrupt replicas, repair state, and event baseline without deleting source code or user configuration.

### 5. Stop the Demo Environment
```bash
./scripts/stop-demo.sh
```
Cleanly stops containers and background processes while leaving source code intact.

---

## Testing & Verification

Run backend unit/integration tests with data race detector enabled:
```bash
cd backend
go test -v -race ./...
```
*(Expected: 27/27 PASS, 0 data races)*

Build the frontend bundle:
```bash
cd frontend
npm run build
```
*(Expected: Clean TypeScript compilation and Vite build)*

Validate Docker Compose configuration:
```bash
docker compose config
```

---

## Live Demo Scenario Walkthrough

Follow this step-by-step sequence during a live 2-minute judge demonstration:

$$\text{UPLOAD} \longrightarrow \text{REPLICATE} \longrightarrow \text{FAIL} \longrightarrow \text{SURVIVE} \longrightarrow \text{DETECT} \longrightarrow \text{SCRUB} \longrightarrow \text{REPAIR} \longrightarrow \text{VERIFY}$$

1. **Start Environment**: `./scripts/start-demo.sh`
2. **Verify Health**: `./scripts/health-check.sh`
3. **Open Dashboard**: Navigate to `http://localhost:3000`
4. **Reset State**: Execute `./scripts/reset-demo.sh` (confirms 3 nodes `HEALTHY`)
5. **Upload File**: Upload `demo.pdf` via the Objects tab
6. **Inspect Replicas**: View 3 replicas ($W=2$), SHA-256 checksum, and Placement Group ID
7. **Crash Storage Node**: Trigger crash on `storage-1` in Chaos Lab
8. **Download Object**: Download `demo.pdf` — verify success from surviving quorum ($R=2$)
9. **Corrupt Storage Node**: Inject silent bit rot into `storage-2`
10. **Trigger Scrub**: Click **Trigger Scrub** — verify `CHECKSUM_MISMATCH` alert
11. **Trigger Repair**: Click **Trigger Repair** — observe state transition `CORRUPT → REPAIRING → HEALTHY`
12. **Post-Repair Download**: Download `demo.pdf` again and verify intact SHA-256 digest
13. **Delete Object**: Delete `demo.pdf` — verify tombstone creation and subsequent `404 Not Found`

---

## Honest Limitations

FT-DOSS is an educational prototype engineered to illustrate core distributed systems concepts. Please note the following honest limitations:
- **$N=3$ Fixed Quorum**: Configured for 3-way replica placement ($N=3$) rather than dynamic $N$-way placement or erasure coding.
- **Storage Engine**: Uses local filesystem directory storage (`./storage_nodes`) rather than raw block devices or NVMe pass-through.
- **Metadata Index**: Operates an in-memory metadata catalog backed by Raft log persistence.
- **No Erasure Coding**: Prototype relies on full-copy quorum replication rather than Reed-Solomon ($8+4$) parity chunks.
- **Single-Tenant Security Model**: Uses local static admin keys and short-lived tickets suited for single-tenant evaluation.
- **Not S3 API Compatible**: Custom JSON REST object storage API rather than AWS S3 XML REST compliance.
- **No Cross-Region Replication**: Designed for intra-datacenter cluster deployment.

*FT-DOSS is not intended for multi-tenant production cloud deployments.*
