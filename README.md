# Distributed Cloud Storage (OCS)

A fault-tolerant, horizontally scalable, multi-chunk distributed object store implemented in Go 1.22.

Built with an HDFS/GFS-style centralized metadata architecture, streaming chunk replication (RF=2), level-triggered autonomous self-healing, and end-to-end SHA-256 integrity checks.

---

## Features

- **Chunked Streaming**: Files are partitioned into 1 MiB chunks on the fly and streamed concurrently across worker nodes without buffering whole files in memory.
- **Replication & Fault Tolerance (RF=2)**: Every chunk is replicated across distinct worker nodes. Survives worker crashes, disconnections, and node reboots with zero downtime for reads and writes.
- **Level-Triggered Auto-Repair**: Declarative reconciler evaluates cluster health, repairs under-replicated chunks, trims over-replicated copies, and garbage collects deleted objects.
- **Bit Rot Detection & Scrubber**: End-to-end SHA-256 verification on ingest, read, repair, and idle background disk scrubbing. Corrupted replicas trigger automatic fallback and background self-healing.
- **Embedded ACID Metadata**: Controller uses embedded SQLite with Write-Ahead Logging (WAL) for sub-millisecond commits with zero external database dependencies.
- **Unified CLI (`ocs`)**: Powerful command-line client supporting file transfers, streaming inspection, custom metadata manipulation, and cluster diagnostics.

---

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Go 1.22+ (for running unit tests locally without Docker)
- `curl`, `sha256sum`, and `bash`

### 1. Launch the Cluster
```bash
# Start controller, 4 worker nodes, and client container
make up
```

Verify cluster status:
```bash
./bin/ocs status
# or curl -H "Authorization: Bearer dev-secret-change-me" http://localhost:8080/api/v1/admin/status
```

### 2. Basic CLI Usage
```bash
# Upload a file
./bin/ocs cp ./my_photo.jpg obj:photos/vacation.jpg --meta "album=holiday,owner=alice"

# Inspect object metadata and chunk replica distribution
./bin/ocs stat obj:photos/vacation.jpg

# Stream / cat directly to stdout
./bin/ocs cat obj:photos/vacation.jpg | sha256sum

# Download the file
./bin/ocs cp obj:photos/vacation.jpg ./downloaded_photo.jpg

# Verify end-to-end integrity
./bin/ocs verify obj:photos/vacation.jpg ./downloaded_photo.jpg

# List objects with prefix
./bin/ocs ls obj:photos/

# Manage custom metadata
./bin/ocs meta set obj:photos/vacation.jpg status=approved
./bin/ocs meta get obj:photos/vacation.jpg
./bin/ocs meta rm obj:photos/vacation.jpg album

# Delete object
./bin/ocs rm obj:photos/vacation.jpg
```

---

## Interactive Demos

The repository includes a comprehensive, automated demonstration suite testing real-world distributed failure and integrity scenarios:

```bash
make demo
# or bash scripts/demo/run_all.sh
```

| Demo Script | Scenario Tested |
|---|---|
| [`d1_object_storage.sh`](scripts/demo/d1_object_storage.sh) | Basic object storage: 3.5 MB random upload, verified download, listing, deletion. |
| [`d2_chunking.sh`](scripts/demo/d2_chunking.sh) | 10.5 MB file split into 11 chunks (10 full 1 MiB + 1 half-sized chunk) & reassembly. |
| [`d3_replication.sh`](scripts/demo/d3_replication.sh) | RF=2 replication across distinct worker nodes and physical on-disk file inspection. |
| [`d4_metadata.sh`](scripts/demo/d4_metadata.sh) | Dynamic custom metadata attach, update, delete key, and `modified_at` tracking. |
| [`d5_failure.sh`](scripts/demo/d5_failure.sh) | Worker node crash (`docker stop worker2`), read/write failover, and auto-healing. |
| [`d6_integrity.sh`](scripts/demo/d6_integrity.sh) | In-place bit rot corruption via `dd`, transparent download fallback, and disk scrubber. |

---

## Configuration Reference

### Controller Environment Variables
| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | Controller HTTP listen address |
| `DB_PATH` | `/data/meta.db` | Path to SQLite metadata database |
| `CLUSTER_TOKEN` | `dev-secret-change-me` | Bearer token for internal cluster auth |
| `CHUNK_SIZE` | `1048576` | Fixed chunk size in bytes (1 MiB) |
| `REPLICATION_FACTOR` | `2` | Desired chunk replica count |
| `WRITE_ACKS` | `2` | Synchronous replica acknowledgments required for commit |
| `UPLOAD_PARALLELISM` | `4` | Max parallel chunk transfers per upload |
| `MAX_CONCURRENT_UPLOADS` | `16` | Global upload admission semaphore |
| `DOWNLOAD_PREFETCH` | `2` | Download pipeline chunk prefetch window |
| `HEARTBEAT_INTERVAL` | `2s` | Expected worker heartbeat period |
| `SUSPECT_AFTER` | `6s` | Grace period before marking worker SUSPECT |
| `DEAD_AFTER` | `15s` | Threshold to declare worker DEAD |
| `REPAIR_CONCURRENCY` | `2` | Bounded concurrent repair worker pool |
| `REPAIR_DELAY` | `5s` | Grace period before repairing dead worker chunks |
| `REPAIR_ENABLED` | `true` | Enable autonomous chunk repair and trimming |
| `STARTUP_GRACE` | `10s` | Startup delay before initial reconciliation pass |
| `RECONCILE_INTERVAL` | `5s` | Periodic reconciliation loop interval |
| `INVENTORY_SYNC_INTERVAL` | `30s` | Periodic block-report sync interval |

### Worker Environment Variables
| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | `worker1` | Unique node identifier |
| `WORKER_ADDR` | `http://worker1:9000` | Advertised reachable worker URL |
| `WORKER_LISTEN` | `:9000` | Worker HTTP listen address |
| `CONTROLLER_URL` | `http://controller:8080`| Controller URL for registration & heartbeats |
| `DATA_DIR` | `/data` | Directory for chunk storage and quarantine |
| `SCRUB_INTERVAL` | `10m` | Interval for full background disk scrub passes |
| `SCRUB_RATE_MBPS` | `50` | I/O throttle rate limit for background scrubber |

---

## Architecture & Design
For detailed architecture diagrams, request sequences, and the 17 architectural decisions, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Development & Testing

```bash
# Run unit and integration tests with race detector (no Docker needed)
make test

# Run code linter / vet
make vet

# Build native binaries in ./bin/
make build

# Tear down the cluster
make down
```
