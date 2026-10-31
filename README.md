# Distributed Cloud Storage (OCS)

> A small, production-shaped distributed object store for learning, demos, and
> reliability experiments.

A fault-tolerant, horizontally scalable, multi-chunk distributed object store implemented in Go.

**OCS** means **Object Cloud Storage** in this project. The command-line
client is named `ocs`, while the Go module is named `objstore`.

Built with an HDFS/GFS-style centralized metadata architecture, streaming chunk replication (RF=2), level-triggered autonomous self-healing, and end-to-end SHA-256 integrity checks.

The controller is the single public entry point: clients do not need to know
where chunks are stored. Workers own chunk bytes, while the controller owns
metadata, placement, liveness, and repair policy.

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quickstart](#quickstart)
- [CLI usage](#cli-usage)
- [Interactive demos](#interactive-demos)
- [HTTP API](#http-api)
- [Configuration](#configuration)
- [Development and testing](#development-and-testing)
- [Repository layout](#repository-layout)
- [Limitations](#limitations)

---

## Features

- **Chunked Streaming**: Files are partitioned into 1 MiB chunks on the fly and streamed concurrently across worker nodes without buffering whole files in memory.
- **Replication & Fault Tolerance (RF=2)**: Every chunk is replicated across distinct worker nodes. Survives worker crashes, disconnections, and node reboots with zero downtime for reads and writes.
- **Level-Triggered Auto-Repair**: Declarative reconciler evaluates cluster health, repairs under-replicated chunks, trims over-replicated copies, and garbage collects deleted objects.
- **Bit Rot Detection & Scrubber**: End-to-end SHA-256 verification on ingest, read, repair, and idle background disk scrubbing. Corrupted replicas trigger automatic fallback and background self-healing.
- **Embedded ACID Metadata**: Controller uses embedded SQLite with Write-Ahead Logging (WAL) for sub-millisecond commits with zero external database dependencies.
- **Unified CLI (`ocs`)**: Powerful command-line client supporting file transfers, streaming inspection, custom metadata manipulation, and cluster diagnostics.

The public API supports object upload/download, prefix listing, metadata read and
patch, deletion, health, and admin status/reconcile/scrub operations. Worker
endpoints are private and require the shared bearer token.

---

## Architecture

```text
ocs CLI / HTTP client
          |
          v
  controller (SQLite metadata, API, placement, repair)
       /       |       \
      v        v        v
 worker1    worker2   worker3 ... worker4
   (chunk files, checksums, scrubber, heartbeat)
```

### Design choices

- **Controller proxy:** clients see one API and never need worker locations.
- **SQLite WAL:** metadata commits are ACID without another service to operate.
- **Fixed 1 MiB chunks:** bounded memory and simple ordered reconstruction.
- **RF=2 and write acknowledgements:** uploads are durable before becoming visible.
- **Level-triggered reconciliation:** desired metadata state converges with worker
  inventories after failures, corruption, restarts, and missed events.
- **End-to-end checksums:** the controller verifies bytes before serving them,
  rather than trusting a worker's local sidecar alone.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for request flows and the full
architecture decision log.

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Go version declared in [`go.mod`](go.mod) (for running unit tests locally without Docker)
- `curl`, `bash`, and either `sha256sum` (Linux) or `shasum` (macOS)

### 1. Configure and launch the cluster
```bash
# Optional: replace the development token before starting the cluster.
cp .env.example .env

# Start the controller, four workers, and client container.
make up
```

Verify cluster status:
```bash
make build
./bin/ocs status
# The same check using curl:
curl -H "Authorization: Bearer ${CLUSTER_TOKEN:-dev-secret-change-me}" \
  http://localhost:8080/api/v1/admin/status
```

### 2. Basic CLI usage
```bash
# Upload a file
./bin/ocs cp ./my_photo.jpg obj:photos/vacation.jpg --meta "album=holiday,owner=alice"

# Inspect object metadata and chunk replica distribution
./bin/ocs stat obj:photos/vacation.jpg

# Stream / cat directly to stdout
./bin/ocs cat obj:photos/vacation.jpg | shasum -a 256

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

## CLI usage

The `ocs` command is the recommended client. It talks only to the controller;
workers remain private inside the Compose network.

| Command | Purpose |
|---|---|
| `ocs cp SOURCE obj:NAME` | Upload a local file |
| `ocs cp obj:NAME DEST` | Download an object |
| `ocs cat obj:NAME` | Stream object bytes |
| `ocs ls obj:PREFIX` | List objects |
| `ocs stat obj:NAME` | Show object and replica metadata |
| `ocs meta get\|set\|rm obj:NAME ...` | Read or change custom metadata |
| `ocs rm obj:NAME` | Delete an object |
| `ocs status` | Show controller and worker health |
| `ocs verify obj:NAME LOCAL_FILE` | Compare an object with a local file |

Run `./bin/ocs` without arguments for the complete option and flag reference.

## Interactive demos

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

## HTTP API

The controller is the public API boundary. Object requests use the following
routes:

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/api/v1/objects` | Upload an object as multipart form data |
| `GET` | `/api/v1/objects` | List objects, optionally filtered by prefix |
| `GET` | `/api/v1/objects/{id}` | Download an object |
| `DELETE` | `/api/v1/objects/{id}` | Delete an object |
| `GET` | `/api/v1/objects/{id}/metadata` | Read object and custom metadata |
| `PATCH` | `/api/v1/objects/{id}/metadata` | Update custom metadata |
| `GET` | `/api/v1/admin/status` | Read cluster and worker status |
| `POST` | `/api/v1/admin/reconcile` | Request an immediate reconciliation pass |
| `POST` | `/api/v1/admin/scrub` | Request worker scrub operations |
| `GET` | `/healthz` | Public liveness check |

Worker routes under `/internal/*` are cluster-internal and are not intended for
direct client use. See [`internal/api/types.go`](internal/api/types.go) and
[`internal/controller/server.go`](internal/controller/server.go) for the
request and response contracts.

## Configuration

The repository includes two Compose files:

- [`docker-compose.yml`](docker-compose.yml) defines the cluster and persistent
  named volumes.
- [`docker-compose.dev-ports.yml`](docker-compose.dev-ports.yml) is a
  development override that exposes worker ports `9001`-`9004` for
  [`scripts/smoke.sh`](scripts/smoke.sh). Worker ports are otherwise private.

The Makefile combines both files for local development. Use the base file alone
when host access to worker HTTP endpoints is not needed:

```bash
docker compose -f docker-compose.yml up -d --build
```

### Controller Environment Variables
| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | Controller HTTP listen address |
| `DB_PATH` | `/data/meta.db` | Path to SQLite metadata database |
| `CLUSTER_TOKEN` | `dev-secret-change-me` | Shared bearer token for internal cluster authentication |
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
| `STARTUP_GRACE` | `15s` | Startup delay before initial reconciliation pass |
| `RECONCILE_INTERVAL` | `5s` | Periodic reconciliation loop interval |
| `INVENTORY_SYNC_INTERVAL` | `5m` | Periodic block-report sync interval |

### Worker Environment Variables
| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | `worker1` | Unique node identifier |
| `WORKER_ADDR` | `http://worker1:9000` | Advertised reachable worker URL |
| `WORKER_LISTEN` | `:9000` | Worker HTTP listen address |
| `CONTROLLER_URL` | `http://controller:8080` | Controller URL for registration and heartbeats |
| `DATA_DIR` | `/data` | Directory for chunk storage and quarantine |
| `SCRUB_INTERVAL` | `10m` | Interval for full background disk scrub passes |
| `SCRUB_RATE_MBPS` | `50` | I/O throttle rate limit for background scrubber |

---

## Development and testing

```bash
# Run unit and integration tests with race detector (Docker is not required)
make test

# Run static checks
make vet

# Format, vet, and run race-enabled tests
make check

# Build native binaries in ./bin/
make build

# Tear down the cluster
make down
```

`make smoke` and `make demo` exercise the live Docker cluster; run `make up`
first. The smoke and demo scripts read `CLUSTER_TOKEN` from `.env` when present.

The demo suite validates basic object operations, chunking, replica placement,
metadata patching, worker failure recovery, corruption fallback, scrubbing, and
repair.

For a clean local teardown, run `make down`. Named volumes are preserved unless
they are explicitly removed with Docker, so object data survives ordinary
container restarts.

## Repository layout

| Path | Responsibility |
|---|---|
| [`README.md`](README.md) | User guide, workflows, configuration, and API overview |
| [`Dockerfile`](Dockerfile) | Multi-stage images for the controller, workers, and CLI |
| [`docker-compose.yml`](docker-compose.yml) | Base local cluster definition |
| [`docker-compose.dev-ports.yml`](docker-compose.dev-ports.yml) | Optional host worker-port override |
| [`Makefile`](Makefile) | Build, test, Docker, smoke-test, and demo commands |
| [`go.mod`](go.mod) / [`go.sum`](go.sum) | Go module and dependency lock data |
| [`cmd/controller`](cmd/controller) | Controller process entrypoint |
| [`cmd/worker`](cmd/worker) | Worker process entrypoint |
| [`cmd/ocs`](cmd/ocs) | CLI entrypoint |
| [`internal/api`](internal/api) | Shared public and internal API types |
| [`internal/common`](internal/common) | Shared configuration, HTTP, IDs, and checksum helpers |
| [`internal/controller`](internal/controller) | API, metadata, placement, and reconciliation |
| [`internal/worker`](internal/worker) | Chunk storage, HTTP API, registration, and scrubbing |
| [`internal/workerclient`](internal/workerclient) | Controller-side worker HTTP client and test fake |
| [`internal/cli`](internal/cli) | CLI commands and controller client |
| [`scripts/smoke.sh`](scripts/smoke.sh) | Worker-level live smoke test |
| [`scripts/demo`](scripts/demo) | End-to-end behavior demonstrations |
| [`scripts/seed.go`](scripts/seed.go) | Development-only metadata seeding helper |
| [`plan.md`](plan.md) | Original project requirements and implementation plan |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Detailed architecture reference |

## Limitations

- The controller is a single metadata authority; it is not itself replicated.
- Authentication uses one shared bearer token intended for local demos.
- Docker Compose is the deployment target; Kubernetes and external orchestration
  are intentionally out of scope.
- The default configuration is development-oriented and should be hardened
  before production use.
