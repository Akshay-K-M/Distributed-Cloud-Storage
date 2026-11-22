# Architecture

Clients use one REST endpoint on port 8080. The Go controller owns the object catalog and coordinates four storage workers. Each worker owns one persistent Docker volume. The Compose client runs the same `ocs` executable that can run on the host.

```text
ocs or curl ── object HTTP requests ──► controller ── SQL ──► SQLite volume
                                           │
                                 chunk and inventory HTTP
                                           │
                                  ┌────────┴────────┐
                                  worker1 … worker4
                                  one volume each
```

Workers initiate registration and heartbeats in the opposite direction. The controller uses their advertised addresses for chunk operations. Internal routes use a shared bearer token. The public API is intended for a local cluster.

| Caller → receiver | Internal endpoint | Purpose |
|---|---|---|
| Worker → controller | `POST /internal/workers/register` | Advertise worker ID, address and capacity |
| Worker → controller | `POST /internal/workers/heartbeat` | Refresh liveness and report usage and quarantine |
| Controller → worker | `PUT`, `GET`, `DELETE /internal/chunks/{chunk_id}` | Store, fetch or remove one replica |
| Controller → worker | `GET /internal/workers/{worker_id}/chunks` | Compare the worker's local index with the catalog |

## What runs in the background?

Docker Compose runs one controller process and four worker processes. Each Go process runs several **goroutines** that share its memory. Go schedules them across operating-system threads, so the number of threads can vary. The client container waits for commands. Each invocation of `ocs` runs for the duration of that command.

Each server's main goroutine starts its HTTP listener in a goroutine and waits for a shutdown signal or server error. Go's HTTP server handles concurrent connections while the following application loops run. Intervals are for the [Compose configuration](../docker-compose.yml).

| Owner | Long-running goroutine | When it runs | What it does |
|---|---|---|---|
| Controller | Registry sweeper | Every 1 s | Checks the age of each worker's last heartbeat. Marks silent workers suspect or dead. A status change wakes the reconciler. |
| Controller | Reconciliation loop | Every 5 s or on a wake signal, after a 10 s startup grace period | Runs garbage collection, reads catalog and worker state, plans replica changes, then waits for the executor to finish them. |
| Controller | Inventory sync loop | Every 30 s | Requests inventories from live workers, compares them with SQLite, updates replica records and asks workers to delete orphaned files. Inventory sync and reconciliation share a lock so they cannot change replica records concurrently with each other. |
| Each worker | Registrar | At startup, then heartbeats every 2 s | Registers the worker, retries failed registration with backoff, then sends liveness, usage and quarantine reports. An unknown-worker reply sends this same goroutine back to registration. |
| Each worker | Scrubber | Every 10 min or on an admin scrub request | Reads local chunk files, verifies checksums and quarantines damaged copies. The registrar reports their IDs in a heartbeat. |

**Where garbage collection fits.** The garbage collector is the first step *inside* a reconciliation pass. It deletes replicas of objects marked `DELETING` from live workers. A failed delete on a reachable worker keeps the object record for retry. An offline worker can retain a file after the catalog entry is removed. When that worker returns, inventory sync identifies the file as an orphan and requests deletion.

**Short-lived goroutines.** These run only when an operation needs them.

- **Replica maintenance.** The planner runs inline in the reconciliation loop. The executor starts a goroutine for each planned repair, trim or corrupt-copy deletion, with up to two actions active at once.
- **Inventory requests.** Worker registration starts a one-off inventory sync for that worker. An admin request can also start a full sync between periodic checks.
- **Object transfers.** The controller admits up to 16 uploads at once. Each upload processes up to four distinct chunks concurrently, and each chunk sends its two initial replica writes in parallel. An aborted upload can launch a cleanup goroutine for copies already written. Downloads fetch and verify chunks in order within the HTTP handler.
- **CLI uploads.** The client uses a goroutine to write the multipart request body while its HTTP client sends it.

## Where one object goes

Suppose a client uploads a 2.5 MiB video. The controller assigns a UUIDv7 and reads the multipart file in 1,048,576-byte chunks. One possible placement is shown below. Each worker listed on a row receives the entire chunk.

| Chunk index | Size | Replica locations |
|---|---:|---|
| 0 | 1 MiB | worker1, worker2 |
| 1 | 1 MiB | worker3, worker4 |
| 2 | 512 KiB | worker2, worker3 |

The controller chooses distinct `ALIVE` workers that report enough capacity, favoring lower used-byte counts. Each worker verifies the incoming size and SHA-256, writes the bytes and a checksum sidecar, then acknowledges. Once every chunk has two acknowledgments, one SQLite transaction commits the object, ordered chunk records and replica locations. Public reads see only committed objects.

A download looks up the map, fetches chunks in index order and checks each against its catalog length and hash. A failed read tries another replica. The controller stages verified bytes in a temporary file so it can return a JSON 503 even if the final chunk is unavailable. Staging costs up to one object's worth of temporary disk per simultaneous download.

Listing and editing labels use SQLite. Deletion first marks the object `DELETING`, immediately hiding its ID from clients. Garbage collection deletes worker files and the catalog entry. If a worker is offline, its next inventory lets the controller clean up leftover files.

## Failure and recovery

| Observation | Controller or worker response |
|---|---|
| A worker misses heartbeats | Heartbeats normally arrive every 2 s. At about 6 s the controller marks the worker suspect, and at about 15 s it marks it dead. New writes use live workers. Reads can try a suspect worker after live candidates. |
| A chunk loses a copy | After a dead worker's 5 s repair delay, the reconciler can read a surviving replica, verify it and write a replacement. Its periodic pass runs every 5 s. |
| A worker rejoins | It rebuilds its local index from disk and registers. Its inventory reveals missing, returning and orphaned copies. Excess live copies can be trimmed back to two. |
| Stored bytes change | Downloads verify each chunk. Workers also scrub their disks every 10 min or on request. They quarantine damaged chunks and report their IDs. Verified copies can supply a repair. |
| The controller restarts | SQLite retains committed objects. Startup removes abandoned upload records. Worker inventories let it remove orphaned files. |

A single worker outage leaves a healthy two-copy deployment readable. Losing every copy of any one chunk makes the object unavailable. A controller outage takes the API offline. The four worker volumes share the Docker host, so this deployment demonstrates worker failure within one machine.

## Find the implementation

| Path | Responsibility |
|---|---|
| `cmd/controller`, `cmd/worker`, `cmd/ocs` | Process startup and wiring |
| `internal/api`, `internal/cli` | Shared wire formats, client commands and HTTP calls |
| `internal/controller/server.go`, `admin.go` | Object API, worker reports and operational endpoints |
| `internal/controller/datapath`, `placement`, `registry` | Byte transfers, worker selection and heartbeat state |
| `internal/controller/metadata`, `reconciler` | SQLite schema and transactions, inventory, repair and deletion |
| `internal/worker`, `internal/workerclient` | Chunk files, scrubbing and controller-to-worker HTTP |
| `scripts/demo`, `scripts/smoke.sh` | Six assignment demonstrations and direct worker checks |

Build commands, public endpoints and demonstration instructions are in the [README](../README.md).
