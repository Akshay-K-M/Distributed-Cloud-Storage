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
