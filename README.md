# Mini distributed cloud object store

Store an arbitrary binary file through one HTTP endpoint. A Go controller splits it into 1 MiB chunks and places two copies of every chunk on different workers. SQLite holds the object catalog. Four Docker workers each own a separate persistent volume.

For a short overview of placement, persistence, failure handling and the source layout, see the [architecture guide](docs/ARCHITECTURE.md).

## Start the cluster

From the project directory, run these commands with Docker Compose and Make installed.

```sh
test -f .env || cp .env.example .env
make up
docker compose exec client ocs status
```

Wait for all four workers to report `ALIVE`. The controller listens on port 8080. The base Compose file keeps workers on the Compose network. The development override used by `make up` also publishes ports 9001–9004 for the worker smoke test.

## Try the complete object lifecycle

This example uses the included client container. Its local files live inside that container.

```sh
docker compose exec client sh -c 'echo "first observation" > /tmp/notes.txt'
docker compose exec client ocs cp /tmp/notes.txt obj:lab/notes.txt --meta owner=alice
docker compose exec client ocs stat obj:lab/notes.txt
docker compose exec client ocs meta set obj:lab/notes.txt owner=bob
docker compose exec client ocs ls obj:lab/
docker compose exec client ocs cp obj:lab/notes.txt /tmp/copy.txt
docker compose exec client cmp /tmp/notes.txt /tmp/copy.txt
docker compose exec client ocs rm obj:lab/notes.txt
```

For files on your own computer, install the Go version declared in `go.mod` and run `make build`. Use `./bin/ocs` in place of `docker compose exec client ocs`. The CLI resolves a name such as `obj:lab/notes.txt` to the ID used by the REST API.

## REST interface

| Method | Path | Input or result | Success |
|---|---|---|---|
| POST | `/api/v1/objects` | Multipart `file`, optional `metadata` JSON object of string labels. Returns the object summary and ID. | 201 |
| GET | `/api/v1/objects/{id}` | Original bytes and content type | 200 |
| GET | `/api/v1/objects/{id}/metadata` | System fields, user labels, ordered chunks and replica locations | 200 |
| PATCH | `/api/v1/objects/{id}/metadata` | JSON such as `{"owner":"bob"}`. A null value removes that key. | 200 |
| GET | `/api/v1/objects` | Object summaries with optional prefix filtering and pagination | 200 |
| DELETE | `/api/v1/objects/{id}` | Hides the object immediately and schedules physical cleanup | 204 |

For example, upload a local file directly with `curl`.

```sh
curl -F 'file=@README.md;type=text/markdown' \
     -F 'metadata={"owner":"alice"}' http://localhost:8080/api/v1/objects
```

API errors include a JSON message. Invalid input returns 400, a missing object 404, a name conflict 409, an internal error 500, and insufficient available storage or readable replicas 503.

## Run the demonstrations

The host-side demos use Bash, Make, Docker Compose, `curl`, `jq`, a local Go toolchain or installed `ocs`, and `sha256sum` or `shasum`. Run them on a local demo cluster. They create objects under `demo1/` through `demo6/`, stop and restart a worker, and damage one replica of a demo object.

```sh
make demo
```

| Demo | Observable result |
|---|---|
| 1. Object storage | A binary download matches its source checksum. Listing and deletion work. |
| 2. Chunking | A 10.5 MiB object has ten full chunks and a 512 KiB tail in the correct order. |
| 3. Replication | Each recorded location matches a physical file with the expected SHA-256 digest. |
| 4. Metadata | Existing labels can be changed, added and removed after upload. |
| 5. Worker failure | A download succeeds during the outage. Two copies are verified on surviving workers and again after restart. |
| 6. Data integrity | A worker quarantines a deliberately damaged chunk. The download matches and two verified copies are restored. |

Run one scenario with, for example, `bash scripts/demo/d5_failure.sh`. It prefers worker2 and selects another replica holder when the test object has no chunks on worker2.

## Development and shutdown

| Command | Purpose |
|---|---|
| `make build` | Compile the controller, worker and CLI into `bin/` |
| `make vet` | Run Go static analysis |
| `make test` | Run the Go test suite with the race detector |
| `make smoke` | Pause the controller, exercise the worker HTTP API, test scrubbing and restart persistence, then restart the controller |
| `make down` | Stop the cluster and retain its named volumes for the next start |

Cluster settings, heartbeat intervals, repair delays and volume mounts are defined in [docker-compose.yml](docker-compose.yml).
