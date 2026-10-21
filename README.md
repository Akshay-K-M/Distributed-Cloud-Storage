# Distributed-Cloud-Storage

Fault-tolerant blob store (mini distributed object store).

Worker nodes that store, serve, verify, delete, and inventory 1 MiB chunks.

```bash
# unit tests (no Docker)
make vet
make test

# four workers with host ports 9001-9004
make up
bash scripts/smoke.sh
make down
```

`CLUSTER_TOKEN` lives in `.env` (default `dev-secret-change-me`) and authenticates every `/internal/*` call. `GET /healthz` is open.

