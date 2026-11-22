#!/usr/bin/env bash
# Exercise the worker API, scrubber, persistence, and idempotent deletion.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  # Export local configuration so curl and Docker commands use the same token.
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
TOKEN="${CLUSTER_TOKEN:-dev-secret-change-me}"
BASE="${WORKER_URL:-http://127.0.0.1:9001}"
AUTH="Authorization: Bearer ${TOKEN}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.dev-ports.yml)
CONTROLLER_STOPPED=0

cleanup() {
  if (( CONTROLLER_STOPPED )); then
    "${COMPOSE[@]}" start controller >/dev/null
  fi
}

echo "== healthz (no auth)"
# Health is intentionally public; all chunk operations below require the token.
curl -fsS "${BASE}/healthz" | grep -q '"status":"ok"'

CHUNK_ID="smoke_$$_0"
TMP="$(mktemp)"
trap 'rm -f "$TMP" "$TMP.got" "$TMP.sha"; cleanup' EXIT
# Direct worker PUTs are intentionally absent from the object catalog. Pause
# the controller throughout this worker-only test so inventory cannot reap them.
"${COMPOSE[@]}" stop controller >/dev/null
CONTROLLER_STOPPED=1
dd if=/dev/urandom of="$TMP" bs=1048576 count=1 status=none
SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"

echo "== PUT 1MiB chunk"
# Upload a full-size chunk and let the worker verify its declared checksum.
CODE="$(curl -sS -o /dev/null -w '%{http_code}' \
  -X PUT "${BASE}/internal/chunks/${CHUNK_ID}" \
  -H "$AUTH" \
  -H "X-Chunk-SHA256: ${SHA}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"$TMP")"
test "$CODE" = "201" || test "$CODE" = "200"

echo "== GET and compare"
# Read the chunk back and compare the raw bytes.
curl -fsS -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID}" -o "$TMP.got"
cmp -s "$TMP" "$TMP.got"

echo "== inventory contains chunk"
# Inventory must expose the newly stored chunk.
curl -fsS -H "$AUTH" "${BASE}/internal/workers/worker1/chunks" | grep -q "$CHUNK_ID"

echo "== corrupt on disk and scrub"
# Corrupt the file directly, then request asynchronous checksum scrubbing.
FILE="$(docker exec worker1 sh -c "find /data/chunks -type f -name '$CHUNK_ID' -print -quit")"
test -n "$FILE"
docker exec worker1 sh -c "dd if=/dev/zero of='$FILE' bs=1 count=8 conv=notrunc status=none"
curl -fsS -o /dev/null -w '%{http_code}' -X POST -H "$AUTH" "${BASE}/internal/scrub" | grep -q 202
# The scrub request is asynchronous, so poll until the corrupt chunk is hidden.
for i in $(seq 1 30); do
  CODE="$(curl -sS -o /dev/null -w '%{http_code}' -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID}" || true)"
  if [[ "$CODE" == "404" ]]; then
    break
  fi
  sleep 0.2
done
test "$CODE" = "404"
curl -fsS -H "$AUTH" "${BASE}/internal/workers/worker1/chunks" | jq -e --arg id "$CHUNK_ID" '.quarantined | index($id) != null' >/dev/null

echo "== PUT a durable chunk, restart, still there"
# Stop the controller during restart so reconciliation cannot delete this test-only orphan.
CHUNK_ID2="smoke_persist_$(date +%s)"
dd if=/dev/urandom of="$TMP" bs=4096 count=1 status=none
SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"
curl -fsS -X PUT "${BASE}/internal/chunks/${CHUNK_ID2}" \
  -H "$AUTH" -H "X-Chunk-SHA256: ${SHA}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"$TMP" >/dev/null
docker restart worker1 >/dev/null
for i in $(seq 1 30); do
  if curl -fsS -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}" -o "$TMP.got"; then
    break
  fi
  sleep 0.3
done
cmp -s "$TMP" "$TMP.got"

echo "== DELETE"
# Deletion is intentionally checked twice because the endpoint is idempotent.
CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}")"
test "$CODE" = "204"
CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}")"
test "$CODE" = "204"

echo "WORKER SMOKE PASS"
