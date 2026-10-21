#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
TOKEN="${CLUSTER_TOKEN:-dev-secret-change-me}"
BASE="${WORKER_URL:-http://127.0.0.1:9001}"
AUTH="Authorization: Bearer ${TOKEN}"

echo "== healthz (no auth)"
curl -fsS "${BASE}/healthz" | grep -q '"status":"ok"'

CHUNK_ID="smoke_0"
TMP="$(mktemp)"
trap 'rm -f "$TMP" "$TMP.got" "$TMP.sha"' EXIT
dd if=/dev/urandom of="$TMP" bs=1048576 count=1 status=none
SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"

echo "== PUT 1MiB chunk"
CODE="$(curl -sS -o /tmp/smoke-put.json -w '%{http_code}' \
  -X PUT "${BASE}/internal/chunks/${CHUNK_ID}" \
  -H "$AUTH" \
  -H "X-Chunk-SHA256: ${SHA}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"$TMP")"
test "$CODE" = "201" || test "$CODE" = "200"

echo "== GET and compare"
curl -fsS -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID}" -o "$TMP.got"
cmp -s "$TMP" "$TMP.got"

echo "== inventory contains chunk"
curl -fsS -H "$AUTH" "${BASE}/internal/workers/worker1/chunks" | grep -q "$CHUNK_ID"

echo "== corrupt on disk and scrub"
FILE="$(docker exec worker1 sh -c "find /data/chunks -type f ! -name '*.meta' | head -n1")"
test -n "$FILE"
docker exec worker1 sh -c "dd if=/dev/zero of='$FILE' bs=1 count=8 conv=notrunc status=none"
curl -fsS -o /dev/null -w '%{http_code}' -X POST -H "$AUTH" "${BASE}/internal/scrub" | grep -q 202
# wait for in-process scrub (trigger is async)
for i in $(seq 1 30); do
  CODE="$(curl -sS -o /dev/null -w '%{http_code}' -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID}" || true)"
  if [[ "$CODE" == "404" ]]; then
    break
  fi
  sleep 0.2
done
test "$CODE" = "404"
curl -fsS -H "$AUTH" "${BASE}/internal/workers/worker1/chunks" | grep -q quarantined

echo "== PUT a durable chunk, restart, still there"
CHUNK_ID2="smoke_persist_0"
dd if=/dev/urandom of="$TMP" bs=4096 count=1 status=none
SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"
curl -fsS -X PUT "${BASE}/internal/chunks/${CHUNK_ID2}" \
  -H "$AUTH" -H "X-Chunk-SHA256: ${SHA}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"$TMP" >/dev/null
docker restart worker1 >/dev/null
for i in $(seq 1 30); do
  if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.3
done
curl -fsS -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}" -o "$TMP.got"
cmp -s "$TMP" "$TMP.got"

echo "== DELETE"
CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}")"
test "$CODE" = "204"
CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H "$AUTH" "${BASE}/internal/chunks/${CHUNK_ID2}")"
test "$CODE" = "204"

echo "WORKER SMOKE PASS"
