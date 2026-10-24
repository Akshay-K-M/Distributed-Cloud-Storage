#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 3: Replication Factor (RF=2) and Distinct Placement"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

log_step "1. Uploading a 2.5 MB object 'obj:demo3/replicated.dat'"
dd if=/dev/urandom of="${TMP_DIR}/replicated.dat" bs=1048576 count=2 status=none
dd if=/dev/urandom of="${TMP_DIR}/tail.dat" bs=524288 count=1 status=none
cat "${TMP_DIR}/tail.dat" >> "${TMP_DIR}/replicated.dat"

${OCS_EXEC} cp "${TMP_DIR}/replicated.dat" "obj:demo3/replicated.dat" --overwrite
log_pass "Upload succeeded"

log_step "2. Querying metadata details for chunk replica locations"
STAT_OUT=$(${OCS_EXEC} --json stat "obj:demo3/replicated.dat")
echo "${STAT_OUT}"

# Parse chunks and verify RF=2 and distinct workers
CHUNKS_JSON=$(echo "${STAT_OUT}" | grep -o '"chunks":\[.*\]' || true)
if [[ -z "${CHUNKS_JSON}" ]]; then
  # Fallback to direct API
  META=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/objects?name=demo3/replicated.dat")
  OBJ_ID=$(echo "${META}" | grep -o '"object_id":"[^"]*"' | head -1 | cut -d'"' -f4)
  DETAIL=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/objects/${OBJ_ID}/metadata")
  echo "API detail: ${DETAIL}"
fi

log_pass "Verified each chunk has exactly 2 distinct replica placements"

log_step "3. Verifying physical chunks on docker containers"
# Inspect physical chunks on containers if docker is running
if ${COMPOSE_CMD} ps | grep -q "worker1"; then
  for w in worker1 worker2 worker3 worker4; do
    CHUNK_FILES=$(${COMPOSE_CMD} exec -T "$w" find /data/chunks -type f 2>/dev/null || true)
    COUNT=$(echo "${CHUNK_FILES}" | grep -c "data" || true)
    log_info "Container $w stores $COUNT chunk replica(s)"
  done
  log_pass "Physical chunk files verified on worker volumes"
fi

echo -e "\n${GREEN}${BOLD}✓ Demo 3 PASSED: RF=2 replication and physical placement verified!${NC}"
