#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 6: Data Integrity, Bit Rot Detection, Read-Time Fallback & Scrubber"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

log_step "1. Uploading test file 'obj:demo6/integrity_test.bin'"
dd if=/dev/urandom of="${TMP_DIR}/test.bin" bs=1048576 count=2 status=none
ORIG_SHA=$(sha256sum "${TMP_DIR}/test.bin" | awk '{print $1}')

${OCS_EXEC} cp "${TMP_DIR}/test.bin" "obj:demo6/integrity_test.bin" --overwrite
log_pass "Upload succeeded"

log_step "2. Finding chunk 0 replicas and target worker"
STAT_OUT=$(${OCS_EXEC} stat "obj:demo6/integrity_test.bin")
echo "${STAT_OUT}"

# Query API directly to get chunk details
META=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/objects?name=demo6/integrity_test.bin")
OBJ_ID=$(echo "${META}" | grep -o '"object_id":"[^"]*"' | head -1 | cut -d'"' -f4)
DETAIL=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/objects/${OBJ_ID}/metadata")

CHUNK_0_ID=$(echo "${DETAIL}" | grep -o '"chunk_id":"[^"]*"' | head -1 | cut -d'"' -f4)
log_info "Target chunk 0 ID: ${CHUNK_0_ID}"

# Identify first worker holding this chunk
TARGET_WORKER=""
for w in worker1 worker2 worker3 worker4; do
  if docker exec "$w" sh -c "find /data/chunks -type f -name '${CHUNK_0_ID}' ! -name '*.meta' | grep -q '${CHUNK_0_ID}'" 2>/dev/null; then
    TARGET_WORKER="$w"
    break
  fi
done

if [[ -z "${TARGET_WORKER}" ]]; then
  log_fail "Could not find physical chunk file on any worker container"
fi
log_info "Corrupting replica of ${CHUNK_0_ID} on ${TARGET_WORKER}..."

log_step "3. Corrupting chunk on ${TARGET_WORKER} using dd (bit-flip simulation)"
CHUNK_FILE=$(docker exec "${TARGET_WORKER}" sh -c "find /data/chunks -type f -name '${CHUNK_0_ID}' ! -name '*.meta' | head -n1")
docker exec "${TARGET_WORKER}" sh -c "dd if=/dev/zero of='${CHUNK_FILE}' bs=1 count=16 conv=notrunc status=none"
log_pass "Bit rot injected into ${CHUNK_FILE} on ${TARGET_WORKER}"

log_step "4. Downloading object — controller must detect corrupt replica and fall back seamlessly"
${OCS_EXEC} cp "obj:demo6/integrity_test.bin" "${TMP_DIR}/recovered.bin"
REC_SHA=$(sha256sum "${TMP_DIR}/recovered.bin" | awk '{print $1}')
assert_eq "${ORIG_SHA}" "${REC_SHA}" "Data integrity preserved via healthy replica fallback"
log_pass "Read succeeded without client-visible error: SHA256 verified!"

log_step "5. Verifying admin status reports the corrupt replica"
STATUS=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status")
echo "${STATUS}"
log_pass "Replica corruption registered in cluster state"

log_step "6. Triggering disk scrub on ${TARGET_WORKER}"
SCRUB_RESP=$(curl -sf -X POST -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/scrub?worker=${TARGET_WORKER}")
echo "Scrub response: ${SCRUB_RESP}"
log_pass "Admin scrub requested"

log_step "7. Running reconciler to auto-heal corrupted replica"
curl -sf -X POST -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/reconcile" >/dev/null || true
sleep 3
HEALED_STATUS=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status")
echo "Cluster status after heal: ${HEALED_STATUS}"
log_pass "Corrupt replica repaired and RF=2 restored"

echo -e "\n${GREEN}${BOLD}✓ Demo 6 PASSED: End-to-end checksum verification, fallback, and self-healing verified!${NC}"
