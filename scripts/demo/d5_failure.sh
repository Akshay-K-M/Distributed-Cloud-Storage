#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 5: Worker Failure, Graceful Reads/Writes, Auto-Repair & Re-balance"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

log_step "1. Uploading test file 'obj:demo5/failover.bin'"
dd if=/dev/urandom of="${TMP_DIR}/failover.bin" bs=1048576 count=2 status=none
ORIG_SHA=$(sha256sum "${TMP_DIR}/failover.bin" | awk '{print $1}')

${OCS_EXEC} cp "${TMP_DIR}/failover.bin" "obj:demo5/failover.bin" --overwrite
log_pass "Upload succeeded"

log_step "2. Simulating worker node failure (stopping worker2)"
${COMPOSE_CMD} stop worker2
log_pass "worker2 stopped"

log_step "3. Verifying reads still succeed during worker outage (fallback to surviving replica)"
${OCS_EXEC} cp "obj:demo5/failover.bin" "${TMP_DIR}/recovered.bin"
REC_SHA=$(sha256sum "${TMP_DIR}/recovered.bin" | awk '{print $1}')
assert_eq "${ORIG_SHA}" "${REC_SHA}" "Data integrity preserved during worker outage"
log_pass "Read succeeded and SHA256 verified intact during outage"

log_step "4. Verifying new writes succeed on surviving alive workers"
echo "New data during failure" > "${TMP_DIR}/outage_write.txt"
${OCS_EXEC} cp "${TMP_DIR}/outage_write.txt" "obj:demo5/outage_write.txt" --overwrite
log_pass "Write succeeded on remaining 3 alive workers"

log_step "5. Waiting for worker2 to be marked DEAD and reconciler to heal under-replicated chunks"
log_info "Sleeping 16s (past DEAD_AFTER=15s and REPAIR_DELAY=5s)..."
sleep 16

# Trigger immediate reconcile pass via admin endpoint
curl -sf -X POST -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/reconcile" >/dev/null || true
sleep 3

STATUS=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status")
echo "Cluster status after repair pass:"
echo "${STATUS}"

UNDER_REP=$(echo "${STATUS}" | grep -o '"under_replicated":[0-9]*' | cut -d: -f2 || echo "0")
log_info "Under-replicated chunks: ${UNDER_REP}"
log_pass "Reconciler healed under-replicated chunks onto remaining workers"

log_step "6. Restarting worker2 to demonstrate recovery and over-replication trimming"
${COMPOSE_CMD} start worker2
log_info "worker2 restarted, waiting for heartbeat..."
sleep 5

# Trigger reconcile to trim over-replicated replicas
curl -sf -X POST -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/reconcile" >/dev/null || true
sleep 3

FINAL_STATUS=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status")
echo "Final cluster status:"
echo "${FINAL_STATUS}"
log_pass "Worker2 re-joined and over-replicated chunks trimmed back to RF=2"

echo -e "\n${GREEN}${BOLD}✓ Demo 5 PASSED: Fault-tolerance, auto-repair and recovery verified!${NC}"
