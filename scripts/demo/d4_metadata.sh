#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 4: Custom Metadata Operations (Attach, Get, Patch, Delete Key)"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Important document payload" > "${TMP_DIR}/doc.txt"

log_step "1. Uploading object with initial custom metadata"
${OCS_EXEC} cp "${TMP_DIR}/doc.txt" "obj:demo4/doc.txt" --meta "env=prod" --meta "author=alice" --overwrite
log_pass "Upload succeeded with initial metadata"

log_step "2. Reading metadata using 'ocs meta get'"
META_GET=$(${OCS_EXEC} meta get "obj:demo4/doc.txt")
echo "${META_GET}"

if ! echo "${META_GET}" | grep -q "env=prod"; then
  log_fail "Expected env=prod in metadata"
fi
if ! echo "${META_GET}" | grep -q "author=alice"; then
  log_fail "Expected author=alice in metadata"
fi
log_pass "Metadata get verified"

log_step "3. Updating metadata using 'ocs meta set'"
${OCS_EXEC} meta set "obj:demo4/doc.txt" "status=reviewed" "author=bob"
SET_OUT=$(${OCS_EXEC} meta get "obj:demo4/doc.txt")
echo "${SET_OUT}"

if ! echo "${SET_OUT}" | grep -q "status=reviewed"; then
  log_fail "Expected status=reviewed in updated metadata"
fi
if ! echo "${SET_OUT}" | grep -q "author=bob"; then
  log_fail "Expected author=bob in updated metadata"
fi
log_pass "Metadata set (update + add) verified"

log_step "4. Removing a key using 'ocs meta rm'"
${OCS_EXEC} meta rm "obj:demo4/doc.txt" "env"
RM_OUT=$(${OCS_EXEC} meta get "obj:demo4/doc.txt")
echo "${RM_OUT}"

if echo "${RM_OUT}" | grep -q "env="; then
  log_fail "Expected env to be removed from metadata"
fi
if ! echo "${RM_OUT}" | grep -q "author=bob"; then
  log_fail "Expected author=bob to remain in metadata"
fi
log_pass "Metadata rm verified"

echo -e "\n${GREEN}${BOLD}✓ Demo 4 PASSED: Metadata operations verified!${NC}"
