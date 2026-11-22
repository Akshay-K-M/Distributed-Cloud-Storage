#!/usr/bin/env bash
# Demo 3 verifies distinct replica placement and physical worker distribution.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 3: Replication Factor (RF=2) and Distinct Placement"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

log_step "1. Uploading a 2.5 MB object 'obj:demo3/replicated.dat'"
# Multiple chunks make placement and replica distribution observable.
dd if=/dev/urandom of="${TMP_DIR}/replicated.dat" bs=1048576 count=2 status=none
dd if=/dev/urandom of="${TMP_DIR}/tail.dat" bs=524288 count=1 status=none
cat "${TMP_DIR}/tail.dat" >> "${TMP_DIR}/replicated.dat"

${OCS_EXEC} cp "${TMP_DIR}/replicated.dat" "obj:demo3/replicated.dat" --overwrite
log_pass "Upload succeeded"

log_step "Checking every chunk against files on the four worker volumes"
check_replicas "obj:demo3/replicated.dat"
log_pass "Each chunk has two distinct, checksum-verified replicas"
