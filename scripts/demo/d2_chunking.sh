#!/usr/bin/env bash
# Demo 2 verifies fixed-size chunking and exact reconstruction of a non-aligned object.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 2: Chunking (10.5 MB -> 11 Chunks with Non-Power-of-2 Last Chunk)"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

# Generate ten full 1 MiB chunks plus a 512 KiB final chunk.
log_step "1. Generating 10.5 MB file (11,010,048 bytes)"
dd if=/dev/urandom of="${TMP_DIR}/test_10.5mb.dat" bs=1048576 count=10 status=none
dd if=/dev/urandom of="${TMP_DIR}/tail.dat" bs=524288 count=1 status=none
cat "${TMP_DIR}/tail.dat" >> "${TMP_DIR}/test_10.5mb.dat"

FILE_SIZE=$(wc -c < "${TMP_DIR}/test_10.5mb.dat")
assert_eq "11010048" "${FILE_SIZE}" "File size must be exactly 11010048 bytes"
ORIG_SHA=$(sha256sum "${TMP_DIR}/test_10.5mb.dat" | awk '{print $1}')
log_pass "Generated 10.5 MB file, SHA256: ${ORIG_SHA}"

log_step "2. Uploading 10.5 MB file to 'obj:demo2/chunked.dat'"
# The controller splits the stream and replicates each chunk.
${OCS_EXEC} cp "${TMP_DIR}/test_10.5mb.dat" "obj:demo2/chunked.dat" --overwrite
log_pass "Upload succeeded"

log_step "3. Inspecting metadata via 'ocs stat' to verify 11 chunks"
# Metadata must expose both the chunk count and the smaller final chunk.
STAT_OUT=$(${OCS_EXEC} stat "obj:demo2/chunked.dat")
echo "${STAT_OUT}"

if ! echo "${STAT_OUT}" | grep -qi "chunks:.*11"; then
  log_fail "Expected 11 chunks reported in stat output"
fi
if ! echo "${STAT_OUT}" | grep -q "524288"; then
  log_fail "Expected 524288 byte final chunk in stat output"
fi
log_pass "Stat reports 11 chunks with 524,288 byte final chunk"

log_step "4. Downloading and verifying SHA-256 reconstruction"
# Reassembly must preserve chunk order and return the original bytes.
${OCS_EXEC} cp "obj:demo2/chunked.dat" "${TMP_DIR}/downloaded_10.5mb.dat"
DL_SHA=$(sha256sum "${TMP_DIR}/downloaded_10.5mb.dat" | awk '{print $1}')
assert_eq "${ORIG_SHA}" "${DL_SHA}" "Reconstructed file SHA256 matches original"
log_pass "Verified full 10.5 MB reconstruction"

echo -e "\n${GREEN}${BOLD}✓ Demo 2 PASSED: 10.5 MB Chunking and reassembly verified!${NC}"
