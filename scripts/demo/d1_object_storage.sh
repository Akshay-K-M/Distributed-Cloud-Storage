#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

log_step "Demo 1: Basic Object Storage Operations (Upload, Download, List, Delete)"

wait_for_cluster
wait_for_workers 4

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

# 1. Upload a 3.5 MB random binary file
log_step "1. Generating 3.5 MB random file"
dd if=/dev/urandom of="${TMP_DIR}/random_3.5mb.bin" bs=1048576 count=3 status=none
dd if=/dev/urandom of="${TMP_DIR}/tail.bin" bs=524288 count=1 status=none
cat "${TMP_DIR}/tail.bin" >> "${TMP_DIR}/random_3.5mb.bin"

ORIG_SHA=$(sha256sum "${TMP_DIR}/random_3.5mb.bin" | awk '{print $1}')
log_info "Generated file size: $(wc -c < "${TMP_DIR}/random_3.5mb.bin") bytes, SHA256: ${ORIG_SHA}"

log_step "2. Uploading 3.5 MB file to ocs as 'obj:demo1/large_file.bin'"
${OCS_EXEC} cp "${TMP_DIR}/random_3.5mb.bin" "obj:demo1/large_file.bin" --overwrite
log_pass "Upload succeeded"

log_step "3. Downloading file and verifying end-to-end SHA-256"
${OCS_EXEC} cp "obj:demo1/large_file.bin" "${TMP_DIR}/downloaded.bin"
DL_SHA=$(sha256sum "${TMP_DIR}/downloaded.bin" | awk '{print $1}')
assert_eq "${ORIG_SHA}" "${DL_SHA}" "Downloaded SHA256 matches original"
log_pass "SHA256 verified successfully: ${DL_SHA}"

log_step "4. Uploading different payload types (text, json)"
echo "Hello, Distributed Object Store!" > "${TMP_DIR}/hello.txt"
${OCS_EXEC} cp "${TMP_DIR}/hello.txt" "obj:demo1/hello.txt" --overwrite

echo '{"demo": true, "phase": 4, "reliable": true}' > "${TMP_DIR}/data.json"
${OCS_EXEC} cp "${TMP_DIR}/data.json" "obj:demo1/data.json" --content-type "application/json" --overwrite
log_pass "Text and JSON files uploaded"

log_step "5. Listing objects with prefix 'obj:demo1/'"
LIST_OUT=$(${OCS_EXEC} ls obj:demo1/)
echo "${LIST_OUT}"
if ! echo "${LIST_OUT}" | grep -q "demo1/large_file.bin"; then
  log_fail "Expected demo1/large_file.bin in listing"
fi
if ! echo "${LIST_OUT}" | grep -q "demo1/hello.txt"; then
  log_fail "Expected demo1/hello.txt in listing"
fi
log_pass "Prefix listing verified"

log_step "6. Verifying checksum using 'ocs verify'"
${OCS_EXEC} verify "obj:demo1/large_file.bin" "${TMP_DIR}/random_3.5mb.bin"
log_pass "ocs verify succeeded"

log_step "7. Deleting object 'obj:demo1/hello.txt'"
${OCS_EXEC} rm "obj:demo1/hello.txt"
log_pass "Object deleted"

echo -e "\n${GREEN}${BOLD}✓ Demo 1 PASSED: Basic Object Storage Operations verified!${NC}"
