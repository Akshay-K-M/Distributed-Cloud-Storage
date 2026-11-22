#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_for_cluster
wait_for_workers 4
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
object=obj:demo6/integrity_test.bin
dd if=/dev/urandom of="$tmp/source" bs=1048576 count=2 status=none
${OCS_EXEC} cp "$tmp/source" "$object" --overwrite
check_replicas "$object"

detail=$(${OCS_EXEC} --json stat "$object")
chunk=$(jq -er '.chunks[0].chunk_id' <<< "$detail")
worker=$(jq -er '.chunks[0].replicas[0].worker_id' <<< "$detail")
log_step "Damaging chunk $chunk on $worker"
bash "$(dirname "${BASH_SOURCE[0]}")/../corrupt-chunk.sh" "$worker" "$chunk"

log_step "Scrubbing the worker and checking for this chunk's quarantine record"
curl -fsS -X POST -H "Authorization: Bearer ${TOKEN}" \
  "${ENDPOINT}/api/v1/admin/scrub?worker=$worker"
detected() {
  ${COMPOSE_CMD} logs --no-color "$worker" | grep 'scrub quarantined chunks' | grep -F "$chunk" >/dev/null
}
wait_until detected
log_pass "Worker detected and quarantined the damaged chunk"

${OCS_EXEC} cp "$object" "$tmp/recovered"
cmp "$tmp/source" "$tmp/recovered"
wait_until check_replicas "$object"
log_pass "Downloaded bytes match and both stored copies pass SHA-256 verification"
