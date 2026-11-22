#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_for_cluster
wait_for_workers 4
tmp=$(mktemp -d)
stopped=""
cleanup() {
  if [[ -n "$stopped" ]]; then ${COMPOSE_CMD} start "$stopped"; fi
  rm -rf "$tmp"
}
trap cleanup EXIT

object=obj:demo5/failover.bin
dd if=/dev/urandom of="$tmp/source" bs=1048576 count=4 status=none
${OCS_EXEC} cp "$tmp/source" "$object" --overwrite
log_step "Placement before failure"
check_replicas "$object"

# Prefer worker2. On a previously populated cluster it may hold none of this object.
before=$(${OCS_EXEC} --json stat "$object")
worker=$(jq -er '
  [.chunks[].replicas[].worker_id] | if index("worker2") != null then "worker2" else first end' <<< "$before")
log_step "Stopping $worker and reading the object through its surviving replicas"
stopped="$worker"
${COMPOSE_CMD} stop "$worker"
log_info "Chunks with one surviving copy immediately after the stop (from the pre-stop map):"
jq -r --arg worker "$worker" '.chunks[] | select(any(.replicas[]; .worker_id == $worker)) | .chunk_id' <<< "$before"
${OCS_EXEC} cp "$object" "$tmp/recovered"
cmp "$tmp/source" "$tmp/recovered"
log_pass "Downloaded bytes match during the outage"

wait_until worker_has_status "$worker" DEAD
log_step "Waiting for two verified copies of every chunk on the surviving workers"
wait_until check_replicas "$object" "$worker"

${COMPOSE_CMD} start "$worker"
stopped=""
wait_until worker_has_status "$worker" ALIVE
log_step "Checking final placement with all four volumes online"
wait_until check_replicas "$object"
log_pass "Worker rejoined and every chunk has exactly two healthy copies"
