#!/usr/bin/env bash
# Shared logging, readiness, and CLI setup for the end-to-end demos.
set -euo pipefail

if ! command -v sha256sum >/dev/null; then
  sha256sum() { shasum -a 256 "$@"; }
fi

# Output colors keep long demo runs easy to scan.
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

ENDPOINT="${OCS_ENDPOINT:-http://localhost:8080}"
if [[ -f .env && -z "${CLUSTER_TOKEN:-}" ]]; then
  source .env
fi
TOKEN="${OCS_TOKEN:-${CLUSTER_TOKEN:-dev-secret-change-me}}"
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"

# Prefer a local client so temporary files remain visible to the CLI.
if [[ -f "./bin/ocs" ]]; then
  OCS_EXEC="./bin/ocs"
elif command -v ocs &>/dev/null; then
  OCS_EXEC="ocs"
elif command -v go &>/dev/null; then
  # Keep demo-generated temporary files visible to the local CLI process.
  OCS_EXEC="go run ./cmd/ocs"
else
  echo "Install the local ocs binary or Go to run the host-side demos." >&2
  exit 1
fi

export OCS_ENDPOINT="${ENDPOINT}"
export OCS_TOKEN="${TOKEN}"

log_step() {
  # Mark a major scenario step.
  echo -e "\n${CYAN}==>${NC} ${BOLD}$1${NC}"
}

log_info() {
  # Print non-failing diagnostic information.
  echo -e "  ${BLUE}info:${NC} $1"
}

log_pass() {
  # Print a successful assertion.
  echo -e "  ${GREEN}PASS:${NC} $1"
}

log_fail() {
  # Stop the current demo as soon as an expectation fails.
  echo -e "  ${RED}FAIL:${NC} $1"
  exit 1
}

assert_eq() {
  # Compare values after removing shell-command formatting whitespace.
  local expected="$1"
  local actual
  actual="$(printf '%s' "$2" | tr -d '[:space:]')"
  local msg="${3:-assertion failed}"
  if [[ "$expected" != "$actual" ]]; then
    log_fail "$msg: expected '$expected', got '$actual'"
  fi
}

wait_for_cluster() {
  # Wait for the public controller health endpoint.
  log_info "Waiting for controller to be healthy at ${ENDPOINT}/healthz..."
  local retries=30
  until curl -sf "${ENDPOINT}/healthz" >/dev/null 2>&1 || [[ $retries -eq 0 ]]; do
    sleep 1
    retries=$((retries - 1))
  done

  if [[ $retries -eq 0 ]]; then
    log_fail "Cluster controller failed to become healthy at ${ENDPOINT}"
  fi
  log_pass "Controller is healthy"
}

wait_for_workers() {
  # Wait until enough workers report ALIVE to the controller.
  local min_workers="${1:-4}"
  log_info "Waiting for at least ${min_workers} workers to be ALIVE..."
  local retries=30
  while [[ $retries -gt 0 ]]; do
    local alive_count
    alive_count=$(curl -sf -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status" | grep -o '"status":"ALIVE"' | wc -l || true)
    if [[ "$alive_count" -ge "$min_workers" ]]; then
      log_pass "${alive_count} workers ALIVE"
      return 0
    fi
    sleep 1
    retries=$((retries - 1))
  done
  log_fail "Timed out waiting for ${min_workers} workers to be ALIVE"
}

# Compare this object's placement map with SHA-256 hashes from real worker files.
# During an outage, only the three running workers can contribute readable copies.
check_replicas() {
  local object="$1" offline="${2:-}" detail id expected actual="" worker rows
  detail=$(${OCS_EXEC} --json stat "$object") || return 1
  id=$(jq -er '.object_id' <<< "$detail") || return 1
  jq -e --arg offline "$offline" '
    (.chunks | length) > 0 and all(.chunks[];
      ([.replicas[] | select(.state == "OK" and .worker_id != $offline)
        | .worker_id] | unique | length) == 2)' <<< "$detail" >/dev/null || return 1
  expected=$(jq -r --arg offline "$offline" '.chunks[] as $c | $c.replicas[]
    | select(.state == "OK" and .worker_id != $offline)
    | [.worker_id, $c.chunk_id, $c.sha256] | @tsv' <<< "$detail" | sort) || return 1
  for worker in worker1 worker2 worker3 worker4; do
    [[ "$worker" == "$offline" ]] && continue
    rows=$(${COMPOSE_CMD} exec -T "$worker" find /data/chunks -type f \
      -name "${id}_*" ! -name '*.meta' -exec sha256sum '{}' ';') || return 1
    if [[ -n "$rows" ]]; then
      actual+=$(awk -v w="$worker" '{n=split($2,p,"/"); print w "\t" p[n] "\t" $1}' <<< "$rows")
      actual+=$'\n'
    fi
  done
  actual=$(printf '%s' "$actual" | sort)
  [[ "$actual" == "$expected" ]] || return 1
  printf '%s\n' "$actual"
}

wait_until() {
  local deadline=$((SECONDS + 90))
  until "$@"; do
    if (( SECONDS >= deadline )); then
      log_fail "Timed out waiting for $*"
    fi
    sleep 2
  done
}

worker_has_status() {
  curl -fsS -H "Authorization: Bearer ${TOKEN}" "${ENDPOINT}/api/v1/admin/status" |
    jq -e --arg id "$1" --arg state "$2" 'any(.workers[]; .worker_id == $id and .status == $state)' >/dev/null
}
