#!/usr/bin/env bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

ENDPOINT="${OCS_ENDPOINT:-http://localhost:8080}"
TOKEN="${CLUSTER_TOKEN:-dev-secret-change-me}"
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"

# Determine ocs binary or client container
if [[ -f "./bin/ocs" ]]; then
  OCS_EXEC="./bin/ocs"
elif command -v ocs &>/dev/null; then
  OCS_EXEC="ocs"
else
  OCS_EXEC="${COMPOSE_CMD} exec -T client ocs"
fi

export OCS_ENDPOINT="${ENDPOINT}"
export OCS_TOKEN="${TOKEN}"

log_step() {
  echo -e "\n${CYAN}==>${NC} ${BOLD}$1${NC}"
}

log_info() {
  echo -e "  ${BLUE}info:${NC} $1"
}

log_pass() {
  echo -e "  ${GREEN}PASS:${NC} $1"
}

log_fail() {
  echo -e "  ${RED}FAIL:${NC} $1"
  exit 1
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local msg="${3:-assertion failed}"
  if [[ "$expected" != "$actual" ]]; then
    log_fail "$msg: expected '$expected', got '$actual'"
  fi
}

wait_for_cluster() {
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
