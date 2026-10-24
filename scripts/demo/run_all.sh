#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/common.sh"

echo -e "${CYAN}${BOLD}"
echo "======================================================================"
echo "    MINI DISTRIBUTED CLOUD OBJECT STORE — END-TO-END DEMO SUITE       "
echo "======================================================================"
echo -e "${NC}"

wait_for_cluster
wait_for_workers 4

DEMOS=(
  "d1_object_storage.sh:Basic Object Storage (Put/Get/List/Delete/Verify)"
  "d2_chunking.sh:Chunking & Assembly (10.5 MB -> 11 Chunks)"
  "d3_replication.sh:Replication Factor & Distinct Placement (RF=2)"
  "d4_metadata.sh:Custom Metadata Operations (Get/Set/Rm)"
  "d5_failure.sh:Worker Failure, Failover & Auto-Healing"
  "d6_integrity.sh:Bit Rot Detection, Read Fallback & Scrubber"
)

declare -a RESULTS=()
OVERALL_SUCCESS=true

for entry in "${DEMOS[@]}"; do
  script="${entry%%:*}"
  title="${entry#*:}"

  echo -e "\n${BLUE}======================================================================${NC}"
  echo -e "${BOLD}Running ${script}: ${title}${NC}"
  echo -e "${BLUE}======================================================================${NC}"

  if bash "${DIR}/${script}"; then
    RESULTS+=("${GREEN}PASS${NC}  ${script} — ${title}")
  else
    RESULTS+=("${RED}FAIL${NC}  ${script} — ${title}")
    OVERALL_SUCCESS=false
  fi
done

echo -e "\n${CYAN}${BOLD}======================================================================${NC}"
echo -e "${BOLD}                    DEMO SUITE EXECUTION SUMMARY                       ${NC}"
echo -e "${CYAN}${BOLD}======================================================================${NC}"

for r in "${RESULTS[@]}"; do
  echo -e "  [${r}"
done

echo -e "${CYAN}======================================================================${NC}"

if [[ "${OVERALL_SUCCESS}" == "true" ]]; then
  echo -e "${GREEN}${BOLD}ALL DEMOS PASSED SUCCESSFULLY! The distributed cloud store is verified.${NC}\n"
  exit 0
else
  echo -e "${RED}${BOLD}SOME DEMOS FAILED. Check logs above for details.${NC}\n"
  exit 1
fi
