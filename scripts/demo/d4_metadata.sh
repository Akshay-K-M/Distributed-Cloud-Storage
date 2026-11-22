#!/usr/bin/env bash
# Change labels on one object and compare its storage records before and after.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_for_cluster
wait_for_workers 4
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
object=obj:demo4/doc.txt
echo "Important document payload" > "$tmp/doc.txt"

${OCS_EXEC} cp "$tmp/doc.txt" "$object" --meta env=prod --meta author=alice --overwrite
${OCS_EXEC} --json stat "$object" > "$tmp/before"
jq -e '.custom_metadata == {"env":"prod","author":"alice"}' "$tmp/before"

${OCS_EXEC} meta set "$object" author=bob status=reviewed
${OCS_EXEC} meta rm "$object" env
${OCS_EXEC} --json stat "$object" > "$tmp/after"
jq -e '.custom_metadata == {"author":"bob","status":"reviewed"}' "$tmp/after"
# Only user labels and their modification timestamp may change.
diff <(jq -S 'del(.custom_metadata, .modified_at)' "$tmp/before") \
     <(jq -S 'del(.custom_metadata, .modified_at)' "$tmp/after")
log_pass "Labels changed on the same object, with the same chunks, replicas and checksums"
