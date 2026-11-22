#!/usr/bin/env bash
# Append one byte to a demo chunk while preserving its original checksum sidecar.
set -euo pipefail
worker="${1:?worker container}"
chunk="${2:?chunk id}"
file=$(docker exec "$worker" find /data/chunks -type f -name "$chunk" -print -quit)
[[ -n "$file" ]] || { echo "Missing chunk $chunk on $worker" >&2; exit 1; }
docker exec "$worker" sh -c 'printf x >> "$1"' sh "$file"
echo "Damaged $chunk on $worker"
