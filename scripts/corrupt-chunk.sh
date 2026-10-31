#!/usr/bin/env bash
# Corrupt one on-disk replica to demonstrate checksum detection and repair.
# Usage: scripts/corrupt-chunk.sh <worker-container> <chunk_id>
set -euo pipefail
WORKER="${1:?worker container}"
CHUNK_ID="${2:?chunk id}"
FILE="$(docker exec "$WORKER" sh -c "find /data/chunks -type f -name '$CHUNK_ID' ! -name '*.meta' | head -n1")"
if [[ -z "$FILE" ]]; then
  echo "chunk $CHUNK_ID not found on $WORKER" >&2
  exit 1
fi
docker exec "$WORKER" sh -c "dd if=/dev/zero of='$FILE' bs=1 count=8 conv=notrunc status=none"
echo "corrupted $FILE on $WORKER"
