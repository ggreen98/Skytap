#!/usr/bin/env bash
set -euo pipefail

URL_FILE="txt_files/ARL_temp_file_list.txt"
DEST_DIR="${DEST_DIR:-ARL_Files}"
mkdir -p "$DEST_DIR"

# PARALLEL_DOWNLOADS controls -j (files at once).
# Set to 1 on machines with spinning HDDs to avoid seek thrashing.
# Set to 4 (default) on SSDs for maximum throughput.
MAX_FILES_AT_ONCE="${PARALLEL_DOWNLOADS:-4}"
CONNS_PER_FILE=16        # aria2c -x
SPLITS_PER_FILE=16       # aria2c -s

echo "Using URL list: $URL_FILE"
echo "Downloading to: $DEST_DIR"
echo "Max files at once: $MAX_FILES_AT_ONCE"
echo "Connections per file: $CONNS_PER_FILE, splits: $SPLITS_PER_FILE"

aria2c --no-conf \
  -x "$CONNS_PER_FILE" \
  -s "$SPLITS_PER_FILE" \
  -j "$MAX_FILES_AT_ONCE" \
  --continue=true \
  --max-tries=5 \
  --file-allocation=none \
  -d "$DEST_DIR" \
  -i "$URL_FILE"
