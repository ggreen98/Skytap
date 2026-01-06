#!/usr/bin/env bash
set -euo pipefail

URL_FILE="txt_files/ARL_temp_file_list.txt"
DEST_DIR="ARL_Files"
mkdir -p "$DEST_DIR"

# Tune these:
MAX_FILES_AT_ONCE=4      # maps to aria2c -j (how many files)
CONNS_PER_FILE=8        # aria2c -x
SPLITS_PER_FILE=8       # aria2c -s

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
