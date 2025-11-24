#!/usr/bin/env bash
set -euo pipefail

MAX_PARALLEL=2    # number of curl jobs at once
URL_FILE="ARL_temp_file_list.txt"
DEST_DIR="$(dirname "$0")/ARL_Files"
mkdir -p "$DEST_DIR"

download_one() {
    local url="$1"
    local name=$(basename "$url")

    curl -L \
         --retry 5 \
         --retry-delay 5 \
         --retry-max-time 600 \
         -C - \
         -o "$DEST_DIR/$name" \
         "$url" \
         >/dev/null 2>/dev/null
}

export -f download_one
export DEST_DIR

grep -v '^[[:space:]]*$' "$URL_FILE" | \
    parallel -j "$MAX_PARALLEL" download_one {}

