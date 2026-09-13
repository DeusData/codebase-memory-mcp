#!/usr/bin/env bash
set -euo pipefail

# Index a single benchmark repository and capture metrics.
# Usage: benchmark-index.sh <binary> <lang> <repo_path> <results_dir>

BINARY="${1:?Usage: benchmark-index.sh <binary> <lang> <repo_path> <results_dir>}"
LANG="${2:?}"
REPO="${3:?}"
RESULTS_DIR="${4:?}"

# The index must run against a daemon rendezvous and cache this run owns: only
# CBM_RUNTIME_DIR moves the rendezvous, and without a private cache the
# benchmark repository was indexed into the operator's live store (#1696).
# shellcheck source=test-runtime.sh
source "$(dirname "${BASH_SOURCE[0]}")/test-runtime.sh"
cbm_test_runtime_init
trap 'cbm_test_runtime_cleanup "$BINARY"' EXIT

# Resolve symlinks
REPO=$(cd "$REPO" && pwd -P)

OUT="$RESULTS_DIR/$LANG"
mkdir -p "$OUT"

echo "INDEX: $LANG ($REPO)"

# Count source files and LOC (exclude .git, vendor, node_modules, build dirs)
FILE_COUNT=$(find "$REPO" -type f \
  ! -path '*/.git/*' ! -path '*/node_modules/*' ! -path '*/vendor/*' \
  ! -path '*/target/*' ! -path '*/build/*' ! -path '*/dist/*' \
  ! -path '*/__pycache__/*' ! -path '*/.cache/*' \
  | wc -l | tr -d ' ')

LOC=$(find "$REPO" -type f \
  ! -path '*/.git/*' ! -path '*/node_modules/*' ! -path '*/vendor/*' \
  ! -path '*/target/*' ! -path '*/build/*' ! -path '*/dist/*' \
  ! -path '*/__pycache__/*' ! -path '*/.cache/*' \
  -exec cat {} + 2>/dev/null | wc -l | tr -d ' ')

echo "$FILE_COUNT" > "$OUT/file-count.txt"
echo "$LOC" > "$OUT/loc.txt"

# Start the private daemon before timing so index-time.txt measures the index
# alone. setup-time.txt keeps the activation cost attributable and
# total-time.txt is their sum — the figure comparable with earlier runs, which
# paid activation inside the index timing whenever no daemon was already warm.
SETUP_START_MS=$(python3 -c "import time; print(int(time.time()*1000))")
if ! "$BINARY" daemon start >/dev/null 2>&1; then
  echo "  $LANG: private daemon did not start" >&2
  exit 1
fi

# Index via CLI and capture timing
START_MS=$(python3 -c "import time; print(int(time.time()*1000))")

INDEX_JSON=$("$BINARY" cli index_repository "{\"repo_path\":\"$REPO\",\"mode\":\"full\"}" 2>/dev/null || echo '{"error":"index failed"}')

END_MS=$(python3 -c "import time; print(int(time.time()*1000))")
ELAPSED=$((END_MS - START_MS))

echo "$INDEX_JSON" > "$OUT/00-index.json"
echo "$ELAPSED" > "$OUT/index-time.txt"
echo "$((START_MS - SETUP_START_MS))" > "$OUT/setup-time.txt"
echo "$((END_MS - SETUP_START_MS))" > "$OUT/total-time.txt"

# Extract node/edge counts (CLI wraps in MCP content envelope)
NODES=$(echo "$INDEX_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
# Unwrap MCP content envelope if present
if 'content' in d:
    inner=json.loads(d['content'][0]['text'])
else:
    inner=d
print(inner.get('nodes',0))
" 2>/dev/null || echo "0")
EDGES=$(echo "$INDEX_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'content' in d:
    inner=json.loads(d['content'][0]['text'])
else:
    inner=d
print(inner.get('edges',0))
" 2>/dev/null || echo "0")
PROJECT=$(echo "$INDEX_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'content' in d:
    inner=json.loads(d['content'][0]['text'])
else:
    inner=d
print(inner.get('project',''))
" 2>/dev/null || echo "")

echo "$NODES" > "$OUT/nodes.txt"
echo "$EDGES" > "$OUT/edges.txt"
echo "$PROJECT" > "$OUT/project.txt"

printf "  %s: %s files, %s LOC, %sms, %s nodes, %s edges\n" \
  "$LANG" "$FILE_COUNT" "$LOC" "$ELAPSED" "$NODES" "$EDGES"
