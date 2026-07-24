#!/usr/bin/env bash
set -euo pipefail

# Index a single benchmark repository and capture metrics.
# Usage: benchmark-index.sh <binary> <lang> <repo_path> <results_dir>

BINARY="${1:?Usage: benchmark-index.sh <binary> <lang> <repo_path> <results_dir>}"
LANG="${2:?}"
REPO="${3:?}"
RESULTS_DIR="${4:?}"

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

# Index via CLI and capture timing
START_MS=$(python3 -c "import time; print(int(time.time()*1000))")

INDEX_JSON=$("$BINARY" cli index_repository "{\"repo_path\":\"$REPO\",\"mode\":\"full\"}" 2>/dev/null || echo '{"error":"index failed"}')

END_MS=$(python3 -c "import time; print(int(time.time()*1000))")
ELAPSED=$((END_MS - START_MS))

echo "$INDEX_JSON" > "$OUT/00-index.json"
echo "$ELAPSED" > "$OUT/index-time.txt"

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
