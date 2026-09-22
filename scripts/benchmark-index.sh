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

# The evaluation plan (docs/EVALUATION_PLAN.md §7) indexes a language here and
# then answers graph questions against that index from its own MCP session.
# A run-private root would be gone before that session starts, so the caller
# may ask for it to be kept: after a SUCCESSFUL run the harness stops its
# daemon, leaves the root in place, records the paths that reach it in
# <results>/<lang>/runtime-root.txt (sourceable), and ownership of the root —
# including its removal — passes to the caller. A failed run cleans up
# regardless: there is no index worth keeping, and nothing must leak. Never on
# by default, or an unattended run accumulates one root per language.
bench_finish() {
  local rc=$?
  [ -z "${BENCH_TMP:-}" ] || rm -rf -- "$BENCH_TMP" || true
  if [ "$rc" -eq 0 ] && [ -n "${CBM_BENCH_KEEP_RUNTIME:-}" ] && [ -d "${OUT:-}" ]; then
    "$BINARY" daemon stop >/dev/null 2>&1 || true
    printf 'CBM_BENCH_RUNTIME_ROOT=%q\nCBM_RUNTIME_DIR=%q\nCBM_CACHE_DIR=%q\n' \
      "$CBM_TEST_RUNTIME_ROOT" "$CBM_RUNTIME_DIR" "$CBM_CACHE_DIR" > "$OUT/runtime-root.txt"
    echo "  $LANG: runtime kept at $CBM_TEST_RUNTIME_ROOT; paths in $OUT/runtime-root.txt" >&2
    return 0
  fi
  cbm_test_runtime_cleanup "$BINARY"
}
BENCH_TMP=""
trap bench_finish EXIT
# The index call keeps its stderr, the way the search-graph twin does: for a
# one-shot CLI that is the only channel a refusal is reported on, and without
# it the failure below could name only its symptom.
BENCH_TMP=$(mktemp -d)
INDEX_ERR="$BENCH_TMP/index-stderr.log"

# Resolve symlinks
REPO=$(cd "$REPO" && pwd -P)
# One pre-escaped spelling of the path for every request below, the way the
# soak harness builds its own: a repository path may legitimately contain a
# quote or a backslash, and hand-built JSON turns that into a parse error.
REPO_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$REPO")

# Elapsed time is read from a monotonic clock, never the wall clock: an NTP
# step mid-run would otherwise skew — or negate — a figure whose whole purpose
# is comparison across runs. Its reference point is fixed per boot on every
# platform CPython supports here, so the three readings below are comparable
# even though each comes from its own process.
bench_now_ms() { python3 -c "import time; print(time.monotonic_ns() // 1000000)"; }

OUT="$RESULTS_DIR/$LANG"
mkdir -p "$OUT"
# A handoff names this run's index or none: the evaluation loop reuses the
# results directory, and a stale one would point the graph session at a root
# step 8 has already removed.
rm -f -- "$OUT/runtime-root.txt"

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
SETUP_START_MS=$(bench_now_ms)
if ! "$BINARY" daemon start >/dev/null 2>&1; then
  echo "  $LANG: private daemon did not start" >&2
  exit 1
fi

# Index via CLI and capture timing
START_MS=$(bench_now_ms)

# The CLI's exit status is the index's verdict. It is recorded here and acted
# on below, once every per-run file is written; an empty response stays valid
# JSON in 00-index.json so the failure is legible there as well.
INDEX_RC=0
INDEX_JSON=$("$BINARY" cli index_repository "{\"repo_path\":$REPO_JSON,\"mode\":\"full\"}" 2>"$INDEX_ERR") ||
  { INDEX_RC=$?; INDEX_JSON="{\"error\":\"index failed\",\"exit\":$INDEX_RC}"; }

END_MS=$(bench_now_ms)
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

# An index that failed, or that named no project, fails the run: after the
# timing files, so the caller keeps its figures, and through the exit status,
# so the caller's loop notices and bench_finish never keeps a root with no
# index in it.
if [ "$INDEX_RC" -ne 0 ] || [ -z "$PROJECT" ]; then
  echo "  $LANG: index failed (cli exit $INDEX_RC, project '$PROJECT'); response in $OUT/00-index.json" >&2
  if [ -s "$INDEX_ERR" ]; then
    echo "--- index stderr ---" >&2
    cat "$INDEX_ERR" >&2
  fi
  echo "--- index response (first 500 bytes) ---" >&2
  printf '%.500s\n' "$INDEX_JSON" >&2
  exit 1
fi

printf "  %s: %s files, %s LOC, %sms, %s nodes, %s edges\n" \
  "$LANG" "$FILE_COUNT" "$LOC" "$ELAPSED" "$NODES" "$EDGES"
