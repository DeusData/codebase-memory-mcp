#!/usr/bin/env bash
set -euo pipefail

# Build and run the nodes_fts `body` column cost isolation (#518).
# Usage: scripts/benchmark-fts-body.sh [rowcount ...]
#
# Measures per-row body tokenisation time and FTS index storage for three
# variants (pre-#518 4-column, #518 body-for-all, body-for-Section/Module only).
# See the header comment in benchmark-fts-body.c for what this does and does
# not measure — in particular it is NOT a substitute for benchmark-index.sh on
# a real corpus.

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
OUT="${TMPDIR:-/tmp}/benchmark-fts-body"
CC_BIN="${CC:-cc}"

echo "Building $OUT ..."
"$CC_BIN" -O2 -o "$OUT" \
  "$ROOT/scripts/benchmark-fts-body.c" \
  "$ROOT/vendored/sqlite3/sqlite3.c" \
  -I"$ROOT/vendored/sqlite3" \
  -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_THREADSAFE=0 \
  -lm ${LDLIBS:-}

# Run from a scratch dir — the harness creates and removes temp .db files in cwd.
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

"$OUT" "$@"
