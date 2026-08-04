#!/usr/bin/env bash
# clang-analyzer memory gate — the canonical entry, used by every venue.
#
# Path-sensitive analysis for leak paths, null derefs and uninitialized reads.
# Findings are gated: each one is either fixed, or carries an argued entry in
# scripts/lint-mem-whitelist.txt that is pinned to the sha256 of the function
# it argues about, so it expires when that function changes. See
# scripts/lint-mem-gate.py.
#
# Locally: make -f Makefile.cbm lint-mem-ci   (or `lint-mem` for the
# non-gating triage view over the same checks).
#
# Usage: scripts/ci/lint-mem.sh [CLANG_TIDY_BINARY]
set -euo pipefail

cd "$(dirname "$0")/../.."

CLANG_TIDY_BIN="${1:-${CLANG_TIDY:-clang-tidy}}"

exec make -f Makefile.cbm lint-mem-ci CLANG_TIDY="$CLANG_TIDY_BIN"
