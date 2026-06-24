#!/usr/bin/env bash
# test_cpp_index_hang.sh — SCALE-TIER regression guard for issue #410:
# "won't finish indexing big C++ codebases".
#
# Root cause: the C/C++ cross-LSP resolve looked up method/symbol overloads with
# a LINEAR scan over the whole project's registered funcs (cbm_registry_lookup_
# method_by_types / _by_args / lookup_symbol_by_* in type_registry.c). On a large
# translation unit (the reported Model.hpp had 10989 defs) this made the per-file
# resolve O(calls × project_funcs) — ~34s for ONE header — so a repo with many
# big headers never finished indexing.
#
# The hang only manifests at SCALE, so this test:
#   1. generates a single large synthetic C++ header whose classes call each
#      other's methods (thousands of method-call sites + thousands of registered
#      methods → triggers the O(n^2) before the fix),
#   2. runs the PROD binary `cli index_repository` on it in a SUBPROCESS with a
#      hard wall-clock timeout (perl alarm — no coreutils `timeout` on macOS),
#   3. FAILS if indexing exceeds the timeout (i.e. hangs), passes if it finishes.
#
# The fixture is tuned to complete in a couple of seconds AFTER the fix while
# blowing well past the timeout on the unfixed O(n^2) code.
#
# This mirrors the fork+WIFSIGNALED crash-isolation pattern, but for a hang:
# a runaway index must never block the test runner.
#
# Usage:
#   bash tests/test_cpp_index_hang.sh
#   CBM_HANG_TIMEOUT=60 CBM_HANG_CLASSES=6000 bash tests/test_cpp_index_hang.sh

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/build/c/codebase-memory-mcp"

# Number of classes/methods in the synthetic TU. Each class has a method that
# calls the next class's method by value-of-known-type → routes through the
# linear overload-lookup hot path. 5000 is enough to make the unfixed code take
# minutes while the fixed code stays well under a couple of seconds.
CLASSES="${CBM_HANG_CLASSES:-5000}"
# Wall-clock budget. The fixed index finishes in ~1-3s; the unfixed O(n^2) needs
# minutes. 45s gives huge head-room for the fixed path on a loaded laptop while
# still catching the hang.
TIMEOUT="${CBM_HANG_TIMEOUT:-45}"

if [ ! -x "$BIN" ]; then
    echo "[hang] building prod binary ..."
    (cd "$ROOT" && ./scripts/build.sh) || { echo "[hang] FAIL — build failed"; exit 1; }
fi
[ -x "$BIN" ] || { echo "[hang] FAIL — binary not found at $BIN"; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; rm -f "$HOME/.cache/codebase-memory-mcp/$PROJ.db"* 2>/dev/null' EXIT

HDR="$WORK/model.hpp"
SRC="$WORK/main.cpp"

echo "[hang] generating synthetic TU: $CLASSES classes/methods ..."
# Header: a chain of classes C0..CN. Each Ck has a method work() that constructs
# the next class and calls its work(). Member calls (obj.work()) on a known local
# type go through cbm_registry_lookup_method_by_types — the O(project_funcs) scan.
{
    echo '#pragma once'
    echo 'namespace model {'
    for ((k=0; k<CLASSES; k++)); do
        next=$(( (k + 1) % CLASSES ))
        printf 'class C%d {\n public:\n  int work();\n  int run() { C%d n; return n.work(); }\n};\n' "$k" "$next"
    done
    echo '} // namespace model'
} > "$HDR"

# A .cpp that defines the out-of-line work() bodies, each calling the next
# class's run() — more cross-method call sites to hammer the resolve walk.
{
    echo '#include "model.hpp"'
    echo 'namespace model {'
    for ((k=0; k<CLASSES; k++)); do
        next=$(( (k + 1) % CLASSES ))
        printf 'int C%d::work() { C%d m; return m.run(); }\n' "$k" "$next"
    done
    echo '} // namespace model'
} > "$SRC"

PROJ="$(printf '%s' "$WORK" | sed 's#^/##; s#[^A-Za-z0-9._-]#-#g')"
rm -f "$HOME/.cache/codebase-memory-mcp/$PROJ.db"* 2>/dev/null

echo "[hang] indexing with ${TIMEOUT}s wall-clock budget ..."
START=$(date +%s)
# perl alarm = portable `timeout`: SIGALRM after $TIMEOUT seconds. exec replaces
# the perl process with the binary so the alarm fires against the index run.
perl -e 'alarm shift; exec @ARGV or exit 127' \
    "$TIMEOUT" "$BIN" cli index_repository "{\"repo_path\":\"$WORK\"}" >/dev/null 2>&1
RC=$?
END=$(date +%s)
ELAPSED=$(( END - START ))

# perl's exec'd child inherits the alarm; SIGALRM (14) → exit code 128+14 = 142.
if [ "$RC" -eq 142 ]; then
    echo "[hang] FAIL — index_repository did not finish within ${TIMEOUT}s (HANG, #410 regression)"
    echo "[hang]        elapsed=${ELAPSED}s classes=${CLASSES}"
    exit 1
fi
if [ "$RC" -ge 128 ]; then
    echo "[hang] FAIL — index crashed by signal (exit=$RC, signal=$((RC - 128)))"
    exit 1
fi
if [ "$RC" -ne 0 ]; then
    echo "[hang] FAIL — index_repository exited non-zero (exit=$RC)"
    exit 1
fi

echo "[hang] PASS — index finished in ${ELAPSED}s (<${TIMEOUT}s budget), classes=${CLASSES}"
exit 0
