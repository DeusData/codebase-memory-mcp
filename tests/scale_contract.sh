#!/usr/bin/env bash
# scale_contract.sh — Real-repo SCALE-TIER contracts for known-bug languages.
#
# Some behavioral bugs only manifest at real-repo scale and cannot be reproduced
# by the small in-process fixtures in tests/test_lang_contract.c:
#   - Java / TypeScript : SIGBUS during parallel extraction on large repos.
#   - C                 : function-call attribution (calls land on the file's
#                         Module node instead of the enclosing Function).
#   - Kotlin            : 0 IMPORTS edges (package -> module resolution).
#
# This tier runs the COMPILED binary on the cbm-bench-validate repos and asserts
# invariants at scale. It is SLOW and needs the local bench repos, so it is
# OPT-IN — not part of the fast `scripts/test.sh` unit run.
#
# Usage:
#   bash tests/scale_contract.sh [lang ...]      # default: kotlin java ts
#   CBM_SCALE_INCLUDE_C=1 bash tests/scale_contract.sh c   # add the slow C tier
#   CBM_BENCH_DIR=/path bash tests/scale_contract.sh       # override repo root
# Exit 0 if all selected contracts pass, non-zero otherwise.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/build/c/codebase-memory-mcp"
BENCH="${CBM_BENCH_DIR:-$HOME/cbm-bench-validate}"
PY="${PYTHON:-python3.9}"
FAILURES=0
RAN=0

# ── repo map (lang -> bench subdir) ──────────────────────────────
repo_for() {
    case "$1" in
        kotlin) echo "$BENCH/ktorio_ktor" ;;
        java)   echo "$BENCH/spring-projects_spring-boot" ;;
        ts)     echo "$BENCH/microsoft_TypeScript" ;;
        c)      echo "$BENCH/linux_mono/copy1" ;;
        *)      echo "" ;;
    esac
}

# project name == cbm_project_name_from_path(): strip leading '/', map every
# char outside [A-Za-z0-9._-] to '-'.
project_name() { printf '%s' "$1" | sed 's#^/##; s#[^A-Za-z0-9._-]#-#g'; }

# Run query_graph and print the first integer in the JSON response (or -1).
query_count() {
    local proj="$1" cypher="$2" resp
    resp="$("$BIN" cli query_graph "{\"project\":\"$proj\",\"query\":\"$cypher\"}" 2>/dev/null)"
    printf '%s' "$resp" | "$PY" -c 'import sys,re
s=sys.stdin.read()
m=re.findall(r"-?\d+", s)
print(m[0] if m else -1)' 2>/dev/null || echo -1
}

assert_lang() {
    local lang="$1" repo
    repo="$(repo_for "$lang")"
    if [ -z "$repo" ] || [ ! -d "$repo" ]; then
        echo "[scale] SKIP $lang — repo not found ($repo)"
        return 0
    fi
    RAN=$((RAN + 1))
    local proj
    proj="$(project_name "$repo")"
    rm -f "$HOME/.cache/codebase-memory-mcp/$proj.db"*

    echo "[scale] $lang — indexing $repo ..."
    "$BIN" cli index_repository "{\"repo_path\":\"$repo\"}" >/dev/null 2>&1
    local rc=$?

    # Contract 1 (all langs): indexing must not crash by signal (rc >= 128).
    if [ "$rc" -ge 128 ]; then
        echo "[scale] FAIL $lang — index crashed by signal (exit=$rc, signal=$((rc - 128)))"
        FAILURES=$((FAILURES + 1))
        return 0
    fi
    echo "[scale] ok   $lang — index did not crash (exit=$rc)"

    # Contract 2 (per-lang invariant).
    case "$lang" in
        kotlin)
            local imports
            imports="$(query_count "$proj" "MATCH ()-[r:IMPORTS]->() RETURN count(r) AS n")"
            if [ "$imports" -ge 1 ] 2>/dev/null; then
                echo "[scale] PASS kotlin — IMPORTS edges present (n>=$imports)"
            else
                echo "[scale] FAIL kotlin — 0 IMPORTS edges (got '$imports')"
                FAILURES=$((FAILURES + 1))
            fi
            ;;
        c)
            # Function nodes that have >=1 outbound CALLS (attribution invariant).
            local fcalls
            fcalls="$(query_count "$proj" "MATCH (a:Function)-[:CALLS]->() RETURN count(DISTINCT a) AS n")"
            if [ "$fcalls" -ge 1 ] 2>/dev/null; then
                echo "[scale] PASS c — Function-sourced CALLS present (functions-with-calls>=$fcalls)"
            else
                echo "[scale] FAIL c — no Function node has outbound CALLS (got '$fcalls')"
                FAILURES=$((FAILURES + 1))
            fi
            ;;
        java | ts)
            echo "[scale] PASS $lang — no-crash contract satisfied"
            ;;
    esac
}

# ── build the prod binary if missing ─────────────────────────────
if [ ! -x "$BIN" ]; then
    echo "[scale] building prod binary ..."
    (cd "$ROOT" && ./scripts/build.sh) || { echo "[scale] FAIL — build failed"; exit 1; }
fi
[ -x "$BIN" ] || { echo "[scale] FAIL — binary not found at $BIN"; exit 1; }

# ── select languages ─────────────────────────────────────────────
LANGS=("$@")
if [ "${#LANGS[@]}" -eq 0 ]; then
    LANGS=(kotlin java ts)
    [ "${CBM_SCALE_INCLUDE_C:-0}" = "1" ] && LANGS+=(c)
fi

for lang in "${LANGS[@]}"; do
    assert_lang "$lang"
done

echo "────────────────────────────────────────────"
if [ "$FAILURES" -eq 0 ]; then
    echo "[scale] All scale contracts passed ($RAN ran)"
    exit 0
fi
echo "[scale] $FAILURES scale contract(s) FAILED ($RAN ran)"
exit 1
