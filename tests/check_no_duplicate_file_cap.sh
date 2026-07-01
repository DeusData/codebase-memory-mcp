#!/usr/bin/env bash
# check_no_duplicate_file_cap.sh — structural single-source-of-truth guard
# for the per-file parse-size cap.
#
# The 100MB per-file cap used to be expressed 7 times: 6x as the misused
# "CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K" literal (pass_calls.c, pass_definitions.c,
# pass_semantic.c, pass_usages.c, pass_k8s.c, pass_parallel.c) plus a twin enum
# PXC_MAX_FILE_BYTES_FACTOR in pass_lsp_cross.c. All 7 collapsed to the single
# named cbm_max_file_bytes() resolver (system_info.c) backed by
# CBM_DEFAULT_MAX_FILE_MB (constants.h). This script asserts the collapse
# STUCK: zero surviving duplicate-literal sites, and the new resolver present
# at all 7 former call sites.
#
# Reads COMMITTED source via `git show HEAD:<path>`, not the working tree —
# a mutation run rewrites the working tree during its sweep, which would
# make a working-tree grep abort as a tool-error mid-run. Reading HEAD keeps
# this check meaningful (and green) regardless of mutation-testing state.
#
# Also pins CBM_PERCENT's legitimate percentage/depth consumers
# (mem.c, vmem.c, cypher.c) as UNTOUCHED — the collapse must not repurpose
# the shared "100 percent" constant (AC row 5, characterization pin).
#
# Usage: bash tests/check_no_duplicate_file_cap.sh [<git-ref>]
#   <git-ref> defaults to HEAD.
# Exit 0 on success, non-zero on failure.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF="${1:-HEAD}"
FAILURES=0

cd "$ROOT" || exit 2

show() {
    git show "${REF}:$1" 2>/dev/null
}

# ── AC row 4: zero surviving duplicate-cap-literal sites ────────────────
FORMER_SITES=(
    "src/pipeline/pass_calls.c"
    "src/pipeline/pass_definitions.c"
    "src/pipeline/pass_semantic.c"
    "src/pipeline/pass_usages.c"
    "src/pipeline/pass_k8s.c"
    "src/pipeline/pass_parallel.c"
    "src/pipeline/pass_lsp_cross.c"
)

echo "[cap-dedup] Checking zero surviving 'CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K' cap expressions..." >&2
hits="$(git grep -n 'CBM_PERCENT \* CBM_SZ_1K \* CBM_SZ_1K' "${REF}" -- src/ 2>/dev/null || true)"
if [[ -n "$hits" ]]; then
    echo "[cap-dedup] FAIL: surviving duplicated cap literal(s):" >&2
    echo "$hits" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "[cap-dedup] PASS: zero surviving CBM_PERCENT*CBM_SZ_1K*CBM_SZ_1K cap expressions" >&2
fi

echo "[cap-dedup] Checking zero surviving PXC_MAX_FILE_BYTES_FACTOR..." >&2
hits="$(git grep -n 'PXC_MAX_FILE_BYTES_FACTOR' "${REF}" -- src/ 2>/dev/null || true)"
if [[ -n "$hits" ]]; then
    echo "[cap-dedup] FAIL: surviving PXC_MAX_FILE_BYTES_FACTOR reference(s):" >&2
    echo "$hits" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "[cap-dedup] PASS: zero surviving PXC_MAX_FILE_BYTES_FACTOR references" >&2
fi

echo "[cap-dedup] Checking cbm_max_file_bytes() present at all 7 former call sites..." >&2
for f in "${FORMER_SITES[@]}"; do
    content="$(show "$f")"
    if [[ -z "$content" ]]; then
        echo "[cap-dedup] FAIL: could not read $f at $REF" >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi
    if ! grep -q 'cbm_max_file_bytes()' <<<"$content"; then
        echo "[cap-dedup] FAIL: $f does not call cbm_max_file_bytes()" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "[cap-dedup] PASS: $f calls cbm_max_file_bytes()" >&2
    fi
done

# ── AC row 5: CBM_PERCENT's legitimate consumers untouched (characterization pin) ──
# Pinned by MINIMUM occurrence count per file, not exact line number — adding
# unrelated code earlier in a file (e.g. new ceiling constants ahead of
# check_pressure() in mem.c) legitimately shifts later line numbers without
# touching the CBM_PERCENT usage itself; an exact-line pin would false-positive
# on that harmless drift. The count floor still catches the real regression
# this row guards against: the collapse repurposing/deleting a legitimate use.
declare -A PIN_COUNTS=(
    ["src/foundation/mem.c"]=2
    ["src/foundation/vmem.c"]=2
    ["src/cypher/cypher.c"]=1
)

echo "[cap-dedup] Pinning CBM_PERCENT's legitimate percentage/depth consumers (must be untouched)..." >&2
for f in "${!PIN_COUNTS[@]}"; do
    content="$(show "$f")"
    if [[ -z "$content" ]]; then
        echo "[cap-dedup] FAIL: could not read $f at $REF" >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi
    expected="${PIN_COUNTS[$f]}"
    actual="$(grep -c 'CBM_PERCENT' <<<"$content")"
    if [[ "$actual" -lt "$expected" ]]; then
        echo "[cap-dedup] FAIL: $f has $actual CBM_PERCENT reference(s), expected >= $expected" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "[cap-dedup] PASS: $f has $actual CBM_PERCENT reference(s) (>= $expected expected)" >&2
    fi
done

# CBM_PERCENT's definition itself must still be 100 (untouched/unrenamed).
constants_content="$(show "src/foundation/constants.h")"
if ! grep -qE 'CBM_PERCENT = 100' <<<"$constants_content"; then
    echo "[cap-dedup] FAIL: CBM_PERCENT definition changed or missing in constants.h" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "[cap-dedup] PASS: CBM_PERCENT = 100 definition untouched" >&2
fi

# ── AC row 11: advisory backpressure not silently deleted ───────────────
# The enforcing ceiling must AUGMENT, not replace, the existing advisory
# backpressure spin (pass_parallel.c) — a repo that overshoots the WARN
# budget but stays under the ABORT ceiling must still back-pressure and
# complete (soft overshoot), i.e. warn != abort. Pin the backpressure loop
# structure (cbm_mem_over_budget() bounded spin) is still present, and
# that the new hard-ceiling call sits alongside it (not inside/replacing
# the spin body — mem_ceiling_abort.sh's row7 healthy-path run is the
# behavioural proof; this is the structural companion).
pp_content="$(show "src/pipeline/pass_parallel.c")"
if [[ -z "$pp_content" ]]; then
    echo "[cap-dedup] FAIL: could not read pass_parallel.c at $REF" >&2
    FAILURES=$((FAILURES + 1))
elif ! grep -q 'cbm_mem_over_budget()' <<<"$pp_content"; then
    echo "[cap-dedup] FAIL: advisory backpressure (cbm_mem_over_budget spin) no longer present in pass_parallel.c" >&2
    FAILURES=$((FAILURES + 1))
elif ! grep -q 'cbm_mem_abort_if_over_ceiling(' <<<"$pp_content"; then
    echo "[cap-dedup] FAIL: enforcing ceiling call (cbm_mem_abort_if_over_ceiling) not wired into pass_parallel.c" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "[cap-dedup] PASS: advisory backpressure AND enforcing ceiling both present in pass_parallel.c (augmented, not replaced)" >&2
fi

# ── Final result ──────────────────────────────────────────────────────
if [[ "$FAILURES" -gt 0 ]]; then
    echo "[cap-dedup] FAILED: $FAILURES check(s) failed." >&2
    exit 1
fi

echo "[cap-dedup] All checks passed (row 4 dedup + row 5 CBM_PERCENT pin) at ${REF}." >&2
exit 0
