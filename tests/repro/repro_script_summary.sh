#!/usr/bin/env bash
# Reproduce scripts/repro.sh selecting the first per-suite count instead of the
# final cumulative total when more than one repro suite runs, while ensuring
# known-green controls are not blanket-labeled as candidate fixes.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-board.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt" "$TMP_ROOT/summary.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

# The copied production script calls make. This test-local function supplies a
# deterministic runner transcript while exercising the production board parser.
make() {
    printf '%s\n' \
        '[SUITE] repro_known_green_controls             2 passed' \
        '[SUITE] repro_runner_filter                    0 passed, 1 failed' \
        '[SUITE] repro_call_argument_usages             0 passed, 17 failed' \
        '────────────────────────────────────────────' \
        '  2 passed, 18 failed'
    return 1
}
export -f make

set +e
output="$(TMPDIR="$TMP_ROOT" CC=true CXX=true GITHUB_STEP_SUMMARY="$TMP_ROOT/summary.txt" \
    bash "$TMP_ROOT/scripts/repro.sh" 2>&1)"
status=$?
set -e

if [[ $status -ne 0 ]]; then
    printf '[repro-board] invariant=production_script_did_not_complete status=%d\n' "$status" >&2
    exit 1
fi

expected_prefix='=== bug-repro board: 18 reproduced (RED), 2 '
if [[ "$output" != *"$expected_prefix"* ]]; then
    actual="$(printf '%s\n' "$output" | grep '^=== bug-repro board:' || true)"
    printf '[repro-board] invariant=cumulative_total_not_reported expected=18/2 actual=%s\n' \
        "${actual:-missing}" >&2
    exit 1
fi

neutral_label='passing control(s) or candidate-fix candidate(s)'
if [[ "$output" != *"$neutral_label"* ]] ||
    ! grep -Fq -- "$neutral_label" "$TMP_ROOT/summary.txt"; then
    actual="$(printf '%s\n' "$output" | grep '^=== bug-repro board:' || true)"
    printf '[repro-board] invariant=passing_controls_blanket_candidate_fixed expected=%q actual=%s\n' \
        "$neutral_label" "${actual:-missing}" >&2
    exit 1
fi

if [[ "$output" == *'passing (candidate-fixed)'* ]] ||
    grep -Fq -- 'PASSING — candidate-fixed' "$TMP_ROOT/summary.txt"; then
    printf '[repro-board] invariant=passing_controls_blanket_candidate_fixed old_label_present=true\n' >&2
    exit 1
fi

printf '[repro-board] cumulative total and neutral passing semantics reported correctly\n'
