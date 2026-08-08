#!/usr/bin/env bash
# Reproduce scripts/repro.sh treating the board as successful even though the
# test framework's valid final aggregate reports one or more skipped tests.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-skipped.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt" "$TMP_ROOT/summary.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

# TEST_SUMMARY appends the skipped count after pass/fail counts. The board must
# parse and report the aggregate faithfully, but it must exit nonzero because a
# board with skipped cases is incomplete.
make() {
    printf '%s\n' \
        '[SUITE] repro_parallel_determinism             0 passed, 2 skipped' \
        '[SUITE] repro_call_argument_usages             1 passed, 17 failed' \
        '────────────────────────────────────────────' \
        '  1 passed, 17 failed, 2 skipped'
    return 1
}
export -f make

set +e
output="$(TMPDIR="$TMP_ROOT" CC=true CXX=true GITHUB_STEP_SUMMARY="$TMP_ROOT/summary.txt" \
    bash "$TMP_ROOT/scripts/repro.sh" 2>&1)"
status=$?
set -e

expected_prefix='=== bug-repro board: 17 reproduced (RED), 1 '
if [[ "$output" != *"$expected_prefix"* ]]; then
    actual="$(printf '%s\n' "$output" | grep '^=== bug-repro board:' || true)"
    printf '[repro-skipped] invariant=skipped_aggregate_misreported expected=17/1 actual=%s\n' \
        "${actual:-missing}" >&2
    exit 1
fi

skip_summary='- **2** case(s) SKIPPED — board is incomplete'
if ! grep -Fq -- "$skip_summary" "$TMP_ROOT/summary.txt"; then
    printf '[repro-skipped] invariant=skipped_count_hidden expected=2\n' >&2
    exit 1
fi

skip_warning='::warning::bug-repro board incomplete — 2 case(s) skipped'
if [[ "$output" != *"$skip_warning"* ]]; then
    printf '[repro-skipped] invariant=skipped_warning_hidden expected=2\n' >&2
    exit 1
fi

if [[ $status -eq 0 ]]; then
    printf '[repro-skipped] invariant=incomplete_board_exited_zero expected=nonzero actual=0\n' >&2
    exit 1
fi

printf '[repro-skipped] skipped aggregate reported and rejected correctly\n'
