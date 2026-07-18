#!/usr/bin/env bash
# Reproduce scripts/repro.sh accepting a selector/build transcript in which the
# runner executes zero tests. A zero-test board proves neither red nor green and
# must be treated as a runner failure.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-zero.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt" "$TMP_ROOT/summary.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

# Supply the exact transcript produced when CBM_REPRO_ONLY matches no suite.
make() {
    printf '%s\n' \
        '────────────────────────────────────────────' \
        '  0 passed'
    return 0
}
export -f make

set +e
output="$(TMPDIR="$TMP_ROOT" CC=true CXX=true GITHUB_STEP_SUMMARY="$TMP_ROOT/summary.txt" \
    bash "$TMP_ROOT/scripts/repro.sh" 2>&1)"
status=$?
set -e

if [[ $status -eq 0 ]]; then
    board="$(printf '%s\n' "$output" | grep '^=== bug-repro board:' || true)"
    printf '[repro-zero] invariant=zero_tests_accepted status=%d board=%s\n' \
        "$status" "${board:-missing}" >&2
    exit 1
fi

if [[ "$output" != *'bug-repro runner executed zero tests'* ]]; then
    printf '[repro-zero] invariant=zero_test_error_missing status=%d\n' "$status" >&2
    exit 1
fi

printf '[repro-zero] zero-test transcript rejected correctly\n'
