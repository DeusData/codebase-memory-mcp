#!/usr/bin/env bash
# Reproduce scripts/repro.sh exiting through `set -e -o pipefail` before its
# explicit build/runner diagnostic when the transcript has no aggregate line.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-missing-summary.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt" "$TMP_ROOT/summary.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

# Model a compile/link failure: make emits a useful error but the test runner
# never reaches its final aggregate summary.
make() {
    printf '%s\n' 'clang: error: linker command failed with exit code 1'
    return 2
}
export -f make

set +e
output="$(TMPDIR="$TMP_ROOT" CC=true CXX=true GITHUB_STEP_SUMMARY="$TMP_ROOT/summary.txt" \
    bash "$TMP_ROOT/scripts/repro.sh" 2>&1)"
status=$?
set -e

if [[ $status -eq 0 ]]; then
    printf '[repro-missing-summary] invariant=build_failure_accepted status=%d\n' "$status" >&2
    exit 1
fi

linker_marker='clang: error: linker command failed with exit code 1'
if [[ "$output" != *"$linker_marker"* ]]; then
    printf '[repro-missing-summary] invariant=mocked_linker_failure_not_observed status=%d\n' \
        "$status" >&2
    exit 1
fi

expected='::error::bug-repro runner did not execute — build or link failure'
if [[ "$output" != *"$expected"* ]]; then
    printf '[repro-missing-summary] invariant=explicit_diagnostic_missing status=%d\n' \
        "$status" >&2
    exit 1
fi

printf '[repro-missing-summary] missing aggregate rejected with explicit diagnostic\n'
