#!/usr/bin/env bash
# Reproduce the no-GITHUB_STEP_SUMMARY fallback writing through /dev/stderr,
# which can be denied even though the process already has a valid stderr FD.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-stderr.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

make() {
    printf '%s\n' \
        '────────────────────────────────────────────' \
        '  1 passed, 1 failed'
    return 1
}
export -f make

set +e
output="$(
    unset GITHUB_STEP_SUMMARY
    TMPDIR="$TMP_ROOT" CC=true CXX=true bash "$TMP_ROOT/scripts/repro.sh" 2>&1
)"
status=$?
set -e

if [[ $status -ne 0 ]]; then
    printf '[repro-stderr] invariant=fallback_run_failed status=%d\n' "$status" >&2
    exit 1
fi
if [[ "$output" == *'Operation not permitted'* ]]; then
    printf '[repro-stderr] invariant=stderr_fallback_permission_denied\n' >&2
    exit 1
fi
if [[ "$output" != *'## Bug-reproduction board'* ||
      "$output" != *'**1** open bug(s) still reproduced'* ]]; then
    printf '[repro-stderr] invariant=stderr_summary_missing\n' >&2
    exit 1
fi

printf '[repro-stderr] summary written through stderr correctly\n'
