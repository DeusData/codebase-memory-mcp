#!/usr/bin/env bash
# Reproduce the compiled repro runner returning success when a selector matches
# no suite. This protects direct `make test-repro`, not only scripts/repro.sh.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUNNER="$PROJECT_ROOT/build/c/test-repro-runner"

if [[ ! -x "$RUNNER" ]]; then
    printf '[repro-runner-zero] invariant=runner_missing path=%s\n' "$RUNNER" >&2
    exit 1
fi

set +e
output="$(CBM_REPRO_ONLY='__cbm_no_such_repro_suite__' "$RUNNER" 2>&1)"
status=$?
set -e

if [[ $status -eq 0 ]]; then
    printf '[repro-runner-zero] invariant=compiled_runner_accepted_zero_tests status=%d\n' \
        "$status" >&2
    exit 1
fi

expected='bug-repro runner executed zero tests'
if [[ "$output" != *"$expected"* ]]; then
    printf '[repro-runner-zero] invariant=zero_test_diagnostic_missing status=%d\n' \
        "$status" >&2
    exit 1
fi

printf '[repro-runner-zero] compiled runner rejected zero tests correctly\n'
