#!/usr/bin/env bash
# The compiled repro runner must not claim every passing control is a fixed bug.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUNNER="$PROJECT_ROOT/build/c/test-repro-runner"

if [[ ! -x "$RUNNER" ]]; then
    printf '[repro-runner-semantics] invariant=runner_missing path=%s\n' "$RUNNER" >&2
    exit 1
fi

set +e
output="$(CBM_REPRO_ONLY=repro_runner_filter "$RUNNER" 2>&1)"
status=$?
set -e

if [[ $status -ne 0 ]]; then
    printf '[repro-runner-semantics] invariant=control_suite_failed status=%d\n' "$status" >&2
    exit 1
fi

old_claim='A row that PASSES means that bug appears FIXED'
if [[ "$output" == *"$old_claim"* ]]; then
    printf '[repro-runner-semantics] invariant=passing_controls_blanket_fixed old_claim=true\n' >&2
    exit 1
fi

neutral_claim='PASS rows may be controls or candidate fixes'
if [[ "$output" != *"$neutral_claim"* ]]; then
    printf '[repro-runner-semantics] invariant=neutral_passing_semantics_missing\n' >&2
    exit 1
fi

printf '[repro-runner-semantics] compiled runner reports neutral passing semantics\n'
