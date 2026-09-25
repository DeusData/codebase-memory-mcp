#!/usr/bin/env bash
# The Windows leg's verdict must not be decided by the exit status alone.
#
# Why: that status travels ssh -> cmd.exe -> msys2_shell.cmd and the chain
# loses it. On 2026-09-18 a leg that printed "7925 passed, 0 failed" and
# "=== All tests passed ===" exited 1, while the identical work run through
# win.sh's other entry exited 0. A channel that turns 0 into 1 can turn 1 into
# 0, and that direction is a FALSE GREEN: a red Windows leg reported as
# passing, which is exactly the class of defect the local ladder exists to
# catch. The log is written by the runner on the VM and cannot be mangled in
# transit, so it decides the test outcome; rc only decides what the log cannot
# see.
#
# This drives the REAL function (vm-run-tests.sh --verdict) rather than a copy,
# so the contract cannot drift from the code it pins.
#
# Usage: tests/test_vm_verdict_contract.sh [repo-root]

set -uo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
RUNNER="$ROOT/test-infrastructure/vm/vm-run-tests.sh"
if [ ! -f "$RUNNER" ]; then
    echo "FAIL: $RUNNER not found" >&2
    exit 1
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-vm-verdict.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT

printf '  7925 passed, 0 failed, 74 skipped\n=== All tests passed ===\n' > "$WORK/green"
printf '  7863 passed, 1 failed, 74 skipped\n' > "$WORK/red"
printf '  7863 passed, 1 failed\n=== All tests passed ===\n' > "$WORK/red_with_marker"
printf '  100 passed, 0 failed\n  200 passed, 3 failed\n=== All tests passed ===\n' > "$WORK/red_later_suite"
printf '  7925 passed, 0 failed, 74 skipped\n' > "$WORK/no_marker"
printf 'configure: something exploded\n' > "$WORK/no_summary"

failures=0
expect() { # description log rc want [mode]
    local description="$1" log="$2" rc="$3" want="$4" mode="${5:-full}" got
    bash "$RUNNER" --verdict "$WORK/$log" "$rc" "$mode" >/dev/null 2>&1
    got=$?
    if [ "$got" -ne "$want" ]; then
        echo "FAIL: $description — verdict $got, expected $want" >&2
        failures=$((failures + 1))
    fi
}

# The observed fault: a complete, green log whose exit status was lost. The
# leg is green; the transport problem is reported, not the tests.
expect "green log with a lost exit status is green" green 1 0
expect "green log with a clean exit status is green" green 0 0

# The dangerous direction. A zero exit status must never launder failures.
expect "failures in the log are red even when rc says 0" red 0 1
expect "failures in the log are red when rc says 1" red 1 1
expect "a completion marker cannot launder failures" red_with_marker 0 1
expect "failures in a LATER suite are still counted" red_later_suite 0 1

# Incomplete runs are never green: a leg that stopped after one suite has a
# summary but no marker.
expect "a summary without the completion marker is red" no_marker 0 1

# No summary at all keeps its own distinct code, so "never ran" stays
# distinguishable from "ran and failed".
expect "a log with no summary is the no-summary guard" no_summary 0 90

# Iteration mode: `test.sh --suites` finishes after the suites it was named and
# prints no completion marker, because it has no end to reach. Demanding one
# turned a passing subset run into a failure — 23 passed, 1 skipped, rc=0,
# reported red (win.sh test artifact, 2026-09-19). A guard against false greens
# must not invent false reds, so the marker is required of the full leg only.
expect "a subset run with no marker is green in iteration mode" no_marker 0 0 iteration
expect "the same log is still red for the full leg" no_marker 0 1 full
# Without a marker to lean on, rc is the only evidence left that a subset run
# ended badly after its last summary, so iteration mode obeys it.
expect "a non-zero status is red in iteration mode" no_marker 1 1 iteration
# Failures still outrank everything, in either mode.
expect "failures are red in iteration mode too" red 0 1 iteration

if [ "$failures" -gt 0 ]; then
    echo "VM verdict contract VIOLATED: $failures case(s)" >&2
    exit 1
fi
echo "VM verdict contract passed (12 cases: lost exit status, false green, partial runs, iteration mode)"
