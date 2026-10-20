#!/usr/bin/env bash
# Lane-gate contract: the two aggregate gates under lane selection.
#
# With lane selection a PR runs a SUBSET of its jobs, and a subset is exactly
# where a silent gate loss hides: before selection a `skipped` job only ever
# meant "not needed for this PR", so ci-ok could wave every skip through. Now a
# selected lane whose jobs were skipped, cancelled or never created looks just
# as quiet. So:
#   - scripts/ci/require-all-green.sh (ci-ok): given the selector's LANES and
#     the run's JOBS, a SELECTED lane must have run and succeeded; an
#     unselected lane may be skipped or absent; memwaste never gates.
#   - scripts/ci/verify-shard-union.sh (shard-completeness): "no manifests" is
#     green only when the selection holds no test leg at all; with legs (or
#     with no selection given: dry run, release) it stays a failure.
# Both scripts are driven with synthetic inputs, never copied.
#
# Usage: tests/test_lane_gate_contract.sh [repo-root]

set -uo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
GREEN_GATE="$ROOT/scripts/ci/require-all-green.sh"
UNION="$ROOT/scripts/ci/verify-shard-union.sh"
for f in "$GREEN_GATE" "$UNION"; do
    if [ ! -f "$f" ]; then
        echo "FAIL: $f not found" >&2
        exit 1
    fi
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-lane-gate.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT
failures=0

# ── ci-ok ────────────────────────────────────────────────────────────────
NEEDS_OK='{"changes":{"result":"success"},"security":{"result":"success"},"lint":{"result":"success"},"test":{"result":"success"},"pr-smoke":{"result":"skipped"},"contracts":{"result":"skipped"}}'
NEEDS_RED='{"changes":{"result":"success"},"security":{"result":"success"},"lint":{"result":"failure"},"test":{"result":"skipped"},"pr-smoke":{"result":"skipped"},"contracts":{"result":"skipped"}}'
T1='["lint","lint-mem","security-static","shard-completeness","unix-macos14","unix-x86","windows"]'

job() { # name conclusion [status]
    printf '{"name":"%s","status":"%s","conclusion":%s}\n' "$1" "${3:-completed}" \
        "$([ "$2" = null ] && echo null || echo "\"$2\"")"
}
t1_jobs() { # the jobs of a green T1 run; $1 = conclusion of the x86 leg
    job "changes" success
    job "security / security-static" success
    job "security / license-gate" skipped
    job "security / codeql-gate" skipped
    job "lint / lint" success
    job "lint / lint-mem" success
    job "test / setup-matrix" success
    job "test / test-unix (ubuntu-latest, gcc, g++, unix-x86, 1/1)" "$1"
    job "test / test-unix (macos-14, cc, c++, unix-macos14, 1/1)" success
    job "test / test-windows (windows-latest, CLANG64, x86_64, windows, 1/1)" success
    job "test / test-windows-guards" skipped
    job "test / test-package-wrappers" skipped
    job "test / test-diag" skipped
    job "test / test-msan" skipped
    job "test / test-lsan-macos" skipped
    job "test / test-tsan" skipped
    job "test / shard-completeness" success
    job "pr-smoke" skipped
    job "contracts" skipped
    job "memwaste" skipped
    job "test-impact-shadow" failure
    job "ci-ok" null in_progress
}

gate() { # description want needs lanes jobs-file|-
    local description="$1" want="$2" needs="$3" lanes="$4" jobs="$5" got
    if [ "$lanes" = "-" ]; then
        RESULTS="$needs" bash "$GREEN_GATE" >"$WORK/out" 2>&1
    else
        RESULTS="$needs" LANES="$lanes" JOBS="$jobs" bash "$GREEN_GATE" >"$WORK/out" 2>&1
    fi
    got=$?
    if [ "$got" -ne "$want" ]; then
        echo "FAIL: ci-ok: $description — exit $got, expected $want" >&2
        sed 's/^/      /' "$WORK/out" >&2
        failures=$((failures + 1))
    fi
}

t1_jobs success >"$WORK/t1-green"
t1_jobs skipped >"$WORK/t1-x86-skipped"
t1_jobs cancelled >"$WORK/t1-x86-cancelled"
grep -v 'unix-x86' "$WORK/t1-green" >"$WORK/t1-x86-missing"
t1_jobs success | sed 's|"lint / lint","status":"completed","conclusion":"success"|"lint / lint","status":"completed","conclusion":"cancelled"|' >"$WORK/t1-lint-cancelled"
t1_jobs success | sed 's|"test / test-diag","status":"completed","conclusion":"skipped"|"test / test-diag","status":"completed","conclusion":"failure"|' >"$WORK/t1-unselected-failed"
t1_jobs success | sed 's|"lint / lint","status":"completed"|"lint / lint","status":"in_progress"|' >"$WORK/t1-lint-running"
{ t1_jobs success; job "memwaste / event-linux" failure; } >"$WORK/t1-memwaste-red"

# Without a selection the gate is exactly the old needs-level gate.
gate "no selection, every stage green or skipped" 0 "$NEEDS_OK" - -
gate "no selection, a failed stage" 1 "$NEEDS_RED" - -
# The selection contract.
gate "selected lanes all succeeded; unselected skipped or absent" 0 "$NEEDS_OK" "$T1" "$WORK/t1-green"
gate "a SELECTED lane that was skipped is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-x86-skipped"
gate "a SELECTED lane that was cancelled is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-x86-cancelled"
gate "a SELECTED lane with no job at all is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-x86-missing"
gate "a cancelled selected single job is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-lint-cancelled"
gate "a selected job that has not completed is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-lint-running"
gate "an unselected lane that ran and FAILED is red" 1 "$NEEDS_OK" "$T1" "$WORK/t1-unselected-failed"
gate "memwaste is report-only even when selected" 0 "$NEEDS_OK" "${T1%]},\"memwaste\"]" "$WORK/t1-memwaste-red"
gate "the needs-level gate still applies under a selection" 1 "$NEEDS_RED" "$T1" "$WORK/t1-green"
# A selection ci-ok cannot trust is red, never "nothing to check".
gate "an empty selection is red (no floor lane)" 1 "$NEEDS_OK" '[]' "$WORK/t1-green"
gate "a selection without lint or contracts is red" 1 "$NEEDS_OK" '["security-static","unix-x86"]' "$WORK/t1-green"
gate "an unknown lane is red" 1 "$NEEDS_OK" '["lint","security-static","no-such-lane"]' "$WORK/t1-green"
gate "a malformed selection is red" 1 "$NEEDS_OK" 'lint,unix-x86' "$WORK/t1-green"
gate "a selection without the job list is red" 1 "$NEEDS_OK" "$T1" "$WORK/does-not-exist"

# Matrix jobs carry their lane in the name; the neighbouring single job
# (test-windows-guards) must never be read as the windows leg.
{
    job "security / security-static" success
    job "lint / lint" success
    job "lint / lint-mem" success
    job "test / test-windows-guards" success
    job "pr-smoke (macos-14, smoke-mac)" success
} >"$WORK/guards-only"
gate "windows-guards is not the windows leg" 1 "$NEEDS_OK" '["lint","lint-mem","security-static","windows"]' "$WORK/guards-only"
gate "pr-smoke matrix name maps to its lane" 0 "$NEEDS_OK" '["lint","lint-mem","security-static","smoke-mac","windows-guards"]' "$WORK/guards-only"

# ── shard-completeness ───────────────────────────────────────────────────
union() { # description want dir [--lanes value]
    local description="$1" want="$2" dir="$3" got
    shift 3
    bash "$UNION" "$dir" "$@" >"$WORK/out" 2>&1
    got=$?
    if [ "$got" -ne "$want" ]; then
        echo "FAIL: shard union: $description — exit $got, expected $want" >&2
        sed 's/^/      /' "$WORK/out" >&2
        failures=$((failures + 1))
    fi
}
manifest() { # dir leg shard slice-suites...
    local dir="$1" leg="$2" shard="$3" sha
    shift 3
    mkdir -p "$dir"
    sha=$(printf '%s\n' a b c | sort | { sha256sum 2>/dev/null || shasum -a 256; } | awk '{print $1}')
    {
        echo "leg=$leg"
        echo "shard=$shard"
        echo "list_sha256=$sha"
        [ -z "${SEL:-}" ] || echo "selection_sha256=$SEL"
        echo "--- slice ---"
        printf '%s\n' "$@"
    } >"$dir/shard-manifest.txt"
}
mkdir -p "$WORK/empty"
manifest "$WORK/whole/m1" ubuntu-latest-gcc 1/1 a b c
manifest "$WORK/lost/m1" ubuntu-latest-gcc 1/2 a
manifest "$WORK/lost/m2" ubuntu-latest-gcc 2/2 b
# A narrowed leg (smart CI): the shards must agree on the selection applied.
SEL=s1 manifest "$WORK/selsame/m1" ubuntu-latest-gcc 1/2 a b
SEL=s1 manifest "$WORK/selsame/m2" ubuntu-latest-gcc 2/2 c
SEL=s1 manifest "$WORK/seldiff/m1" ubuntu-latest-gcc 1/2 a b
SEL=s2 manifest "$WORK/seldiff/m2" ubuntu-latest-gcc 2/2 c
SEL=s1 manifest "$WORK/selhalf/m1" ubuntu-latest-gcc 1/2 a b
manifest "$WORK/selhalf/m2" ubuntu-latest-gcc 2/2 c

union "no manifests, no selection given (dry run / release)" 1 "$WORK/empty"
union "no manifests, selection 'all'" 1 "$WORK/empty" --lanes all
union "no manifests and no test leg selected" 0 "$WORK/empty" --lanes '["lint","lint-mem","security-static","windows-guards"]'
union "no download dir at all and no test leg selected" 0 "$WORK/never-created" --lanes '["lint","lint-mem","security-static"]'
union "no manifests but a unix leg selected" 1 "$WORK/empty" --lanes '["lint","security-static","unix-x86"]'
union "no manifests but the windows leg selected" 1 "$WORK/empty" --lanes '["lint","security-static","windows"]'
union "a complete 1/1 leg" 0 "$WORK/whole" --lanes '["lint","security-static","unix-x86"]'
union "a lost slice is still a gate-quality loss" 1 "$WORK/lost" --lanes '["lint","security-static","unix-x86"]'
union "a malformed selection is a usage error" 2 "$WORK/empty" --lanes 'lint,unix-x86'
union "shards that applied one test selection" 0 "$WORK/selsame" --lanes '["lint","security-static","unix-x86"]'
union "shards that applied different test selections" 1 "$WORK/seldiff" --lanes '["lint","security-static","unix-x86"]'
union "a narrowed shard next to a full one" 1 "$WORK/selhalf" --lanes '["lint","security-static","unix-x86"]'

if [ "$failures" -ne 0 ]; then
    echo "LANE GATE CONTRACT VIOLATED: $failures case(s)" >&2
    exit 1
fi
echo "lane-gate contract OK (ci-ok lane strictness + shard-union selection)"
