#!/usr/bin/env bash
# soak-legs.sh — THE canonical soak leg entry. Every venue calls this file.
#
# Runs the soak SEQUENCE — the quick soak, then the read-only #581 query-leak
# detector — with the completion-summary guard applied to each leg. Before this
# file existed the sequence was hand-copied six times across _soak.yml, once in
# vm-run-tests.sh and once in docker-compose.yml; venues drifted (CI soaked
# without the query-leak guard's temp hardening, local soaked without the
# query-leak leg at all). Venue-specific text is now limited to provisioning;
# the sequence lives here and only here.
#
# Callers: .github/workflows/_soak.yml (all platforms) · win.sh soak (via
# vm-run-tests.sh --soak) · test-infrastructure/run.sh soak-linux (compose).
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/soak-legs.sh [--legs LIST] <binary> <duration-minutes>

The canonical soak entry: identical in local CI, PR CI, dry run and release.
Runs each requested leg through scripts/soak-test.sh and hard-fails any leg
that exits zero without printing its completion summary (a crashed or killed
soak must never read as green).

Legs (default: quick,query-leak — the release-gating sequence):
  quick        scripts/soak-test.sh <binary> <minutes>
               Default soak: periodic reindex + query churn + crash-recovery.
               Results in soak-results/.
  query-leak   CBM_SOAK_MODE=query-leak, --skip-crash-test, results in
               soak-results-query-leak/. The #581 detector: after the initial
               index it NEVER reindexes or mutates, so any RSS growth is a
               query-path leak, not WAL/indexing churn.

Options:
  --legs LIST  Comma-separated subset, e.g. --legs quick. The ASan soak jobs
               use this (a 15-min single-leg soak under ASan); local iteration
               may too. Order is preserved; unknown names fail fast.
  -h, --help   This text.

Environment:
  ASAN_OPTIONS etc. pass through to the binary unchanged.
  RESULTS_DIR is owned by this script (per-leg); do not pre-set it.

Examples:
  scripts/soak-legs.sh build/c/codebase-memory-mcp 10          # both legs, 10m each
  scripts/soak-legs.sh --legs quick build/c/codebase-memory-mcp 15   # ASan-style
EOF
}

LEGS="quick,query-leak"
while [ $# -gt 0 ]; do
    case "$1" in
    --legs) LEGS="${2:?--legs needs a comma-separated list}"; shift ;;
    --legs=*) LEGS="${1#--legs=}" ;;
    -h | --help) usage; exit 0 ;;
    --*) echo "soak-legs: unknown option '$1'. Please consult --help." >&2; exit 2 ;;
    *) break ;;
    esac
    shift
done

BINARY="${1:?soak-legs: missing <binary>. Please consult --help.}"
DURATION="${2:?soak-legs: missing <duration-minutes>. Please consult --help.}"
case "$DURATION" in
'' | *[!0-9]*) echo "soak-legs: duration must be a positive integer (minutes). Please consult --help." >&2; exit 2 ;;
esac
[ "$DURATION" -gt 0 ] || { echo "soak-legs: duration must be positive" >&2; exit 2; }
# Windows builds produce <name>.exe; resolve it here so every venue passes the
# same plain path and carries no per-platform binary-name logic of its own.
# Resolve whenever the .exe EXISTS, not only when the plain name fails -x:
# msys resolves the suffix-less name transparently (it IS -x), but soak-test
# keys its native-Windows handling — cygpath'd CBM_CACHE_DIR, coproc stdio —
# off the literal .exe suffix. A suffix-less native binary therefore received
# a POSIX-form cache path, mis-rooting the daemon logs and silently disabling
# diagnostics (both hosted-runner Windows soak legs; reproduced on the VM's
# CLANG64 environment with a suffix-less invocation).
if [[ "$BINARY" != *.exe ]] && [ -x "${BINARY}.exe" ]; then
    BINARY="${BINARY}.exe"
fi
[ -x "$BINARY" ] || { echo "soak-legs: binary '$BINARY' missing or not executable — build first" >&2; exit 2; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

run_leg() {
    local leg="$1"
    local log
    # No suffix after the X run: BSD mktemp substitutes only TRAILING X's, so
    # a template like ...XXXXXX.log creates a near-literal file on macOS and
    # every second run collides with the first run's leftover ("File exists").
    log="$(mktemp "${TMPDIR:-/tmp}/cbm-soak-leg-XXXXXX")"
    echo "=== soak-legs: leg=${leg} binary=${BINARY} duration=${DURATION}m ==="
    local rc=0
    case "$leg" in
    quick)
        "$ROOT/scripts/soak-test.sh" "$BINARY" "$DURATION" 2>&1 | tee "$log"
        rc="${PIPESTATUS[0]}"
        ;;
    query-leak)
        CBM_SOAK_MODE=query-leak RESULTS_DIR=soak-results-query-leak \
            "$ROOT/scripts/soak-test.sh" "$BINARY" "$DURATION" --skip-crash-test 2>&1 | tee "$log"
        rc="${PIPESTATUS[0]}"
        ;;
    *)
        echo "soak-legs: unknown leg '$leg' (known: quick, query-leak)" >&2
        rm -f -- "$log"
        return 2
        ;;
    esac
    # A soak whose log lacks the completion summary did not validly run to the
    # end, whatever its exit code says — same guard class as the test runner's.
    if ! grep -Fq '=== soak-test: PASSED ===' "$log"; then
        echo "GUARD: soak leg '${leg}' produced no completion summary; treating as" \
            "failure (soak rc=$rc)" >&2
        rm -f -- "$log"
        return 90
    fi
    rm -f -- "$log"
    return "$rc"
}

IFS=',' read -r -a leg_list <<<"$LEGS"
[ "${#leg_list[@]}" -gt 0 ] || { echo "soak-legs: --legs resolved to nothing" >&2; exit 2; }
for leg in "${leg_list[@]}"; do
    run_leg "$leg"
done
echo "=== soak-legs: all legs passed (${LEGS}) ==="
