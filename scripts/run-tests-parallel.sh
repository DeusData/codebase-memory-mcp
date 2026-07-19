#!/usr/bin/env bash
# run-tests-parallel.sh — run every registered test suite as parallel
# processes of the already-built test-runner.
#
# ZERO-LOSS CONTRACT (gate quality must be identical to the sequential run):
#   1. The suite list comes from `test-runner --list-suites`, which is printed
#      by the SAME macro table that executes suites — the list cannot drift
#      from reality by construction.
#   2. UNION GUARD: after the run, the set of suites that actually produced a
#      result is compared against that list; any difference (a suite that
#      never ran, or ran twice) fails the gate loudly. A newly added suite is
#      picked up automatically on the next invocation.
#   3. Per-suite pass/fail/skip counts are summed and reported in the same
#      "N passed[, M failed][, K skipped]" shape as the sequential runner, so
#      before/after totals are directly comparable.
#   4. ANY suite failing, crashing (nonzero exit), or missing ⇒ exit 1.
#
# Usage: run-tests-parallel.sh <path-to-test-runner> [jobs]
#   jobs defaults to CBM_TEST_PAR_JOBS, then the CPU count.

set -uo pipefail

RUNNER="${1:?usage: run-tests-parallel.sh <path-to-test-runner> [jobs]}"
JOBS="${2:-${CBM_TEST_PAR_JOBS:-}}"

if [ -z "$JOBS" ]; then
    if command -v nproc >/dev/null 2>&1; then
        JOBS=$(nproc)
    elif command -v sysctl >/dev/null 2>&1; then
        JOBS=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
    else
        JOBS=4
    fi
fi

LOGDIR="$(dirname "$RUNNER")/test-logs"
rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

SUITES_FILE="$LOGDIR/suites.txt"
RESULTS_FILE="$LOGDIR/results.txt"

# tr strips the CR that the Windows CRT appends to every stdout line — a
# suites file with CRLF endings made the runner reject every name
# ("arena\r" is an unknown suite) and fail all 104 suites on CI.
if ! "$RUNNER" --list-suites | tr -d '\r' > "$SUITES_FILE"; then
    echo "FAIL: test-runner --list-suites exited nonzero" >&2
    exit 1
fi
NSUITES=$(wc -l < "$SUITES_FILE" | tr -d ' ')
if [ "$NSUITES" -lt 1 ] || grep -qvE '^[a-z0-9_]+$' "$SUITES_FILE"; then
    echo "FAIL: suite list empty or malformed (runner too old for --list-suites?)" >&2
    exit 1
fi
# Timing-sensitive suites run SEQUENTIALLY after the parallel wave: they
# spawn subprocesses / watch the filesystem / bind ports with fixed
# deadlines, and a saturated 4-core CI runner starves those deadlines into
# flakes (3 cli-suite failures on the ubuntu legs of the first CI run).
# Same suites, same tests, same gates — only the schedule differs; the
# union guard below still checks the COMBINED result set.
# stack_overflow_a/b/c: their giant-recursion ASan allocations stall ~100x
# when co-STARTED with a large wave on Apple Silicon (2s staggered vs ~230s
# simultaneous — a local scheduler/zone quirk, not contention: job count
# does not change it). Staggered in the tail they cost seconds.
SERIAL_SUITES="cli subprocess watcher incremental httpd ui index_resilience mcp \
    stack_overflow_a stack_overflow_b stack_overflow_c"
is_serial() {
    case " $SERIAL_SUITES " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}
PAR_FILE="$LOGDIR/suites-parallel.txt"
SER_FILE="$LOGDIR/suites-serial.txt"
: > "$PAR_FILE"
: > "$SER_FILE"
while IFS= read -r sname; do
    if is_serial "$sname"; then
        echo "$sname" >> "$SER_FILE"
    else
        echo "$sname" >> "$PAR_FILE"
    fi
done < "$SUITES_FILE"
echo "=== parallel test run: $NSUITES suites ($(wc -l < "$SER_FILE" | tr -d ' ') serial-tail), $JOBS jobs ==="

export RUNNER LOGDIR RESULTS_FILE
run_one() {
    s="$1"
    t0=$SECONDS
    "$RUNNER" "$s" > "$LOGDIR/$s.log" 2>&1
    rc=$?
    secs=$((SECONDS - t0))
    summary=$(grep -E '^  [0-9]+ passed' "$LOGDIR/$s.log" | tail -1)
    pass=$(printf '%s' "$summary" | sed -n 's/^  \([0-9]*\) passed.*/\1/p')
    failn=$(printf '%s' "$summary" | sed -n 's/.* \([0-9]*\) failed.*/\1/p')
    skip=$(printf '%s' "$summary" | sed -n 's/.* \([0-9]*\) skipped.*/\1/p')
    # A single short echo line is an atomic append (< PIPE_BUF).
    echo "$s rc=$rc pass=${pass:-0} fail=${failn:-0} skip=${skip:-0} secs=$secs" >> "$RESULTS_FILE"
}
export -f run_one

xargs -P "$JOBS" -I{} bash -c 'run_one "$@"' _ {} < "$PAR_FILE"
# Serial tail: quiet machine for the deadline-sensitive suites.
while IFS= read -r sname; do
    run_one "$sname"
done < "$SER_FILE"

# ── Union guard: every listed suite produced exactly one result ──
MISSING=$(comm -23 <(sort "$SUITES_FILE") <(awk '{print $1}' "$RESULTS_FILE" | sort -u))
DUPES=$(awk '{print $1}' "$RESULTS_FILE" | sort | uniq -d)
if [ -n "$MISSING" ] || [ -n "$DUPES" ]; then
    echo "FAIL: shard union does not match --list-suites (GATE-QUALITY LOSS)" >&2
    [ -n "$MISSING" ] && echo "  never ran: $MISSING" >&2
    [ -n "$DUPES" ] && echo "  ran twice: $DUPES" >&2
    exit 1
fi

TOTAL_PASS=$(awk -F'pass=' '{split($2,a," "); s+=a[1]} END{print s+0}' "$RESULTS_FILE")
TOTAL_FAIL=$(awk -F'fail=' '{split($2,a," "); s+=a[1]} END{print s+0}' "$RESULTS_FILE")
TOTAL_SKIP=$(awk -F'skip=' '{split($2,a," "); s+=a[1]} END{print s+0}' "$RESULTS_FILE")
BAD_RC=$(grep -cv ' rc=0 ' "$RESULTS_FILE" || true)

echo "── 8 slowest suites ──"
sort -t= -k6 -rn "$RESULTS_FILE" | head -8
grep -v ' rc=0 ' "$RESULTS_FILE" || true
for f in $(grep -v ' rc=0 ' "$RESULTS_FILE" | awk '{print $1}'); do
    echo "──── $f: every failure site ────"
    grep -B2 -A8 "FAIL" "$LOGDIR/$f.log" | head -120
    echo "──── $f: last 15 lines ────"
    tail -15 "$LOGDIR/$f.log"
done

echo "────────────────────────────────────────────"
echo "  $TOTAL_PASS passed, $TOTAL_FAIL failed, $TOTAL_SKIP skipped  ($NSUITES suites, $JOBS jobs)"
echo "────────────────────────────────────────────"

if [ "$TOTAL_FAIL" -gt 0 ] || [ "$BAD_RC" -gt 0 ]; then
    exit 1
fi
exit 0
