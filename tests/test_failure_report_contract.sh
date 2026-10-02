#!/usr/bin/env bash
# A red suite's summary must show WHY it is red, whatever killed it.
#
# Why: a Windows sanitizer job went red on one suite that aborted under
# AddressSanitizer, and the job log held nothing a reader could act on:
#
#   ──── index_resilience: every failure site ────
#   ──── index_resilience: last 15 lines ────
#     (the AddressSanitizer shadow-byte legend)
#   ==6636==ABORTING
#
# The summary grepped the suite log for "FAIL" only, and a sanitizer report
# never contains that word. Its useful part (error kind, access size, top
# frames) is at the START of the report, far above the last 15 lines. With no
# log artifact either, the cause had to be found by reading code a day later.
#
# This drives the REAL functions (scripts/suite-failure-report.sh, which the
# parallel harness sources) with fabricated suite logs written the way
# tests/test_framework.h writes them, so the contract cannot drift from the
# code it pins. No build, no network, no waiting: every case is a file in and
# text out.
#
# Usage: tests/test_failure_report_contract.sh [repo-root]   (root override so
# the contract can be shown to FAIL against a tree without the change)

set -uo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
HELPER="$ROOT/scripts/suite-failure-report.sh"
DRIVER="$ROOT/scripts/run-tests-parallel.sh"
SCHEDULER="$ROOT/scripts/run-test-wave.py"

for required in "$HELPER" "$DRIVER" "$SCHEDULER"; do
    if [ ! -f "$required" ]; then
        echo "FAIL: $required not found" >&2
        exit 1
    fi
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-failure-report.XXXXXX") || exit 1
trap 'rm -rf "$WORK"' EXIT

failures=0
cases=0
fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}
# Substring tests stay in the shell: piping into `grep -q` under pipefail can
# hand the writer EPIPE and report a satisfied match as status 141.
expect_has() { # description text needle
    cases=$((cases + 1))
    case "$2" in
    *"$3"*) ;;
    *) fail "$1 — the summary lacks: $3" ;;
    esac
}
expect_lacks() { # description text needle
    cases=$((cases + 1))
    case "$2" in
    *"$3"*) fail "$1 — the summary must not contain: $3" ;;
    esac
}
expect_same() { # description actual expected
    cases=$((cases + 1))
    if [ "$2" != "$3" ]; then
        fail "$1 — output differs from the recorded one"
        printf '%s\n' "$3" > "$WORK/expected.txt"
        printf '%s\n' "$2" > "$WORK/actual.txt"
        diff "$WORK/expected.txt" "$WORK/actual.txt" >&2 || true
    fi
}

summary_of() { # suite log-name
    bash "$HELPER" summary "$1" "$WORK/$2" 2>&1
}
# Only the sanitizer / crash section of a summary.
report_section() {
    printf '%s\n' "$1" | awk '
        /: sanitizer \/ crash report / { on = 1; next }
        /: last 15 lines / { on = 0 }
        on'
}

# The framework's own line shapes (tests/test_framework.h): RUN_TEST prints
# "  %-55s" and flushes BEFORE the test body, then "PASS\n" after it.
running() { printf '  %-55s' "$1"; }
passed() { printf '  %-55sPASS\n' "$1"; }
banner() { printf '\n  codebase-memory-mcp  C test suite\n\n=== %s ===\n' "$1"; }
RULE="────────────────────────────────────────────"
totals() { printf '\n%s\n  %s\n%s\n\n' "$RULE" "$1" "$RULE"; }

# ── 1. AddressSanitizer abort: the incident ──────────────────────────────────
# The report starts on the running test's own line (the name was flushed with
# no newline) and ends in the shadow dump, the legend and ABORTING.
{
    banner index_resilience
    passed resilience_reopens_after_clean_shutdown
    passed resilience_recovers_truncated_journal
    running resilience_rejects_oversized_header
    cat <<'EOF'
=================================================================
==123==ERROR: AddressSanitizer: stack-buffer-overflow on address 0x00a1b2c3d4e6 at pc 0x7ff6a1b2c3d4 bp 0x00a1b2c3d400 sp 0x00a1b2c3d3f8
WRITE of size 22 at 0x00a1b2c3d4e6 thread T0
    #0 0x7ff6a1b2c3d3 in __asan_memcpy (test-runner+0x1402c3d3)
    #1 0x7ff6a1b2d111 in fixture_copy_header src/fixture_header.c:88
    #2 0x7ff6a1b2e222 in test_resilience_rejects_oversized_header tests/test_fixture.c:141

Address 0x00a1b2c3d4e6 is located in stack of thread T0 at offset 54 in frame
    #0 0x7ff6a1b2d000 in fixture_copy_header src/fixture_header.c:61

  This frame has 1 object(s):
    [32, 54) 'magic' (line 63) <== Memory access at offset 54 overflows this variable
HINT: this may be a false positive if your program uses some custom stack unwind mechanism, swapcontext or vfork
SUMMARY: AddressSanitizer: stack-buffer-overflow src/fixture_header.c:88 in fixture_copy_header
Shadow bytes around the buggy address:
  0x00a1b2c3d200: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x00a1b2c3d280: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
=>0x00a1b2c3d480: f1 f1 f1 f1 00 00[06]f3 f3 f3 f3 f3 00 00 00 00
  0x00a1b2c3d500: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
Shadow byte legend (one shadow byte represents 8 application bytes):
  Addressable:           00
  Partially addressable: 01 02 03 04 05 06 07
  Heap left redzone:       fa
  Freed heap region:       fd
  Stack left redzone:      f1
  Stack mid redzone:       f2
  Stack right redzone:     f3
  Stack after return:      f5
  Stack use after scope:   f8
  Global redzone:          f9
  Global init order:       f6
  Poisoned by user:        f7
  Container overflow:      fc
  Array cookie:            ac
  Intra object redzone:    bb
  ASan internal:           fe
  Left alloca redzone:     ca
  Right alloca redzone:    cb
==123==ABORTING
EOF
} > "$WORK/asan_abort.log"

out=$(summary_of index_resilience asan_abort.log)
section=$(report_section "$out")
expect_has "ASan abort: the error line" "$section" \
    "==123==ERROR: AddressSanitizer: stack-buffer-overflow on address 0x00a1b2c3d4e6"
expect_has "ASan abort: the access line" "$section" "WRITE of size 22 at 0x00a1b2c3d4e6 thread T0"
expect_has "ASan abort: frame #0" "$section" "#0 0x7ff6a1b2c3d3 in __asan_memcpy"
expect_has "ASan abort: frame #1" "$section" "#1 0x7ff6a1b2d111 in fixture_copy_header src/fixture_header.c:88"
expect_has "ASan abort: frame #2" "$section" \
    "#2 0x7ff6a1b2e222 in test_resilience_rejects_oversized_header tests/test_fixture.c:141"
expect_has "ASan abort: the report's own summary line" "$section" \
    "SUMMARY: AddressSanitizer: stack-buffer-overflow src/fixture_header.c:88 in fixture_copy_header"
expect_has "ASan abort: the running test is named" "$section" \
    "running test: resilience_rejects_oversized_header"
expect_lacks "ASan abort: a finished test is not blamed" "$section" "resilience_recovers_truncated_journal"
expect_lacks "ASan abort: the shadow dump stays out of the report" "$section" "Shadow byte"
# The legend lines are two-space-indented words, the shape a short test line
# would have; none of them may be taken for a test.
expect_lacks "ASan abort: a legend line is not a test" "$section" "running test: Addressable"
expect_has "ASan abort: the older sections are still there" "$out" \
    "──── index_resilience: every failure site ────"
expect_has "ASan abort: the tail is still there" "$out" "──── index_resilience: last 15 lines ────"
expect_has "ASan abort: the tail still ends the log" "$out" "==123==ABORTING"

# The same log as a Windows CRT writes it: CRLF line endings.
awk '{ printf "%s\r\n", $0 }' "$WORK/asan_abort.log" > "$WORK/asan_abort_crlf.log"
section=$(report_section "$(summary_of index_resilience asan_abort_crlf.log)")
expect_has "ASan abort (CRLF): the error line" "$section" "==123==ERROR: AddressSanitizer: stack-buffer-overflow"
expect_has "ASan abort (CRLF): frame #2" "$section" "#2 0x7ff6a1b2e222 in test_resilience_rejects_oversized_header"
expect_has "ASan abort (CRLF): the running test is named" "$section" \
    "running test: resilience_rejects_oversized_header"
expect_lacks "ASan abort (CRLF): the shadow dump stays out" "$section" "Shadow byte"

# ── 2. UBSan diagnostic in a suite that otherwise passes ─────────────────────
# Recoverable UBSan prints one line to stderr and the test carries on, so its
# PASS lands on the next line. Nothing in the log says FAIL.
{
    banner arith
    passed arith_adds_small_values
    running arith_scales_offsets
    printf "src/fixture_math.c:42:17: runtime error: signed integer overflow: 2147483647 + 1 cannot be represented in type 'int'\n"
    printf 'PASS\n'
    passed arith_clamps_ranges
    totals "3 passed"
} > "$WORK/ubsan_recoverable.log"

out=$(summary_of arith ubsan_recoverable.log)
section=$(report_section "$out")
expect_has "UBSan: the runtime error line" "$section" \
    "src/fixture_math.c:42:17: runtime error: signed integer overflow: 2147483647 + 1"
expect_has "UBSan: the running test is named" "$section" "running test: arith_scales_offsets"
expect_lacks "UBSan: the report does not swallow the tests after it" "$section" "arith_clamps_ranges"
expect_has "UBSan: the tail is still there" "$out" "──── arith: last 15 lines ────"

# ── 3. Plain assertion failure: byte-for-byte what the harness printed before ─
# Recorded from the harness at 96c3f41c (grep -B2 -A8 "FAIL" | head -120, then
# tail -15), written out here line by line rather than recomputed, so a change
# to either section shows up as a difference.
{
    banner plain_fail
    for i in 01 02 03 04 05 06 07 08 09 10 11 12; do passed "parses_case_$i"; done
    running parses_nested_blocks
    printf '  FAIL tests/x.c:12: ASSERT(depth == 3)\n'
    for i in 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do passed "parses_case_$i"; done
    totals "30 passed, 1 failed"
} > "$WORK/plain_fail.log"

expected=$(
    echo "──── plain_fail: every failure site ────"
    passed parses_case_11
    passed parses_case_12
    running parses_nested_blocks
    printf '  FAIL tests/x.c:12: ASSERT(depth == 3)\n'
    for i in 13 14 15 16 17 18 19 20; do passed "parses_case_$i"; done
    echo "──── plain_fail: last 15 lines ────"
    for i in 21 22 23 24 25 26 27 28 29 30; do passed "parses_case_$i"; done
    totals "30 passed, 1 failed"
)
expect_same "plain FAIL: unchanged output" "$(summary_of plain_fail plain_fail.log)" "$expected"

# ── 4. Neither a FAIL nor a report: the tail still appears, nothing is added ──
{
    banner quiet_exit
    for i in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20; do passed "holds_case_$i"; done
    totals "20 passed"
} > "$WORK/quiet_exit.log"

expected=$(
    echo "──── quiet_exit: every failure site ────"
    echo "──── quiet_exit: last 15 lines ────"
    for i in 11 12 13 14 15 16 17 18 19 20; do passed "holds_case_$i"; done
    totals "20 passed"
)
expect_same "no FAIL, no report: unchanged output" "$(summary_of quiet_exit quiet_exit.log)" "$expected"

# ── 5. Silent death: no report, no FAIL, no completion summary ───────────────
# Trap-mode UBSan (Windows on ARM) dies on an illegal instruction and prints
# nothing. The running test is then the ONLY evidence the log holds.
{
    banner shift_width
    passed widens_before_shift
    running shifts_within_width
} > "$WORK/silent_death.log"

out=$(summary_of shift_width silent_death.log)
expect_has "silent death: the running test is named" "$out" \
    "running test: shifts_within_width (the log ends without a completion summary)"
expect_has "silent death: the tail is still there" "$out" "──── shift_width: last 15 lines ────"

# ── 6. A report AFTER the last test finished: nothing is blamed as running ───
{
    banner leaky
    passed allocates_and_releases
    passed keeps_one_buffer
    totals "2 passed"
    cat <<'EOF'
=================================================================
==77==ERROR: LeakSanitizer: detected memory leaks

Direct leak of 64 byte(s) in 1 object(s) allocated from:
    #0 0x5581aa in malloc (test-runner+0x5581aa)
    #1 0x5592bb in fixture_keep_buffer src/fixture_buffer.c:19

SUMMARY: AddressSanitizer: 64 byte(s) leaked in 1 allocation(s).
EOF
} > "$WORK/lsan_at_exit.log"

section=$(report_section "$(summary_of leaky lsan_at_exit.log)")
expect_has "leak at exit: the error line" "$section" "==77==ERROR: LeakSanitizer: detected memory leaks"
expect_has "leak at exit: the allocation frame" "$section" "#1 0x5592bb in fixture_keep_buffer src/fixture_buffer.c:19"
expect_has "leak at exit: the summary line" "$section" "SUMMARY: AddressSanitizer: 64 byte(s) leaked"
expect_has "leak at exit: the last test is reported as finished" "$section" \
    "last finished test: keeps_one_buffer (the report comes after it)"
expect_lacks "leak at exit: no test is blamed as running" "$section" "running test:"

# ── 7. ThreadSanitizer and MemorySanitizer open with WARNING, not ERROR ──────
{
    banner racy
    running counter_is_shared_safely
    cat <<'EOF'
==================
WARNING: ThreadSanitizer: data race (pid=4242)
  Write of size 4 at 0x7b0400000000 by thread T1:
    #0 fixture_bump src/fixture_counter.c:9 (test-runner+0x4a1b2c)

  Previous read of size 4 at 0x7b0400000000 by main thread:
    #0 fixture_read src/fixture_counter.c:14 (test-runner+0x4a1c3d)

SUMMARY: ThreadSanitizer: data race src/fixture_counter.c:9 in fixture_bump
==================
EOF
} > "$WORK/tsan_race.log"
section=$(report_section "$(summary_of racy tsan_race.log)")
expect_has "TSan: the warning line" "$section" "WARNING: ThreadSanitizer: data race (pid=4242)"
expect_has "TSan: the second stack" "$section" "#0 fixture_read src/fixture_counter.c:14"
expect_has "TSan: the running test is named" "$section" "running test: counter_is_shared_safely"

{
    banner uninit
    running reads_only_initialized_fields
    cat <<'EOF'
==55==WARNING: MemorySanitizer: use-of-uninitialized-value
    #0 0x4c1d2e in fixture_sum src/fixture_sum.c:27:9

SUMMARY: MemorySanitizer: use-of-uninitialized-value src/fixture_sum.c:27:9 in fixture_sum
Exiting
EOF
} > "$WORK/msan_uninit.log"
section=$(report_section "$(summary_of uninit msan_uninit.log)")
expect_has "MSan: the warning line" "$section" "==55==WARNING: MemorySanitizer: use-of-uninitialized-value"
expect_has "MSan: the frame" "$section" "#0 0x4c1d2e in fixture_sum src/fixture_sum.c:27:9"
expect_lacks "MSan: the report stops at its summary line" "$section" "Exiting"

# ── 8. A very deep stack is capped, and its summary line survives the cap ────
{
    banner deep
    running recurses_without_bound
    printf '=================================================================\n'
    printf '==9==ERROR: AddressSanitizer: stack-overflow on address 0x7ffc00000000\n'
    i=0
    while [ "$i" -lt 100 ]; do
        printf '    #%d 0x55aa00 in fixture_recurse src/fixture_recurse.c:7\n' "$i"
        i=$((i + 1))
    done
    printf '\nSUMMARY: AddressSanitizer: stack-overflow src/fixture_recurse.c:7 in fixture_recurse\n'
    printf '==9==ABORTING\n'
} > "$WORK/deep_stack.log"
section=$(report_section "$(summary_of deep deep_stack.log)")
expect_has "deep stack: the top frame" "$section" "#0 0x55aa00 in fixture_recurse"
expect_lacks "deep stack: frame 60 is past the cap" "$section" "#60 0x55aa00"
expect_has "deep stack: the cut is announced" "$section" "[report cut at 40 lines; the suite log has the rest]"
expect_has "deep stack: the summary line survives" "$section" \
    "SUMMARY: AddressSanitizer: stack-overflow src/fixture_recurse.c:7 in fixture_recurse"
cases=$((cases + 1))
section_lines=$(printf '%s\n' "$section" | wc -l | tr -d ' ')
if [ "$section_lines" -gt 60 ]; then
    fail "deep stack: the report section is not bounded ($section_lines lines)"
fi

# ── 9. Signal and timeout lines a suite's own output may carry ───────────────
{
    banner spawns
    running child_is_reaped
    printf 'child 4711 killed by signal 11\n'
    printf 'Segmentation fault: 11\n'
} > "$WORK/signal_lines.log"
section=$(report_section "$(summary_of spawns signal_lines.log)")
expect_has "signals: killed by signal" "$section" "child 4711 killed by signal 11"
expect_has "signals: segmentation fault" "$section" "Segmentation fault: 11"
expect_has "signals: the running test is named" "$section" "running test: child_is_reaped"

# ── 10. Which logs a red run keeps for upload ────────────────────────────────
# Kept: every suite of the slice that did not record rc=0, including one that
# was still in flight (a log but no result line). Not kept: green suites.
mkdir "$WORK/logs"
for suite in green_one red_assert red_abort in_flight never_started; do
    echo "$suite" >> "$WORK/logs/suites-shard.txt"
done
for suite in green_one red_assert red_abort in_flight; do
    echo "log of $suite" > "$WORK/logs/$suite.log"
done
{
    echo "green_one rc=0 pass=10 fail=0 skip=0 secs=1"
    echo "red_assert rc=1 pass=9 fail=1 skip=0 secs=1"
    echo "red_abort rc=-6 pass=4 fail=0 skip=0 secs=2"
} > "$WORK/logs/results.txt"
bash "$HELPER" collect "$WORK/logs" "$WORK/logs/results.txt" "$WORK/logs/suites-shard.txt" >/dev/null 2>&1
kept=""
for path in "$WORK/logs/failed"/*; do
    [ -e "$path" ] && kept="$kept${path##*/} "
done
cases=$((cases + 1))
for wanted in in_flight.log red_abort.log red_assert.log results.txt; do
    case " $kept" in
    *" $wanted "*) ;;
    *) fail "collect: $wanted was not kept (kept: $kept)" ;;
    esac
done
for unwanted in green_one.log never_started.log; do
    case " $kept" in
    *" $unwanted "*) fail "collect: $unwanted must not be kept (kept: $kept)" ;;
    esac
done
cases=$((cases + 1))
if [ "$(cat "$WORK/logs/failed/red_abort.log" 2>/dev/null)" != "log of red_abort" ]; then
    fail "collect: a kept log is not a copy of the suite log"
fi

mkdir "$WORK/green"
echo "green_one" > "$WORK/green/suites-shard.txt"
echo "log of green_one" > "$WORK/green/green_one.log"
echo "green_one rc=0 pass=10 fail=0 skip=0 secs=1" > "$WORK/green/results.txt"
bash "$HELPER" collect "$WORK/green" "$WORK/green/results.txt" "$WORK/green/suites-shard.txt" >/dev/null 2>&1
cases=$((cases + 1))
if [ -e "$WORK/green/failed" ]; then
    fail "collect: a green run must not create a failed/ directory"
fi

# ── 11. The scheduler's result line for a suite that died mid-run ────────────
# No completion summary is ever printed by an aborted suite, and the result
# line used to say pass=0 about a suite that had finished tests. The fixture
# exits on its own; the timeouts below are ceilings it never approaches.
cat > "$WORK/fake_runner.py" <<'PY'
import os
import sys

suite = sys.argv[-1]
if suite == "prints_nothing":
    os._exit(0)
sys.stdout.write("\n=== %s ===\n" % suite)
sys.stdout.write("  %-55sPASS\n" % "first_finishes")
if suite == "ends_with_summary":
    # One PASS marker in the log, but the suite's own summary says otherwise:
    # the summary is the count, the markers are never consulted.
    sys.stdout.write("\n  5 passed, 1 failed, 2 skipped\n\n")
    sys.stdout.flush()
    os._exit(1)
sys.stdout.write("  %-55s" % "second_logs_then_finishes")
sys.stdout.flush()
sys.stderr.write("level=info msg=working\n")
sys.stderr.flush()
sys.stdout.write("PASS\n")
sys.stdout.write("  %-55s" % "third_never_returns")
sys.stdout.flush()
os._exit(3)
PY
printf 'dies_mid_suite\nends_with_summary\nprints_nothing\n' > "$WORK/wave-suites.txt"
: > "$WORK/wave-results.txt"
python3 "$SCHEDULER" \
    --suite-file "$WORK/wave-suites.txt" \
    --log-dir "$WORK/wave-logs" \
    --results-file "$WORK/wave-results.txt" \
    --jobs 1 \
    --timeout 60 \
    --slow-timeout 60 \
    --kill-grace 1 \
    "$(command -v python3)" "$WORK/fake_runner.py" >/dev/null 2>"$WORK/wave-stderr.txt"
wave_rc=$?
result_lines=$(cat "$WORK/wave-results.txt")
cases=$((cases + 1))
case "$result_lines" in
"dies_mid_suite rc=3 pass=2 fail=0 skip=0 secs="*) ;;
*)
    fail "scheduler: a suite that died after two finished tests reported '$result_lines' (wave rc=$wave_rc)"
    cat "$WORK/wave-stderr.txt" >&2
    ;;
esac
expect_has "scheduler: a suite with a summary line is never recounted" "$result_lines" \
    "ends_with_summary rc=1 pass=5 fail=1 skip=2 secs="
expect_has "scheduler: a suite that ran nothing still counts nothing" "$result_lines" \
    "prints_nothing rc=97 pass=0 fail=0 skip=0 secs="
# ...and the log the scheduler wrote names the test that never returned.
cp "$WORK/wave-logs/dies_mid_suite.log" "$WORK/dies_mid_suite.log" 2>/dev/null
expect_has "scheduler log: the running test is named" "$(summary_of dies_mid_suite dies_mid_suite.log)" \
    "running test: third_never_returns (the log ends without a completion summary)"

# ── 12. One implementation: the harness uses these functions, not a copy ─────
driver_text=$(cat "$DRIVER")
expect_has "harness: sources the shared file" "$driver_text" \
    "source \"\$SCRIPT_DIR/suite-failure-report.sh\""
expect_has "harness: prints the summary through the shared function" "$driver_text" \
    "suite_failure_summary \"\$f\""
expect_has "harness: keeps failing logs through the shared function" "$driver_text" \
    "collect_failed_suite_logs \"\$LOGDIR\""
expect_lacks "harness: no second copy of the summary" "$driver_text" "every failure site"
expect_has "harness: keeps the logs on every way out" "$driver_text" \
    "trap 'collect_failed_suite_logs \"\$LOGDIR\" \"\$RESULTS_FILE\" \"\$SHARD_EXPECT\"' EXIT"

# ── 13. As the harness uses them: sourced, its shell options, an EXIT trap ───
# The trap must never change the status the harness ends with: a trap that
# turned exit 1 into exit 0 would report a red run as green.
cat > "$WORK/as_harness.sh" <<'EOF'
set -uo pipefail
source "$1"
LOGDIR="$2"
RESULTS_FILE="$LOGDIR/results.txt"
SHARD_EXPECT="$LOGDIR/suites-shard.txt"
trap 'collect_failed_suite_logs "$LOGDIR" "$RESULTS_FILE" "$SHARD_EXPECT"' EXIT
suite_failure_summary red_assert "$LOGDIR/red_assert.log"
exit "$3"
EOF
for status in 7 1 0; do
    rm -rf "$WORK/logs/failed"
    bash "$WORK/as_harness.sh" "$HELPER" "$WORK/logs" "$status" > "$WORK/as_harness.out" 2>&1
    got=$?
    cases=$((cases + 1))
    if [ "$got" -ne "$status" ]; then
        fail "EXIT trap: the harness status $status came back as $got"
    fi
    cases=$((cases + 1))
    if [ ! -f "$WORK/logs/failed/red_abort.log" ]; then
        fail "EXIT trap: the failing logs were not kept on exit $status"
    fi
done
expect_has "sourced: the summary prints as it does when executed" "$(cat "$WORK/as_harness.out")" \
    "──── red_assert: every failure site ────"

if [ "$failures" -gt 0 ]; then
    echo "suite failure-report contract VIOLATED: $failures of $cases check(s)" >&2
    exit 1
fi
echo "suite failure-report contract passed ($cases checks: sanitizer reports, running test, unchanged FAIL output, kept logs, result line, exit status)"
