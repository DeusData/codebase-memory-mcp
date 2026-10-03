#!/usr/bin/env bash
# suite-failure-report.sh — what a red suite's log has to say, and which logs
# to keep. The ONE implementation behind the parallel harness's end-of-run
# failure summary (scripts/run-tests-parallel.sh sources this file), so every
# venue that reaches the harness through scripts/test.sh prints the same thing:
# hosted CI, the Linux container leg and the Windows VM leg.
#
# Why it is a file of its own: tests/test_failure_report_contract.sh drives
# these functions with fabricated suite logs. Inline in the harness they could
# only be exercised by a real red run, which is how the gap below went unseen.
#
# The gap: the summary used to grep a failing suite's log for "FAIL" and then
# show its last 15 lines. A sanitizer report contains no "FAIL", and its useful
# part (error kind, access size, top frames) is at the START of the report,
# while the last 15 lines of an AddressSanitizer abort are the shadow-byte
# legend. A Windows sanitizer job therefore went red with an empty "every
# failure site" section and a legend, and the cause had to be found by reading
# code. A trap-mode UBSan build (Windows on ARM) is worse still: it dies on an
# illegal instruction and prints nothing at all, so the only evidence in the
# log is which test was running.

suite_failure_report_usage() {
    cat <<'EOF'
Usage: scripts/suite-failure-report.sh summary <suite> <suite-log>
       scripts/suite-failure-report.sh collect <log-dir> <results-file> <slice-file>

Internal helper of scripts/run-tests-parallel.sh (reached through
scripts/test.sh); the two modes exist so the contract test can drive the real
functions with fabricated logs.

  summary  Print the failure summary of one suite log:
             1. every "FAIL" site with context (at most 120 lines);
             2. the sanitizer / crash report, only when the log holds one:
                the test that was running, the first sanitizer report up to
                its own SUMMARY line (at most 40 lines), every further
                "runtime error:" / sanitizer header / SUMMARY line (at most
                10), and signal or timeout lines (at most 5);
             3. the last 15 lines.
  collect  Copy the log of every suite of <slice-file> that did not record
           rc=0 in <results-file> into <log-dir>/failed/, together with the
           results file. Creates nothing when every suite is green. CI uploads
           that directory when a test job fails.

Exit codes: 0 success · 2 usage.
EOF
}

# The sanitizer / crash section of the summary, header included. Prints
# NOTHING for a log that has no sanitizer report, no signal line and a
# completion summary, so an ordinary assertion failure reads exactly as it did
# before this section existed.
#
# awk rather than grep because three things have to be known together: where
# the first report starts, which test line precedes it, and where the report
# stops being useful. Portable awk only (BSD awk on macOS, mawk on Ubuntu,
# gawk under MSYS2): no interval expressions, no character classes, no gensub.
#
# The 40-line cap: a report normally ends before it, at its own SUMMARY line
# (error line, access line, the faulting stack, then what the address belongs
# to). The cap is for the report that does not end soon — a use-after-free
# carries three stacks, a recursion overflow hundreds of frames — and 40 lines
# keep the error line, the access line and the top of the faulting stack, which
# is the part that names the code. It is a third of the 120 lines the
# failure-site section may print, so one suite's whole summary stays under 200
# lines. A SUMMARY line beyond the cut is still printed, under "further
# sanitizer lines".
suite_crash_report() {
    local suite="$1" log="$2"
    [ -r "$log" ] || return 0
    awk -v header="──── $suite: sanitizer / crash report ────" \
        -v block_max=40 -v other_max=10 -v signal_max=5 '
    # A test line is what RUN_TEST prints: two spaces, then the test name
    # left-justified in 55 columns ("  %-55s"), flushed BEFORE the test body
    # runs. That flush is why the last test line ahead of a report names the
    # test that was running: nothing the test itself buffers can precede it.
    function test_name(s,    head, rest) {
        if (substr(s, 1, 2) != "  ") return ""
        head = substr(s, 3, 55)
        if (length(head) < 55) return ""
        if (head !~ /^[A-Za-z_][A-Za-z0-9_]* *$/) return ""
        if (head ~ / $/) {
            sub(/ +$/, "", head)
            return head
        }
        # A name of 55 characters or more runs straight into what follows it.
        rest = substr(s, 3)
        match(rest, /^[A-Za-z_][A-Za-z0-9_]*/)
        head = substr(rest, 1, RLENGTH)
        sub(/(PASS|SKIP)$/, "", head)
        return head
    }
    {
        line = $0
        sub(/\r$/, "", line) # Windows suite logs are CRLF
        if (line ~ /^  [0-9]+ passed/) has_summary = 1
        is_header = (line ~ /ERROR: (Address|Leak|Thread|Memory|UndefinedBehavior)Sanitizer/ || line ~ /WARNING: (Thread|Memory)Sanitizer/)
        is_start = (is_header || line ~ /runtime error:/)
        captured = 0

        if (!started) {
            name = test_name(line)
            if (name != "") {
                current = name
                current_done = 0
            }
            # The start line never finishes a test: a diagnostic appended to
            # the name line is printed while that test is still running.
            if (!is_start && current != "" && (line ~ /PASS$/ || line ~ /SKIP \(/ || line ~ /FAIL /))
                current_done = 1
        }

        if (is_start && !started) {
            started = 1
            # A recoverable UBSan diagnostic is one line plus, at most, its
            # stack; every other report runs to its own SUMMARY line.
            ubsan_line = !is_header
            block[++block_count] = line
            captured = 1
        } else if (started && !closed) {
            if (ubsan_line) {
                if (line ~ /^[ \t]+#[0-9]+ / || line ~ /SUMMARY: UndefinedBehaviorSanitizer/) {
                    block[++block_count] = line
                    captured = 1
                } else {
                    closed = 1
                }
            } else if (line ~ /^Shadow bytes around/ || line ~ /==ABORTING/) {
                closed = 1 # the shadow dump and legend explain nothing
            } else {
                block[++block_count] = line
                captured = 1
                if (line ~ /SUMMARY: [A-Za-z]*Sanitizer/) closed = 1
            }
            if (!closed && block_count >= block_max) {
                closed = 1
                cut = 1
            }
        }

        if (!captured) {
            if (is_start || line ~ /SUMMARY: [A-Za-z]*Sanitizer/) {
                other_total++
                if (other_total <= other_max) other[other_total] = line
            } else if (line ~ /Segmentation fault|Abort trap|killed by signal|timed out/ && line !~ /FAIL/) {
                # FAIL lines are already shown by the failure-site section.
                signal_total++
                if (signal_total <= signal_max) signals[signal_total] = line
            }
        }
    }
    END {
        died_silently = (!started && !has_summary && current != "")
        if (!started && other_total == 0 && signal_total == 0 && !died_silently) exit 0
        print header
        if (started && current == "")
            print "running test: none (the report precedes the first test line)"
        else if (started && current_done)
            print "last finished test: " current " (the report comes after it)"
        else if (started)
            print "running test: " current
        else if (died_silently && current_done)
            print "last finished test: " current " (the log ends without a completion summary)"
        else if (died_silently)
            print "running test: " current " (the log ends without a completion summary)"
        for (i = 1; i <= block_count; i++) print block[i]
        if (cut) print "[report cut at " block_max " lines; the suite log has the rest]"
        if (other_total > 0) {
            print "further sanitizer lines:"
            for (i = 1; i <= other_total && i <= other_max; i++) print other[i]
            if (other_total > other_max) print "[" (other_total - other_max) " more not shown]"
        }
        if (signal_total > 0) {
            print "signal / timeout lines:"
            for (i = 1; i <= signal_total && i <= signal_max; i++) print signals[i]
            if (signal_total > signal_max) print "[" (signal_total - signal_max) " more not shown]"
        }
    }' "$log"
}

# The failure summary of one suite. Sections 1 and 3 are byte-for-byte what
# the harness printed before; section 2 appears only when it has content.
suite_failure_summary() {
    local suite="$1" log="$2"
    echo "──── $suite: every failure site ────"
    grep -B2 -A8 "FAIL" "$log" | head -120
    suite_crash_report "$suite" "$log"
    echo "──── $suite: last 15 lines ────"
    tail -15 "$log"
    return 0
}

# Keep the logs a red run needs: every suite of this shard's slice that did
# not record rc=0 — failed, crashed, timed out, or still in flight when the
# scheduler itself gave up (a log but no result line). One awk decides the
# set, so a green run costs a single process and creates no directory.
collect_failed_suite_logs() {
    local logdir="$1" results="$2" slice="$3"
    local dest="$logdir/failed" suite kept=0
    # An existing directory only: an empty or wrong <log-dir> must never turn
    # the rm below into a removal somewhere else.
    [ -d "$logdir" ] || return 0
    rm -rf "$dest"
    if [ ! -f "$results" ] || [ ! -f "$slice" ]; then
        return 0
    fi
    while IFS= read -r suite; do
        [ -f "$logdir/$suite.log" ] || continue
        mkdir -p "$dest" && cp "$logdir/$suite.log" "$dest/$suite.log" && kept=$((kept + 1))
    done < <(awk 'FILENAME == ARGV[1] { if ($2 == "rc=0") green[$1] = 1; next }
                  NF && !($1 in green) { print $1 }' "$results" "$slice")
    if [ "$kept" -gt 0 ]; then
        cp "$results" "$dest/results.txt"
        echo "failing-suite logs kept in $dest ($kept suite log(s) + results.txt)"
    fi
    return 0
}

# Executed (not sourced): the contract test's entry.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    set -uo pipefail
    case "${1:-}" in
    -h | --help)
        suite_failure_report_usage
        exit 0
        ;;
    summary)
        [ $# -eq 3 ] || { echo "suite-failure-report: summary needs <suite> <suite-log>. Please consult --help." >&2; exit 2; }
        suite_failure_summary "$2" "$3"
        ;;
    collect)
        [ $# -eq 4 ] || { echo "suite-failure-report: collect needs <log-dir> <results-file> <slice-file>. Please consult --help." >&2; exit 2; }
        collect_failed_suite_logs "$2" "$3" "$4"
        ;;
    *)
        echo "suite-failure-report: unknown mode '${1:-}'. Please consult --help." >&2
        exit 2
        ;;
    esac
fi
