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

# Windows: shape the build directory like a real user checkout BEFORE the
# suites run. MSYS2's Cygwin layer writes POSIX-emulating DACLs — including
# CREATOR OWNER (S-1-3-0) mutation grants — onto directories its tools
# touch, and workspace drive roots additionally inherit Authenticated-Users
# Modify; the activation transaction's source-directory policy correctly
# refuses both, which would fail the install-flow tests on the environment,
# not the code. Stamping must happen AFTER the build (the builders are the
# ones re-writing the DACL), so it lives here rather than in workflow
# setup. Two idempotent steps: protect the DIRECTORY (inheritance flags are
# directory-only — a /T re-root leaves files with empty deny-all DACLs),
# then /reset the children to re-inherit the clean set.
stamp_windows_build_dir() {
    local when="$1"
    case "$(uname -s 2>/dev/null)" in
    MINGW* | MSYS*) ;;
    *) return 0 ;;
    esac
    local runner_dir_w me norm_out stamp_out reset_out
    runner_dir_w="$(cygpath -w "$(dirname "$RUNNER")")"
    me="$(whoami | tr -d '\r')"
    # Normalize FIRST: some runner images stamp EXPLICIT (non-inherited)
    # Authenticated-Users ACEs onto the workspace tree, which /inheritance:r
    # cannot strip and /grant:r does not touch (it replaces only the granted
    # SIDs' own entries). /reset drops every explicit ACE and restores pure
    # inheritance, so the protect-and-grant below starts from a known shape
    # regardless of image provisioning.
    norm_out=$(MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" /reset /Q 2>&1) ||
        echo "WARN: build-dir DACL normalize ($when) failed: $norm_out"
    stamp_out=$(MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" /inheritance:r \
        /grant:r "${me}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' \
        /Q 2>&1) || echo "WARN: build-dir DACL stamp ($when) failed (user=$me dir=$runner_dir_w): $stamp_out"
    reset_out=$(MSYS2_ARG_CONV_EXCL='*' icacls "${runner_dir_w}\\*" /reset /T /C /Q 2>&1) ||
        echo "WARN: build-dir child DACL reset ($when) failed: $(printf '%s' "$reset_out" | tail -2)"
    # The stamp is load-bearing for the install-flow suites: verify it and say
    # so, in either direction — a silent stamp once cost a full CI round to
    # even see WHETHER it had run.
    if MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" 2>/dev/null |
        grep -qE 'Authenticated Users|CREATOR OWNER'; then
        echo "FAIL: build-dir DACL still grants cross-account mutation after $when stamp:"
        MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" 2>&1 | head -8
        exit 1
    else
        echo "build-dir DACL stamped clean ($when, $runner_dir_w, user=$me)"
    fi
}
stamp_windows_build_dir pre-wave

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

# CBM_TEST_SHARD="i/N" runs this invocation's deterministic slice of the
# suite list so CI can spread one platform's suites across N runner jobs.
# Unset (or "1/1") selects everything — the sharding path is inert unless a
# workflow opts in. The slice is a pure function of (--list-suites, i, N):
# every shard recomputes the same assignment, so N jobs with indices 1..N
# cover the full list by construction, and each job's union guard below
# proves it ran exactly its slice.
SHARD_INDEX=1
SHARD_TOTAL=1
if [ -n "${CBM_TEST_SHARD:-}" ]; then
    if ! printf '%s' "$CBM_TEST_SHARD" | grep -qE '^[0-9]+/[0-9]+$'; then
        echo "FAIL: CBM_TEST_SHARD must be i/N, got '$CBM_TEST_SHARD'" >&2
        exit 1
    fi
    SHARD_INDEX="${CBM_TEST_SHARD%%/*}"
    SHARD_TOTAL="${CBM_TEST_SHARD##*/}"
    if [ "$SHARD_TOTAL" -lt 1 ] || [ "$SHARD_INDEX" -lt 1 ] ||
        [ "$SHARD_INDEX" -gt "$SHARD_TOTAL" ]; then
        echo "FAIL: CBM_TEST_SHARD out of range: $CBM_TEST_SHARD" >&2
        exit 1
    fi
fi

# Deal the known-heavy suites round-robin FIRST so no shard receives a
# second heavy suite before every shard holds one — shard wall time is
# bounded by its heaviest member, and naive modulo can stack store_arch and
# daemon_runtime (the two slowest sanitized suites) onto one job.
shard_filter() {
    awk -v idx="$SHARD_INDEX" -v total="$SHARD_TOTAL" '
        BEGIN {
            nh = split("store_arch daemon_runtime incremental cli extraction " \
                       "watcher daemon_ipc subprocess httpd py_lsp_stress " \
                       "grammar_regression mcp daemon_frontend pipeline", h, " ")
        }
        { present[$0] = NR; lines[NR] = $0 }
        END {
            # Deal heavies in WEIGHT order (the static list above), not file
            # order: the point is that the two slowest suites land on
            # different shards, which file-order dealing does not guarantee.
            n = 0
            for (i = 1; i <= nh; i++)
                if (h[i] in present) { order[++n] = h[i]; taken[h[i]] = 1 }
            for (i = 1; i <= NR; i++)
                if (!(lines[i] in taken)) order[++n] = lines[i]
            for (i = 1; i <= n; i++) if ((i - 1) % total == idx - 1) print order[i]
        }
    '
}
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
# The daemon-family suites spawn coordinated worker subprocesses (a re-exec
# of this ASan runner plus the full admission handshake) and bind local
# endpoints under fixed readiness deadlines (3 s marker waits in
# index_supervisor); the saturated 3-core macOS CI runners starve those
# deadlines into deterministic failures while an idle machine passes 6/6.
# They also all rendezvous through the shared per-account runtime namespace,
# which the quiet tail keeps free of cross-suite admission traffic.
SERIAL_SUITES="cli subprocess watcher incremental httpd ui index_resilience mcp \
    stack_overflow_a stack_overflow_b stack_overflow_c \
    index_supervisor daemon_application daemon_runtime daemon_frontend \
    daemon_bootstrap daemon_ipc"
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

# The parallel wave and the serial tail are sharded separately: every shard
# keeps its own quiet tail for the deadline-sensitive suites (its runner is
# a whole machine, so the tail is at least as quiet as before sharding).
shard_filter < "$PAR_FILE" > "$PAR_FILE.shard" && mv "$PAR_FILE.shard" "$PAR_FILE"
shard_filter < "$SER_FILE" > "$SER_FILE.shard" && mv "$SER_FILE.shard" "$SER_FILE"
SHARD_EXPECT="$LOGDIR/suites-shard.txt"
cat "$PAR_FILE" "$SER_FILE" > "$SHARD_EXPECT"
NSHARD=$(wc -l < "$SHARD_EXPECT" | tr -d ' ')
echo "=== parallel test run: $NSHARD of $NSUITES suites (shard ${SHARD_INDEX}/${SHARD_TOTAL}, $(wc -l < "$SER_FILE" | tr -d ' ') serial-tail), $JOBS jobs ==="

export RUNNER LOGDIR RESULTS_FILE
run_one() {
    s="$1"
    t0=$SECONDS
    # Per-suite wall-clock ceiling so a wedged suite fails LOUDLY instead of
    # blocking the run (and the single local build slot) indefinitely. The
    # `incremental` suite legitimately re-indexes large fixtures (minutes) so
    # it gets a wider ceiling until the template-DB fixture refactor lands;
    # `daemon_runtime` measures ~610s SOLO on arm64 under ASan (10-round
    # loop), so on an overloaded 4-job CI runner the 900s default is a
    # slowness kill, not a hang detector — it joins the slow tier.
    # Uses `timeout` where available (always in the Linux container / CI); on a
    # host without it the suite runs uncapped (no regression vs before).
    case "$s" in
        incremental | store_arch | daemon_runtime) st="${CBM_SUITE_TIMEOUT_SLOW:-3600}" ;;
        *) st="${CBM_SUITE_TIMEOUT:-900}" ;;
    esac
    if command -v timeout >/dev/null 2>&1; then
        timeout --kill-after=15 "$st" "$RUNNER" "$s" > "$LOGDIR/$s.log" 2>&1
        rc=$?
        if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
            echo "  FAIL: suite '$s' exceeded ${st}s wall clock (killed as hung)" \
                >> "$LOGDIR/$s.log"
        fi
    else
        "$RUNNER" "$s" > "$LOGDIR/$s.log" 2>&1
        rc=$?
    fi
    secs=$((SECONDS - t0))
    summary=$(grep -E '^  [0-9]+ passed' "$LOGDIR/$s.log" | tail -1)
    # A suite that exits 0 WITHOUT printing its completion summary ran zero
    # tests as far as anyone can prove (mis-parsed argv, drifted registration
    # macro, early return) — that is a failure, not a green with pass=0.
    if [ "$rc" -eq 0 ] && [ -z "$summary" ]; then
        rc=97
        echo "  FAIL: suite '$s' exited 0 without a completion summary (ran nothing?)" \
            >> "$LOGDIR/$s.log"
    fi
    pass=$(printf '%s' "$summary" | sed -n 's/^  \([0-9]*\) passed.*/\1/p')
    failn=$(printf '%s' "$summary" | sed -n 's/.* \([0-9]*\) failed.*/\1/p')
    skip=$(printf '%s' "$summary" | sed -n 's/.* \([0-9]*\) skipped.*/\1/p')
    # A single short echo line is an atomic append (< PIPE_BUF).
    echo "$s rc=$rc pass=${pass:-0} fail=${failn:-0} skip=${skip:-0} secs=$secs" >> "$RESULTS_FILE"
}
export -f run_one

xargs -P "$JOBS" -I{} bash -c 'run_one "$@"' _ {} < "$PAR_FILE"

# Tail scheduling in two phases. The FLEX suites are timing-shaped but do
# not rendezvous through the shared per-account daemon runtime namespace,
# so a small fixed overlap (CBM_TAIL_JOBS, default 2) is safe and converts
# idle cores into wall time — the old fully-serial tail ran them one at a
# time on an idle machine. The EXCL group (daemon-family plus the suites
# that drive daemon one-shots or supervisor rendezvous) then runs strictly
# sequentially on a machine exactly as quiet as the old tail gave it.
TAIL_EXCL="cli mcp index_supervisor daemon_application daemon_runtime \
    daemon_frontend daemon_bootstrap daemon_ipc"
is_tail_excl() {
    case " $TAIL_EXCL " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}
FLEX_FILE="$LOGDIR/suites-tail-flex.txt"
EXCL_FILE="$LOGDIR/suites-tail-excl.txt"
: > "$FLEX_FILE"
: > "$EXCL_FILE"
while IFS= read -r sname; do
    if is_tail_excl "$sname"; then
        echo "$sname" >> "$EXCL_FILE"
    else
        echo "$sname" >> "$FLEX_FILE"
    fi
done < "$SER_FILE"
# Wave suites spawn Cygwin-family tooling that can rewrite the build
# directory's DACL behind the first stamp (observed: an arm shard whose
# tail held the install-flow suites failed the source-directory policy
# minutes after a clean pre-wave stamp, while its sibling shard passed).
# Re-stamp at the tail boundary so the deadline-sensitive tail — which
# hosts those suites — always starts from the verified-clean shape.
stamp_windows_build_dir pre-tail
xargs -P "${CBM_TAIL_JOBS:-2}" -I{} bash -c 'run_one "$@"' _ {} < "$FLEX_FILE"
while IFS= read -r sname; do
    run_one "$sname"
done < "$EXCL_FILE"

# Machine-checkable manifest for CI's cross-shard completeness job: it
# proves at runtime that the shards of one leg agree on N and on the full
# suite list, and that the union of their slices IS that list — the guard
# against a mis-plumbed CBM_TEST_SHARD (two jobs running the same slice
# passes every per-shard check but silently drops a slice; only a
# cross-shard view catches it).
{
    echo "leg=${CBM_TEST_LEG:-local}"
    echo "shard=${SHARD_INDEX}/${SHARD_TOTAL}"
    echo "list_sha256=$(sort "$SUITES_FILE" | { sha256sum 2>/dev/null || shasum -a 256; } | awk '{print $1}')"
    echo "--- slice ---"
    cat "$SHARD_EXPECT"
} > "$LOGDIR/shard-manifest.txt"

# ── Union guard: every suite in this shard's slice produced exactly one
# result. The slice is deterministic, so N green shard jobs = full coverage;
# a shard that ran anything more, less, or twice fails here. ──
MISSING=$(comm -23 <(sort "$SHARD_EXPECT") <(awk '{print $1}' "$RESULTS_FILE" | sort -u))
EXTRA=$(comm -13 <(sort "$SHARD_EXPECT") <(awk '{print $1}' "$RESULTS_FILE" | sort -u))
DUPES=$(awk '{print $1}' "$RESULTS_FILE" | sort | uniq -d)
if [ -n "$MISSING" ] || [ -n "$EXTRA" ] || [ -n "$DUPES" ]; then
    echo "FAIL: shard union does not match its --list-suites slice (GATE-QUALITY LOSS)" >&2
    [ -n "$MISSING" ] && echo "  never ran: $MISSING" >&2
    [ -n "$EXTRA" ] && echo "  outside slice: $EXTRA" >&2
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
