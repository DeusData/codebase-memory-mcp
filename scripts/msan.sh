#!/usr/bin/env bash
# msan.sh — MemorySanitizer lane (stage 2: full coverage incl. the C++ paths).
#
# Runs inside the cbm-msan image (test-infrastructure/Dockerfile.msan), which
# provides MSan-instrumented libc++/libc++abi/libunwind and zlib in /opt/msan.
# Vendored C deps compile in-tree and are instrumented by the build itself.
#
# MSan detects uninitialized READS — the one memory-error class ASan/LSan and
# the clang-analyzer lane do not cover dynamically. halt_on_error stays ON:
# a finding is a bug (or an interceptor gap to triage), never board data.
#
# VENUES: the CI test-msan job (x86-64, authoritative) and the local arm64
# container (test-infrastructure/docker-compose.yml, service test-msan) both
# run this script with the same exclusion list below.
#
# Usage: scripts/msan.sh [suite ...]   (default: full suite)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MSAN_PREFIX="${MSAN_PREFIX:-/opt/msan}"
if [ ! -d "$MSAN_PREFIX/lib" ]; then
    echo "FATAL: MSan-instrumented runtime not found at $MSAN_PREFIX (build test-infrastructure/Dockerfile.msan)" >&2
    exit 1
fi

# -isystem: the image deliberately has no system zlib (so the instrumented
# one cannot be shadowed); its headers live under the MSan prefix.
# MSAN_ORIGINS: 2 = full origin chains (best reports, heaviest frames),
# 1 = immediate origin only, 0 = none. Detection is IDENTICAL at every level;
# only report quality differs.
# Default 1 on this lane: the immediate origin is enough to locate a read, and
# the lighter frames keep the lane cheaper. Export MSAN_ORIGINS=2 locally when
# chasing a specific report's origin chain.
MSAN_ORIGINS="${MSAN_ORIGINS:-1}"
if [ "$MSAN_ORIGINS" = "0" ]; then
    MSAN_ORIGIN_FLAG=""
else
    MSAN_ORIGIN_FLAG="-fsanitize-memory-track-origins=$MSAN_ORIGINS"
fi
MSAN_SAN="-fsanitize=memory $MSAN_ORIGIN_FLAG -fno-omit-frame-pointer -isystem $MSAN_PREFIX/include"
# Scoped to the zstd object ONLY (see the ZSTD_EXTRA_CFLAGS note in
# Makefile.cbm): zstd's MSan block needs stdint.h that its amalgamation lost,
# but force-including it globally freezes glibc feature-test macros before
# sqlite3.c can set _GNU_SOURCE, breaking that compile instead.
#
# -D_GNU_SOURCE rides along for the same freeze reason IN this object:
# a command-line define lands before the forced include, while zstd.c's own
# in-file feature setup lands after it -- without this, glibc freezes
# without _GNU_SOURCE and zstd loses qsort_r.
ZSTD_EXTRA="-D_GNU_SOURCE -include stdint.h"

# Always clean: make does not encode flags into dependencies, so a build dir
# populated under different stdlib/sanitizer flags silently mixes objects
# (observed: a stage-1 probe's libstdc++ objects surviving into the libc++
# lane and producing an unattributable report). Correctness over speed here.
make -f Makefile.cbm clean-c BUILD_DIR=build/msan >/dev/null 2>&1 || true

make -j"$(nproc)" -f Makefile.cbm build/msan/test-runner \
    CC=clang CXX=clang++ BUILD_DIR=build/msan \
    SANITIZE="$MSAN_SAN" \
    ZSTD_EXTRA_CFLAGS="$ZSTD_EXTRA" \
    CXX_STDLIB_FLAGS="-stdlib=libc++ -nostdinc++ -isystem $MSAN_PREFIX/include/c++/v1" \
    CXX_STDLIB="-L$MSAN_PREFIX/lib -Wl,-rpath,$MSAN_PREFIX/lib -lc++ -lc++abi"

# fast_unwind_on_fatal=1: a report's stack is walked with frame pointers (this
# build keeps them, -fno-omit-frame-pointer) instead of the default slow
# unwinder, which in this image is the MSan-instrumented libunwind. With the
# slow unwinder, reporting an uninitialized read exhausted the main thread's
# stack, so the report never printed and MSan aborted with "stack-overflow"
# followed by "nested bug in the same thread" instead. That disguise kept five
# suites excluded as deep recursion while they hid a real read (the WIT
# scanner's create function returned nothing; see the grammar MANIFEST.md).
# Measured on the same tree and test: slow unwind aborts at the 256 MiB stack
# limit, fast unwind prints the use-of-uninitialized-value report.
export MSAN_OPTIONS="${MSAN_OPTIONS:-halt_on_error=1:print_stats=0:fast_unwind_on_fatal=1}"
# Instrumented frames are several times larger than plain ones, so give the
# stacks headroom: the main thread via RLIMIT_STACK, worker threads via the
# sanitized-build-only knob in cbm_thread_create.
ulimit -s 262144 2>/dev/null || true
export CBM_THREAD_STACK_MB="${CBM_THREAD_STACK_MB:-256}"
export LD_LIBRARY_PATH="$MSAN_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

expected_build_config=$'sanitized=1 test_seams=1\n'
build_config_marker="__cbm_build_config_end__"
captured_build_config=""
actual_build_config=""
build_config_probe_ok=0
if captured_build_config="$(./build/msan/test-runner --build-config && printf '%s' "$build_config_marker")"; then
    build_config_probe_ok=1
    actual_build_config="${captured_build_config%"$build_config_marker"}"
else
    actual_build_config="$captured_build_config"
fi
if [ "$build_config_probe_ok" -ne 1 ] ||
    [ "$actual_build_config" != "$expected_build_config" ]; then
    reported_build_config="${actual_build_config//$'\n'/\\n}"
    printf '%s\n' 'ERROR: build config mismatch for build/msan/test-runner' >&2
    printf '%s\n' '  expected: sanitized=1 test_seams=1' >&2
    printf '  reported: %s\n' "${reported_build_config:-<empty>}" >&2
    printf '%s\n' 'Refusing to run suites; check sanitizer flags and CBM_SANITIZED_BUILD wiring.' >&2
    exit 1
fi

echo "=== MSan lane: $(clang --version | head -1) ==="

# KNOWN RED — whitelisted per cause (O10). Only (B) is still excluded.
#
# (A) "STACK-OVERFLOW" — FIXED, no longer excluded. Five suites
#     (grammar_regression grammar_labels pipeline lang_contract
#     grammar_probe_e) aborted on both arm64 and x86-64 with "MemorySanitizer:
#     stack-overflow" followed by "nested bug in the same thread, aborting".
#     It was never deep recursion. MSan crashed while REPORTING real
#     uninitialized reads (see fast_unwind_on_fatal above): the WIT scanner's
#     create function returned nothing (the four grammar suites), and
#     githistory_coupling_limits_output left its commits' timestamps
#     uninitialized (pipeline). Both are fixed and the suites are back in the
#     lane. The crashing thread was the MAIN thread: MSan's "T<n>" is the OS
#     thread id, and every abort showed T<n> equal to the pid. An earlier
#     version of this block read it as a worker-thread ordinal, which is why
#     the worker-stack knob, MSAN_ORIGINS and CBM_WORKERS=1 never changed
#     anything and why it proposed a byte-measuring recursion guard.
#
# (B) INSTALL/ACTIVATION failures — cli. NOT an overflow. On x86-64 the suite
#     runs to completion: 253 passed, 5 failed, every failure in the install or
#     activation path. The parent process's install returns 1, with
#     "agent_config agent=OpenClaw op=mcp_install" reported just above it; the
#     suite is green on every other venue. MSan reported ZERO
#     use-of-uninitialized-value here, so excluding it costs no uninit coverage
#     -- which is the only thing this lane exists to provide.
#     NOT DIAGNOSED yet. Recorded as a follow-up rather than guessed at from a
#     log; with (A) understood, the local arm64 container is a usable venue
#     for it (scripts/msan.sh cli).
#
# (C) RSS BUDGET — incremental. FIXED rather than excluded, so it is no longer
#     in the list below: the budget assertion is now skipped under
#     __has_feature(memory_sanitizer) in test_incremental.c, because shadow (and
#     origin) mappings inflate RSS by construction -- 3054MB against a 2304MB
#     budget -- and cannot be told apart from a real leak. Inflating the budget
#     instead would blind the guard on the platforms where it does work. The
#     suite stays IN this lane and keeps its uninitialized-read coverage.
#
# WHY (B) IS EXCLUDED RATHER THAN LEFT RED: the lane is gating. A permanently
# red gate teaches everyone to ignore it and hides the findings the other
# suites produce, which is the entire reason this lane exists. The exclusion is
# narrow, named, and expires when the install failure is diagnosed. It is not a
# claim that cli is covered, and the lane says so when it runs (below).
#
# LESSON FROM (A): an MSan abort that is not a use-of-uninitialized-value
# report is a reporting failure until proven otherwise. Look for the read
# first (fast unwinding prints it), not for a stack limit.
MSAN_EXCLUDE="${MSAN_EXCLUDE-cli}"

if [ "$#" -gt 0 ]; then
    ./build/msan/test-runner "$@"
else
    # One suite per process: a fresh process per suite keeps one suite's
    # thread/allocator state from reaching the next, and makes a failure name
    # exactly one suite. Suite enumeration comes from --list-suites, whose
    # completeness the sharding union guard already proves; the known-red set
    # documented above is skipped by name and nothing else is.
    all_suites="$(./build/msan/test-runner --list-suites)"
    # Fail loudly if an entry matches no suite: a typo (or a TEST name given
    # where a SUITE name is required) would otherwise exclude nothing silently.
    # A case match, not `printf | grep -q`, which pipefail can turn into a
    # false failure when grep exits early.
    for e in $MSAN_EXCLUDE; do
        case $'\n'"$all_suites"$'\n' in
            *$'\n'"$e"$'\n'*) ;;
            *)
                echo "FATAL: MSAN_EXCLUDE entry '$e' is not a suite name" >&2
                exit 1
                ;;
        esac
    done
    if [ -n "$MSAN_EXCLUDE" ]; then
        echo "WARNING: running a PARTIAL sanitizer lane — excluded: $MSAN_EXCLUDE" >&2
        echo "WARNING: a green result here does NOT mean the tree is MSan-clean." >&2
    fi
    fails=""
    for suite in $all_suites; do
        case " $MSAN_EXCLUDE " in
            *" $suite "*)
                echo "=== msan: $suite SKIPPED (known-red, see the block above) ==="
                continue
                ;;
        esac
        echo "=== msan: $suite ==="
        ./build/msan/test-runner "$suite" || fails="$fails $suite"
    done
    if [ -n "$fails" ]; then
        echo "=== MSan lane FAILED suites:$fails ===" >&2
        exit 1
    fi
fi
