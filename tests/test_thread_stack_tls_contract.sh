#!/usr/bin/env bash
# Static TLS must fit inside the SMALLEST stack any thread asks for.
#
# Why this exists: glibc places a thread's static TLS block inside the stack
# allocation it makes for that thread. So growing a thread-local — anywhere in
# the image, in a file that has nothing to do with threads — silently shrinks
# the usable stack of every thread, and once the block no longer fits the
# smallest requested stack, pthread_create returns EINVAL for that thread and
# ONLY that thread.
#
# What that looked like in practice (PR #2233): ~45 KB of new thread-local
# caches (a 512-slot field cache, per-depth tree cursors and two parked arenas —
# a CBMArena is ~4 KB because it carries blocks[256] + block_sizes[256]) pushed
# static TLS from 47.9 KB to 93.5 KB. The 64 KB parent-death watchdog thread
# then could not start, the index worker correctly refused to run without
# process-tree containment and SIGKILLed its own group, and every venue reported
# the same uninformative "index worker ended with killed (signal 9)". Local
# macOS and arm64 Linux legs were fully green throughout: nothing on the ladder
# built x86-64, and the arm64 TLS layout stayed just under the line.
#
# The gate: PT_TLS MemSiz of the built product binary must stay a safe margin
# below the smallest stack size the code requests for a thread. That is an ELF
# property, so the check is Linux-only; the macOS host leg skips with a reason
# (Mach-O has no PT_TLS, and its TLS is not carved from the thread stack).
#
# Usage: tests/test_thread_stack_tls_contract.sh [binary]

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="${1:-${CBM_TEST_BINARY:-}}"

if [ -z "$BINARY" ] || [ ! -f "$BINARY" ]; then
    echo "FAIL: no product binary to inspect (pass one, or set CBM_TEST_BINARY)" >&2
    exit 1
fi

case "$(uname -s)" in
Linux) ;;
*)
    echo "SKIP: static-TLS budget is an ELF/glibc property (Mach-O keeps TLS off" \
        "the thread stack) — the Linux legs own this contract"
    exit 0
    ;;
esac

if ! command -v readelf >/dev/null 2>&1; then
    echo "SKIP: readelf not available to read PT_TLS"
    exit 0
fi

# The smallest stack the code asks for, read from the source rather than
# duplicated here: a future smaller stack must tighten this gate automatically.
smallest_kb=$(grep -ho '[A-Z_]*STACK_SIZE = [0-9]* \* CBM_SZ_1K' "$ROOT"/src/*.c "$ROOT"/src/*/*.c |
    sed 's/.* = \([0-9]*\) \* CBM_SZ_1K/\1/' | sort -n | head -1)
if [ -z "$smallest_kb" ]; then
    echo "FAIL: could not find any <NAME>_STACK_SIZE in the sources — the gate lost its input" >&2
    exit 1
fi
smallest=$((smallest_kb * 1024))

tls_hex=$(readelf -lW "$BINARY" | awk '$1 == "TLS" { print $6 }' | head -1)
if [ -z "$tls_hex" ]; then
    echo "SKIP: binary has no PT_TLS segment (nothing to bound)"
    exit 0
fi
tls=$((tls_hex))

# The thread descriptor and guard share that allocation too, so the budget is
# not the whole stack. A quarter of it is reserve enough to notice growth long
# before a thread stops being creatable.
budget=$((smallest - smallest / 4))

echo "static TLS ${tls} bytes; smallest requested thread stack ${smallest} bytes; budget ${budget}"
if [ "$tls" -ge "$budget" ]; then
    echo "FAIL: static TLS (${tls} B) is within a quarter of the smallest thread stack" \
        "(${smallest} B). glibc takes the TLS block out of that stack, so this is how" \
        "a thread silently stops being creatable — move large thread-locals to the" \
        "heap behind a thread-local POINTER (see cbm.c's field cache/cursor pool)." >&2
    exit 1
fi
echo "Thread-stack TLS contract passed"
