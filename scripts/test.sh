#!/bin/bash
# test.sh — Clean build + run all C tests with ASan + UBSan.
#
# Usage:
#   scripts/test.sh                          # Auto-detect everything
#   scripts/test.sh --arch x86_64            # Force x86_64 build
#   scripts/test.sh CC=gcc-14 CXX=g++-14    # Override compiler
#
# This script is the SINGLE source of truth for running tests.
# Used identically in local development and CI workflows.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Parse --arch flag before sourcing env.sh
for arg in "$@"; do
    case "$arg" in
        --arch) :;; # next arg is the value, handled below
        arm64|x86_64)
            # Check if previous arg was --arch
            if [[ "${prev_arg:-}" == "--arch" ]]; then
                export CBM_ARCH="$arg"
            fi
            ;;
    esac
    prev_arg="$arg"
done

# Also support --arch=value
for arg in "$@"; do
    case "$arg" in
        --arch=*) export CBM_ARCH="${arg#--arch=}" ;;
    esac
done

# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

# Forward CC/CXX and collect make-passthrough args
MAKE_ARGS=""
for arg in "$@"; do
    case "$arg" in
        CC=*|CXX=*) export "${arg}" ;;
        --arch|--arch=*) ;; # already handled
        arm64|x86_64) ;; # already handled
        *=*) MAKE_ARGS="$MAKE_ARGS $arg" ;; # forward any VAR=VAL to make
    esac
done

print_env "test.sh"

# Verify compiler supports target arch
verify_compiler "$CC"

# Step 1: Clean
scripts/clean.sh

# Step 2 + 3: Build and run tests (Makefile applies $ARCHFLAGS on macOS)
make -j"$NPROC" -f Makefile.cbm test $MAKE_ARGS

# Step 4: C++ large-TU index-hang regression guard (#410). Runs the PROD binary
# in a subprocess with a wall-clock timeout — a hang must fail, not block the run.
# Opt-in via CBM_RUN_HANG_TEST=1 (it needs the prod binary, which the ASan unit
# run above does not build). Skipped by default so the fast unit run stays fast.
if [ "${CBM_RUN_HANG_TEST:-0}" = "1" ]; then
    echo "=== Step 4: C++ index-hang regression (#410) ==="
    bash "$ROOT/tests/test_cpp_index_hang.sh"
fi

# Step 5: Parent-death watchdog regression (#406/#407). Builds the prod stdio
# binary and verifies it self-exits when its launching parent is killed.
echo "=== Step 5: parent-death watchdog regression (#406/#407) ==="
make -j"$NPROC" -f Makefile.cbm cbm $MAKE_ARGS
bash "$ROOT/tests/test_parent_watchdog.sh"

# Step 6: security-strings URL allow-list regression. The MSYS2 CLANG64 toolchain
# bakes its package-tracker URL into the static Windows .exe; the binary string
# audit must allow-list it (Windows-only — Linux smoke never saw it).
echo "=== Step 6: security-strings allow-list regression ==="
bash "$ROOT/tests/test_security_strings_allowlist.sh"

echo "=== All tests passed ==="
