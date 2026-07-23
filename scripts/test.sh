#!/usr/bin/env bash
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
# shellcheck source=path-safety.sh
source "$ROOT/scripts/path-safety.sh"

# Forward CC/CXX and collect make-passthrough args. BUILD_DIR is honored for
# the explicit target path below so containerized legs can build in their own
# directory instead of clobbering the host's native build/c artifacts.
# MAKE_ARGS is an ARRAY so a VAR=VAL whose value contains spaces (the
# windows-11-arm leg passes SANITIZE with four flags) survives as ONE make
# argument. The old string accumulation re-split it at every expansion and
# make swallowed the second flag's leading -f as its makefile option.
MAKE_ARGS=()
BUILD_DIR="build/c"
for arg in "$@"; do
    case "$arg" in
        CC=*|CXX=*) export "${arg}" ;;
        --arch|--arch=*) ;; # already handled
        arm64|x86_64) ;; # already handled
        BUILD_DIR=*) BUILD_DIR="${arg#BUILD_DIR=}"; MAKE_ARGS+=("$arg") ;;
        *=*) MAKE_ARGS+=("$arg") ;; # forward any VAR=VAL to make
    esac
done

print_env "test.sh"

# Step 0: fast build/security harness regressions run before the compiler-heavy
# suite. The Windows package surface is static here; native launcher behavior is
# exercised by scripts/test-windows.ps1.
echo "=== Step 0a: build directory safety contract ==="
bash "$ROOT/tests/test_build_dir_safety.sh"

echo "=== Step 0b: Windows VM worktree sync contract ==="
bash "$ROOT/tests/test_vm_worktree_manifest.sh"

echo "=== Step 0c: UI development proxy security contract ==="
bash "$ROOT/tests/test_ui_dev_proxy_security.sh"

echo "=== Step 0d: daemon soak recovery contract ==="
bash "$ROOT/tests/test_soak_daemon_recovery_contract.sh"

echo "=== Step 0e: Windows launcher bundle contract ==="
bash "$ROOT/tests/test_windows_bundle_contract.sh"

echo "=== Step 0f: tree-sitter runtime Makefile dependencies ==="
bash "$ROOT/tests/test_makefile_ts_runtime_dependencies.sh"

echo "=== Step 0g: security fuzz harness self-test ==="
bash "$ROOT/tests/test_security_fuzz_harness.sh"

# Verify compiler supports target arch
verify_compiler "$CC"

# Step 1: Clean (scoped to this leg's build directory)
BUILD_DIR="$BUILD_DIR" scripts/clean.sh

# Step 2 + 3: Build, then run every suite as parallel processes (identical
# gate quality — see the ZERO-LOSS CONTRACT in scripts/run-tests-parallel.sh:
# the suite set is enumerated from the runner itself and union-guarded, and
# pass/fail/skip totals aggregate to the same numbers as the sequential run).
# CBM_TEST_SEQUENTIAL=1 restores the single-process runner.
make -j"$NPROC" -f Makefile.cbm "$BUILD_DIR/test-runner" ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
if [ "${CBM_TEST_SEQUENTIAL:-0}" = "1" ]; then
    make -f Makefile.cbm test ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
else
    make -f Makefile.cbm test-par ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
fi

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
make -j"$NPROC" -f Makefile.cbm cbm ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
bash "$ROOT/tests/test_parent_watchdog.sh"

# Step 5b: worker-mode parent-death watchdog (#845). A supervised index worker
# (`cli --index-worker …`) whose supervisor dies must self-exit instead of
# indexing on as an orphan. Reuses the prod binary built in Step 5.
echo "=== Step 5b: worker-mode watchdog regression (#845) ==="
bash "$ROOT/tests/test_worker_watchdog.sh"

# Step 6: security-strings URL allow-list regression. The MSYS2 CLANG64 toolchain
# bakes its package-tracker URL into the static Windows .exe; the binary string
# audit must allow-list it (Windows-only — Linux smoke never saw it).
echo "=== Step 6: security-strings allow-list regression ==="
bash "$ROOT/tests/test_security_strings_allowlist.sh"

echo "=== All tests passed ==="
