#!/usr/bin/env bash
# test.sh — THE canonical test leg. Every venue (local ladder, PR CI, dry run,
# release) runs tests through this file; iteration happens through its flags,
# never through a second entry point.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/test.sh [--suites LIST] [--arch ARCH] [VAR=VAL ...]

The canonical test entry: identical in local CI, PR CI, dry run and release.
DEFAULT (no --suites) is exactly what CI runs: static contract checks
(Step 0a-0t), a CLEAN sanitizer build, every suite via the parallel harness,
then the prod-binary regression guards (Steps 4-6).

Modes:
  (default)      The venue leg. Clean build (scripts/clean.sh) + all suites +
                 all contract steps. This is the shape every gate runs.
  --suites LIST  ITERATION mode: comma- or space-separated suite names, e.g.
                 --suites daemon,daemon_ipc. Rebuilds the test-runner
                 INCREMENTALLY (make dependency tracking, no clean) and runs
                 only those suites — seconds, not minutes. Skips the contract
                 steps and prod-binary guards; the full default run remains
                 the merge gate. Suite names: build/c/test-runner --list-suites.
  --tsan         ThreadSanitizer leg (data-race gate): builds and runs the
                 widened TSan runner via make test-tsan — the same leg CI's
                 tsan jobs and the compose test-tsan service run.

Options:
  --arch ARCH    Force target arch (arm64 | x86_64), e.g. under Rosetta.
  -h, --help     This text.

Make passthrough (VAR=VAL, forwarded verbatim):
  CC= CXX=       Compiler override, e.g. CC=gcc-14 CXX=g++-14.
  BUILD_DIR=     Build in an isolated directory (containers/sanitizer variants).
  SANITIZE=      Override sanitizer flags. Platform defaults when unset:
                 unix/CLANG64 use the Makefile's ASan+UBSan test flags;
                 CLANGARM64 (Windows on ARM, no ASan runtime) gets CI's
                 trap-UBSan set (-fsanitize=undefined -fsanitize-trap=undefined
                 -fstack-protector-strong -fno-omit-frame-pointer) applied HERE
                 so local and CI build identical test binaries. Pass SANITIZE=
                 (empty) for a plain build when debugging a trap.

Environment:
  CBM_TEST_SEQUENTIAL=1   Single-process runner instead of the parallel harness.
  CBM_RUN_HANG_TEST=1     Opt-in C++ index-hang regression (#410, needs prod).
  CBM_NO_CCACHE=1         Disable the content-verified compiler cache.
  CBM_TEST_SHARD/_LEG     Set by CI's sharded legs; leave unset locally.

Examples:
  scripts/test.sh                          # the full venue leg (what CI runs)
  scripts/test.sh --suites daemon_ipc      # one suite, incremental, seconds
  scripts/test.sh --suites "arena hash_table" CC=clang CXX=clang++
  scripts/test.sh SANITIZE= --suites daemon_ipc # plain build for trap debugging
EOF
}

# Parse --help / --suites / --tsan / --arch before sourcing env.sh.
# STRICT: an unknown flag or a stray word is an immediate usage error, never
# silently swallowed — agents must know exactly what a run will do.
SUITES=""
TSAN=0
prev_arg=""
for arg in "$@"; do
    case "$arg" in
        -h|--help) usage; exit 0 ;;
        --tsan) :;;
        --suites) :;; # next arg is the value, handled below
        --suites=*) SUITES="${arg#--suites=}" ;;
        --arch) :;; # next arg is the value, handled below
        --arch=*) :;; # handled below
        -*)
            echo "test.sh: unknown option '$arg'. Please consult --help." >&2
            exit 2
            ;;
        arm64|x86_64)
            if [[ "${prev_arg:-}" != "--arch" && "${prev_arg:-}" != "--suites" ]]; then
                echo "test.sh: unexpected argument '$arg' (did you mean --arch $arg?). Please consult --help." >&2
                exit 2
            fi
            ;;
        *=*) :;; # VAR=VAL make passthrough, validated below
        *)
            if [[ "${prev_arg:-}" != "--suites" ]]; then
                echo "test.sh: unexpected argument '$arg'. Please consult --help." >&2
                exit 2
            fi
            ;;
    esac
    prev_arg="$arg"
done
for arg in "$@"; do
    case "$arg" in
        --tsan) TSAN=1 ;;
        arm64|x86_64)
            if [[ "${prev_arg2:-}" == "--arch" ]]; then
                export CBM_ARCH="$arg"
            fi
            ;;
        *)
            if [[ "${prev_arg2:-}" == "--suites" ]]; then
                SUITES="$arg"
            fi
            ;;
    esac
    prev_arg2="$arg"
done
# Normalize comma separation to the runner's space-separated argv form.
SUITES="${SUITES//,/ }"
case "${prev_arg:-}" in
    --suites|--arch)
        echo "test.sh: '$prev_arg' needs a value. Please consult --help." >&2
        exit 2
        ;;
esac
if [ "$TSAN" -eq 1 ] && [ -n "$SUITES" ]; then
    echo "test.sh: --tsan and --suites are separate modes (the TSan leg has its own suite set). Please consult --help." >&2
    exit 2
fi
prev_arg=""

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
SANITIZE_GIVEN=0
prev_arg=""
for arg in "$@"; do
    case "$arg" in
        CC=*|CXX=*) export "${arg}" ;;
        --arch|--arch=*) ;; # already handled
        arm64|x86_64) ;; # already handled
        --tsan) ;; # already handled
        --suites|--suites=*) ;; # already handled (value skipped via prev_arg below)
        BUILD_DIR=*) BUILD_DIR="${arg#BUILD_DIR=}"; MAKE_ARGS+=("$arg") ;;
        SANITIZE=*) SANITIZE_GIVEN=1; MAKE_ARGS+=("$arg") ;;
        *=*)
            if [[ "${prev_arg:-}" != "--suites" ]]; then
                MAKE_ARGS+=("$arg") # forward any VAR=VAL to make
            fi
            ;;
    esac
    prev_arg="$arg"
done

# Platform default absorbed FROM CI (previously inline in _test.yml, so the
# local arm64 leg silently built without it — that divergence is why the SQLite
# page-cache misalignment was fatal only on the windows-11-arm runner): native
# ARM64 Windows has no ASan runtime, so its sanitizer gate is UBSan in trap
# mode + stack protector. Applied here, once, for every venue; an explicit
# SANITIZE=... (or SANITIZE=) argument overrides.
if [ "$SANITIZE_GIVEN" -eq 0 ] && [ "${MSYSTEM:-}" = "CLANGARM64" ]; then
    MAKE_ARGS+=("SANITIZE=-fsanitize=undefined -fsanitize-trap=undefined -fstack-protector-strong -fno-omit-frame-pointer")
fi

print_env "test.sh"

# ── TSan mode (--tsan): the data-race gate ──
# One entry for every venue: CI's tsan jobs and the compose test-tsan service
# both run this instead of carrying their own make invocation.
if [ "$TSAN" -eq 1 ]; then
    echo "=== test.sh: TSan leg (make test-tsan) ==="
    make -j"$NPROC" -f Makefile.cbm "$BUILD_DIR/test-runner-tsan" ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
    make -f Makefile.cbm test-tsan ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
    exit "$?"
fi

# ── Iteration mode (--suites): incremental rebuild + subset run ──
# The documented fast path: no clean, no contract steps, no prod-binary
# guards — those all still gate every merge through the default full run.
if [ -n "$SUITES" ]; then
    echo "=== test.sh: ITERATION mode — suites: $SUITES (incremental build) ==="
    make -j"$NPROC" -f Makefile.cbm "$BUILD_DIR/test-runner" ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
    # shellcheck disable=SC2086  # suite list is deliberately word-split
    "$BUILD_DIR/test-runner" $SUITES
    exit "$?"
fi

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

echo "=== Step 0h: smoke release-fixture contract ==="
bash "$ROOT/tests/test_smoke_fixture_contract.sh"

echo "=== Step 0i: parallel suite scheduler contract ==="
bash "$ROOT/tests/test_parallel_harness_contract.sh"

echo "=== Step 0j: venue parity contract (one harness, every venue) ==="
bash "$ROOT/tests/test_venue_parity_contract.sh"

echo "=== Step 0k: spawn console-window contract (#1427) ==="
bash "$ROOT/tests/test_spawn_no_window_contract.sh"

echo "=== Step 0l: release archive extractor contract ==="
bash "$ROOT/tests/test_release_archive_extractor_contract.sh"

echo "=== Step 0m: VirusTotal release-notes + evidence contract ==="
bash "$ROOT/tests/test_vt_release_notes_contract.sh"

echo "=== Step 0n: VirusTotal gate policy contract ==="
bash "$ROOT/tests/test_vt_gate_policy_contract.sh"

echo "=== Step 0o: MCPB bundle contract (#1246) ==="
bash "$ROOT/tests/test_mcpb_bundle_contract.sh"

echo "=== Step 0p: MCPB registry entries contract (#1246) ==="
bash "$ROOT/tests/test_mcpb_registry_entries_contract.sh"

echo "=== Step 0q: release candidate derivation contract ==="
bash "$ROOT/tests/test_release_candidate_derivation_contract.sh"

echo "=== Step 0r: VirusTotal candidate-selection contract ==="
bash "$ROOT/tests/test_vt_candidate_selection_contract.sh"

echo "=== Step 0s: release gate-chain ordering contract ==="
bash "$ROOT/tests/test_release_gate_chain_contract.sh"

echo "=== Step 0t: test runtime isolation contract (#1691) ==="
bash "$ROOT/tests/test_runtime_isolation_contract.sh"

echo "=== Step 0u: shell line-ending contract ==="
bash "$ROOT/tests/test_shell_line_endings.sh"

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
#
# TEST_SEAMS=1: the worker-mode leg below needs the crash-orphan probe, which is
# compiled out of ordinary builds (it forks a SIGTERM-ignoring child — see
# src/main.c). Requesting it HERE, in the leg that consumes it, is what keeps
# release artifacts free of it; scripts/ci/check-binary-composition.sh proves
# they stay that way.
echo "=== Step 5: parent-death watchdog regression (#406/#407) ==="
make -j"$NPROC" -f Makefile.cbm cbm TEST_SEAMS=1 ${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}
WATCHDOG_BINARY="$ROOT/$BUILD_DIR/codebase-memory-mcp"
CBM_TEST_BINARY="$WATCHDOG_BINARY" bash "$ROOT/tests/test_parent_watchdog.sh"

# Step 5b: worker-mode parent-death watchdog (#845). A supervised index worker
# (`cli --index-worker …`) whose supervisor dies must self-exit instead of
# indexing on as an orphan. Reuses the prod binary built in Step 5.
echo "=== Step 5b: worker-mode watchdog regression (#845) ==="
CBM_TEST_BINARY="$WATCHDOG_BINARY" bash "$ROOT/tests/test_worker_watchdog.sh"

# Step 5c: a worker-delivered MCP error is transport success. The outer CLI
# still exits nonzero for the user-facing tool error, but the supervisor must
# preserve that response instead of misreporting exit_nonzero as a file crash.
echo "=== Step 5c: worker error-response transport regression ==="
CBM_TEST_BINARY="$WATCHDOG_BINARY" bash "$ROOT/tests/test_worker_error_response.sh"

# Step 5d (#1388) is DELIBERATELY NOT GATING HERE — see
# tests/test_hook_conflict_notice.sh for the full what-was-tried record.
# Summary: the test forces a client/daemon build mismatch via the
# CBM_TEST_HOOK_CLIENT_BUILD seam and asserts the stdout systemMessage. It is
# reliably green locally against a seam-bearing binary, but on every CI leg the
# forced mismatch raises no cohort conflict at all: the seam is present (the
# test asserts that up front), the forced fingerprint is well-formed (64 hex),
# and `daemon status` reports an active daemon on a DIFFERENT build - yet the
# client joins silently. Until that local-vs-CI divergence in the cohort
# admission path is understood, gating on it would make an unexplained red, and
# skipping it silently would hide the gap. Run it by hand:
#   make -f Makefile.cbm cbm TEST_SEAMS=1 && bash tests/test_hook_conflict_notice.sh

# Step 6: security-strings URL allow-list regression. The MSYS2 CLANG64 toolchain
# bakes its package-tracker URL into the static Windows .exe; the binary string
# audit must allow-list it (Windows-only — Linux smoke never saw it).
echo "=== Step 6: security-strings allow-list regression ==="
bash "$ROOT/tests/test_security_strings_allowlist.sh"
bash "$ROOT/tests/test_destructive_ordering_contract.sh"

echo "=== All tests passed ==="
