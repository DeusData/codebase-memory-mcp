#!/usr/bin/env bash
# vm-run-tests.sh — run C test suites on the Windows VM under a CI-shaped
# protected temp root. Runs ON the VM (MSYS2 shell), invoked by win.sh
# test / ubsan-test / trap-ubsan-test / soak.
#
# Why the temp root: the daemon/coordination suites fail closed on the
# MSYS-shared /tmp (C:\msys64\tmp), whose ancestry grants mutation rights to
# Authenticated Users — running them there produces security refusals, not
# test signal. CI gives the harness a per-user root under the profile with an
# owner-stamped, protected current-SID DACL (.github/workflows/_test.yml
# "Create protected per-user temp root"); this script mirrors that exactly so
# the VM leg validates what CI will see.
#
# Why the guard: this leg once piped through `tail -40`, and a suite that
# never validly ran looked green — 40 Windows failures reached CI unseen.
# Output now streams in full, and a run whose log lacks the runner's
# completion summary is a hard failure regardless of exit code.
set -uo pipefail

usage() {
    cat <<'EOF'
Usage: bash test-infrastructure/vm/vm-run-tests.sh <suite...> | --par | --soak <minutes>

VM-side provisioning wrapper (runs ON the Windows VM, invoked by win.sh —
call `win.sh test|test-par|soak` from the host, not this file). Supplies the
CI-shaped protected per-user temp root (scripts/ci/new-protected-temp-root.ps1)
and full-output logging with a completion-summary guard, then routes into the
CANONICAL entries:

  <suite...>       scripts/test.sh --suites <list>   (iteration mode)
  --par            scripts/test.sh                   (the full venue leg)
  --soak <mins>    scripts/soak-legs.sh              (both CI soak legs)

Environment:
  CBM_VM_RUNNER      sanitizer-variant runner path (ubsan/trap-ubsan builds);
                     switches to direct-runner mode for those iteration tools.
  CBM_VM_SOAK_BINARY product binary for --soak (default build/c/codebase-memory-mcp.exe)
  CBM_VM_TEST_LOG    VM-side log path (default /tmp/win-test.log)

Exit codes: 0 success · 2 usage · 90 = GUARD: no completion summary (a run
that died without its summary must never read as green) · else the leg's code.
EOF
}
case "${1:-}" in
-h | --help) usage; exit 0 ;;
esac

RUNNER="${CBM_VM_RUNNER:-}"
LOG="${CBM_VM_TEST_LOG:-/tmp/win-test.log}"

[ $# -ge 1 ] || { echo "vm-run-tests: missing arguments. Please consult --help." >&2; exit 2; }
if [ "$1" = "--soak" ]; then
    [ $# -eq 2 ] || { echo "usage: vm-run-tests.sh --soak <positive-minutes>" >&2; exit 2; }
    duration="$2"
    case "$duration" in
    ''|*[!0-9]*) echo "usage: vm-run-tests.sh --soak <positive-minutes>" >&2; exit 2 ;;
    esac
    [ "$duration" -gt 0 ] ||
        { echo "usage: vm-run-tests.sh --soak <positive-minutes>" >&2; exit 2; }
    binary="${CBM_VM_SOAK_BINARY:-build/c/codebase-memory-mcp.exe}"
    [ -x "$binary" ] || { echo "ERROR: binary '$binary' missing — build first" >&2; exit 2; }
    artifact="$binary"
elif [ -n "$RUNNER" ]; then
    # Explicit CBM_VM_RUNNER = the sanitizer-variant iteration mode (ubsan /
    # trap-ubsan runners built into their own BUILD_DIRs by win.sh). Those
    # builds carry non-default flags, so they keep the direct-runner path.
    [ -x "$RUNNER" ] || { echo "ERROR: runner '$RUNNER' missing — build first" >&2; exit 2; }
    artifact="$RUNNER"
else
    # Default path: suites run through the canonical scripts/test.sh (which
    # builds its own runner, same as CI's test jobs). ACL-protect the build
    # directory it will use.
    artifact="build/c/test-runner"
fi

# Stale roots from earlier runs are removed up front; the current root is kept
# after the run for post-mortem inspection. The root itself is created by the
# same script CI uses (scripts/ci/new-protected-temp-root.ps1) so the two venues
# cannot drift apart on the ACL shape the daemon suites are validated against.
root_windows="$(MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile \
    -ExecutionPolicy Bypass \
    -File "$(cygpath -w scripts/ci/new-protected-temp-root.ps1)" \
    -Prefix 'cbm-vm-tmp-' -PruneStale \
    -ProtectDir "$(cygpath -w "$(dirname "$artifact")")" | tr -d '\r')"
[ -n "$root_windows" ] || { echo "ERROR: protected temp root creation failed" >&2; exit 2; }

TEMP="$(cygpath -m "$root_windows")"
TMP="$TEMP"
TMPDIR="$(cygpath -u "$root_windows")"
export TEMP TMP TMPDIR

# The runner's directory must look like a real user checkout: repos under a
# profile carry no Authenticated-Users ACE, but C:\cbm (like CI's workspace
# drive) inherits Modify for Authenticated Users from the drive root, which
# the activation transaction's source-directory policy correctly refuses —
# install-flow tests would then fail on the environment, not the code.
# Two steps, both idempotent: protect the DIRECTORY (inheritance flags are
# directory-only — a /T re-root leaves files with empty, deny-all DACLs),
# then /reset the children so they re-inherit the clean set from it.
runner_dir_w="$(cygpath -w "$(dirname "$artifact")")"
me="$(whoami | tr -d '\r')"
MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" /inheritance:r \
    /grant:r "${me}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' \
    /Q >/dev/null 2>&1 || true
MSYS2_ARG_CONV_EXCL='*' icacls "${runner_dir_w}\\*" /reset /T /C /Q >/dev/null 2>&1 || true

echo "=== vm-run-tests: runner=$RUNNER temp=$TEMP suites: $* ==="

if [ "$1" = "--soak" ]; then
    # The sequence (quick + #581 query-leak) and its completion guards live in
    # the canonical entry scripts/soak-legs.sh — the same file _soak.yml and the
    # compose soak service run. This wrapper only supplies the CI-shaped
    # protected temp root (above) and the persistent VM-side log.
    echo "=== vm-run-tests: soak binary=$binary temp=$TEMP ${duration}m/leg ==="
    scripts/soak-legs.sh "$binary" "$duration" 2>&1 | tee "$LOG"
    exit "${PIPESTATUS[0]}"
fi

# Both paths run the CANONICAL test entry scripts/test.sh under this protected
# environment — the same file every CI test leg runs:
#   --par         the full venue leg (clean build + contracts + parallel suites)
#   <suite...>    scripts/test.sh --suites — the documented iteration mode
# Only an explicit CBM_VM_RUNNER (sanitizer-variant builds) bypasses test.sh.
if [ -n "$RUNNER" ]; then
    if [ "$1" = "--par" ]; then
        bash scripts/run-tests-parallel.sh "$RUNNER" 2>&1 | tee "$LOG"
        rc="${PIPESTATUS[0]}"
    else
        "$RUNNER" "$@" 2>&1 | tee "$LOG"
        rc="${PIPESTATUS[0]}"
    fi
elif [ "$1" = "--par" ]; then
    scripts/test.sh CC=clang CXX=clang++ 2>&1 | tee "$LOG"
    rc="${PIPESTATUS[0]}"
else
    scripts/test.sh --suites "$*" CC=clang CXX=clang++ 2>&1 | tee "$LOG"
    rc="${PIPESTATUS[0]}"
fi

if ! grep -Eq '[0-9]+ passed' "$LOG"; then
    echo "GUARD: test runner produced no completion summary — the suites did" \
         "not validly run; treating as failure (runner rc=$rc)" >&2
    exit 90
fi
exit "$rc"
