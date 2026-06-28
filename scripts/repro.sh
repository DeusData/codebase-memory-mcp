#!/bin/bash
# repro.sh — Build + run the cumulative BUG-REPRODUCTION suite (test-repro).
#
# Unlike test.sh (the gating suite, must be GREEN), this suite is RED by design:
# every case reproduces an open bug. So we distinguish two outcomes:
#   - BUILD/LINK failure  → real breakage → exit non-zero (fail the CI job).
#   - Test redness        → EXPECTED → report the count, exit 0 (green board).
#
# Usage: scripts/repro.sh [CC=clang] [CXX=clang++] [--arch arm64|x86_64]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --arch before sourcing env.sh (mirrors test.sh)
prev_arg=""
for arg in "$@"; do
    case "$arg" in
        arm64|x86_64) [[ "$prev_arg" == "--arch" ]] && export CBM_ARCH="$arg" ;;
        --arch=*) export CBM_ARCH="${arg#--arch=}" ;;
    esac
    prev_arg="$arg"
done

# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

MAKE_ARGS=""
for arg in "$@"; do
    case "$arg" in
        CC=*|CXX=*) export "${arg?}" ;;
        --arch|--arch=*|arm64|x86_64) ;;
        *=*) MAKE_ARGS="$MAKE_ARGS $arg" ;;
    esac
done

print_env "repro.sh"
verify_compiler "$CC"

OUT="$ROOT/repro-out.txt"
# A RED reproduction fails its assertion and returns EARLY — before any cleanup —
# so LeakSanitizer would flag benign harness leaks on every red store-level test
# and abort. The board's signal is the FAIL rows, not leak-cleanliness (the leak
# BUG #581 gets a dedicated RSS-growth test, not LSan). Disable leak detection
# only; ASan's real checks (use-after-free, overflow) stay ON.
export ASAN_OPTIONS="detect_leaks=0${ASAN_OPTIONS:+:$ASAN_OPTIONS}"

# test-repro both builds and runs the runner; tolerate its non-zero (red) exit.
set +e
$ARCH_PREFIX make -j"$NPROC" -f Makefile.cbm test-repro $MAKE_ARGS 2>&1 | tee "$OUT"
set -e

# The runner prints a "<N> passed[, <M> failed]" summary line only if it actually
# ran. No summary line ⇒ the build/link failed ⇒ real breakage.
if ! grep -qE '[0-9]+ passed' "$OUT"; then
    echo "::error::bug-repro runner did not execute — build or link failure"
    exit 1
fi

reproduced=$(grep -oE '[0-9]+ failed' "$OUT" | head -1 | grep -oE '[0-9]+' || echo 0)
green=$(grep -oE '[0-9]+ passed' "$OUT" | head -1 | grep -oE '[0-9]+' || echo 0)

{
    echo "## Bug-reproduction board — ${OS:-$(uname -s)} ${ARCH:-}"
    echo ""
    echo "- **${reproduced}** open bug(s) still reproduced (RED — expected)"
    echo "- **${green}** case(s) PASSING — candidate-fixed → verify + close the issue + promote the guard to the gating suite"
} >> "${GITHUB_STEP_SUMMARY:-/dev/stderr}"

echo "=== bug-repro board: ${reproduced} reproduced (RED), ${green} passing (candidate-fixed) ==="
# Green board: the suite ran. Redness is the data, not a job failure.
exit 0
