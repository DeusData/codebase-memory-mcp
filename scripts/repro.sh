#!/usr/bin/env bash
# repro.sh — Build + run the cumulative BUG-REPRODUCTION suite (test-repro).
#
# Unlike test.sh (the gating suite, must be GREEN), this suite tracks open RED
# reproductions alongside GREEN controls and regression guards. We distinguish:
#   - BUILD/LINK failure  → real breakage → exit non-zero (fail the CI job).
#   - Reproduced RED cases → expected board data → report the count, exit 0.
#   - Any skipped case     → incomplete board → report it, exit non-zero.
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

OUT="$(mktemp "${TMPDIR:-/tmp}/cbm-repro-out.XXXXXX")"
if [[ -z "$OUT" ]]; then
    echo "::error::unable to create bug-repro transcript"
    exit 1
fi
trap 'rm -f "$OUT"' EXIT
# A RED reproduction fails its assertion and returns EARLY — before any cleanup —
# so LeakSanitizer would flag benign harness leaks on every red store-level test
# and abort. The board's signal is the FAIL rows, not leak-cleanliness (the leak
# BUG #581 gets a dedicated RSS-growth test, not LSan). Disable leak detection
# only; ASan's real checks (use-after-free, overflow) stay ON.
export ASAN_OPTIONS="detect_leaks=0${ASAN_OPTIONS:+:$ASAN_OPTIONS}"

# test-repro both builds and runs the runner; tolerate its non-zero (red) exit.
set +e
make -j"$NPROC" -f Makefile.cbm test-repro $MAKE_ARGS 2>&1 | tee "$OUT"
set -e

# The runner's final aggregate is the only line that starts with the count.
# Per-suite lines also contain "N passed, M failed", so selecting the first
# match undercounts every multi-suite run.
summary_line="$(grep -E '^[[:space:]]*[0-9]+ passed(, [0-9]+ failed)?(, [0-9]+ skipped)?[[:space:]]*$' "$OUT" | tail -1 || true)"
if [[ -z "$summary_line" ]]; then
    echo "::error::bug-repro runner did not execute — build or link failure"
    exit 1
fi

green="$(printf '%s\n' "$summary_line" | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+')"
reproduced="$(printf '%s\n' "$summary_line" | grep -oE '[0-9]+ failed' | grep -oE '[0-9]+' || echo 0)"
skipped="$(printf '%s\n' "$summary_line" | grep -oE '[0-9]+ skipped' | grep -oE '[0-9]+' || echo 0)"

if ((green + reproduced == 0)); then
    echo "::error::bug-repro runner executed zero tests"
    exit 1
fi

emit_board_summary() {
    echo "## Bug-reproduction board — ${OS:-$(uname -s)} ${ARCH:-}"
    echo ""
    echo "- **${reproduced}** open bug(s) still reproduced (RED — expected)"
    echo "- **${green}** case(s) passing control(s) or candidate-fix candidate(s); verify each RED→GREEN transition before closing an issue"
    if ((skipped > 0)); then
        echo "- **${skipped}** case(s) SKIPPED — board is incomplete"
    fi
}

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    emit_board_summary >> "$GITHUB_STEP_SUMMARY"
else
    emit_board_summary >&2
fi

echo "=== bug-repro board: ${reproduced} reproduced (RED), ${green} passing control(s) or candidate-fix candidate(s) ==="
if ((skipped > 0)); then
    echo "::warning::bug-repro board incomplete — ${skipped} case(s) skipped"
    exit 1
fi

# Green board: the suite ran. Redness is the data, not a job failure.
exit 0
