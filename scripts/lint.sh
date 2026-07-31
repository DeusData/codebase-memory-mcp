#!/usr/bin/env bash
# lint.sh — Run all linters (clang-tidy + cppcheck + clang-format).
#
# Usage:
#   scripts/lint.sh                                    # All 3 linters
#   scripts/lint.sh CLANG_FORMAT=clang-format-20       # Override formatter
#   scripts/lint.sh --ci                               # CI mode (skip clang-tidy)
#
# This script is the SINGLE source of truth for linting.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"

usage() {
    cat <<'EOF'
Usage: scripts/lint.sh [--ci] [VAR=VAL ...]

The canonical lint entry: CI (_lint.yml) calls this script; it drives the same
Makefile.cbm lint targets `make lint`/`make lint-ci` run, plus the no-skips
policy check. Runs clang-tidy + cppcheck + clang-format by default.

Options:
  --ci            CI mode: cppcheck + clang-format only (no clang-tidy) —
                  exactly what _lint.yml gates on. `make lint-ci` uses this.
  VAR=VAL         Forwarded to make, e.g. CLANG_FORMAT=clang-format-20.
                  NOTE: the project formatter is the Homebrew LLVM build; a
                  standalone clang-format-20 produces false whole-file drift.
  -h, --help      This text.
EOF
}

# Check for --ci flag (skip clang-tidy for platforms without it)
CI_ONLY=false
MAKE_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
        usage; exit 0
    elif [ "$arg" = "--ci" ]; then
        CI_ONLY=true
    else
        # STRICT: only VAR=VAL make passthrough beyond the known flags.
        case "$arg" in
        *=*) MAKE_ARGS+=("$arg") ;;
        *)
            echo "lint.sh: unknown argument '$arg'. Please consult --help." >&2
            exit 2
            ;;
        esac
    fi
done

print_env "lint.sh"

# No-skips policy: every test must pass or fail. The only tolerable skip is a
# genuinely platform-specific test (SKIP_PLATFORM / #ifdef). Runs in both modes.
echo "=== no-skips policy (tests pass or fail) ==="
bash "$ROOT/scripts/check-no-test-skips.sh"

if $CI_ONLY; then
    echo "=== CI mode: cppcheck + clang-format ==="
    make -j2 -f Makefile.cbm lint-ci "${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}"
    echo "=== CI linters passed ==="
else
    echo "=== Full lint: clang-tidy + cppcheck + clang-format ==="
    make -j3 -f Makefile.cbm lint "${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}"
fi

echo "=== All linters passed ==="
