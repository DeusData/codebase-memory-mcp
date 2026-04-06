#!/bin/bash
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

# Check for --ci flag (skip clang-tidy for platforms without it)
CI_ONLY=false
MAKE_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--ci" ]; then
        CI_ONLY=true
    else
        MAKE_ARGS+=("$arg")
    fi
done

print_env "lint.sh"

if $CI_ONLY; then
    echo "=== CI mode: cppcheck + clang-format ==="
    $ARCH_PREFIX make -j2 -f Makefile.cbm lint-ci "${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}"
    echo "=== CI linters passed ==="
else
    echo "=== Full lint: clang-tidy + cppcheck + clang-format ==="
    $ARCH_PREFIX make -j3 -f Makefile.cbm lint "${MAKE_ARGS[@]+"${MAKE_ARGS[@]}"}"
fi

echo "=== All linters passed ==="
