#!/usr/bin/env bash
# check-no-test-skips.sh — Enforce the no-skips test policy in the lint phase.
#
# Every test must PASS or FAIL. The ONLY tolerable skip is a genuinely
# platform-specific test that cannot run on the current OS (e.g. a Windows-only
# test on macOS); those must use the SKIP_PLATFORM() macro (or #ifdef
# compile-gating). A plain SKIP() — or any direct tf_skip_count manipulation —
# in a test source means a setup/environment/resource failure is being hidden
# instead of surfaced as a failure. That fails this check.
#
# Rationale: a test that cannot establish its preconditions has FAILED, not been
# "skipped". Convert such SKIP() to FAIL("reason").
#
# Usage: bash scripts/check-no-test-skips.sh   (exit 0 = clean, 1 = violations)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
violations=0

# 1. Plain SKIP( in test sources.
#    - SKIP_PLATFORM( never matches "SKIP(" (the char after SKIP is '_').
#    - The SKIP()/FAIL()/SKIP_PLATFORM() macro DEFINITIONS live in
#      tests/test_framework.h (a .h), which the tests/*.c glob does not scan.
while IFS= read -r hit; do
    echo "[no-skips] FORBIDDEN SKIP(): $hit"
    violations=$((violations + 1))
done < <(grep -rnE '(^|[^A-Za-z0-9_])SKIP\(' "$ROOT"/tests/*.c 2>/dev/null || true)

# 2. Direct tf_skip_count increment in a test source (only the framework's
#    SKIP_PLATFORM macro, defined in the .h, may touch it).
while IFS= read -r hit; do
    echo "[no-skips] FORBIDDEN tf_skip_count manipulation: $hit"
    violations=$((violations + 1))
done < <(grep -rnE 'tf_skip_count[[:space:]]*(\+\+|\+=|--)' "$ROOT"/tests/*.c 2>/dev/null || true)

if [ "$violations" -gt 0 ]; then
    echo ""
    echo "[no-skips] FAIL: $violations violation(s). Tests must pass or fail."
    echo "  setup / environment / resource failures  ->  FAIL(\"reason\")"
    echo "  genuinely platform-specific tests         ->  SKIP_PLATFORM(\"reason\") or #ifdef"
    exit 1
fi

echo "[no-skips] OK — no forbidden skips in tests/*.c (SKIP_PLATFORM allowed for platform-only tests)"
exit 0
