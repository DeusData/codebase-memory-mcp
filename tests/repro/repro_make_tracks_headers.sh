#!/usr/bin/env bash
# Reproduce linked targets ignoring shared test or production-header changes,
# which can execute a stale binary after a header-only semantic fix.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MAKE_DB="$(mktemp "${TMPDIR:-/tmp}/cbm-repro-make-db.XXXXXX")"
trap 'rm -f "$MAKE_DB"' EXIT

cd "$PROJECT_ROOT"
make -f Makefile.cbm -pn build/c/test-repro-runner >"$MAKE_DB"
repro_target_line="$(grep '^build/c/test-repro-runner:' "$MAKE_DB" | head -1 || true)"
test_target_line="$(grep '^build/c/test-runner:' "$MAKE_DB" | head -1 || true)"
tsan_target_line="$(grep '^build/c/test-runner-tsan:' "$MAKE_DB" | head -1 || true)"
production_target_line="$(grep '^build/c/codebase-memory-mcp:' "$MAKE_DB" | head -1 || true)"

if [[ "$repro_target_line" != *'tests/repro/repro_harness.h'* ||
      "$repro_target_line" != *'tests/test_framework.h'* ||
      "$repro_target_line" != *'tests/test_helpers.h'* ]]; then
    printf '[repro-make-headers] invariant=shared_headers_missing_from_target\n' >&2
    exit 1
fi

missing_test_header=0
for target_name in build/c/test-runner build/c/test-runner-tsan; do
    target_line="$test_target_line"
    if [[ "$target_name" == 'build/c/test-runner-tsan' ]]; then
        target_line="$tsan_target_line"
    fi
    for header in tests/test_framework.h tests/test_helpers.h tests/grammar_cases.h; do
        if [[ "$target_line" != *"$header"* ]]; then
            printf '[repro-make-headers] invariant=test_header_missing_from_target target=%s header=%s\n' \
                "$target_name" "$header" >&2
            missing_test_header=1
        fi
    done
done
missing_production_header=0
for target_name in build/c/test-runner build/c/test-repro-runner build/c/test-runner-tsan \
                   build/c/codebase-memory-mcp; do
    case "$target_name" in
        build/c/test-runner) target_line="$test_target_line" ;;
        build/c/test-repro-runner) target_line="$repro_target_line" ;;
        build/c/test-runner-tsan) target_line="$tsan_target_line" ;;
        *) target_line="$production_target_line" ;;
    esac
    for header in src/pipeline/lsp_resolve.h internal/cbm/lsp/type_registry.h; do
        if [[ "$target_line" != *"$header"* ]]; then
            printf '[repro-make-headers] invariant=production_header_missing_from_target target=%s header=%s\n' \
                "$target_name" "$header" >&2
            missing_production_header=1
        fi
    done
done
if [[ "$missing_test_header" -ne 0 || "$missing_production_header" -ne 0 ]]; then
    exit 1
fi

printf '[repro-make-headers] shared headers tracked by runner and production targets\n'
