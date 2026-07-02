#!/usr/bin/env bash
# check-source-safety.sh — source-level safety and protocol-output guard.
#
# This complements clang/cppcheck by enforcing project conventions that generic
# linters cannot infer:
#   - MCP/server runtime code must not write protocol-breaking text to stdout.
#   - Production source must not add unsafe or ambiguous fixed-buffer string-copy helpers.
#   - New production diffs should use CBM platform wrappers for env/fs APIs.
#   - New production diffs should use CBM allocation/duplication wrappers.
set -uo pipefail

ROOT="${CBM_SOURCE_SAFETY_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
violations=0

add_violation() {
    echo "[source-safety] $*"
    violations=$((violations + 1))
}

is_allowed_unsafe_string_hit() {
    local hit="$1"
    case "$hit" in
        "$ROOT/src/foundation/compat.c":*"strcpy(tmpl, buf);"*) return 0 ;;
        src/foundation/compat.c:*"strcpy(tmpl, buf);"*) return 0 ;;
    esac
    return 1
}

grep_source() {
    local pattern="$1"
    shift
    if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git -C "$ROOT" grep -nE "$pattern" -- "$@" 2>/dev/null || true
    else
        local paths=()
        local p
        for p in "$@"; do
            paths+=("$ROOT/$p")
        done
        grep -RInE "$pattern" "${paths[@]}" 2>/dev/null || true
    fi
}

while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    case "$hit" in
        */vendored/*|*/build/*|internal/cbm/vendored/*) continue ;;
    esac
    if ! is_allowed_unsafe_string_hit "$hit"; then
        add_violation "unsafe string API in production source: $hit"
    fi
done < <(
    grep_source '\b(strcpy|strncpy|strcat|sprintf|gets)[[:space:]]*\(' src internal cmd
)

while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    case "$hit" in
        */vendored/*|*/build/*|internal/cbm/vendored/*) continue ;;
    esac
    add_violation "stdout write in MCP/server pipeline code: $hit"
done < <(
    grep_source '\b(printf|puts|putchar)[[:space:]]*\(|fprintf[[:space:]]*\([[:space:]]*stdout' \
        src/mcp src/pipeline src/graph_buffer src/semantic internal/cbm
)

if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    diff_text="$(
        git -C "$ROOT" diff --unified=0 -- src internal cmd 2>/dev/null
        git -C "$ROOT" diff --cached --unified=0 -- src internal cmd 2>/dev/null
    )"
    while IFS= read -r line; do
        case "$line" in
            +++*|"+" ) continue ;;
            +*)
                if [[ "$line" =~ (^|[^A-Za-z0-9_])(unlink|rename|remove|rmdir|mkdir|getenv|setenv|unsetenv|mkstemp|mkdtemp)[[:space:]]*\( ]]; then
                    add_violation "new raw env/fs API in production diff, use CBM compat wrapper or document an exception: $line"
                fi
                if [[ "$line" =~ (^|[^A-Za-z0-9_])strdup[[:space:]]*\( ]]; then
                    add_violation "new raw strdup in production diff, use cbm_strdup or a local ownership wrapper: $line"
                fi
                ;;
        esac
    done <<< "$diff_text"
fi

if [ "$violations" -gt 0 ]; then
    echo ""
    echo "[source-safety] FAIL: $violations violation(s)."
    echo "  Use cbm_safe_getenv/cbm_setenv/cbm_unsetenv and compat_fs wrappers where applicable."
    echo "  Keep MCP stdio stdout reserved for JSON-RPC protocol messages; diagnostics go to stderr/logs."
    exit 1
fi

echo "[source-safety] OK — protocol stdout and source safety checks passed"
exit 0
