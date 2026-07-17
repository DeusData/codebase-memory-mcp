#!/usr/bin/env bash
set -euo pipefail

# Activate the repository's tracked hooks for this clone.
# Both pre-commit and commit-msg live in scripts/hooks.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

check_hook_files() {
    for hook in pre-commit commit-msg; do
        [ -x "$ROOT/scripts/hooks/$hook" ] || {
            echo "error: scripts/hooks/$hook is missing or not executable" >&2
            return 1
        }
    done
    bash -n "$ROOT/scripts/hooks/pre-commit" "$ROOT/scripts/hooks/commit-msg"
}

self_test_hooks() {
    check_hook_files
    author_name="Hook Self Test"
    author_email="hook-self-test@example.invalid"
    message=$(mktemp)
    trap 'rm -f "$message"' RETURN
    printf 'test: valid sign-off\n\nSigned-off-by: %s <%s>\n' "$author_name" "$author_email" > "$message"
    GIT_AUTHOR_NAME="$author_name" GIT_AUTHOR_EMAIL="$author_email" \
        "$ROOT/scripts/hooks/commit-msg" "$message"
    printf 'test: mismatched sign-off\n\nSigned-off-by: Other <other@example.com>\n' > "$message"
    if GIT_AUTHOR_NAME="$author_name" GIT_AUTHOR_EMAIL="$author_email" \
        "$ROOT/scripts/hooks/commit-msg" "$message" >/dev/null 2>&1; then
        echo "error: commit-msg accepted a mismatched sign-off" >&2
        rm -f "$message"
        trap - RETURN
        return 1
    fi
    rm -f "$message"
    trap - RETURN
}

check_hooks() {
    configured="$(git -C "$ROOT" config --local --get core.hooksPath || true)"
    [ "$configured" = "scripts/hooks" ] || {
        echo "error: core.hooksPath is '$configured', expected 'scripts/hooks'" >&2
        return 1
    }
    self_test_hooks
}

if [ "${1:-}" = "--self-test" ]; then
    self_test_hooks
    echo "git hooks: behavioral self-test passed"
    exit 0
fi

if [ "${1:-}" = "--check" ]; then
    check_hooks
    echo "git hooks: configured and executable"
    exit 0
fi

if [ "$#" -gt 0 ]; then
    echo "usage: $0 [--check|--self-test]" >&2
    exit 2
fi

chmod 755 "$ROOT/scripts/hooks/pre-commit" "$ROOT/scripts/hooks/commit-msg"
git -C "$ROOT" config --local core.hooksPath scripts/hooks
check_hooks
echo "installed: scripts/hooks/pre-commit and scripts/hooks/commit-msg"
