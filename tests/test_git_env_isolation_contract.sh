#!/usr/bin/env bash
# Contract (#2003): the test runner must be immune to an inherited git
# repository environment.
#
# Git exports GIT_DIR, GIT_INDEX_FILE, ... into hooks, and those variables take
# precedence over `git -C <dir>`. The pre-commit hook runs the suite, so every
# fixture git command in the runner used to act on the committer's REAL
# repository: fixture assertions failed, and branches, commits and linked
# worktrees were written into the contributor's clone. The runner now clears
# git's local-env list at startup (th_clear_git_repo_env in tests/test_helpers.h).
#
# This drives the real runner with those variables aimed at a throwaway decoy
# repository and asserts both halves: the git fixture suite passes, and the
# decoy is left exactly as it was (no refs, no linked worktrees).
#
# Usage: tests/test_git_env_isolation_contract.sh [path/to/test-runner]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="${1:-$ROOT/build/c/test-runner}"
SUITE="git_context"

if [ ! -x "$RUNNER" ]; then
    echo "FAIL: test runner not found at '$RUNNER'; build it first (make -f Makefile.cbm build/c/test-runner)." >&2
    exit 1
fi
if ! command -v git >/dev/null 2>&1; then
    echo "SKIP: git is not on PATH, so there is no git environment to leak."
    exit 0
fi

# This script's own git calls must address the decoy, not whatever repository
# the caller (e.g. the pre-commit hook) exported.
while IFS= read -r variable; do
    unset "$variable"
done < <(git rev-parse --local-env-vars)

WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-git-env-XXXXXX")"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

decoy_state() {
    printf 'refs:\n'
    git -C "$1" for-each-ref
    printf 'worktrees:\n'
    git -C "$1" worktree list --porcelain
}

# Baseline: the same suite in a clean environment. Comparing summaries (not just
# exit codes) matters: with GIT_WORK_TREE inherited, fixture commits fail and
# the suite SKIPs as "git not available" instead of failing -- a silent green.
BASE_LOG="$WORK/baseline.log"
"$RUNNER" "$SUITE" >"$BASE_LOG" 2>&1 || {
    echo "FAIL: '$SUITE' is red even in a clean environment; fix that first." >&2
    grep -E 'FAIL|passed' "$BASE_LOG" >&2 || true
    exit 1
}

summary() { grep -E '^ *[0-9]+ passed' "$1" | tail -1; }
BASE_SUMMARY="$(summary "$BASE_LOG")"
FAILED=0

# Two inherited shapes: GIT_DIR alone (the reported `GIT_DIR=... scripts/test.sh`
# case: fixture git commands write into the decoy) and the full set a hook
# exports (fixture commits fail, so the suite degrades to SKIPs).
check_variant() {
    local name="$1"
    local decoy="$WORK/decoy-$name"
    shift
    git init -q "$decoy"
    local before
    before="$(decoy_state "$decoy")"
    local -a assignments=()
    local var
    for var in "$@"; do
        assignments+=("$var=$decoy$(git_env_suffix "$var")")
    done
    local log="$WORK/runner-$name.log"
    local rc
    set +e
    env "${assignments[@]}" "$RUNNER" "$SUITE" >"$log" 2>&1
    rc=$?
    set -e
    local env_summary after
    env_summary="$(summary "$log")"
    after="$(decoy_state "$decoy")"
    if [ "$rc" -ne 0 ]; then
        echo "FAIL[$name]: '$SUITE' exited $rc with an inherited git environment." >&2
        grep -E 'FAIL|passed' "$log" >&2 || true
        FAILED=1
    fi
    if [ -z "$BASE_SUMMARY" ] || [ "$BASE_SUMMARY" != "$env_summary" ]; then
        echo "FAIL[$name]: '$SUITE' behaves differently with an inherited git environment." >&2
        echo "  clean env:     ${BASE_SUMMARY:-<no summary>}" >&2
        echo "  inherited env: ${env_summary:-<no summary>}" >&2
        grep -E 'FAIL|SKIP' "$log" >&2 || true
        FAILED=1
    fi
    if [ "$before" != "$after" ]; then
        echo "FAIL[$name]: the test runner modified the repository named by the inherited GIT_DIR." >&2
        echo "--- before ---" >&2
        echo "$before" >&2
        echo "--- after ---" >&2
        echo "$after" >&2
        FAILED=1
    fi
}

# Where each exported variable points inside a repository, as a hook sees it.
git_env_suffix() {
    case "$1" in
        GIT_WORK_TREE) printf '' ;;
        GIT_INDEX_FILE) printf '/.git/index' ;;
        *) printf '/.git' ;;
    esac
}

check_variant git-dir GIT_DIR
check_variant hook-env GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR

if [ "$FAILED" -ne 0 ]; then
    exit 1
fi
echo "PASS: test runner ignores an inherited git repository environment (#2003)."
