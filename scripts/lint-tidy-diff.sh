#!/usr/bin/env bash
# lint-tidy-diff.sh — clang-tidy scoped to the lines this commit changes.
#
# `make lint-tidy` (the full sweep) checks the entire LINT_SRCS tree, which
# currently surfaces ~5,100 pre-existing findings unrelated to any given
# commit (see Makefile.cbm's `lint-tidy` target). That is fine for a manual
# deep audit but makes the pre-commit hook unusable: it blocks every commit,
# including ones that touch files with zero findings on the touched lines.
#
# This script runs the same clang-tidy binary/config against the SAME staged
# .c files, but passes clang-tidy's own `-line-filter` so only lines the
# commit actually added/modified can produce a diagnostic. Pre-existing
# findings on untouched lines — in the same file or elsewhere — are not
# reported.
#
# Invoked by `make lint-tidy-diff` (scripts/hooks/pre-commit), which supplies
# the resolved clang-tidy binary, then LINT_SRCS, then `--`, then the
# compiler flags — the same argv shape as the `lint-tidy` target itself.
#
# Known limitation: only staged *.c files that are members of LINT_SRCS are
# checked (matching the full sweep's own scope: vendored code and tests/ are
# never clang-tidy'd). clang-tidy reports a header's diagnostics under
# whatever path it was #include-d with, which is not reliably the same
# string as its path in the working tree, so a commit that touches only a
# header is not covered here even if a .c file including it also changed;
# run `make lint-tidy` for a full pass in that case.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ "$#" -lt 1 ]; then
    echo "usage: lint-tidy-diff.sh <clang-tidy-bin> [LINT_SRCS...] -- [CFLAGS...]" >&2
    exit 2
fi

CLANG_TIDY="$1"
shift

LINT_SRCS=()
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    LINT_SRCS+=("$1")
    shift
done
[ "$#" -gt 0 ] && shift # drop the `--` separator
CFLAGS=("$@")

# bash 3.2 (macOS default, see scripts/check-lsp-originality.sh) has neither
# `mapfile` nor associative arrays — read loop + linear membership check.
is_lint_src() {
    local needle="$1" f
    [ "${#LINT_SRCS[@]}" -eq 0 ] && return 1
    for f in "${LINT_SRCS[@]}"; do
        [ "$f" = "$needle" ] && return 0
    done
    return 1
}

# Staged .c files this commit actually changed (added/copied/modified/
# renamed — a delete has no new lines to check) that are members of
# LINT_SRCS.
CHANGED=()
while IFS= read -r f; do
    [ -n "$f" ] && is_lint_src "$f" && CHANGED+=("$f")
done < <(git diff --cached --name-only --diff-filter=ACMR -- '*.c')

if [ "${#CHANGED[@]}" -eq 0 ]; then
    echo "lint-tidy-diff: no staged changes to a linted .c file, skipping clang-tidy"
    exit 0
fi

# For each changed file, turn its staged hunks into a clang-tidy line-filter
# range: `@@ -a,b +c,d @@` -> lines [c, c+d-1]. Zero-context (-U0) so a range
# never includes untouched, pre-existing lines. Pure-deletion hunks (d == 0)
# add no new lines and are skipped.
LINE_FILTER='['
FILTER_SEP=''
TARGETS=()
for f in "${CHANGED[@]}"; do
    [ -f "$f" ] || continue # renamed away / not present in the working tree
    ranges=''
    range_sep=''
    while IFS= read -r hunk; do
        if [[ "$hunk" =~ ^@@\ -[0-9]+(,[0-9]+)?\ \+([0-9]+)(,([0-9]+))?\ @@ ]]; then
            new_start="${BASH_REMATCH[2]}"
            new_count="${BASH_REMATCH[4]:-1}"
            [ "$new_count" -eq 0 ] && continue
            new_end=$((new_start + new_count - 1))
            ranges+="${range_sep}[${new_start},${new_end}]"
            range_sep=','
        fi
    done < <(git diff --cached -U0 --diff-filter=ACMR -- "$f" | grep -E '^@@ ')
    [ -z "$ranges" ] && continue

    LINE_FILTER+="${FILTER_SEP}{\"name\":\"${f}\",\"lines\":[${ranges}]}"
    FILTER_SEP=','
    TARGETS+=("$f")
done
LINE_FILTER+=']'

if [ "${#TARGETS[@]}" -eq 0 ]; then
    echo "lint-tidy-diff: staged .c changes are deletion-only, skipping clang-tidy"
    exit 0
fi

echo "lint-tidy-diff: ${#TARGETS[@]} changed file(s), diff-scoped clang-tidy"
"$CLANG_TIDY" --quiet "-line-filter=${LINE_FILTER}" "${TARGETS[@]}" -- "${CFLAGS[@]}"
