#!/usr/bin/env bash
# Regression guard: this repo's own parse-coverage report must stay useful.
#
# A coverage range is advice — "these lines are missing from the graph, read
# them". Advice stops being advice when it names most of the file, and it
# stops being honest when the list was clipped without saying so. Both things
# happened here before (#963): src/cli/cli.c reported its whole 13,046 lines
# as one range, and two caps in series dropped ranges with no signal at all.
#
# THIS GATE COMPARES AGAINST THE MERGE BASE, NOT AGAINST A FIXED NUMBER.
# It indexes the base tree and the head tree with the SAME binary and fails
# only on a finding the branch added. An earlier version failed against a
# ceiling checked into a file, which meant main could move the number and turn
# every open pull request red for a reason none of them caused. That happened
# once already (#1972, the count went 58 -> 59 because main gained a file),
# and #1824 will do it again and larger — Blazor .razor files map to C#, the
# markup lands in ERROR regions by design, and the count rises by roughly the
# repo's .razor count. A coverage improvement must not read as a gate failure.
#
# WHAT THIS CANNOT SEE. Both trees are indexed with the same binary, so the
# comparison isolates what the TREE changed. A branch that changes the
# extractor itself moves the base side and the head side together, and this
# gate will not fail on it. Catching that needs the base commit's own binary,
# which means a second full build — about twelve minutes against the twenty-six
# seconds this whole step takes. What still covers it: the FLOOR asserted in
# tests/test_index_resilience.c stops the signal being switched off, and the
# absolute counts for both sides are printed below on every run, so a jump is
# visible in the log even when it does not fail.
#
# Usage: self-index-coverage-gate.sh <path-to-codebase-memory-mcp-binary>
#
# NOTE ON PLATFORM: the ranges depend on which conditional-compilation branches
# the preprocessor keeps. On a machine where _WIN32 is defined the discarded
# branches swap and a different set of lines is flagged. That is why this runs
# on ONE CI leg and asserts proportions rather than exact line numbers.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/ci/self-index-coverage-gate.sh <path-to-codebase-memory-mcp-binary>

Index the merge base and this branch with the given binary, and fail when the
branch made the repository's own parse-coverage report worse (#963).

Four checks. Each one reads index_status for both trees and fails only on a
finding present on this branch and absent at the merge base:
  1. A file reports a whole-file parse failure (parse_unusable).
  2. A range list was clipped without saying so (a trailing "+<N>" marker).
  3. A single range covers more than 25% of a file of 200 lines or more.
  4. The flagged-file count is higher than the merge base's count.

The base commit is taken from COVERAGE_GATE_BASE_SHA, or from the first parent
when HEAD is a merge commit (CI checks out refs/pull/N/merge), or from
`git merge-base origin/main HEAD`.

Data files, both beside this script:
  coverage-gate-allowlist.txt   paths the checks skip, each with a written
                                reason above it.
  parse-partial-baseline.txt    a written record of the count, reported but
                                NOT enforced. Nothing fails on it.

Environment:
  COVERAGE_GATE_BASE_SHA The commit to compare against.
  MAX_SINGLE_RANGE_PCT   Share limit for check 3 (default 25).
  MIN_FILE_LINES         Files below this are exempt from check 3 (default 200).

Exit 0 when every check passes, 1 when any fails, 2 on a bad argument.

Options:
  -h, --help        This text.
EOF
}

BIN=""
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help) usage; exit 0 ;;
    -*) echo "self-index-coverage-gate: unknown argument '$1'. Please consult --help." >&2; exit 2 ;;
    *)
        [ -z "$BIN" ] || {
            echo "self-index-coverage-gate: one binary path only. Please consult --help." >&2
            exit 2
        }
        BIN="$1"
        ;;
    esac
    shift
done
[ -n "$BIN" ] || { echo "self-index-coverage-gate: need a binary path. Please consult --help." >&2; exit 2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ALLOWLIST="${REPO_ROOT}/scripts/ci/coverage-gate-allowlist.txt"
BASELINE_FILE="${REPO_ROOT}/scripts/ci/parse-partial-baseline.txt"

# Share of a file one range may cover before it stops being useful advice.
# The worst real offender today is src/cli/cli.c at 3.9%, so this has room.
MAX_SINGLE_RANGE_PCT="${MAX_SINGLE_RANGE_PCT:-25}"
# Files below this are exempt: a 5-line fixture with a 3-line range is 60% of
# itself and says nothing about report quality.
MIN_FILE_LINES="${MIN_FILE_LINES:-200}"

command -v jq >/dev/null || { echo "FAIL: jq is required"; exit 1; }

WORK="$(mktemp -d)"
# The runtime dir holds a unix socket, and a socket path has a hard length
# limit (~104 bytes). macOS puts mktemp under /var/folders/<long>/T/, which
# blows that limit and fails with "secure CLI coordination could not be
# created (endpoint)". Keep the runtime dir short and separate from the cache.
RUNTIME="/tmp/cbm-gate.$$"
BASE_TREE="${WORK}/base-tree"
cleanup() {
    # The base tree is a real git worktree, so it has a registration in the
    # repository that outlives a plain rm -rf. Retire it first, then the dirs.
    if [ -d "$BASE_TREE" ]; then
        git -C "$REPO_ROOT" worktree remove --force "$BASE_TREE" >/dev/null 2>&1 || true
    fi
    git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1 || true
    rm -rf "$WORK" "$RUNTIME"
}
trap cleanup EXIT
export CBM_RUNTIME_DIR="$RUNTIME"
mkdir -p "$RUNTIME"

FAILURES=0
note_failure() { echo "FAIL: $*"; FAILURES=$((FAILURES + 1)); }

# ── Which commit are we comparing against? ────────────────────────────────
# CI checks out refs/pull/N/merge, so the first parent IS the base commit.
# The env var comes from the workflow and wins, because it is the value
# GitHub itself used to build that merge.
resolve_base() {
    if [ -n "${COVERAGE_GATE_BASE_SHA:-}" ]; then
        printf '%s' "$COVERAGE_GATE_BASE_SHA"
        return 0
    fi
    if git -C "$REPO_ROOT" rev-parse --verify -q 'HEAD^2' >/dev/null 2>&1; then
        git -C "$REPO_ROOT" rev-parse 'HEAD^1'
        return 0
    fi
    git -C "$REPO_ROOT" merge-base origin/main HEAD 2>/dev/null || return 1
}

BASE_SHA="$(resolve_base || true)"
[ -n "$BASE_SHA" ] || { echo "FAIL: could not work out the base commit — set COVERAGE_GATE_BASE_SHA"; exit 1; }

# The CI checkout is shallow, so the base commit's tree may not be present.
if ! git -C "$REPO_ROOT" cat-file -e "${BASE_SHA}^{tree}" 2>/dev/null; then
    echo "==> fetching base commit ${BASE_SHA}"
    git -C "$REPO_ROOT" fetch --no-tags --depth=1 origin "$BASE_SHA" >/dev/null 2>&1 || {
        echo "FAIL: could not fetch base commit ${BASE_SHA}"; exit 1; }
fi

# A real worktree rather than an archive: the head side IS a git checkout, and
# the indexer's git passes must see the same shape on both sides. It lives
# outside REPO_ROOT so the head index never walks into it.
git -C "$REPO_ROOT" worktree add --detach "$BASE_TREE" "$BASE_SHA" >/dev/null 2>&1 || {
    echo "FAIL: could not check out base commit ${BASE_SHA}"; exit 1; }

# Allowlisted paths, comments and blanks stripped. Applied to BOTH sides, so
# an allowlisted path can neither fail the branch nor mask a base finding.
ALLOWED="${WORK}/allowed.txt"
: > "$ALLOWED"
[ -f "$ALLOWLIST" ] && sed -e 's/#.*//' -e 's/[[:space:]]*$//' "$ALLOWLIST" \
    | grep -v '^$' > "$ALLOWED" || true

# ── Index one tree and write its findings as sorted path lists ────────────
# Writes <prefix>.unusable, <prefix>.truncated, <prefix>.wide and
# <prefix>.count. Fails the run outright when index_status clipped its own
# file list, because the three list-based checks would then judge only part of
# the tree and still print PASS — the same silent clipping this gate exists to
# catch.
analyze_tree() {
    tree_root="$1"
    prefix="$2"
    label="$3"

    cache="${WORK}/cache-${label}"
    mkdir -p "$cache"
    export CBM_CACHE_DIR="$cache"

    echo "==> indexing ${label} tree with $(basename "$BIN")"
    "$BIN" cli index_repository --repo-path "$tree_root" --mode full --json \
        > "${prefix}.index.json" 2>"${prefix}.index.err" || {
            echo "FAIL: index_repository exited non-zero for the ${label} tree"
            tail -20 "${prefix}.index.err"; exit 1; }

    project="$(jq -r '.structuredContent.project // empty' "${prefix}.index.json")"
    [ -n "$project" ] || { echo "FAIL: index_repository did not name a ${label} project"; exit 1; }

    "$BIN" cli index_status --project "$project" --json > "${prefix}.status.json" 2>/dev/null || {
        echo "FAIL: index_status exited non-zero for the ${label} tree"; exit 1; }

    status="${prefix}.status.json"

    for cls in parse_partial parse_unusable; do
        clipped="$(jq -r --arg c "$cls" '.structuredContent[$c].truncated // false' "$status")"
        if [ "$clipped" = "true" ]; then
            note_failure "index_status clipped its ${cls} file list on the ${label} tree — the checks below would see only part of it"
        fi
    done

    # 1. Whole-file parse failures.
    jq -r '.structuredContent.parse_unusable.files[]?.path' "$status" \
        | grep -vxF -f "$ALLOWED" 2>/dev/null | sort -u > "${prefix}.unusable" || : > "${prefix}.unusable"

    # 2. Range lists the producer's cap clipped, marked with a trailing "+<N>".
    jq -r '.structuredContent.parse_partial.files[]?
        | select(.error_ranges? // "" | test("\\+[0-9]+$")) | .path' "$status" \
        | grep -vxF -f "$ALLOWED" 2>/dev/null | sort -u > "${prefix}.truncated" || : > "${prefix}.truncated"

    # 3. One range covering more than its share of the file. The line count
    #    comes from the tree being analysed, because a file can grow or shrink
    #    between the two commits.
    : > "${prefix}.wide"
    while IFS=$'\t' read -r path ranges; do
        [ -n "$path" ] || continue
        grep -qxF "$path" "$ALLOWED" && continue
        [ -f "${tree_root}/${path}" ] || continue
        total="$(wc -l < "${tree_root}/${path}" | tr -d ' ')"
        [ "$total" -ge "$MIN_FILE_LINES" ] || continue
        widest="$(printf '%s' "$ranges" | tr ',' '\n' | grep '^[0-9]' \
            | awk -F- '{d=$2-$1+1; if (d>m) m=d} END {print m+0}')"
        pct="$(awk -v a="$widest" -v b="$total" 'BEGIN{printf "%.1f", 100*a/b}')"
        over="$(awk -v p="$pct" -v lim="$MAX_SINGLE_RANGE_PCT" 'BEGIN{print (p>lim)?1:0}')"
        if [ "$over" = "1" ]; then
            printf '%s\t%s\t%s\t%s\n' "$path" "$widest" "$pct" "$total" >> "${prefix}.wide"
        fi
    done < <(jq -r '.structuredContent.parse_partial.files[]?
        | "\(.path)\t\(.error_ranges // "")"' "$status")
    sort -u -o "${prefix}.wide" "${prefix}.wide"

    # 4. The flagged-file count.
    jq -r '.structuredContent.parse_partial.count // 0' "$status" > "${prefix}.count"
}

analyze_tree "$BASE_TREE" "${WORK}/base" base
analyze_tree "$REPO_ROOT" "${WORK}/head" head

BASE_COUNT="$(cat "${WORK}/base.count")"
HEAD_COUNT="$(cat "${WORK}/head.count")"

# ── The four checks, each on what the branch ADDED ────────────────────────
while read -r p; do
    [ -n "$p" ] || continue
    note_failure "$p reports a whole-file parse failure on this branch and not at the merge base"
done < <(comm -13 "${WORK}/base.unusable" "${WORK}/head.unusable")

while read -r p; do
    [ -n "$p" ] || continue
    note_failure "$p carries a +N truncation marker on this branch and not at the merge base — its range list was clipped"
done < <(comm -13 "${WORK}/base.truncated" "${WORK}/head.truncated")

# Compare by path, not by the whole row: a file already over the share at the
# merge base must not fail here just because the range moved by a line.
cut -f1 "${WORK}/base.wide" | sort -u > "${WORK}/base.wide.paths"
while IFS=$'\t' read -r path widest pct total; do
    [ -n "$path" ] || continue
    grep -qxF "$path" "${WORK}/base.wide.paths" && continue
    note_failure "$path has one range of ${widest} lines — ${pct}% of ${total}, over ${MAX_SINGLE_RANGE_PCT}%, and it was within the share at the merge base"
done < "${WORK}/head.wide"

if [ "$HEAD_COUNT" -gt "$BASE_COUNT" ]; then
    note_failure "parse_partial_count is ${HEAD_COUNT} on this branch against ${BASE_COUNT} at the merge base"
fi

# ── Report ───────────────────────────────────────────────────────────────
# parse-partial-baseline.txt is a written record, not a gate. It is printed so
# a reader can see the count drift, and nothing fails on it: enforcing it is
# what turned an unrelated branch red when main moved (#1972).
RECORDED="$(awk '{ sub(/#.*/, "") } match($0, /[0-9]+/) { print substr($0, RSTART, RLENGTH); exit }' \
    "$BASELINE_FILE" 2>/dev/null || echo "")"
echo "==> base ${BASE_SHA}: parse_partial=${BASE_COUNT} unusable=$(wc -l < "${WORK}/base.unusable" | tr -d ' ')"
echo "==> head:            parse_partial=${HEAD_COUNT} unusable=$(wc -l < "${WORK}/head.unusable" | tr -d ' ')"
echo "==> allowlisted=$(wc -l < "$ALLOWED" | tr -d ' ') recorded=${RECORDED:-none} (record only, not enforced)"
if [ -n "$RECORDED" ] && [ "$HEAD_COUNT" != "$RECORDED" ]; then
    echo "NOTE: parse_partial_count is ${HEAD_COUNT}, and $(basename "$BASELINE_FILE") records ${RECORDED}."
    echo "NOTE: that is not a failure. If this branch changed the extractor, both sides above moved together and this gate cannot tell."
fi

if [ "$FAILURES" -gt 0 ]; then
    echo "FAIL: ${FAILURES} coverage-gate check(s) failed"
    exit 1
fi
echo "PASS: this branch did not make the parse-coverage report worse than the merge base"
