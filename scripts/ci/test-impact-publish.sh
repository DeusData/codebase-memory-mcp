#!/usr/bin/env bash
# test-impact-publish.sh — publish the team artifact bundle of the checked-out
# main commit (.github/workflows/test-impact-artifact.yml).
#
# Incremental (a push): the previous commit's bundle is the graph base and its
# coverage map the starting point; only the suites the push's own selection
# picks are re-run with coverage and merged in (scripts/test-impact/
# coverage-merge.py). The oldest observation is carried forward, so an
# incremental map ages out unless a full refresh runs.
# Full (weekly, manual, or no usable previous bundle): every suite is re-run
# and the graph is built from scratch. A weekly run ALSO builds the graph
# incrementally from the previous bundle and fails when the two graphs'
# content digests differ: an incremental index must equal a full one.
#
# Usage: scripts/ci/test-impact-publish.sh --binary PATH --runner PATH --out DIR
#          --mode incremental|full [--before SHA] [--llvm-bin DIR] [--platform LABEL]
# Exit: 0 = published · 1 = failed · 2 = usage error.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="" RUNNER="" OUT="" MODE="" BEFORE="" LLVM_BIN="" PLATFORM="linux-x86_64-clang21"
while [ $# -gt 0 ]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        --runner) RUNNER="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --before) BEFORE="$2"; shift 2 ;;
        --llvm-bin) LLVM_BIN="$2"; shift 2 ;;
        --platform) PLATFORM="$2"; shift 2 ;;
        *) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done
[ -n "$BINARY" ] && [ -n "$RUNNER" ] && [ -n "$OUT" ] || exit 2
case "$MODE" in incremental | full) ;; *) exit 2 ;; esac

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# A private daemon rendezvous: the socket path must fit sun_path.
export CBM_RUNTIME_DIR="$(mktemp -d /tmp/cbm-rt.XXXXXX)"
export CBM_CACHE_DIR="$WORK/cache"
mkdir -p "$WORK/engine" && chmod 700 "$WORK/engine"
HEAD_SHA="$(git -C "$ROOT" rev-parse HEAD)"
PREVIOUS=""

if [ -n "$BEFORE" ]; then
    if scripts/ci/test-impact-bundle.sh --fetch "$BEFORE" "$WORK/previous"; then
        PREVIOUS="$WORK/previous"
    fi
fi
if [ "$MODE" = incremental ] && [ -z "$PREVIOUS" ]; then
    echo "test-impact-publish: no previous bundle for ${BEFORE:-?}: full refresh"
    MODE=full
fi

map() { # out [suite...]
    local out="$1"; shift
    local args=()
    for suite in "$@"; do args+=(--suite "$suite"); done
    python3 "$ROOT/scripts/test-impact/coverage-map.py" --runner "$RUNNER" --out "$out" \
        ${LLVM_BIN:+--llvm-bin "$LLVM_BIN"} ${args[@]+"${args[@]}"}
}

OBSERVED=""
if [ "$MODE" = full ]; then
    OBSERVED="$(date +%s)"
    map "$WORK/coverage"
else
    # The push's own selection decides what to re-run (whole suites).
    "$BINARY" test-impact select --repo "$ROOT" --base "$BEFORE" --work "$WORK/engine" \
        --artifact "$PREVIOUS" --artifact-commit "$BEFORE" --artifact-verified \
        --platform "$PLATFORM" > "$WORK/selection.json"
    python3 "$ROOT/scripts/test-impact/selection.py" suites "$WORK/selection.json" > "$WORK/suites.txt"
    "$RUNNER" --list-suites > "$WORK/listed.txt"
    if grep -qx '\*' "$WORK/suites.txt"; then
        OBSERVED="$(date +%s)"
        map "$WORK/coverage"
    else
        mapfile -t rerun < <(sort -u "$WORK/suites.txt")
        map "$WORK/fresh" ${rerun[@]+"${rerun[@]}"}
        python3 "$ROOT/scripts/test-impact/coverage-merge.py" --previous "$PREVIOUS/coverage" \
            --fresh "$WORK/fresh" --suites "$WORK/listed.txt" --commit "$HEAD_SHA" \
            --out "$WORK/coverage"
    fi
fi

publish() { # out [previous]
    "$BINARY" test-impact publish --repo "$ROOT" --out "$1" --work "$WORK/engine" \
        --coverage "$WORK/coverage" ${OBSERVED:+--observed-at "$OBSERVED"} \
        --platform "$PLATFORM" ${2:+--previous "$2"}
}
if [ "$MODE" = full ]; then
    publish "$OUT"
    if [ -n "$PREVIOUS" ]; then
        publish "$WORK/incremental" "$PREVIOUS"
        field() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }
        # Topology (what selection reads) must be equal; edge properties and the
        # corpus-statistical edges are reported, not gated (recorded defects).
        full_topology="$(field "$OUT/receipt.json" graph_topology_sha256)"
        incr_topology="$(field "$WORK/incremental/receipt.json" graph_topology_sha256)"
        if [ "$full_topology" != "$incr_topology" ]; then
            echo "test-impact-publish: incremental topology differs from the full one ($incr_topology != $full_topology)" >&2
            exit 1
        fi
        if [ "$(field "$OUT/receipt.json" graph_content_sha256)" != \
             "$(field "$WORK/incremental/receipt.json" graph_content_sha256)" ]; then
            echo "::warning::incremental graph content (edge properties, similarity edges) differs from the full one; topology is equal"
        fi
        echo "test-impact-publish: incremental topology equals the full one ($full_topology)"
    fi
else
    publish "$OUT" "$PREVIOUS"
fi
echo "test-impact-publish: bundle of $HEAD_SHA in $OUT ($MODE)"
