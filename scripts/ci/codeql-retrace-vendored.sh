#!/usr/bin/env bash
# codeql-retrace-vendored.sh — prebuilt C/C++ translation units to retrace,
# for the PR c-cpp CodeQL scan (.github/workflows/codeql.yml).
#
# A PR's c-cpp scan compiles the third-party objects BEFORE CodeQL starts
# tracing (scripts/build.sh --vendored-only), so vendored C stays out of the
# analysis, while ALL first-party C is traced -- except the vendored files the
# PR itself changes, including grammar/runtime wrappers. This lists their
# compilation entrypoints; scripts/build.sh --reuse-vendored --retrace-from
# <list> then rebuilds their objects under tracing. Use direct prerequisites
# for make -W: its shallow wildcard dependencies miss nested headers/sources,
# and compiler include paths can contain aliases such as ../unix/prim.c.
#
# The changed files come from the PR merge commit (HEAD^1 is the base it was
# merged onto, as in pr.yml's file-list fallback; needs fetch-depth 2) unless
# --changed FILE gives them. An undeterminable diff (no pull request, no
# parent, git failing, an unreadable FILE) is NOT an error and never narrows
# the scan: the list is empty and the scan still covers all first-party C.
#
# Usage: scripts/ci/codeql-retrace-vendored.sh [--changed FILE] OUT
#   OUT is written with the entrypoints to retrace, one per line (may be
#   empty). The scope test is scripts/ci/select-lanes.sh --codeql-scope c-cpp.
# Exit: 0 = list written · 2 = usage error.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CHANGED_FILE=""
OUT=""
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    --changed)
        CHANGED_FILE="${2:-}"
        [ -n "$CHANGED_FILE" ] || {
            echo "codeql-retrace-vendored.sh: --changed needs a file. Please consult --help." >&2
            exit 2
        }
        shift
        ;;
    -*)
        echo "codeql-retrace-vendored.sh: unknown option '$1'. Please consult --help." >&2
        exit 2
        ;;
    *)
        if [ -n "$OUT" ]; then
            echo "codeql-retrace-vendored.sh: more than one output file. Please consult --help." >&2
            exit 2
        fi
        OUT="$1"
        ;;
    esac
    shift
done
if [ -z "$OUT" ]; then
    echo "codeql-retrace-vendored.sh: no output file. Please consult --help." >&2
    exit 2
fi

changed=""
determined=false
if [ -n "$CHANGED_FILE" ]; then
    if [ -r "$CHANGED_FILE" ]; then
        changed=$(cat "$CHANGED_FILE")
        determined=true
    fi
elif [ "${GITHUB_EVENT_NAME:-}" = pull_request ]; then
    if changed=$(git -C "$ROOT" diff --name-only --no-renames HEAD^1 HEAD 2>/dev/null); then
        determined=true
    fi
fi

: >"$OUT"
if ! $determined; then
    echo "diff undetermined: no vendored file to retrace; the scan covers all first-party C"
    exit 0
fi
printf '%s\n' "$changed" |
    bash "$ROOT/scripts/ci/select-lanes.sh" --codeql-scope c-cpp |
    while IFS= read -r path; do
        entrypoints=()
        case "$path" in
        internal/cbm/grammar_*.c | internal/cbm/ts_runtime.c)
            entrypoints=("$path")
            ;;
        vendored/mimalloc/*) entrypoints=(vendored/mimalloc/src/static.c) ;;
        vendored/sqlite3/*) entrypoints=(vendored/sqlite3/sqlite3.c) ;;
        vendored/tre/*) entrypoints=(vendored/tre/tre_all.c) ;;
        internal/cbm/vendored/lz4/*)
            entrypoints=(internal/cbm/vendored/lz4/lz4.c internal/cbm/vendored/lz4/lz4hc.c)
            ;;
        internal/cbm/vendored/zstd/*) entrypoints=(internal/cbm/vendored/zstd/zstd.c) ;;
        internal/cbm/vendored/ts_runtime/*) entrypoints=(internal/cbm/ts_runtime.c) ;;
        internal/cbm/vendored/grammars/*)
            family="${path#internal/cbm/vendored/grammars/}"
            wrapper="internal/cbm/grammar_${family%%/*}.c"
            if [ -f "$ROOT/$wrapper" ]; then
                entrypoints=("$wrapper")
            else
                # Shared or unfamiliar grammar code can have many consumers.
                entrypoints=("$ROOT"/internal/cbm/grammar_*.c)
            fi
            ;;
        internal/cbm/vendored/common/*)
            entrypoints=("$ROOT"/internal/cbm/grammar_*.c)
            ;;
        internal/cbm/vendored/* | vendored/*)
            # Other vendored code is compiled with the first-party build.
            entrypoints=("$path")
            ;;
        esac
        for entrypoint in "${entrypoints[@]+"${entrypoints[@]}"}"; do
            entrypoint="${entrypoint#"$ROOT"/}"
            if [ -f "$ROOT/$entrypoint" ]; then printf '%s\n' "$entrypoint"; fi
        done
    done | LC_ALL=C sort -u >"$OUT"
count=$(wc -l <"$OUT" | tr -d ' ')
echo "prebuilt translation units to retrace: $count"
cat "$OUT"
