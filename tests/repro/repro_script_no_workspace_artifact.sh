#!/usr/bin/env bash
# Reproduce scripts/repro.sh leaving its generated runner transcript in the
# repository root after a completed board run.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-repro-artifact.XXXXXX")"

cleanup() {
    rm -f "$TMP_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/env.sh"
    rm -f "$TMP_ROOT/repro-out.txt" "$TMP_ROOT/summary.txt"
    rmdir "$TMP_ROOT/scripts" "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$TMP_ROOT/scripts"
cp "$PROJECT_ROOT/scripts/repro.sh" "$TMP_ROOT/scripts/repro.sh"
cp "$PROJECT_ROOT/scripts/env.sh" "$TMP_ROOT/scripts/env.sh"

make() {
    printf '%s\n' \
        '────────────────────────────────────────────' \
        '  1 passed'
    return 0
}
export -f make

TMPDIR="$TMP_ROOT" CC=true CXX=true GITHUB_STEP_SUMMARY="$TMP_ROOT/summary.txt" \
    bash "$TMP_ROOT/scripts/repro.sh" >/dev/null 2>&1

if [[ -e "$TMP_ROOT/repro-out.txt" ]]; then
    printf '[repro-artifact] invariant=workspace_transcript_left_behind path=repro-out.txt\n' >&2
    exit 1
fi

printf '[repro-artifact] workspace transcript cleaned correctly\n'
