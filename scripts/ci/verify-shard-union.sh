#!/usr/bin/env bash
# verify-shard-union.sh — prove the sharded test legs lost nothing.
#
# Canonical CI step (called by _test.yml's shard-completeness job on the
# downloaded shard-manifest-* artifacts). Lived inline in workflow YAML until
# 2026-07-26; the venue-parity contract forbids logic in workflow run-blocks.
#
# For every leg it asserts: all shards agree on the shard count, indices form
# exactly 1..n, every shard saw the same full suite list, and the UNION of the
# shard slices equals that list — a rename/re-shard can never silently drop a
# suite (gate-quality loss) without failing here. A narrowed leg (PR CI test
# selection) records the selected list and its selection_sha256; all shards
# must agree on that too.
#
# No manifests at all is a failure — unless the PR's lane selection (--lanes,
# the JSON list from scripts/ci/select-lanes.sh) holds no test leg, in which
# case no leg ran by design. Without --lanes (or with "all": dry run,
# release) every leg is expected, exactly as before lane selection.
#
# Usage: scripts/ci/verify-shard-union.sh <manifests-dir> [--lanes <json-list>|all]
set -eu

case "${1:-}" in
-h | --help)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

MANIFEST_DIR="${1:?usage: verify-shard-union.sh <manifests-dir> (see --help)}"
LANES=all
if [ "${2:-}" = "--lanes" ]; then
    LANES="${3:-}"
elif [ -n "${2:-}" ]; then
    echo "verify-shard-union.sh: unknown argument '$2'. Please consult --help." >&2
    exit 2
fi
case "$LANES" in
all | \[*\]) ;;
*)
    echo "verify-shard-union.sh: --lanes must be 'all' or a JSON list. Please consult --help." >&2
    exit 2
    ;;
esac

files=""
if [ -d "$MANIFEST_DIR" ]; then
    files=$(find "$MANIFEST_DIR" -name shard-manifest.txt | sort)
fi
if [ -z "$files" ]; then
    # The test legs are the sharded ones: unix-* and windows (matched with its
    # quotes, so the windows-guards lane never reads as the windows leg).
    case "$LANES" in
    all | *\"unix-* | *\"windows\"*)
        echo "FAIL: no shard manifests were uploaded" >&2
        exit 1
        ;;
    esac
    echo "OK: no test leg was selected ($LANES) — no manifests expected"
    exit 0
fi
rc=0
for leg in $(grep -h '^leg=' $files | sort -u | sed 's/^leg=//'); do
    leg_files=$(grep -l "^leg=$leg\$" $files)
    n=$(grep -h '^shard=' $leg_files | sed 's|.*/||' | sort -u)
    if [ "$(printf '%s\n' "$n" | wc -l)" -ne 1 ]; then
        echo "FAIL: $leg shards disagree on shard count: $n" >&2
        rc=1
        continue
    fi
    indices=$(grep -h '^shard=' $leg_files | sed 's/^shard=//;s|/.*||' | sort -n)
    if [ "$indices" != "$(seq 1 "$n")" ]; then
        echo "FAIL: $leg shard indices [$indices] != 1..$n" >&2
        rc=1
        continue
    fi
    list_sha=$(grep -h '^list_sha256=' $leg_files | sed 's/^list_sha256=//' | sort -u)
    if [ "$(printf '%s\n' "$list_sha" | wc -l)" -ne 1 ]; then
        echo "FAIL: $leg shards saw different suite lists" >&2
        rc=1
        continue
    fi
    # A narrowed leg: every shard must have applied the same test selection
    # (two selections can name the same suites and different tests).
    sel_sha=$(for f in $leg_files; do
        grep -h '^selection_sha256=' "$f" || echo "selection_sha256=none"
    done | sort -u)
    if [ "$(printf '%s\n' "$sel_sha" | wc -l)" -ne 1 ]; then
        echo "FAIL: $leg shards applied different test selections" >&2
        rc=1
        continue
    fi
    union_sha=$(for f in $leg_files; do
        sed -n '/^--- slice ---$/,$p' "$f" | tail -n +2
    done | sort | { sha256sum 2>/dev/null || shasum -a 256; } | awk '{print $1}')
    if [ "$union_sha" != "$list_sha" ]; then
        echo "FAIL: $leg union of shard slices != full suite list (GATE-QUALITY LOSS)" >&2
        rc=1
        continue
    fi
    echo "OK: $leg — $n shard(s), union of slices == full suite list"
done
exit "$rc"
