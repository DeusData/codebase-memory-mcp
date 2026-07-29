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
# suite (gate-quality loss) without failing here.
#
# Usage: scripts/ci/verify-shard-union.sh <manifests-dir>
set -eu

case "${1:-}" in
-h | --help)
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

MANIFEST_DIR="${1:?usage: verify-shard-union.sh <manifests-dir> (see --help)}"

files=$(find "$MANIFEST_DIR" -name shard-manifest.txt | sort)
if [ -z "$files" ]; then
    echo "FAIL: no shard manifests were uploaded" >&2
    exit 1
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
    union_sha=$(for f in $leg_files; do
        sed -n '/^--- slice ---$/,$p' "$f" | tail -n +2
    done | sort | sha256sum | awk '{print $1}')
    if [ "$union_sha" != "$list_sha" ]; then
        echo "FAIL: $leg union of shard slices != full suite list (GATE-QUALITY LOSS)" >&2
        rc=1
        continue
    fi
    echo "OK: $leg — $n shard(s), union of slices == full suite list"
done
exit "$rc"
