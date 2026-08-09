#!/usr/bin/env bash
# Contract for the external UI asset pack. The pack deliberately stays
# uncompressed and independently scannable: this is an asset boundary, not an
# attempt to hide frontend bytes from security tooling.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-ui-pack-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

make_real_dir_link() {
    local target="$1" link="$2"
    if MSYS=winsymlinks:nativestrict ln -s -- "$target" "$link" 2>/dev/null &&
        [ -L "$link" ]; then
        return 0
    fi
    rm -rf -- "$link"
    case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
        cmd //c "mklink /J \"$(cygpath -w "$link")\" \"$(cygpath -w "$target")\"" \
            >/dev/null 2>&1 || true
        ;;
    esac
    [ -L "$link" ]
}

mkdir -p "$WORK/a/assets" "$WORK/b/assets"
printf '%s\n' '<!doctype html><title>Codebase Memory — Graph</title>' > "$WORK/a/index.html"
printf '%s\n' 'console.log("transparent-ui-pack")' > "$WORK/a/assets/app.js"
printf '%s\n' 'body { color: #fff; }' > "$WORK/a/assets/app.css"

# Copy in a deliberately different creation order. Filesystem enumeration,
# mtimes, and source-root names must not affect the output.
cp "$WORK/a/assets/app.css" "$WORK/b/assets/app.css"
cp "$WORK/a/index.html" "$WORK/b/index.html"
cp "$WORK/a/assets/app.js" "$WORK/b/assets/app.js"
touch -t 200101010101 "$WORK/a/index.html"
touch -t 203001010101 "$WORK/b/index.html"

mkdir -p "$WORK/a-out" "$WORK/b-out"
node "$ROOT/scripts/pack-ui-assets.mjs" \
    "$WORK/a" "$WORK/a-out" "$WORK/a-manifest.c"
node "$ROOT/scripts/pack-ui-assets.mjs" \
    "$WORK/b" "$WORK/b-out" "$WORK/b-manifest.c"

PACK_A="$(find "$WORK/a-out" -maxdepth 1 -type f -name 'cbm-ui-*.pack')"
PACK_B="$(find "$WORK/b-out" -maxdepth 1 -type f -name 'cbm-ui-*.pack')"
[ -n "$PACK_A" ] && [ -n "$PACK_B" ]
[ "$(basename "$PACK_A")" = "$(basename "$PACK_B")" ]
cmp "$PACK_A" "$PACK_B"
cmp "$WORK/a-manifest.c" "$WORK/b-manifest.c"

python3 - "$PACK_A" "$WORK/a-manifest.c" <<'PY'
from __future__ import annotations

import hashlib
import pathlib
import re
import struct
import sys

pack_path = pathlib.Path(sys.argv[1])
manifest_path = pathlib.Path(sys.argv[2])
data = pack_path.read_bytes()
manifest = manifest_path.read_text(encoding="utf-8")

(
    magic,
    version,
    header_bytes,
    flags,
    count,
    entry_bytes,
    index_offset,
    index_size,
    paths_offset,
    paths_size,
    payload_offset,
    payload_size,
    total_size,
) = struct.unpack_from("<8sHHIIIQQQQQQQ", data, 0)
assert magic == b"CBMUIPK\0", "wrong pack magic"
assert version == 1
assert header_bytes == 80
assert flags == 0
assert count == 3
assert entry_bytes == 24
assert index_offset == 80
assert index_size == count * entry_bytes
assert paths_offset == index_offset + index_size
assert payload_offset == paths_offset + paths_size
assert payload_size == len(data) - payload_offset
assert total_size == len(data)

cursor = index_offset
entries = []
for _ in range(count):
    path_offset, path_len, mime_id, cache_id, offset, length = struct.unpack_from(
        "<IHBBQQ", data, cursor
    )
    cursor += 24
    path = data[
        paths_offset + path_offset:paths_offset + path_offset + path_len
    ].decode("ascii")
    entries.append((path, path_offset, path_len, mime_id, cache_id, offset, length))

assert cursor == paths_offset
assert [entry[0] for entry in entries] == [
    "/assets/app.css", "/assets/app.js", "/index.html"
]
assert [entry[3] for entry in entries] == [3, 2, 1]
assert [entry[4] for entry in entries] == [2, 2, 1]

path_cursor = 0
for path, path_offset, path_len, *_rest in entries:
    assert path_offset == path_cursor, f"non-contiguous path table for {path}"
    path_cursor += path_len
assert path_cursor == paths_size

payload_cursor = 0
for path, _path_offset, _path_len, _mime, _cache, offset, length in entries:
    assert offset == payload_cursor, f"non-contiguous payload for {path}"
    payload_cursor += length
assert payload_cursor == payload_size

# The bytes remain visible to scanners and SBOM/license tooling. Compression
# or encoding would make this assertion fail.
assert b"transparent-ui-pack" in data
assert b"<!doctype html>" in data

digest = hashlib.sha256(data).hexdigest()
assert re.search(rf'CBM_UI_ASSET_SHA256\[\]\s*=\s*"{digest}"', manifest)
assert re.search(rf"CBM_UI_ASSET_SIZE\s*=\s*UINT64_C\({len(data)}\)", manifest)
assert re.search(
    rf'CBM_UI_ASSET_PACK_NAME\[\]\s*=\s*"cbm-ui-{digest}\.pack"', manifest
)
assert pack_path.name == f"cbm-ui-{digest}.pack"
PY

# A source symlink makes the build depend on bytes outside the declared dist
# tree. MSYS2's default `ln -s` silently deep-copies its target, so require a
# real native link and fall back to an NTFS junction before testing rejection.
mkdir "$WORK/outside-assets"
printf '%s\n' 'outside the declared UI root' > "$WORK/outside-assets/linked.html"
if ! make_real_dir_link "$WORK/outside-assets" "$WORK/a/assets/linked"; then
    echo "FAIL: could not create a real symlink/junction fixture" >&2
    exit 1
fi
if rejected_output="$(node "$ROOT/scripts/pack-ui-assets.mjs" \
    "$WORK/a" "$WORK/a-out" "$WORK/rejected.c" 2>&1)"; then
    echo "FAIL: UI pack generator accepted a symlink" >&2
    exit 1
fi
case "$rejected_output" in
*"symbolic links are not allowed"*) ;;
*)
    echo "FAIL: UI pack generator rejected the symlink for the wrong reason" >&2
    printf '%s\n' "$rejected_output" >&2
    exit 1
    ;;
esac

echo "PASS: deterministic, transparent, hash-bound UI asset pack"
