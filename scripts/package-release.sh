#!/usr/bin/env bash
# package-release.sh — canonical, byte-preserving release packager.
#
# Candidate derivation (including strip and macOS signing) happens before this
# boundary. This script accepts one already-final executable plus its expected
# SHA-256 and only copies that byte sequence into the release containers. A
# hash mismatch at any boundary is fatal.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/package-release.sh <goos> <goarch> \
         --selected-binary FILE --expected-sha256 HEX [OPTIONS]

Package an already-final release executable without changing its bytes.

Required:
  --selected-binary FILE       selected candidate to package
  --expected-sha256 HEX        SHA-256 of FILE (64 hexadecimal characters)

Options:
  --third-party-notices FILE   pre-generated notices file (otherwise generated)
  --out-dir DIR                output directory (default: repository root)

Environment:
  VERSION                      MCPB manifest version (v-prefix accepted;
                               default: 0.0.0-dev)

Targets are the eight existing release products:
  linux/{amd64,arm64,amd64-portable,arm64-portable}
  darwin/{amd64,arm64}
  windows/{amd64,arm64}

Archive contents — one executable, no sidecars:
  unix:    codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md (.tar.gz)
  windows: codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md (.zip)

MCPB bundle (.mcpb): every darwin/windows target and static linux target.
The executable member in every produced container is verified against
--expected-sha256 before anything is published.
EOF
}

GOOS=""
GOARCH=""
SELECTED_BINARY=""
EXPECTED_SHA256=""
THIRD_PARTY_NOTICES=""
OUT_DIR="$ROOT"
expect_value=""
for arg in "$@"; do
    case "$expect_value" in
    selected-binary) SELECTED_BINARY="$arg"; expect_value=""; continue ;;
    expected-sha256) EXPECTED_SHA256="$arg"; expect_value=""; continue ;;
    third-party-notices) THIRD_PARTY_NOTICES="$arg"; expect_value=""; continue ;;
    out-dir) OUT_DIR="$arg"; expect_value=""; continue ;;
    esac
    case "$arg" in
    -h | --help) usage; exit 0 ;;
    --selected-binary) expect_value="selected-binary" ;;
    --selected-binary=*) SELECTED_BINARY="${arg#--selected-binary=}" ;;
    --expected-sha256) expect_value="expected-sha256" ;;
    --expected-sha256=*) EXPECTED_SHA256="${arg#--expected-sha256=}" ;;
    --third-party-notices) expect_value="third-party-notices" ;;
    --third-party-notices=*) THIRD_PARTY_NOTICES="${arg#--third-party-notices=}" ;;
    --out-dir) expect_value="out-dir" ;;
    --out-dir=*) OUT_DIR="${arg#--out-dir=}" ;;
    -*)
        echo "package-release: unknown option '$arg'. Please consult --help." >&2
        exit 2
        ;;
    *=*)
        echo "package-release: build variables are not accepted; package already-final bytes." >&2
        exit 2
        ;;
    *)
        if [ -z "$GOOS" ]; then GOOS="$arg"
        elif [ -z "$GOARCH" ]; then GOARCH="$arg"
        else
            echo "package-release: unexpected argument '$arg'. Please consult --help." >&2
            exit 2
        fi
        ;;
    esac
done

[ -z "$expect_value" ] || {
    echo "package-release: --$expect_value needs a value." >&2
    exit 2
}
[ -n "$GOOS" ] && [ -n "$GOARCH" ] || { usage >&2; exit 2; }
case "$GOOS/$GOARCH" in
linux/amd64 | linux/arm64 | linux/amd64-portable | linux/arm64-portable | \
darwin/amd64 | darwin/arm64 | windows/amd64 | windows/arm64) ;;
*)
    echo "package-release: unsupported release target '$GOOS/$GOARCH'." >&2
    exit 2
    ;;
esac
[ -n "$SELECTED_BINARY" ] || {
    echo "package-release: --selected-binary is required." >&2
    exit 2
}
[ -f "$SELECTED_BINARY" ] || {
    echo "package-release: selected binary not found: $SELECTED_BINARY" >&2
    exit 2
}
[ -n "$EXPECTED_SHA256" ] || {
    echo "package-release: --expected-sha256 is required." >&2
    exit 2
}
EXPECTED_SHA256="$(printf '%s' "$EXPECTED_SHA256" | tr '[:upper:]' '[:lower:]')"
if ! [[ "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "package-release: --expected-sha256 must be exactly 64 hexadecimal characters." >&2
    exit 2
fi

# Resolve all caller-owned inputs before creating the private packaging area.
SELECTED_BINARY="$(cd "$(dirname "$SELECTED_BINARY")" && pwd -P)/$(basename "$SELECTED_BINARY")"
if [ -n "$THIRD_PARTY_NOTICES" ]; then
    [ -f "$THIRD_PARTY_NOTICES" ] || {
        echo "package-release: notices file not found: $THIRD_PARTY_NOTICES" >&2
        exit 2
    }
    THIRD_PARTY_NOTICES="$(cd "$(dirname "$THIRD_PARTY_NOTICES")" && pwd -P)/$(basename "$THIRD_PARTY_NOTICES")"
fi
OUT_DIR="$(mkdir -p "$OUT_DIR" && cd "$OUT_DIR" && pwd -P)"
NAME="codebase-memory-mcp-${GOOS}-${GOARCH}"

sha256_file() {
    python3 - "$1" <<'PY'
import hashlib
import pathlib
import sys

digest = hashlib.sha256()
with pathlib.Path(sys.argv[1]).open("rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

assert_expected_hash() {
    local path="$1"
    local label="$2"
    local actual
    actual="$(sha256_file "$path")"
    if [ "$actual" != "$EXPECTED_SHA256" ]; then
        echo "package-release: $label SHA-256 mismatch: expected $EXPECTED_SHA256, got $actual" >&2
        return 1
    fi
}

extract_zip_member() {
    local archive="$1"
    local member="$2"
    local destination="$3"
    python3 - "$archive" "$member" "$destination" <<'PY'
import pathlib
import shutil
import stat
import sys
import zipfile

archive_path = pathlib.Path(sys.argv[1])
member_name = sys.argv[2]
destination = pathlib.Path(sys.argv[3])
with zipfile.ZipFile(archive_path, "r") as archive:
    matches = [info for info in archive.infolist() if info.filename == member_name]
    if len(matches) != 1:
        raise SystemExit(f"package-release: ZIP must contain exactly one {member_name}")
    info = matches[0]
    mode = (info.external_attr >> 16) & 0xFFFF
    if info.is_dir() or (mode and stat.S_IFMT(mode) not in (0, stat.S_IFREG)):
        raise SystemExit(f"package-release: ZIP member is not regular: {member_name}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with archive.open(info, "r") as source, destination.open("xb") as output:
        shutil.copyfileobj(source, output, length=1024 * 1024)
PY
}

# Hash the caller-owned file before taking the per-target output lock. A bad
# selection must not create any artifact or packaging residue.
assert_expected_hash "$SELECTED_BINARY" "selected binary" || exit 2

if [ "$GOOS" = "windows" ]; then
    STAGED_BINARY_NAME="codebase-memory-mcp.exe"
    INSTALLER="install.ps1"
    ARCHIVE_OUT="$OUT_DIR/$NAME.zip"
else
    STAGED_BINARY_NAME="codebase-memory-mcp"
    INSTALLER="install.sh"
    ARCHIVE_OUT="$OUT_DIR/$NAME.tar.gz"
fi
MCPB_OUT="$OUT_DIR/$NAME.mcpb"
BUILD_MCPB=0
case "$GOOS/$GOARCH" in
darwin/* | windows/* | linux/*-portable) BUILD_MCPB=1 ;;
esac

LOCK_DIR="$OUT_DIR/.${NAME}.package.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "package-release: target is already being packaged (lock exists): $LOCK_DIR" >&2
    exit 2
fi
cleanup() {
    local status=$?
    trap - EXIT
    rm -rf "$LOCK_DIR"
    exit "$status"
}
trap cleanup EXIT

if [ -e "$ARCHIVE_OUT" ] || [ -e "$MCPB_OUT" ]; then
    echo "package-release: refusing to overwrite an existing $NAME release container." >&2
    exit 2
fi

PACK_DIR="$LOCK_DIR/stage"
OUTPUT_DIR="$LOCK_DIR/output"
VERIFY_DIR="$LOCK_DIR/verify"
mkdir -p "$PACK_DIR" "$OUTPUT_DIR" "$VERIFY_DIR"
STAGED_BINARY="$PACK_DIR/$STAGED_BINARY_NAME"
cp "$SELECTED_BINARY" "$STAGED_BINARY"
# upload-artifact transports do not preserve Unix mode. Restoring the container
# metadata is allowed; chmod does not alter the selected file's byte sequence.
chmod 0755 "$STAGED_BINARY"
assert_expected_hash "$STAGED_BINARY" "staged binary" || exit 2

# The composition gate is read-only and runs against the exact executable that
# will enter both containers. Candidate derivation and scanning precede this
# packaging boundary.
bash scripts/ci/check-binary-composition.sh "$STAGED_BINARY" || exit 2
assert_expected_hash "$STAGED_BINARY" "composition-gated binary" || exit 2
assert_expected_hash "$SELECTED_BINARY" "selected binary after staging" || exit 2

cp LICENSE "$INSTALLER" "$PACK_DIR/"
if [ -n "$THIRD_PARTY_NOTICES" ]; then
    cp "$THIRD_PARTY_NOTICES" "$PACK_DIR/THIRD_PARTY_NOTICES.md"
else
    scripts/gen-third-party-notices.sh "$PACK_DIR/THIRD_PARTY_NOTICES.md"
fi

ARCHIVE_TMP="$OUTPUT_DIR/$(basename "$ARCHIVE_OUT")"
if [ "$GOOS" = "windows" ]; then
    (
        cd "$PACK_DIR"
        zip -q -X "$ARCHIVE_TMP" \
            codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md
    )
    mkdir "$VERIFY_DIR/archive"
    extract_zip_member "$ARCHIVE_TMP" "$STAGED_BINARY_NAME" \
        "$VERIFY_DIR/archive/$STAGED_BINARY_NAME"
else
    # BSD tar otherwise materializes macOS extended attributes as hidden
    # AppleDouble `._*` members, violating the exact four-file inventory.
    COPYFILE_DISABLE=1 tar -czf "$ARCHIVE_TMP" -C "$PACK_DIR" \
        codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md
    mkdir "$VERIFY_DIR/archive"
    tar -xzf "$ARCHIVE_TMP" -C "$VERIFY_DIR/archive" "$STAGED_BINARY_NAME"
fi
assert_expected_hash "$VERIFY_DIR/archive/$STAGED_BINARY_NAME" "archive executable" || exit 2

build_mcpb_bundle() {
    local out
    out="$OUTPUT_DIR/$(basename "$MCPB_OUT")"
    local stage="$LOCK_DIR/mcpb-stage"
    local entry="server/$STAGED_BINARY_NAME"
    local platform
    case "$GOOS" in
    darwin) platform="darwin" ;;
    windows) platform="win32" ;;
    linux) platform="linux" ;;
    esac
    command -v zip >/dev/null 2>&1 || {
        echo "package-release: zip is required to build $NAME.mcpb" >&2
        return 1
    }
    mkdir -p "$stage/server"
    cp "$STAGED_BINARY" "$stage/$entry"
    chmod 0755 "$stage/$entry"
    cp "$PACK_DIR/LICENSE" "$PACK_DIR/THIRD_PARTY_NOTICES.md" "$stage/server/"
    local mcpb_version="${VERSION:-0.0.0-dev}"
    mcpb_version="${mcpb_version#v}"
    cat >"$stage/manifest.json" <<EOF
{
  "manifest_version": "0.3",
  "name": "codebase-memory-mcp",
  "display_name": "Codebase Memory",
  "version": "$mcpb_version",
  "description": "Codebase knowledge graph for AI agents — 161 languages, sub-ms queries, 99% fewer tokens.",
  "author": { "name": "DeusData", "url": "https://github.com/DeusData" },
  "repository": { "type": "git", "url": "https://github.com/DeusData/codebase-memory-mcp" },
  "homepage": "https://deusdata.github.io/codebase-memory-mcp/",
  "license": "MIT",
  "server": {
    "type": "binary",
    "entry_point": "$entry",
    "mcp_config": {
      "command": "\${__dirname}/$entry",
      "args": []
    }
  },
  "compatibility": {
    "platforms": ["$platform"]
  }
}
EOF
    (
        cd "$stage"
        zip -q -X "$out" \
            manifest.json "$entry" server/LICENSE server/THIRD_PARTY_NOTICES.md
    )
    mkdir "$VERIFY_DIR/mcpb"
    extract_zip_member "$out" "$entry" "$VERIFY_DIR/mcpb/$entry"
    assert_expected_hash "$VERIFY_DIR/mcpb/$entry" "MCPB executable" || return 1
}

if [ "$BUILD_MCPB" -eq 1 ]; then
    build_mcpb_bundle || exit 2
fi

# Recheck both byte owners after every packaging operation. No archive tool,
# manifest step, or future gate may mutate either one unnoticed.
assert_expected_hash "$STAGED_BINARY" "final staged binary" || exit 2
assert_expected_hash "$SELECTED_BINARY" "final selected binary" || exit 2

# Publish only after every container has been built and verified. Because the
# temporary outputs live below OUT_DIR, a hard link provides an atomic
# create-if-absent operation on the same filesystem; unlike mv, it cannot
# replace a path that appeared after the preflight check.
publish_no_clobber() {
    local source="$1"
    local destination="$2"
    if ! ln "$source" "$destination"; then
        echo "package-release: refusing to overwrite release container: $destination" >&2
        return 1
    fi
    rm -f "$source"
}

publish_no_clobber "$ARCHIVE_TMP" "$ARCHIVE_OUT" || exit 2
if [ "$BUILD_MCPB" -eq 1 ]; then
    if ! publish_no_clobber "$OUTPUT_DIR/$(basename "$MCPB_OUT")" "$MCPB_OUT"; then
        rm -f "$ARCHIVE_OUT"
        exit 2
    fi
fi

echo "=== package-release: $ARCHIVE_OUT (selected sha256 $EXPECTED_SHA256) ==="
if [ "$BUILD_MCPB" -eq 1 ]; then
    echo "=== package-release: $MCPB_OUT (same selected sha256) ==="
fi
