#!/usr/bin/env bash
# Contract: scripts/package-release.sh builds the MCPB bundles the MCP
# Registry advertises (#1246) — exactly where eligible, never elsewhere.
#
# A broken bundle is worse than none: an mcpb registry entry points hosts at
# one-click install, so a wrong member set, a manifest that mis-names the
# entry point, or a lost executable bit fails AT THE USER, on a machine we
# never see. This contract pins the bundle's shape at its single canonical
# producer, for every target family, on every leg.
#
# The stub is a REAL compiled executable, so the composition gate runs
# genuinely: format detection, the ELF segment checks (-z separate-code) and
# the needle scans. The gate's two positive needles — the artifact canary and
# the SQLite OMIT_LOAD_EXTENSION marker — live in .rodata. This test packages
# the same opaque selected bytes under foreign target labels deliberately; this
# boundary proves it never invokes strip/codesign and never changes bytes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/cbm-mcpb-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

BUILD_DIR="$FIX/build"
mkdir -p "$BUILD_DIR"
cat >"$FIX/stub.c" <<'EOF'
#include <stdio.h>
static const char keep[] = "codebase-memory-mcp OMIT_LOAD_EXTENSION";
int main(void) { puts(keep); return 0; }
EOF
SELECTED="$BUILD_DIR/codebase-memory-mcp"
case "$(uname -s)" in
MINGW* | MSYS* | CYGWIN*) SELECTED="${SELECTED}.exe" ;;
esac
"${CC:-cc}" -O0 -o "$SELECTED" "$FIX/stub.c"
[ -f "$SELECTED" ] || {
    echo "MCPB contract compiler produced no selected-binary fixture" >&2
    exit 1
}
SELECTED_SHA="$(python3 - "$SELECTED" <<'PY'
import hashlib
import pathlib
import sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
printf '%s\n' 'contract notices sentinel' >"$FIX/THIRD_PARTY_NOTICES.md"

# A pure packager has no reason to resolve any mutation tool, and its byte
# verification uses the already-required Python runtime rather than adding an
# undeclared unzip dependency. Loud sentinels catch either regression.
mkdir -p "$FIX/forbidden-tools"
for tool in strip llvm-strip codesign unzip; do
    # These variables expand inside the generated sentinel, not in this shell.
    # shellcheck disable=SC2016
    printf '%s\n' '#!/usr/bin/env bash' \
        'printf "%s\\n" "$(basename "$0")" >>"$FORBIDDEN_TOOL_LOG"' \
        'exit 97' >"$FIX/forbidden-tools/$tool"
    chmod +x "$FIX/forbidden-tools/$tool"
done
export FORBIDDEN_TOOL_LOG="$FIX/forbidden-tools.log"
export PATH="$FIX/forbidden-tools:$PATH"

package() { # goos goarch out-subdir [VERSION value]
    local goos="$1" goarch="$2" out="$FIX/$3"
    mkdir -p "$out"
    if [ "$#" -ge 4 ]; then
        VERSION="$4" bash "$ROOT/scripts/package-release.sh" "$goos" "$goarch" \
            --selected-binary "$SELECTED" --expected-sha256 "$SELECTED_SHA" \
            --third-party-notices "$FIX/THIRD_PARTY_NOTICES.md" --out-dir "$out"
    else
        env -u VERSION bash "$ROOT/scripts/package-release.sh" "$goos" "$goarch" \
            --selected-binary "$SELECTED" --expected-sha256 "$SELECTED_SHA" \
            --third-party-notices "$FIX/THIRD_PARTY_NOTICES.md" --out-dir "$out"
    fi
}

echo "--- packaging the four target families from one stub build tree"
package darwin arm64 darwin v0.0.0-contract >/dev/null
package windows amd64 windows v0.0.0-contract >/dev/null
package linux amd64-portable portable v0.0.0-contract >/dev/null
package linux amd64 plainlinux v0.0.0-contract >/dev/null
package darwin arm64 unversioned >/dev/null

if [ -e "$FORBIDDEN_TOOL_LOG" ]; then
    echo "package-release invoked a forbidden mutation tool:" >&2
    sed 's/^/  /' "$FORBIDDEN_TOOL_LOG" >&2
    exit 1
fi

echo "--- hash mismatch fails before publishing any container"
mkdir -p "$FIX/hash-mismatch"
if bash "$ROOT/scripts/package-release.sh" linux arm64 \
    --selected-binary "$SELECTED" \
    --expected-sha256 0000000000000000000000000000000000000000000000000000000000000000 \
    --third-party-notices "$FIX/THIRD_PARTY_NOTICES.md" \
    --out-dir "$FIX/hash-mismatch" >"$FIX/hash-mismatch.out" 2>&1; then
    echo "package-release accepted an incorrect selected-binary hash" >&2
    exit 1
fi
if find "$FIX/hash-mismatch" -type f -name 'codebase-memory-mcp-*' | grep -q .; then
    echo "hash mismatch left a release container behind" >&2
    exit 1
fi

echo "--- existing release containers are never overwritten"
PLAIN_ARCHIVE="$FIX/plainlinux/codebase-memory-mcp-linux-amd64.tar.gz"
BEFORE_ARCHIVE_SHA="$(python3 - "$PLAIN_ARCHIVE" <<'PY'
import hashlib
import pathlib
import sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
if package linux amd64 plainlinux v0.0.0-contract >"$FIX/no-clobber.out" 2>&1; then
    echo "package-release overwrote an existing release container" >&2
    exit 1
fi
AFTER_ARCHIVE_SHA="$(python3 - "$PLAIN_ARCHIVE" <<'PY'
import hashlib
import pathlib
import sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
test "$BEFORE_ARCHIVE_SHA" = "$AFTER_ARCHIVE_SHA"

python3 - "$FIX" "$SELECTED" <<'PY'
import hashlib
import json
import pathlib
import stat
import sys
import tarfile
import zipfile

fix = pathlib.Path(sys.argv[1])
selected = pathlib.Path(sys.argv[2]).read_bytes()
selected_sha = hashlib.sha256(selected).hexdigest()
failures = []


def fail(message):
    failures.append(message)


def check_bundle(path, *, binary, platform, version):
    if not path.is_file():
        fail(f"missing bundle: {path.name} in {path.parent.name}/")
        return
    with zipfile.ZipFile(path) as bundle:
        names = sorted(bundle.namelist())
        expected = sorted(["manifest.json", binary, "server/LICENSE",
                           "server/THIRD_PARTY_NOTICES.md"])
        if names != expected:
            fail(f"{path.name}: member set {names} != {expected}")
            return
        info = bundle.getinfo(binary)
        mode = (info.external_attr >> 16) & 0xFFFF
        if not mode & stat.S_IXUSR:
            fail(f"{path.name}: {binary} lost its executable bit (mode {oct(mode)})")
        bundled = bundle.read(binary)
        manifest = json.loads(bundle.read("manifest.json"))
        notices = bundle.read("server/THIRD_PARTY_NOTICES.md")
    if bundled != selected:
        fail(f"{path.name}: executable bytes differ from selected SHA-256 {selected_sha}")
    if notices != b"contract notices sentinel\n":
        fail(f"{path.name}: did not preserve the supplied notices file")
    if manifest.get("version") != version:
        fail(f"{path.name}: manifest version {manifest.get('version')!r} != {version!r}")
    server = manifest.get("server") or {}
    if server.get("type") != "binary":
        fail(f"{path.name}: server.type must be 'binary'")
    if server.get("entry_point") != binary:
        fail(f"{path.name}: entry_point {server.get('entry_point')!r} != {binary!r}")
    command = (server.get("mcp_config") or {}).get("command")
    if command != "${__dirname}/" + binary:
        fail(f"{path.name}: mcp_config.command {command!r} does not target the bundled binary")
    platforms = (manifest.get("compatibility") or {}).get("platforms")
    if platforms != [platform]:
        fail(f"{path.name}: compatibility.platforms {platforms!r} != {[platform]!r}")


check_bundle(fix / "darwin" / "codebase-memory-mcp-darwin-arm64.mcpb",
             binary="server/codebase-memory-mcp", platform="darwin",
             version="0.0.0-contract")
check_bundle(fix / "windows" / "codebase-memory-mcp-windows-amd64.mcpb",
             binary="server/codebase-memory-mcp.exe", platform="win32",
             version="0.0.0-contract")
check_bundle(fix / "portable" / "codebase-memory-mcp-linux-amd64-portable.mcpb",
             binary="server/codebase-memory-mcp", platform="linux",
             version="0.0.0-contract")

# Without VERSION the manifest must say so loudly, not invent a release.
check_bundle(fix / "unversioned" / "codebase-memory-mcp-darwin-arm64.mcpb",
             binary="server/codebase-memory-mcp", platform="darwin",
             version="0.0.0-dev")

# Eligibility is a fence, not a default: the glibc-dynamic linux build gets
# an archive but NO bundle.
plain = fix / "plainlinux"
if not (plain / "codebase-memory-mcp-linux-amd64.tar.gz").is_file():
    fail("plain linux target must still produce its tar.gz")
mcpbs = list(plain.glob("*.mcpb"))
if mcpbs:
    fail(f"glibc-dynamic linux target must not produce a bundle: {[p.name for p in mcpbs]}")


def check_archive(path, binary):
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            packaged = archive.read(binary)
            notices = archive.read("THIRD_PARTY_NOTICES.md")
    else:
        with tarfile.open(path, "r:gz") as archive:
            member = archive.extractfile(binary)
            notice_member = archive.extractfile("THIRD_PARTY_NOTICES.md")
            packaged = member.read() if member else b""
            notices = notice_member.read() if notice_member else b""
    if packaged != selected:
        fail(f"{path.name}: archive executable differs from selected SHA-256 {selected_sha}")
    if notices != b"contract notices sentinel\n":
        fail(f"{path.name}: archive did not preserve the supplied notices file")


check_archive(fix / "darwin" / "codebase-memory-mcp-darwin-arm64.tar.gz",
              "codebase-memory-mcp")
check_archive(fix / "windows" / "codebase-memory-mcp-windows-amd64.zip",
              "codebase-memory-mcp.exe")
check_archive(fix / "portable" / "codebase-memory-mcp-linux-amd64-portable.tar.gz",
              "codebase-memory-mcp")
check_archive(fix / "plainlinux" / "codebase-memory-mcp-linux-amd64.tar.gz",
              "codebase-memory-mcp")

if pathlib.Path(sys.argv[2]).read_bytes() != selected:
    fail("package-release mutated the caller-owned selected binary")

if failures:
    print("MCPB BUNDLE CONTRACT VIOLATED:")
    for message in failures:
        print(f"  - {message}")
    sys.exit(1)

print("mcpb bundle contract OK (selected bytes hash-bound and immutable, "
      "archive/MCPB members byte-identical, mutation tools unused, no-clobber, "
      "darwin/windows/static-linux eligibility exact)")
PY
