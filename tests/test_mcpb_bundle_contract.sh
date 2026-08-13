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
# the SQLite OMIT_LOAD_EXTENSION marker — live in .rodata. STRIP=true no-ops
# the strip stage only because this contract packages FOREIGN goos targets
# from one host, and a host strip tool cannot legitimately serve them all;
# the real strip runs with the real binary in the artifact-flow smoke lane
# (scripts/ci/smoke-artifact.sh) on every leg.
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
"${CC:-cc}" -O0 -o "$BUILD_DIR/codebase-memory-mcp" "$FIX/stub.c"

package() { # goos goarch out-subdir [VERSION value]
    local goos="$1" goarch="$2" out="$FIX/$3"
    mkdir -p "$out"
    if [ "$#" -ge 4 ]; then
        VERSION="$4" STRIP=true BUILD_DIR="$BUILD_DIR" \
            bash "$ROOT/scripts/package-release.sh" "$goos" "$goarch" --out-dir "$out"
    else
        env -u VERSION STRIP=true BUILD_DIR="$BUILD_DIR" \
            bash "$ROOT/scripts/package-release.sh" "$goos" "$goarch" --out-dir "$out"
    fi
}

echo "--- packaging the four target families from one stub build tree"
package darwin arm64 darwin v0.0.0-contract >/dev/null
package windows amd64 windows v0.0.0-contract >/dev/null
package linux amd64-portable portable v0.0.0-contract >/dev/null
package linux amd64 plainlinux v0.0.0-contract >/dev/null
package darwin arm64 unversioned >/dev/null

python3 - "$FIX" <<'PY'
import json
import pathlib
import stat
import sys
import zipfile

fix = pathlib.Path(sys.argv[1])
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
        manifest = json.loads(bundle.read("manifest.json"))
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

if failures:
    print("MCPB BUNDLE CONTRACT VIOLATED:")
    for message in failures:
        print(f"  - {message}")
    sys.exit(1)

print("mcpb bundle contract OK (darwin/windows/static-linux bundles exact, "
      "manifest binds the bundled binary, exec bit preserved, version "
      "stamped with 0.0.0-dev fallback, glibc-dynamic linux excluded)")
PY
