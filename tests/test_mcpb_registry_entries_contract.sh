#!/usr/bin/env bash
# Contract: scripts/ci/gen-mcpb-registry-entries.sh turns checksums.txt into
# the MCPB package entries the MCP Registry publishes.
#
# These entries are the only place clients learn a bundle's sha256 — clients
# verify downloads against it, so a wrong hash bricks one-click install and a
# silently EMPTY entry set publishes a manifest that quietly un-lists the
# bundles (#1522's lesson: empty results must be loud). The real repo
# server.json is the fixture, so schema drift there surfaces here.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/scripts/ci/gen-mcpb-registry-entries.sh"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/cbm-mcpb-entries.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

# "$BASH" by explicit argv — on native Windows a bare "bash" resolves to the
# WSL stub (same trap the extractor contract documents).
python3 - "$ROOT" "$FIX" "$BASH" <<'PY'
import json
import pathlib
import shutil
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
fix = pathlib.Path(sys.argv[2])
bash_executable = sys.argv[3]
generator = root / "scripts" / "ci" / "gen-mcpb-registry-entries.sh"

SHA_A = "a" * 64
SHA_B = "b" * 64
failures = []


def fail(message):
    failures.append(message)


def fresh_fixture():
    shutil.copy(root / "server.json", fix / "server.json")
    (fix / "checksums.txt").write_text(
        f"{SHA_B}  codebase-memory-mcp-windows-amd64.zip\n"
        f"{SHA_B}  codebase-memory-mcp-windows-amd64.mcpb\n"
        f"{SHA_A}  codebase-memory-mcp-darwin-arm64.mcpb\n",
        encoding="utf-8",
    )


def run(checksums_name="checksums.txt"):
    return subprocess.run(
        [bash_executable, str(generator), str(fix / "server.json"),
         str(fix / checksums_name), "v9.9.9"],
        capture_output=True, text=True,
    )


def mcpb_packages():
    manifest = json.loads((fix / "server.json").read_text(encoding="utf-8"))
    return (manifest,
            [p for p in manifest.get("packages", [])
             if p.get("registryType") == "mcpb"])


# ── happy path ──────────────────────────────────────────────────────────────
fresh_fixture()
result = run()
if result.returncode != 0:
    fail(f"generator failed on a valid fixture: {result.stderr[-300:]}")
manifest, mcpb = mcpb_packages()

kinds = [p.get("registryType") for p in manifest.get("packages", [])]
if kinds[:2] != ["npm", "pypi"]:
    fail(f"npm/pypi entries must survive, in order, ahead of mcpb: {kinds}")
if len(mcpb) != 2:
    fail(f"expected 2 mcpb entries (only .mcpb lines count), got {len(mcpb)}")
else:
    first = mcpb[0]
    wanted_url = ("https://github.com/DeusData/codebase-memory-mcp/releases/"
                  "download/v9.9.9/codebase-memory-mcp-darwin-arm64.mcpb")
    if first.get("identifier") != wanted_url:
        fail(f"darwin identifier wrong: {first.get('identifier')}")
    if first.get("version") != "9.9.9":
        fail(f"version must drop the v prefix: {first.get('version')}")
    if first.get("fileSha256") != SHA_A:
        fail(f"fileSha256 wrong: {first.get('fileSha256')}")
    if (first.get("transport") or {}).get("type") != "stdio":
        fail(f"transport must be stdio: {first.get('transport')}")
    # The registry requires the identifier to satisfy its "contains mcp" rule
    # and clients resolve it as a release asset URL — pin both for every entry.
    for package in mcpb:
        url = package.get("identifier", "")
        if not (url.startswith("https://github.com/")
                and "/releases/download/v9.9.9/" in url
                and url.endswith(".mcpb")):
            fail(f"mcpb identifier must be a release-asset URL: {url}")

# ── idempotency: a registry-job re-run must not grow the manifest ───────────
result = run()
if result.returncode != 0:
    fail(f"re-run failed: {result.stderr[-300:]}")
_, mcpb = mcpb_packages()
if len(mcpb) != 2:
    fail(f"re-run duplicated mcpb entries: {len(mcpb)}")

# ── fail-closed: zero bundles, malformed hash ───────────────────────────────
fresh_fixture()
(fix / "checksums-none.txt").write_text(
    f"{SHA_B}  codebase-memory-mcp-windows-amd64.zip\n", encoding="utf-8")
result = run("checksums-none.txt")
if result.returncode == 0:
    fail("a checksums file without .mcpb lines must be fatal, not an empty publish")
elif "no .mcpb lines" not in result.stderr:
    fail(f"zero-bundle failure does not name the contract: {result.stderr[-200:]}")

fresh_fixture()
with (fix / "checksums.txt").open("a", encoding="utf-8") as handle:
    handle.write("deadbeef  codebase-memory-mcp-darwin-amd64.mcpb\n")
result = run()
if result.returncode == 0:
    fail("a malformed sha256 must be fatal")
elif "malformed sha256" not in result.stderr:
    fail(f"malformed-sha failure does not name the contract: {result.stderr[-200:]}")

if failures:
    print("MCPB REGISTRY ENTRIES CONTRACT VIOLATED:")
    for message in failures:
        print(f"  - {message}")
    sys.exit(1)

print("mcpb registry entries contract OK (entries exact against the live "
      "server.json, idempotent, fail-closed on zero bundles and malformed hashes)")
PY
