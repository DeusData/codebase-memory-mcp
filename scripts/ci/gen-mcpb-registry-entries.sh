#!/usr/bin/env bash
# gen-mcpb-registry-entries.sh — append MCPB package entries to server.json.
#
# The npm/PyPI entries are static repo content, but MCPB entries carry a
# per-release fileSha256, so they can only exist at publish time. This script
# derives them from the same checksums.txt the draft release ships (and
# attests), never by re-hashing anything itself: one checksum authority.
#
# Usage: gen-mcpb-registry-entries.sh <server.json> <checksums.txt> <release-tag>
#
#   release-tag   the GitHub release tag, v-prefixed (v0.11.0); it forms the
#                 download URL. The package version field drops the prefix.
#
# Idempotent: existing mcpb entries are replaced, never duplicated, so a
# re-run of the registry job cannot grow the manifest. A checksums file
# without a single .mcpb line is a hard failure — publishing a manifest that
# silently un-lists the bundles would hide a broken artifact chain
# (the #1522 silent-empty-results lesson).
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <server.json> <checksums.txt> <release-tag>" >&2
    exit 2
fi

command -v python3 >/dev/null 2>&1 || {
    echo "FAIL: python3 is required to generate MCPB registry entries" >&2
    exit 1
}

python3 - "$@" <<'PY'
import json
import pathlib
import re
import sys

server_json = pathlib.Path(sys.argv[1])
checksums = pathlib.Path(sys.argv[2])
tag = sys.argv[3]

if not tag:
    print("FAIL: empty release tag", file=sys.stderr)
    raise SystemExit(1)
for path in (server_json, checksums):
    if not path.is_file():
        print(f"FAIL: missing {path}", file=sys.stderr)
        raise SystemExit(1)

manifest = json.loads(server_json.read_text(encoding="utf-8"))
repo_url = (manifest.get("repository") or {}).get("url")
if not repo_url:
    print(f"FAIL: {server_json} has no repository.url to build download URLs from",
          file=sys.stderr)
    raise SystemExit(1)
repo_url = repo_url.rstrip("/")
version = tag[1:] if tag.startswith("v") else tag

entries = []
for line in checksums.read_text(encoding="utf-8").splitlines():
    parts = line.split()
    if len(parts) != 2 or not parts[1].endswith(".mcpb"):
        continue
    sha, name = parts
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        print(f"FAIL: malformed sha256 for {name} in {checksums}", file=sys.stderr)
        raise SystemExit(1)
    entries.append((name, sha))

if not entries:
    print(f"FAIL: no .mcpb lines in {checksums}", file=sys.stderr)
    raise SystemExit(1)

packages = [package for package in manifest.get("packages", [])
            if package.get("registryType") != "mcpb"]
for name, sha in sorted(entries):
    packages.append({
        "registryType": "mcpb",
        "identifier": f"{repo_url}/releases/download/{tag}/{name}",
        "version": version,
        "fileSha256": sha,
        "transport": {"type": "stdio"},
    })
    print(f"mcpb entry: {name} ({sha})")
manifest["packages"] = packages

server_json.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(f"appended {len(entries)} mcpb package entries to {server_json} for {tag}")
PY
