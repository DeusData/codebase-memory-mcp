#!/usr/bin/env bash
# Extract the release archives into a flat directory of scannable files.
#
# Turns the per-platform artifacts (codebase-memory-mcp-<goos>-<goarch>.tar.gz /
# .zip, produced by the canonical package-release.sh) into one directory holding
# the actual executables, each renamed to its platform so a scan report names
# something a human can act on. The install scripts are copied in beside them:
# they ship in the release too, and a compromised installer matters as much as a
# compromised binary.
#
# Usage: extract-release-archives.sh <archive-dir> <output-dir>
#
# Lives in scripts/ rather than inline in the workflow because the venue-parity
# contract requires it: a venue may provision, plumb artifacts, or call a
# canonical leg script, and this is leg logic. It also means the dry run and the
# release can extract identically instead of drifting apart in two YAML copies.
set -euo pipefail

ARCHIVE_DIR="${1:?extract-release-archives: missing <archive-dir>}"
OUT_DIR="${2:?extract-release-archives: missing <output-dir>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "$OUT_DIR"
shopt -s nullglob

# Counts BINARIES recovered from archives, deliberately not "files in OUT_DIR".
# The install scripts below are copied in unconditionally, so a whole-directory
# count is never zero and would have reported success on an empty input — a
# clean VirusTotal run over nothing, which is the precise false green this
# script exists to prevent. Caught by running it against an empty directory.
extracted=0

for archive in "$ARCHIVE_DIR"/*.tar.gz; do
  name="$(basename "$archive" .tar.gz)"
  tar -xzf "$archive" -C "$OUT_DIR" 2>/dev/null || true
  if [ -f "$OUT_DIR/codebase-memory-mcp" ]; then
    mv "$OUT_DIR/codebase-memory-mcp" "$OUT_DIR/${name}"
    extracted=$((extracted + 1))
  fi
done

for archive in "$ARCHIVE_DIR"/*.zip; do
  name="$(basename "$archive" .zip)"
  unzip -o "$archive" -d "$OUT_DIR" >/dev/null 2>&1 || true
  if [ -f "$OUT_DIR/codebase-memory-mcp.exe" ]; then
    mv "$OUT_DIR/codebase-memory-mcp.exe" "$OUT_DIR/${name}.exe"
    extracted=$((extracted + 1))
  fi
done

if [ "$extracted" -eq 0 ]; then
  echo "FAIL: no binaries extracted from '$ARCHIVE_DIR' — a scan here would have"
  echo "      reported clean without examining a single byte"
  ls -la "$ARCHIVE_DIR" 2>/dev/null || true
  exit 1
fi

# Shipped alongside the binaries, so scanned alongside them: a compromised
# installer matters as much as a compromised executable.
cp "$ROOT/install.sh" "$OUT_DIR/" 2>/dev/null || true
cp "$ROOT/install.ps1" "$OUT_DIR/" 2>/dev/null || true

ls -la "$OUT_DIR"
total="$(find "$OUT_DIR" -maxdepth 1 -type f | wc -l | tr -d ' ')"
echo "extracted $extracted binarie(s); $total file(s) queued for scanning"
