#!/usr/bin/env bash
# Withhold the selected executables from the full-surface VirusTotal pass.
#
# WHY (measured, not assumed). The verify pass used to submit every extracted
# object, selected executables included, on the stated grounds that "VirusTotal
# is content-addressed, so identical bytes return the analysis it already holds
# instead of re-running 70+ engines". That is not what happens. On the v0.10.5
# release all EIGHT re-submissions produced a NEW analysis: same VirusTotal
# file-id, a timestamp 47 minutes later.
#
#   candidate: file-id=2c00f485...  ts=1786795957  (12:12:37Z)
#   verify   : file-id=2c00f485...  ts=1786798758  (12:59:18Z)
#
# Re-analysing identical bytes re-rolls a probabilistic classifier, and it
# answered differently in the same hour, in both directions:
#
#   82750cd1 (linux-amd64)   microsoft-ml -> clean
#   6d3c5be6 (darwin-arm64)  clean        -> microsoft-ml
#
# The published release notes then cited the candidate verdict while the linked
# page showed the verify verdict, so the table said "clean" for a binary
# VirusTotal was flagging and vice versa. Nothing about the binaries differed:
# same sha256 in both scans.
#
# The second scan also proved nothing the first did not. Identity is already
# established by hash: verify-release-selection.py reconciles every published
# container to the selected bytes before this step runs, and checksums.txt binds
# the same digests publicly. A re-scan adds no assurance and one more roll.
#
# What still gets scanned is everything the candidate pass never saw: install.sh,
# install.ps1, LICENSE, THIRD_PARTY_NOTICES.md, the MCPB manifest.json and the
# unpacked UI assets. install.sh and install.ps1 are the highest-consequence
# non-executable bytes we publish - users pipe them straight into a shell - and
# that coverage is untouched.
#
# Fails closed: if nothing matches, the selection binding is broken; if nothing
# is left, the surface scan would silently become a no-op.
#
# Usage: exclude-rescanned-selected-objects.sh <objects-dir> <release-selection.tsv> [manifest-out]
set -euo pipefail

OBJECTS_DIR="${1:?usage: exclude-rescanned-selected-objects.sh <objects-dir> <selection.tsv> [manifest-out]}"
SELECTION="${2:?usage: exclude-rescanned-selected-objects.sh <objects-dir> <selection.tsv> [manifest-out]}"
MANIFEST="${3:-}"

test -d "$OBJECTS_DIR" || { echo "error: no objects directory: $OBJECTS_DIR" >&2; exit 1; }
test -s "$SELECTION" || { echo "error: no selection evidence: $SELECTION" >&2; exit 1; }

head -n 1 "$SELECTION" | grep -qx '# cbm-release-selection-v1' || {
  echo "error: wrong evidence marker in $SELECTION" >&2; exit 1; }

# selected_sha256 is the 4th column; skip the '#' metadata block and the header.
selected="$(mktemp)"
withheld="$(mktemp)"
trap 'rm -f "$selected" "$withheld"' EXIT
# `|| true` on the grep: with `set -o pipefail` a zero-match grep would abort the
# script here, before the explicit check below could say why. A guard that exits
# silently is not a guard.
awk -F '\t' '/^#/ {next} $1=="target" {next} {print $4}' "$SELECTION" \
  | { grep -E '^[0-9a-f]{64}$' || true; } | sort -u > "$selected"
if [ ! -s "$selected" ]; then
  echo "error: no selected sha256 values in $SELECTION — the selection" >&2
  echo "       evidence names no shipped bytes, so nothing can be matched." >&2
  exit 1
fi

kept=0
for f in "$OBJECTS_DIR"/*; do
  [ -f "$f" ] || continue
  sha="$(sha256sum "$f" | awk '{print $1}')"
  if grep -qx "$sha" "$selected"; then
    printf '%s\t%s\n' "$sha" "$(basename "$f")" >> "$withheld"
    rm -f -- "$f"
  else
    kept=$((kept + 1))
  fi
done

count="$(wc -l < "$withheld" | tr -d ' ')"
if [ "$count" -eq 0 ]; then
  echo "error: no extracted object matched a selected sha256 — the release" >&2
  echo "       containers do not carry the bytes the selection recorded." >&2
  exit 1
fi
if [ "$kept" -eq 0 ]; then
  echo "error: every extracted object was withheld; the full-surface scan" >&2
  echo "       would cover nothing." >&2
  exit 1
fi

if [ -n "$MANIFEST" ]; then
  {
    echo "# cbm-virustotal-withheld-v1"
    echo "# reason=already-scanned-as-candidate"
    echo "# evidence=virustotal-candidate-results.tsv"
    printf 'sha256\tobject\n'
    cat "$withheld"
  } > "$MANIFEST"
fi

echo "withheld $count already-scanned selected executable(s); $kept object(s) remain for the surface scan"
