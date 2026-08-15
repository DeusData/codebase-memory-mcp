#!/usr/bin/env bash
# Add the legacy ui-* names to checksums.txt, carrying the canonical digest.
#
# publish-legacy-aliases.sh publishes byte-identical ui-*-named copies of the
# release archives so already-released 0.9.x updaters stop 404ing (#1538). It
# runs after verify, so those copies inherit the hash-bound VirusTotal verdicts
# — and it therefore left them out of checksums.txt entirely, on the reasoning
# that the file "covers the canonical names current installers request".
#
# That reasoning has a hole. The aliases exist ONLY for 0.9.x updaters, and
# those verify the NAME they asked for. So the alias fixed the 404 and moved the
# failure one step later: the updater downloads the archive, cannot find its name
# in checksums.txt, and refuses to install it (#1134):
#
#   warning: codebase-memory-mcp-ui-darwin-arm64.tar.gz not found in checksums.txt
#   error: refusing to install an unverified download
#
# An alias is a copy, so its sha256 is by construction the digest already
# computed for the canonical archive. Emitting that digest under the legacy name
# introduces no new bytes and no new scan surface. This runs BEFORE the
# attestation step so the attested artifact covers both names.
#
# The alias rule MUST stay in step with publish-legacy-aliases.sh: .tar.gz and
# .zip only, never an already-ui-* name. A name here with no published asset is
# as broken as an asset with no name, so this fails closed when it matches
# nothing.
#
# Usage: append-legacy-alias-checksums.sh <checksums-file>
set -euo pipefail

CHECKSUMS="${1:?usage: append-legacy-alias-checksums.sh <checksums-file>}"

if [ ! -s "$CHECKSUMS" ]; then
  echo "error: $CHECKSUMS is missing or empty" >&2
  exit 1
fi

aliases="$(mktemp)"
trap 'rm -f "$aliases"' EXIT

awk '
  $2 ~ /^codebase-memory-mcp-/ &&
  $2 !~ /^codebase-memory-mcp-ui-/ &&
  ($2 ~ /\.tar\.gz$/ || $2 ~ /\.zip$/) {
    alias = $2
    sub(/^codebase-memory-mcp-/, "codebase-memory-mcp-ui-", alias)
    print $1 "  " alias
  }
' "$CHECKSUMS" > "$aliases"

if [ ! -s "$aliases" ]; then
  echo "error: no canonical archives matched the ui-* alias rule in $CHECKSUMS;" >&2
  echo "       publish-legacy-aliases.sh would then publish assets that" >&2
  echo "       checksums.txt does not cover (#1134)." >&2
  exit 1
fi

cat "$aliases" >> "$CHECKSUMS"
echo "added $(wc -l < "$aliases" | tr -d ' ') legacy alias checksum line(s); $CHECKSUMS now covers $(wc -l < "$CHECKSUMS" | tr -d ' ') names"
