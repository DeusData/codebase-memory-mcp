#!/usr/bin/env bash
# Append the Security Verification section to the release notes: per-binary
# sha256 + VirusTotal links. This step only runs after check-virustotal.sh
# passed, and that gate is ZERO tolerance — any detection blocks the release —
# so "0 detections" here is a verified statement, never an assumption.
# Expects: GH_TOKEN, VERSION; run from the verify job workspace (binaries/).
set -euo pipefail

TABLE="\n\n## Security Verification\n\n"
TABLE+="All release binaries scanned with 70+ antivirus engines — **0 detections**.\n\n"
TABLE+="| Binary | SHA-256 | VirusTotal |\n"
TABLE+="|--------|---------|------------|\n"

for bin in binaries/codebase-memory-mcp-*; do
  [ -f "$bin" ] || continue
  name=$(basename "$bin")
  sha256=$(sha256sum "$bin" 2>/dev/null | awk '{print $1}' \
    || shasum -a 256 "$bin" | awk '{print $1}')
  label=$(echo "$name" | sed 's/^codebase-memory-mcp-//' | sed 's/\.exe$//')
  short="${sha256:0:20}..."
  vt_url="https://www.virustotal.com/gui/file/${sha256}/detection"
  TABLE+="| \`${label}\` | \`${short}\` | [0 detections ✅](${vt_url}) |\n"
done

CURRENT=$(gh release view "$VERSION" \
  --json body --jq '.body // ""' --repo "$GITHUB_REPOSITORY")
printf '%s%b' "$CURRENT" "$TABLE" > /tmp/release_notes.md
gh release edit "$VERSION" \
  --notes-file /tmp/release_notes.md --repo "$GITHUB_REPOSITORY"
