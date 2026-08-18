#!/usr/bin/env bash
# Publish byte-identical ui-*-named copies of the release archives.
#
# Every codebase-memory-mcp 0.9.x binary in the field asks for
# codebase-memory-mcp-ui-<platform>.<ext> when its (now-removed) variant chooser
# is answered "ui". v0.10.0 consolidated to a single archive per platform with
# the UI always embedded, so that asset stopped existing and those updaters 404
# with no path forward but a manual reinstall (#1538, discussion #1526).
# Publishing the same bytes under the legacy names makes already-released
# updaters work again with no action from the user.
#
# Runs after verification: each alias is a copy of a container whose executable
# was hash-bound to the selected, pre-smoke VirusTotal candidate. They are absent
# from checksums.txt, which covers the canonical names current installers request.
#
# Usage: publish-legacy-aliases.sh <version-tag> <repository>
set -euo pipefail

VERSION="${1:?usage: publish-legacy-aliases.sh <version-tag> <repository>}"
REPOSITORY="${2:?usage: publish-legacy-aliases.sh <version-tag> <repository>}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

gh release download "$VERSION" --dir "$work" --repo "$REPOSITORY" \
  --pattern 'codebase-memory-mcp-*.tar.gz' --pattern 'codebase-memory-mcp-*.zip'

shopt -s nullglob
aliases=()
for archive in "$work"/codebase-memory-mcp-*; do
  base="$(basename "$archive")"
  case "$base" in
  codebase-memory-mcp-ui-*) continue ;;
  esac
  alias_path="$work/${base/codebase-memory-mcp-/codebase-memory-mcp-ui-}"
  cp -- "$archive" "$alias_path"
  aliases+=("$alias_path")
done

if [ "${#aliases[@]}" -eq 0 ]; then
  echo "error: no canonical archives found to alias for $VERSION" >&2
  exit 1
fi

gh release upload "$VERSION" "${aliases[@]}" --clobber --repo "$REPOSITORY"
echo "published ${#aliases[@]} legacy alias asset(s) for $VERSION"
