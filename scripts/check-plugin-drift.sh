#!/usr/bin/env bash
# Regenerate the Claude Code plugin tree and fail if it differs from the
# pre-generation tree. Single source of truth = the C emitter and server.json.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SERVER_VERSION=""
while IFS= read -r candidate; do
  if [[ -z "$SERVER_VERSION" ]]; then
    SERVER_VERSION="$candidate"
  elif [[ "$candidate" != "$SERVER_VERSION" ]]; then
    echo "error: server.json contains mismatched versions: $SERVER_VERSION and $candidate" >&2
    exit 1
  fi
done < <(sed -nE 's/^[[:space:]]*"version"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' server.json)

if [[ -z "$SERVER_VERSION" ]]; then
  echo "error: server.json does not contain a version" >&2
  exit 1
fi

REQUESTED_VERSION="${1:-$SERVER_VERSION}"
VERSION="${REQUESTED_VERSION#v}"
if [[ "$VERSION" != "$SERVER_VERSION" ]]; then
  echo "error: requested release $VERSION does not match server.json $SERVER_VERSION" >&2
  exit 1
fi

SNAPSHOT_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/cbm-plugin-drift.XXXXXX")
trap 'rm -rf "$SNAPSHOT_ROOT"' EXIT
mkdir -p "$SNAPSHOT_ROOT/plugin"
if [[ -d plugin ]]; then
  cp -R plugin/. "$SNAPSHOT_ROOT/plugin/"
fi

scripts/sync-plugin.sh "$VERSION"

# Compare the pre-generation tree with the regenerated tree. This catches
# added, changed, and deleted files in CI while also allowing developers to
# verify freshly synchronized (but not yet committed) output locally.
if ! diff -ru "$SNAPSHOT_ROOT/plugin" plugin/; then
  echo "error: plugin/ is stale. Run scripts/sync-plugin.sh $VERSION and commit the result." >&2
  git status --porcelain -- plugin/ >&2
  exit 1
fi
echo "plugin/ is in sync for version $VERSION"
