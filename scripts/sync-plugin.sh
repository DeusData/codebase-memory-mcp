#!/usr/bin/env bash
# Regenerate the committed Claude Code plugin from the native emitter.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SERVER_VERSION=$(grep -m1 '"version"' server.json |
  sed -E 's/.*"version"[^"]*"([^"]+)".*/\1/')
REQUESTED_VERSION="${1:-$SERVER_VERSION}"
VERSION="${REQUESTED_VERSION#v}"

if [[ -z "$VERSION" ]]; then
  echo "error: plugin version must not be empty" >&2
  exit 1
fi

scripts/build.sh --version "$VERSION"

BIN="$ROOT/build/c/codebase-memory-mcp"
if [[ -f "${BIN}.exe" ]]; then
  BIN="${BIN}.exe"
fi
"$BIN" emit-plugin "$ROOT/plugin" --version "$VERSION"

echo "plugin/ regenerated for version $VERSION"
