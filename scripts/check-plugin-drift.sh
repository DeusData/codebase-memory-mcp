#!/usr/bin/env bash
# Regenerate the Claude Code plugin tree and fail if it differs from the
# committed one. Single source of truth = the C strings in src/cli/cli.c.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VERSION=$(grep -m1 '"version"' server.json | sed -E 's/.*"version"[^"]*"([^"]+)".*/\1/')

scripts/build.sh --version "$VERSION"
build/c/codebase-memory-mcp emit-plugin ./plugin --version "$VERSION"

# git status --porcelain catches untracked (??), modified (M), and deleted (D)
# in one shot — plain `git diff` misses brand-new emitted files.
if [ -n "$(git status --porcelain -- plugin/)" ]; then
  echo "error: plugin/ is stale. Run scripts/check-plugin-drift.sh locally and commit the result." >&2
  git status --porcelain -- plugin/ >&2
  git diff -- plugin/ >&2
  exit 1
fi
echo "plugin/ is in sync with src/cli/cli.c"
