#!/usr/bin/env bash
# Install the local code graph and optionally embed the diagnostic dashboard.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="${CBM_BIN:-$ROOT/build/c/codebase-memory-mcp}"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
WITH_UI=0
AUTO_INDEX=1
AUTO_INDEX_LIMIT=50000
CHECK_ONLY=0
TARGET=""

usage() {
  cat <<'EOF'
codebase-memory-mcp installer

Usage: bash install-combined.sh [flags] [repository]

  --with-ui          build the embedded diagnostic dashboard
  --no-auto-index    leave automatic indexing disabled
  --auto-index-limit N
                     maximum tracked files for automatic indexing (default: 50000)
  --check            verify the installation without changing it
  -h, --help         show this help

This installer does not install hooks, agents, planners, model providers or persistent
conversation memory.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --with-ui) WITH_UI=1 ;;
    --no-auto-index) AUTO_INDEX=0 ;;
    --auto-index-limit)
      shift
      [ "$#" -gt 0 ] || { echo "missing value for --auto-index-limit" >&2; exit 2; }
      AUTO_INDEX_LIMIT="$1"
      case "$AUTO_INDEX_LIMIT" in *[!0-9]*|'') echo "invalid auto-index limit" >&2; exit 2 ;; esac
      ;;
    --check) CHECK_ONLY=1 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown flag: $1" >&2; exit 2 ;;
    *) TARGET="$1" ;;
  esac
  shift
done
TARGET="${TARGET:-$PWD}"

if [ "$CHECK_ONLY" = "1" ]; then
  exec env CBM_BIN="$BIN" CODEX_HOME="$CODEX_HOME" \
    bash "$ROOT/scripts/cbm-doctor.sh" "$TARGET"
fi

[ -d "$TARGET" ] || { echo "repository does not exist: $TARGET" >&2; exit 2; }

if [ "$WITH_UI" = "1" ]; then
  "$ROOT/scripts/build.sh" --with-ui
elif [ ! -x "$BIN" ]; then
  "$ROOT/scripts/build.sh"
fi

"$BIN" install --client codex --with-instructions -y

if [ "$AUTO_INDEX" = "1" ]; then
  "$BIN" config set auto_index true
  "$BIN" config set auto_index_limit "$AUTO_INDEX_LIMIT"
fi

"$BIN" cli index_repository   "$(printf '{"repo_path":"%s","mode":"structural"}' "$TARGET")"

env CBM_BIN="$BIN" CODEX_HOME="$CODEX_HOME" \
  bash "$ROOT/scripts/cbm-doctor.sh" "$TARGET"

echo "Installed: local graph engine, MCP registration and graph-first instructions."
echo "Restart Codex once if this was the first installation."
