#!/usr/bin/env bash
# Combined installer: codebase-memory-mcp (memory layer) + fablize (procedure layer).
# One command sets up both. The C core is built if needed, registered as an MCP server for
# your agents, then the fablize disciplines are applied to the current project.
# Usage: bash install-combined.sh [target-project-dir]   (default: current directory)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:-$PWD}"
BIN="$ROOT/build/c/codebase-memory-mcp"

echo "=== codebase-memory-mcp + fablize — combined install ==="

# 1. Memory layer: build the binary if it isn't there yet.
if [ ! -x "$BIN" ]; then
  echo "[1/3] Building the memory engine (codebase-memory-mcp)..."
  "$ROOT/scripts/build.sh"
else
  echo "[1/3] Memory engine already built: $BIN"
fi

# 2. Memory layer: register the MCP server + agent instruction files.
echo "[2/3] Registering the MCP server with your agents..."
"$BIN" install -y || {
  echo "  ! 'install' returned non-zero — configure the MCP server manually (see README)."; }

# 3. Procedure layer: apply the fablize disciplines to the target project.
echo "[3/3] Applying the fablize procedure layer..."
bash "$ROOT/fablize/install.sh" "$TARGET"

echo
echo "=== Done. Both layers installed. ==="
echo "  Memory  : codebase-memory-mcp MCP tools (search_graph, trace_path, get_architecture, …)"
echo "  Method  : fablize disciplines in $TARGET (see INTEGRATION.md)"
echo "  Re-run 'bash fablize/install.sh <dir>' to add the disciplines to another project."
