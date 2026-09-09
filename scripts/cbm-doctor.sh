#!/usr/bin/env bash
# Read-only health check for the local graph integration.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CBM_BIN:-$ROOT/build/c/codebase-memory-mcp}"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
TARGET="${1:-$PWD}"
FAILURES=0

pass() { printf '  ✓ %s\n' "$1"; }
fail() { printf '  ✗ %s\n' "$1" >&2; FAILURES=$((FAILURES + 1)); }
has() { [ -f "$1" ] && grep -qF "$2" "$1" 2>/dev/null; }

echo "codebase-memory-mcp doctor"
echo "  repository: $TARGET"

if [ -x "$BIN" ]; then pass "graph engine is executable"; else fail "graph engine missing: $BIN"; fi
if [ -d "$TARGET" ]; then pass "repository exists"; else fail "repository missing: $TARGET"; fi

CONFIG="$CODEX_HOME/config.toml"
AGENTS="$CODEX_HOME/AGENTS.md"
if has "$CONFIG" '[mcp_servers.codebase-memory-mcp]'; then pass "Codex MCP registration"; else fail "Codex MCP registration missing"; fi
if has "$AGENTS" '<!-- codebase-memory-mcp:start -->'; then pass "graph-first instructions"; else fail "graph-first instructions missing"; fi

if [ -x "$BIN" ]; then
  AUTO_INDEX="$("$BIN" config get auto_index 2>/dev/null | tr -d '[:space:]')"
  [ "$AUTO_INDEX" = "true" ] && pass "automatic indexing enabled" || fail "automatic indexing disabled"
  CBM_MCP_TOOLSETS=admin "$BIN" cli list_projects '{}' >/dev/null 2>&1 \
    && pass "persistent graph store is readable" \
    || fail "persistent graph store is not readable"
fi

if [ "$FAILURES" -eq 0 ]; then
  echo "Result: healthy."
  exit 0
fi
echo "Result: unhealthy — $FAILURES check(s) failed." >&2
exit 1
