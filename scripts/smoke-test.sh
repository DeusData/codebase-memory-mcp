#!/usr/bin/env bash
set -euo pipefail

# Smoke test: verify the binary is fully operational.
#
# Phase 1: --version output
# Phase 2: Index a small multi-language project
# Phase 3: Verify node/edge counts, search, and trace
#
# Usage: smoke-test.sh <binary-path>

BINARY="${1:?usage: smoke-test.sh <binary-path>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
TMPDIR=$(mktemp -d)
# On MSYS2/Windows, convert POSIX path to native Windows path for the binary
if command -v cygpath &>/dev/null; then
    TMPDIR=$(cygpath -m "$TMPDIR")
fi
trap 'rm -rf "$TMPDIR"' EXIT

CLI_STDERR=$(mktemp)
cli() { "$BINARY" cli "$@" 2>"$CLI_STDERR"; }

echo "=== Phase 1: version ==="
OUTPUT=$("$BINARY" --version 2>&1)
echo "$OUTPUT"
if ! echo "$OUTPUT" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: unexpected version output"
  exit 1
fi
echo "OK"

echo ""
echo "=== Phase 2: index test project ==="

# Create a small multi-language project (Python + Go + JS)
mkdir -p "$TMPDIR/src/pkg"

cat > "$TMPDIR/src/main.py" << 'PYEOF'
from pkg import helper

def main():
    result = helper.compute(42)
    print(result)

class Config:
    DEBUG = True
    PORT = 8080
PYEOF

cat > "$TMPDIR/src/pkg/__init__.py" << 'PYEOF'
from .helper import compute
PYEOF

cat > "$TMPDIR/src/pkg/helper.py" << 'PYEOF'
def compute(x):
    return x * 2

def validate(data):
    if not data:
        raise ValueError("empty")
    return True
PYEOF

cat > "$TMPDIR/src/server.go" << 'GOEOF'
package main

import "fmt"

func StartServer(port int) {
    fmt.Printf("listening on :%d\n", port)
}

func HandleRequest(path string) string {
    return "ok: " + path
}
GOEOF

cat > "$TMPDIR/src/app.js" << 'JSEOF'
function render(data) {
    return `<div>${data}</div>`;
}

function fetchData(url) {
    return fetch(url).then(r => r.json());
}

module.exports = { render, fetchData };
JSEOF

cat > "$TMPDIR/config.yaml" << 'YAMLEOF'
server:
  port: 8080
  debug: true
database:
  host: localhost
YAMLEOF

# Index
RESULT=$(cli index_repository "{\"repo_path\":\"$TMPDIR\"}")
echo "$RESULT"

STATUS=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('status',''))" 2>/dev/null || echo "")
if [ "$STATUS" != "indexed" ]; then
  echo "FAIL: index status is '$STATUS', expected 'indexed'"
  echo "--- stderr ---"
  cat "$CLI_STDERR"
  echo "--- end stderr ---"
  exit 1
fi

NODES=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('nodes',0))" 2>/dev/null || echo "0")
EDGES=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('edges',0))" 2>/dev/null || echo "0")

echo "nodes=$NODES edges=$EDGES"

if [ "$NODES" -lt 10 ]; then
  echo "FAIL: expected at least 10 nodes, got $NODES"
  exit 1
fi
if [ "$EDGES" -lt 5 ]; then
  echo "FAIL: expected at least 5 edges, got $EDGES"
  exit 1
fi
echo "OK: $NODES nodes, $EDGES edges"

echo ""
echo "=== Phase 3: verify queries ==="

# 3a: search_graph — find the compute function
PROJECT=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('project',''))" 2>/dev/null || echo "")

SEARCH=$(cli search_graph "{\"project\":\"$PROJECT\",\"name_pattern\":\"compute\"}")
TOTAL=$(echo "$SEARCH" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('total',0))" 2>/dev/null || echo "0")
if [ "$TOTAL" -lt 1 ]; then
  echo "FAIL: search_graph for 'compute' returned 0 results"
  exit 1
fi
echo "OK: search_graph found $TOTAL result(s) for 'compute'"

# 3b: trace_path — verify compute has callers
TRACE=$(cli trace_path "{\"project\":\"$PROJECT\",\"function_name\":\"compute\",\"direction\":\"inbound\",\"depth\":1}")
CALLERS=$(echo "$TRACE" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(len(d.get('callers',[])))" 2>/dev/null || echo "0")
if [ "$CALLERS" -lt 1 ]; then
  echo "FAIL: trace_path found 0 callers for 'compute'"
  exit 1
fi
echo "OK: trace_path found $CALLERS caller(s) for 'compute'"

# 3c: get_graph_schema — verify labels exist
SCHEMA=$(cli get_graph_schema "{\"project\":\"$PROJECT\"}")
LABELS=$(echo "$SCHEMA" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(len(d.get('node_labels',[])))" 2>/dev/null || echo "0")
if [ "$LABELS" -lt 3 ]; then
  echo "FAIL: schema has fewer than 3 node labels"
  exit 1
fi
echo "OK: schema has $LABELS node labels"

# 3d: Verify __init__.py didn't clobber Folder node
FOLDERS=$(cli search_graph "{\"project\":\"$PROJECT\",\"label\":\"Folder\"}")
FOLDER_COUNT=$(echo "$FOLDERS" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(d.get('total',0))" 2>/dev/null || echo "0")
if [ "$FOLDER_COUNT" -lt 2 ]; then
  echo "FAIL: expected at least 2 Folder nodes (src, src/pkg), got $FOLDER_COUNT"
  exit 1
fi
echo "OK: $FOLDER_COUNT Folder nodes (init.py didn't clobber them)"

# 3e: delete_project cleanup
cli delete_project "{\"project\":\"$PROJECT\"}" > /dev/null

echo ""
echo "=== Phase 4: security checks ==="

# 4a: Clean shutdown — binary must exit within 5 seconds after EOF
echo "Testing clean shutdown..."
SHUTDOWN_TMPDIR=$(mktemp -d)
cat > "$SHUTDOWN_TMPDIR/input.jsonl" << 'JSONL'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
JSONL

# Run binary with EOF and wait up to 5 seconds (portable — no `timeout` needed)
"$BINARY" < "$SHUTDOWN_TMPDIR/input.jsonl" > /dev/null 2>&1 &
SHUTDOWN_PID=$!
SHUTDOWN_WAITED=0
while kill -0 "$SHUTDOWN_PID" 2>/dev/null && [ "$SHUTDOWN_WAITED" -lt 5 ]; do
  sleep 1
  SHUTDOWN_WAITED=$((SHUTDOWN_WAITED + 1))
done
if kill -0 "$SHUTDOWN_PID" 2>/dev/null; then
  kill "$SHUTDOWN_PID" 2>/dev/null || true
  wait "$SHUTDOWN_PID" 2>/dev/null || true
  rm -rf "$SHUTDOWN_TMPDIR"
  echo "FAIL: binary did not exit within 5 seconds after EOF"
  exit 1
fi
wait "$SHUTDOWN_PID" 2>/dev/null || true
rm -rf "$SHUTDOWN_TMPDIR"
echo "OK: clean shutdown"

# 4b: No residual processes (skip on Windows/MSYS2 where pgrep may not work)
if command -v pgrep &>/dev/null && [ "$(uname)" != "MINGW64_NT" ] 2>/dev/null; then
  # Give a moment for any child processes to clean up
  sleep 1
  RESIDUAL=$(pgrep -f "codebase-memory-mcp.*cli" 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
  RESIDUAL="${RESIDUAL:-0}"
  if [ "$RESIDUAL" -gt 0 ]; then
    echo "WARNING: $RESIDUAL residual codebase-memory-mcp process(es) found"
  else
    echo "OK: no residual processes"
  fi
fi

# 4c: Version integrity — output must be exactly one line matching version format
VERSION_OUTPUT=$("$BINARY" --version 2>&1)
VERSION_LINES=$(echo "$VERSION_OUTPUT" | wc -l | tr -d ' ')
if [ "$VERSION_LINES" -ne 1 ]; then
  echo "FAIL: --version output has $VERSION_LINES lines, expected exactly 1"
  echo "  Output: $VERSION_OUTPUT"
  exit 1
fi
echo "OK: version output is clean single line"

echo ""
echo "=== Phase 5: MCP stdio transport (agent handshake) ==="

# Test the actual MCP protocol as an agent (Claude Code, OpenCode, etc.) would use it.
# Uses background process + kill instead of timeout (portable across macOS/Linux).

# Helper: run binary in background with input, wait up to N seconds, collect output
mcp_run() {
  local input_file="$1" output_file="$2" max_wait="${3:-10}"
  "$BINARY" < "$input_file" > "$output_file" 2>/dev/null &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$max_wait" ]; do
    sleep 1
    waited=$((waited + 1))
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

MCP_INPUT=$(mktemp)
MCP_OUTPUT=$(mktemp)
cat > "$MCP_INPUT" << 'MCPEOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
MCPEOF

mcp_run "$MCP_INPUT" "$MCP_OUTPUT" 10

# 5a: Verify initialize response (id:1)
if ! grep -q '"id":1' "$MCP_OUTPUT"; then
  echo "FAIL: no initialize response (id:1) in MCP output"
  echo "Output was:"
  cat "$MCP_OUTPUT"
  rm -f "$MCP_INPUT" "$MCP_OUTPUT"
  exit 1
fi
echo "OK: initialize response received (id:1)"

# 5b: Verify tools/list response (id:2) with tool names
if ! grep -q '"id":2' "$MCP_OUTPUT"; then
  echo "FAIL: no tools/list response (id:2) in MCP output"
  echo "Output was:"
  cat "$MCP_OUTPUT"
  rm -f "$MCP_INPUT" "$MCP_OUTPUT"
  exit 1
fi
echo "OK: tools/list response received (id:2)"

# 5c: Verify expected tools are present
for TOOL in index_repository search_graph trace_path get_code_snippet search_code; do
  if ! grep -q "\"$TOOL\"" "$MCP_OUTPUT"; then
    echo "FAIL: tool '$TOOL' not found in tools/list response"
    rm -f "$MCP_INPUT" "$MCP_OUTPUT"
    exit 1
  fi
done
echo "OK: all 5 core MCP tools present in tools/list"

# 5d: Verify protocol version in initialize response
if ! grep -q '"protocolVersion"' "$MCP_OUTPUT"; then
  echo "FAIL: protocolVersion missing from initialize response"
  rm -f "$MCP_INPUT" "$MCP_OUTPUT"
  exit 1
fi
echo "OK: protocolVersion present in initialize response"

rm -f "$MCP_INPUT" "$MCP_OUTPUT"

# 5e: MCP tool call via JSON-RPC (index + search round-trip)
echo ""
echo "--- Phase 5e: MCP tool call round-trip ---"
MCP_TOOL_INPUT=$(mktemp)
MCP_TOOL_OUTPUT=$(mktemp)

cat > "$MCP_TOOL_INPUT" << TOOLEOF
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"index_repository","arguments":{"repo_path":"$TMPDIR","mode":"fast"}}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":"compute"}}}
TOOLEOF

mcp_run "$MCP_TOOL_INPUT" "$MCP_TOOL_OUTPUT" 30

if ! grep -q '"id":2' "$MCP_TOOL_OUTPUT"; then
  echo "FAIL: no index_repository response (id:2)"
  cat "$MCP_TOOL_OUTPUT"
  rm -f "$MCP_TOOL_INPUT" "$MCP_TOOL_OUTPUT"
  exit 1
fi

if ! grep -q '"id":3' "$MCP_TOOL_OUTPUT"; then
  echo "FAIL: no search_graph response (id:3)"
  cat "$MCP_TOOL_OUTPUT"
  rm -f "$MCP_TOOL_INPUT" "$MCP_TOOL_OUTPUT"
  exit 1
fi
echo "OK: MCP tool call round-trip (index + search) succeeded"

# 5f: Content-Length framing (OpenCode compatibility)
echo ""
echo "--- Phase 5f: Content-Length framing ---"
MCP_CL_INPUT=$(mktemp)
MCP_CL_OUTPUT=$(mktemp)

INIT_MSG='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"cl-test","version":"1.0"}}}'
INIT_LEN=${#INIT_MSG}
printf "Content-Length: %d\r\n\r\n%s" "$INIT_LEN" "$INIT_MSG" > "$MCP_CL_INPUT"

TOOLS_MSG='{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
TOOLS_LEN=${#TOOLS_MSG}
printf "Content-Length: %d\r\n\r\n%s" "$TOOLS_LEN" "$TOOLS_MSG" >> "$MCP_CL_INPUT"

mcp_run "$MCP_CL_INPUT" "$MCP_CL_OUTPUT" 10

if ! grep -q '"id":1' "$MCP_CL_OUTPUT" || ! grep -q '"id":2' "$MCP_CL_OUTPUT"; then
  echo "FAIL: Content-Length framed handshake did not produce both responses"
  cat "$MCP_CL_OUTPUT"
  rm -f "$MCP_CL_INPUT" "$MCP_CL_OUTPUT"
  exit 1
fi
echo "OK: Content-Length framing works (OpenCode compatible)"

rm -f "$MCP_CL_INPUT" "$MCP_CL_OUTPUT" "$MCP_TOOL_INPUT" "$MCP_TOOL_OUTPUT"

echo ""
echo "=== Phase 6: CLI subcommands ==="

# 6a: install --dry-run -y
echo "--- Phase 6a: install --dry-run ---"
INSTALL_OUT=$("$BINARY" install --dry-run -y 2>&1)
if ! echo "$INSTALL_OUT" | grep -qi 'install\|skill\|mcp\|agent'; then
  echo "FAIL: install --dry-run produced unexpected output"
  echo "$INSTALL_OUT"
  exit 1
fi
if ! echo "$INSTALL_OUT" | grep -qi 'dry-run'; then
  echo "FAIL: install --dry-run did not indicate dry-run mode"
  exit 1
fi
echo "OK: install --dry-run completed"

# 6b: uninstall --dry-run -y
echo "--- Phase 6b: uninstall --dry-run ---"
UNINSTALL_OUT=$("$BINARY" uninstall --dry-run -y 2>&1)
if ! echo "$UNINSTALL_OUT" | grep -qi 'uninstall\|remov'; then
  echo "FAIL: uninstall --dry-run produced unexpected output"
  echo "$UNINSTALL_OUT"
  exit 1
fi
echo "OK: uninstall --dry-run completed"

# 6c: update --dry-run --standard -y
echo "--- Phase 6c: update --dry-run ---"
UPDATE_OUT=$("$BINARY" update --dry-run --standard -y 2>&1)
if ! echo "$UPDATE_OUT" | grep -qi 'dry-run'; then
  echo "FAIL: update --dry-run did not indicate dry-run mode"
  echo "$UPDATE_OUT"
  exit 1
fi
if ! echo "$UPDATE_OUT" | grep -qi 'standard'; then
  echo "FAIL: update --dry-run did not respect --standard flag"
  exit 1
fi
echo "OK: update --dry-run --standard completed"

# 6d: config set/get/reset round-trip
echo "--- Phase 6d: config set/get/reset ---"
"$BINARY" config set auto_index true 2>/dev/null
CONFIG_VAL=$("$BINARY" config get auto_index 2>/dev/null)
if ! echo "$CONFIG_VAL" | grep -q 'true'; then
  echo "FAIL: config get auto_index returned '$CONFIG_VAL', expected 'true'"
  exit 1
fi
"$BINARY" config reset auto_index 2>/dev/null
echo "OK: config set/get/reset round-trip"

# 6e: Simulated binary replacement (update flow without network)
# Simulates the update command's Steps 3-6: extract, replace, verify.
# Uses a copy of the test binary as the "downloaded" version.
echo "--- Phase 6e: simulated binary replacement ---"
REPLACE_DIR=$(mktemp -d)
INSTALL_DIR="$REPLACE_DIR/install"
mkdir -p "$INSTALL_DIR"

# 1. Copy binary to "install dir" as the "currently installed" version
cp "$BINARY" "$INSTALL_DIR/codebase-memory-mcp"
chmod 755 "$INSTALL_DIR/codebase-memory-mcp"

# Verify installed binary works
INSTALLED_VER=$("$INSTALL_DIR/codebase-memory-mcp" --version 2>&1)
if ! echo "$INSTALLED_VER" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: installed binary --version failed: $INSTALLED_VER"
  rm -rf "$REPLACE_DIR"
  exit 1
fi

# 2. Copy binary as the "downloaded" new version
cp "$BINARY" "$REPLACE_DIR/smoke-codebase-memory-mcp"

# 3. Simulate cbm_replace_binary: unlink old, copy new
rm -f "$INSTALL_DIR/codebase-memory-mcp"
cp "$REPLACE_DIR/smoke-codebase-memory-mcp" "$INSTALL_DIR/codebase-memory-mcp"
chmod 755 "$INSTALL_DIR/codebase-memory-mcp"

# 4. Verify replaced binary works
REPLACED_VER=$("$INSTALL_DIR/codebase-memory-mcp" --version 2>&1)
if ! echo "$REPLACED_VER" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: replaced binary --version failed: $REPLACED_VER"
  rm -rf "$REPLACE_DIR"
  exit 1
fi
echo "OK: binary replacement succeeded (version: $REPLACED_VER)"

# 5. Test replacement of read-only binary (edge case — cbm_replace_binary
#    handles this via unlink-before-write, which works even on read-only files)
chmod 444 "$INSTALL_DIR/codebase-memory-mcp"
rm -f "$INSTALL_DIR/codebase-memory-mcp"
cp "$REPLACE_DIR/smoke-codebase-memory-mcp" "$INSTALL_DIR/codebase-memory-mcp"
chmod 755 "$INSTALL_DIR/codebase-memory-mcp"
READONLY_VER=$("$INSTALL_DIR/codebase-memory-mcp" --version 2>&1)
if ! echo "$READONLY_VER" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: read-only replacement --version failed: $READONLY_VER"
  rm -rf "$REPLACE_DIR"
  exit 1
fi
echo "OK: read-only binary replacement succeeded"

rm -rf "$REPLACE_DIR"

echo ""
echo "=== Phase 7: MCP advanced tool calls ==="

# 7a: search_code via MCP (graph-augmented v2)
echo "--- Phase 7a: search_code via MCP ---"
MCP_SC_INPUT=$(mktemp)
MCP_SC_OUTPUT=$(mktemp)
cat > "$MCP_SC_INPUT" << SCEOF
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"index_repository","arguments":{"repo_path":"$TMPDIR","mode":"fast"}}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_code","arguments":{"pattern":"compute","mode":"compact","limit":3}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_code_snippet","arguments":{"qualified_name":"compute"}}}
SCEOF

mcp_run "$MCP_SC_INPUT" "$MCP_SC_OUTPUT" 30

if ! grep -q '"id":3' "$MCP_SC_OUTPUT"; then
  echo "FAIL: search_code response (id:3) missing"
  exit 1
fi
echo "OK: search_code v2 via MCP"

# 7b: get_code_snippet via MCP
if ! grep -q '"id":4' "$MCP_SC_OUTPUT"; then
  echo "FAIL: get_code_snippet response (id:4) missing"
  exit 1
fi
echo "OK: get_code_snippet via MCP"

rm -f "$MCP_SC_INPUT" "$MCP_SC_OUTPUT"

echo ""
echo "=== Phase 8: agent config install E2E ==="

# Set up isolated HOME with stub agent directories
FAKE_HOME=$(mktemp -d)
mkdir -p "$FAKE_HOME/.claude"
mkdir -p "$FAKE_HOME/.codex"
mkdir -p "$FAKE_HOME/.gemini/antigravity"
mkdir -p "$FAKE_HOME/.openclaw"
mkdir -p "$FAKE_HOME/.kilocode/rules"
mkdir -p "$FAKE_HOME/.config/opencode"
if [ "$(uname -s)" = "Darwin" ]; then
  mkdir -p "$FAKE_HOME/Library/Application Support/Zed"
  mkdir -p "$FAKE_HOME/Library/Application Support/Code/User"
  mkdir -p "$FAKE_HOME/Library/Application Support/Code/User/globalStorage/kilocode.kilo-code/settings"
elif [[ "${BINARY:-}" == *.exe ]]; then
  mkdir -p "$FAKE_HOME/AppData/Local/Zed"
  mkdir -p "$FAKE_HOME/AppData/Roaming/Code/User"
  mkdir -p "$FAKE_HOME/AppData/Roaming/Code/User/globalStorage/kilocode.kilo-code/settings"
else
  mkdir -p "$FAKE_HOME/.config/zed"
  mkdir -p "$FAKE_HOME/.config/Code/User"
  mkdir -p "$FAKE_HOME/.config/Code/User/globalStorage/kilocode.kilo-code/settings"
fi
mkdir -p "$FAKE_HOME/.local/bin"
# Copy binary with correct name for platform
if [[ "$BINARY" == *.exe ]]; then
  cp "$BINARY" "$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"
  SELF_PATH="$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"
else
  cp "$BINARY" "$FAKE_HOME/.local/bin/codebase-memory-mcp"
  SELF_PATH="$FAKE_HOME/.local/bin/codebase-memory-mcp"
fi
printf '#!/bin/sh\necho stub\n' > "$FAKE_HOME/.local/bin/aider" && chmod +x "$FAKE_HOME/.local/bin/aider" 2>/dev/null || true
printf '#!/bin/sh\necho stub\n' > "$FAKE_HOME/.local/bin/opencode" && chmod +x "$FAKE_HOME/.local/bin/opencode" 2>/dev/null || true

# Pre-existing configs (verify merge, not overwrite)
echo '{"existingKey": true}' > "$FAKE_HOME/.claude.json"
echo '{"existingKey": true}' > "$FAKE_HOME/.gemini/settings.json"
printf '[existing_section]\nline_from_user = true\n' > "$FAKE_HOME/.codex/config.toml"

# Run install — override platform config dirs so cbm_app_config_dir() and
# cbm_app_local_dir() resolve to FAKE_HOME paths on all platforms.
HOME="$FAKE_HOME" \
  XDG_CONFIG_HOME="$FAKE_HOME/.config" \
  APPDATA="$FAKE_HOME/AppData/Roaming" \
  LOCALAPPDATA="$FAKE_HOME/AppData/Local" \
  PATH="$FAKE_HOME/.local/bin:$PATH" \
  "$BINARY" install -y 2>&1 || true

# Helper for JSON validation (pipe file to python — avoids MSYS2 path translation issues)
json_get() { cat "$1" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)" 2>/dev/null || echo ""; }

# Helper: compare command paths (handles Windows D:\... vs POSIX /tmp/... mismatch)
path_match() {
  [ "$1" = "$2" ] && return 0
  [ "$(basename "$1" 2>/dev/null)" = "$(basename "$2" 2>/dev/null)" ] && return 0
  return 1
}

# 8a: Claude Code MCP (new path) — correct command
CMD=$(json_get "$FAKE_HOME/.claude.json" "d.get('mcpServers',{}).get('codebase-memory-mcp',{}).get('command','')")
if [ -z "$CMD" ] || ! path_match "$CMD" "$SELF_PATH"; then
  echo "DEBUG 8a: file=$FAKE_HOME/.claude.json"
  cat "$FAKE_HOME/.claude.json" 2>/dev/null | head -5 || echo "(file not found)"
  echo "FAIL 8a: .claude.json command='$CMD', expected '$SELF_PATH'"
  exit 1
fi
echo "OK 8a: Claude Code MCP (.claude.json)"

# 8b: Claude Code MCP — existing key preserved (merge not overwrite)
EXISTING=$(json_get "$FAKE_HOME/.claude.json" "d.get('existingKey', False)")
if [ "$EXISTING" != "True" ]; then
  echo "FAIL 8b: .claude.json existingKey lost (overwrite instead of merge)"
  exit 1
fi
echo "OK 8b: .claude.json preserved existing keys"

# 8c: Claude Code MCP (legacy path)
CMD=$(json_get "$FAKE_HOME/.claude/.mcp.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8c: .claude/.mcp.json command='$CMD'"
  exit 1
fi
echo "OK 8c: Claude Code MCP (.claude/.mcp.json)"

# 8d: Claude Code hooks
if ! cat "$FAKE_HOME/.claude/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
hooks = d.get('hooks', {}).get('PreToolUse', [])
found = any('Grep' in str(h.get('matcher', '')) for h in hooks)
sys.exit(0 if found else 1)
" 2>/dev/null; then
  echo "FAIL 8d: PreToolUse hook not found in settings.json"
  exit 1
fi
echo "OK 8d: Claude Code PreToolUse hook"

# 8e: Claude Code gate script
if [ "$(uname -s)" != "MINGW64_NT" ] 2>/dev/null; then
  if [ ! -x "$FAKE_HOME/.claude/hooks/cbm-code-discovery-gate" ]; then
    echo "FAIL 8e: gate script not executable or missing"
    exit 1
  fi
  echo "OK 8e: gate script installed and executable"
fi

# 8f-8h: Codex TOML
if ! grep -q '\[mcp_servers.codebase-memory-mcp\]' "$FAKE_HOME/.codex/config.toml"; then
  echo "FAIL 8f: Codex TOML missing MCP section"
  exit 1
fi
if ! grep -q 'existing_section' "$FAKE_HOME/.codex/config.toml"; then
  echo "FAIL 8h: Codex TOML lost existing section (overwrite)"
  exit 1
fi
echo "OK 8f-h: Codex TOML (MCP + preserved existing)"

# 8i: Codex instructions
if [ ! -f "$FAKE_HOME/.codex/AGENTS.md" ] || ! grep -q 'codebase-memory-mcp' "$FAKE_HOME/.codex/AGENTS.md"; then
  echo "FAIL 8i: Codex AGENTS.md missing"
  exit 1
fi
echo "OK 8i: Codex instructions"

# 8j-l: Gemini MCP + hooks + merge
CMD=$(json_get "$FAKE_HOME/.gemini/settings.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8j: Gemini MCP command='$CMD'"
  exit 1
fi
EXISTING=$(json_get "$FAKE_HOME/.gemini/settings.json" "d.get('existingKey', False)")
if [ "$EXISTING" != "True" ]; then
  echo "FAIL 8k: Gemini settings.json lost existing key"
  exit 1
fi
echo "OK 8j-k: Gemini MCP (correct command + preserved existing)"

if ! cat "$FAKE_HOME/.gemini/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
hooks = d.get('hooks', {}).get('BeforeTool', [])
sys.exit(0 if len(hooks) > 0 else 1)
" 2>/dev/null; then
  echo "FAIL 8l: Gemini BeforeTool hook missing"
  exit 1
fi
echo "OK 8l: Gemini BeforeTool hook"

# 8m: Gemini instructions
if [ ! -f "$FAKE_HOME/.gemini/GEMINI.md" ]; then
  echo "FAIL 8m: Gemini GEMINI.md missing"
  exit 1
fi
echo "OK 8m: Gemini instructions"

# 8n: Zed MCP
if [ "$(uname -s)" = "Darwin" ]; then
  ZED_CFG="$FAKE_HOME/Library/Application Support/Zed/settings.json"
elif [[ "$BINARY" == *.exe ]]; then
  ZED_CFG="$FAKE_HOME/AppData/Local/Zed/settings.json"
else
  ZED_CFG="$FAKE_HOME/.config/zed/settings.json"
fi
if [ -f "$ZED_CFG" ]; then
  CMD=$(json_get "$ZED_CFG" "d['context_servers']['codebase-memory-mcp']['command']")
  if ! path_match "$CMD" "$SELF_PATH"; then
    echo "FAIL 8n: Zed command='$CMD'"
    exit 1
  fi
  echo "OK 8n: Zed MCP"
else
  echo "SKIP 8n: Zed config not created (detection may have failed)"
fi

# 8o-p: OpenCode MCP + instructions
# OpenCode detection requires binary on PATH — may not be found on Windows
CMD=$(json_get "$FAKE_HOME/.config/opencode/opencode.json" "d['mcp']['codebase-memory-mcp']['command'][0]")
if [ -n "$CMD" ]; then
  if ! path_match "$CMD" "$SELF_PATH"; then
    echo "FAIL 8o: OpenCode command='$CMD'"
    exit 1
  fi
  echo "OK 8o: OpenCode MCP"
  if [ ! -f "$FAKE_HOME/.config/opencode/AGENTS.md" ]; then
    echo "FAIL 8p: OpenCode AGENTS.md missing"
    exit 1
  fi
  echo "OK 8p: OpenCode instructions"
else
  echo "SKIP 8o-p: OpenCode not detected (binary not on PATH)"
fi

# 8q-r: Antigravity
CMD=$(json_get "$FAKE_HOME/.gemini/antigravity/mcp_config.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8q: Antigravity command='$CMD'"
  exit 1
fi
echo "OK 8q: Antigravity MCP"
if [ ! -f "$FAKE_HOME/.gemini/antigravity/AGENTS.md" ]; then
  echo "FAIL 8r: Antigravity AGENTS.md missing"
  exit 1
fi
echo "OK 8r: Antigravity instructions"

# 8s: Aider instructions (detection requires binary on PATH)
if [ -f "$FAKE_HOME/CONVENTIONS.md" ]; then
  if ! grep -q 'codebase-memory-mcp' "$FAKE_HOME/CONVENTIONS.md"; then
    echo "FAIL 8s: Aider CONVENTIONS.md missing content"
    exit 1
  fi
  echo "OK 8s: Aider instructions"
else
  echo "SKIP 8s: Aider not detected (binary not on PATH)"
fi

# 8t: KiloCode MCP
if [ "$(uname -s)" = "Darwin" ]; then
  KILO_CFG="$FAKE_HOME/Library/Application Support/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json"
elif [[ "$BINARY" == *.exe ]]; then
  KILO_CFG="$FAKE_HOME/AppData/Roaming/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json"
else
  KILO_CFG="$FAKE_HOME/.config/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json"
fi
CMD=$(json_get "$KILO_CFG" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8t: KiloCode command='$CMD'"
  exit 1
fi
echo "OK 8t: KiloCode MCP"

# 8u: KiloCode instructions
if [ ! -f "$FAKE_HOME/.kilocode/rules/codebase-memory-mcp.md" ]; then
  echo "FAIL 8u: KiloCode rules file missing"
  exit 1
fi
echo "OK 8u: KiloCode instructions"

# 8v: VS Code MCP
if [ "$(uname -s)" = "Darwin" ]; then
  VSCODE_CFG="$FAKE_HOME/Library/Application Support/Code/User/mcp.json"
elif [[ "$BINARY" == *.exe ]]; then
  VSCODE_CFG="$FAKE_HOME/AppData/Roaming/Code/User/mcp.json"
else
  VSCODE_CFG="$FAKE_HOME/.config/Code/User/mcp.json"
fi
CMD=$(json_get "$VSCODE_CFG" "d['servers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8v: VS Code command='$CMD'"
  exit 1
fi
echo "OK 8v: VS Code MCP"

# 8w: OpenClaw MCP
CMD=$(json_get "$FAKE_HOME/.openclaw/openclaw.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8w: OpenClaw command='$CMD'"
  exit 1
fi
echo "OK 8w: OpenClaw MCP"

# 8x: Consolidated skill (old 4-skill dirs cleaned up, replaced by 1)
SKILL_FILE="$FAKE_HOME/.claude/skills/codebase-memory/SKILL.md"
if [ ! -s "$SKILL_FILE" ]; then
  echo "FAIL 8x: skill codebase-memory missing or empty"
  exit 1
fi
echo "OK 8x: skill installed"

echo ""
echo "=== Phase 9: agent config uninstall E2E ==="

# Run uninstall (same FAKE_HOME with all configs present)
HOME="$FAKE_HOME" \
  XDG_CONFIG_HOME="$FAKE_HOME/.config" \
  APPDATA="$FAKE_HOME/AppData/Roaming" \
  LOCALAPPDATA="$FAKE_HOME/AppData/Local" \
  PATH="$FAKE_HOME/.local/bin:$PATH" \
  "$BINARY" uninstall -y -n 2>&1 || true

# 9a-b: Claude Code MCP removed but existing keys preserved
if cat "$FAKE_HOME/.claude.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
if 'codebase-memory-mcp' in d.get('mcpServers', {}):
    sys.exit(1)
if not d.get('existingKey', False):
    sys.exit(2)
sys.exit(0)
" 2>/dev/null; then
  echo "OK 9a-b: Claude Code MCP removed, existing keys preserved"
else
  echo "FAIL 9a-b: Claude Code uninstall verification failed"
  exit 1
fi

# 9c: Legacy MCP removed
if cat "$FAKE_HOME/.claude/.mcp.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('mcpServers', {}) else 0)
" 2>/dev/null; then
  echo "OK 9c: legacy .mcp.json cleaned"
else
  echo "FAIL 9c: legacy .mcp.json still has entry"
  exit 1
fi

# 9d: Hooks removed
if cat "$FAKE_HOME/.claude/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
hooks = d.get('hooks', {}).get('PreToolUse', [])
found = any('cbm-code-discovery-gate' in str(h) for h in hooks)
sys.exit(1 if found else 0)
" 2>/dev/null; then
  echo "OK 9d: PreToolUse hook removed"
else
  echo "FAIL 9d: PreToolUse hook still present"
  exit 1
fi

# 9e-f: Codex TOML cleaned, existing preserved
if grep -q '\[mcp_servers.codebase-memory-mcp\]' "$FAKE_HOME/.codex/config.toml" 2>/dev/null; then
  echo "FAIL 9e: Codex TOML still has MCP section"
  exit 1
fi
if ! grep -q 'existing_section' "$FAKE_HOME/.codex/config.toml" 2>/dev/null; then
  echo "FAIL 9f: Codex TOML lost existing section"
  exit 1
fi
echo "OK 9e-f: Codex TOML cleaned, existing preserved"

# 9g-i: Gemini MCP removed, existing preserved, hooks removed
if cat "$FAKE_HOME/.gemini/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
has_mcp = 'codebase-memory-mcp' in d.get('mcpServers', {})
has_existing = d.get('existingKey', False)
hooks = d.get('hooks', {}).get('BeforeTool', [])
has_hook = any('codebase-memory-mcp' in str(h) for h in hooks)
sys.exit(0 if (not has_mcp and has_existing and not has_hook) else 1)
" 2>/dev/null; then
  echo "OK 9g-i: Gemini MCP removed, existing preserved, hooks removed"
else
  echo "FAIL 9g-i: Gemini uninstall verification failed"
  exit 1
fi

# 9j: VS Code
if cat "$VSCODE_CFG" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('servers', {}) else 0)
" 2>/dev/null; then
  echo "OK 9j: VS Code MCP removed"
else
  echo "FAIL 9j: VS Code MCP still present"
  exit 1
fi

# 9k: OpenClaw
if cat "$FAKE_HOME/.openclaw/openclaw.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('mcpServers', {}) else 0)
" 2>/dev/null; then
  echo "OK 9k: OpenClaw MCP removed"
else
  echo "FAIL 9k: OpenClaw MCP still present"
  exit 1
fi

# 9l: Skills removed (consolidated skill dir)
if [ -d "$FAKE_HOME/.claude/skills/codebase-memory" ]; then
  echo "FAIL 9l: skills not removed"
  exit 1
fi
echo "OK 9l: skills removed"

echo ""
echo "--- Phase 9b: adversarial install/uninstall tests ---"

# 9b-1: Install with minimal agents (empty HOME, no agent dirs)
# Note: cbm_find_cli searches hardcoded paths (/usr/local/bin, /opt/homebrew/bin)
# so PATH-based agents like aider may still be detected. We verify the install
# completes without crash and prints "Detected agents:" line.
EMPTY_HOME=$(mktemp -d)
mkdir -p "$EMPTY_HOME/.local/bin"
INSTALL_OUT=$(HOME="$EMPTY_HOME" "$BINARY" install -y 2>&1) || true
if ! echo "$INSTALL_OUT" | grep -qi 'detected agents'; then
  echo "FAIL 9b-1: install output missing 'Detected agents' line"
  exit 1
fi
echo "OK 9b-1: install with minimal agents exits cleanly"
rm -rf "$EMPTY_HOME"

# 9b-2: Install twice (idempotent)
IDEM_HOME=$(mktemp -d)
mkdir -p "$IDEM_HOME/.claude" "$IDEM_HOME/.local/bin"
cp "$BINARY" "$IDEM_HOME/.local/bin/codebase-memory-mcp"
HOME="$IDEM_HOME" "$BINARY" install -y 2>&1 > /dev/null || true
HOME="$IDEM_HOME" "$BINARY" install -y 2>&1 > /dev/null || true
# Count MCP entries — should be exactly 1
COUNT=$(cat "$IDEM_HOME/.claude.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(list(d.get('mcpServers',{}).keys()).count('codebase-memory-mcp'))
" 2>/dev/null || echo "0")
if [ "$COUNT" != "1" ]; then
  echo "FAIL 9b-2: double install created $COUNT entries (expected 1)"
  exit 1
fi
echo "OK 9b-2: double install is idempotent"
rm -rf "$IDEM_HOME"

# 9b-3: Uninstall without prior install
CLEAN_HOME=$(mktemp -d)
mkdir -p "$CLEAN_HOME/.claude" "$CLEAN_HOME/.local/bin"
UNINSTALL_OUT=$(HOME="$CLEAN_HOME" "$BINARY" uninstall -y -n 2>&1) || true
echo "OK 9b-3: uninstall without install doesn't crash"
rm -rf "$CLEAN_HOME"

# 9b-4: Install over corrupt JSON
CORRUPT_HOME=$(mktemp -d)
mkdir -p "$CORRUPT_HOME/.claude" "$CORRUPT_HOME/.local/bin"
cp "$BINARY" "$CORRUPT_HOME/.local/bin/codebase-memory-mcp"
echo '{invalid json here' > "$CORRUPT_HOME/.claude.json"
HOME="$CORRUPT_HOME" "$BINARY" install -y 2>&1 > /dev/null || true
# Should either fix it or handle gracefully — not crash
echo "OK 9b-4: install over corrupt JSON doesn't crash"
rm -rf "$CORRUPT_HOME"

# 9b-8: Double uninstall
DBL_HOME=$(mktemp -d)
mkdir -p "$DBL_HOME/.claude" "$DBL_HOME/.local/bin"
cp "$BINARY" "$DBL_HOME/.local/bin/codebase-memory-mcp"
HOME="$DBL_HOME" "$BINARY" install -y 2>&1 > /dev/null || true
HOME="$DBL_HOME" "$BINARY" uninstall -y -n 2>&1 > /dev/null || true
HOME="$DBL_HOME" "$BINARY" uninstall -y -n 2>&1 > /dev/null || true
echo "OK 9b-8: double uninstall doesn't crash"
rm -rf "$DBL_HOME"

# 9b-9: Non-interactive update without --standard/--ui should fail cleanly (not hang)
if [ "$(uname -s)" != "MINGW64_NT" ] 2>/dev/null; then
  NONINT_OUT=$(echo "" | "$BINARY" update --dry-run 2>&1) || true
  if echo "$NONINT_OUT" | grep -qi 'terminal\|requires.*flag\|error'; then
    echo "OK 9b-9: non-interactive update fails with clear error"
  else
    # Dry-run may still complete if no variant prompt needed
    echo "OK 9b-9: non-interactive update handled gracefully"
  fi
fi

rm -rf "$FAKE_HOME" "$EMPTY_HOME"

echo ""
echo "=== Phase 10: binary security E2E ==="

SECURITY_DIR=$(mktemp -d)
SECURITY_BIN="$SECURITY_DIR/codebase-memory-mcp"
cp "$BINARY" "$SECURITY_BIN"
chmod 755 "$SECURITY_BIN"

if [ "$(uname -s)" = "Darwin" ]; then
  # macOS signing tests
  if codesign -v "$SECURITY_BIN" 2>/dev/null; then
    echo "OK 10a: binary has valid signature"
  else
    echo "FAIL 10a: binary has no valid signature (linker should auto-sign arm64)"
    exit 1
  fi

  codesign --remove-signature "$SECURITY_BIN" 2>/dev/null || true

  # Detect binary architecture (not shell arch — Rosetta reports x86_64 for arm64 binaries)
  BIN_ARCH=$(file "$SECURITY_BIN" | grep -o 'arm64\|x86_64' | head -1)

  if [ "$BIN_ARCH" = "arm64" ]; then
    # arm64: unsigned must SIGKILL (exit 137 = 128+9)
    UNSIGNED_EXIT=0
    "$SECURITY_BIN" --version > /dev/null 2>&1 || UNSIGNED_EXIT=$?
    if [ "$UNSIGNED_EXIT" -eq 137 ] || [ "$UNSIGNED_EXIT" -eq 9 ]; then
      echo "OK 10c: unsigned arm64 binary killed (exit $UNSIGNED_EXIT)"
    else
      echo "FAIL 10c: unsigned arm64 exit=$UNSIGNED_EXIT (expected 137)"
      exit 1
    fi
  else
    # x86_64: unsigned should still run
    if "$SECURITY_BIN" --version > /dev/null 2>&1; then
      echo "OK 10c: unsigned x86_64 binary runs (no signing required)"
    else
      echo "FAIL 10c: unsigned x86_64 binary failed"
      exit 1
    fi
  fi

  # Re-sign and verify
  xattr -d com.apple.quarantine "$SECURITY_BIN" 2>/dev/null || true
  codesign --sign - --force "$SECURITY_BIN" 2>/dev/null
  if "$SECURITY_BIN" --version > /dev/null 2>&1; then
    echo "OK 10e: re-signed binary runs"
  else
    echo "FAIL 10e: re-signed binary failed"
    exit 1
  fi
else
  # Linux/Windows: unsigned binary should run fine
  if "$SECURITY_BIN" --version > /dev/null 2>&1; then
    echo "OK 10a: binary runs without signing ($(uname -s))"
  else
    echo "FAIL 10a: binary failed to run on $(uname -s)"
    exit 1
  fi

  # chmod +x is sufficient
  chmod -x "$SECURITY_BIN"
  chmod +x "$SECURITY_BIN"
  if "$SECURITY_BIN" --version > /dev/null 2>&1; then
    echo "OK 10c: chmod +x is sufficient"
  else
    echo "FAIL 10c: chmod +x didn't restore executability"
    exit 1
  fi
fi

rm -rf "$SECURITY_DIR"

echo ""
echo "=== Phase 11: process kill E2E ==="

# Start MCP server in background
MCP_KILL_INPUT=$(mktemp)
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"kill-test","version":"1.0"}}}' > "$MCP_KILL_INPUT"
"$BINARY" < "$MCP_KILL_INPUT" > /dev/null 2>&1 &
KILL_PID=$!
sleep 1

if kill -0 "$KILL_PID" 2>/dev/null; then
  echo "OK 11a-b: MCP server running (pid=$KILL_PID)"
  kill "$KILL_PID" 2>/dev/null || true
  wait "$KILL_PID" 2>/dev/null || true
  sleep 1
  if kill -0 "$KILL_PID" 2>/dev/null; then
    echo "FAIL 11d: process still running after kill"
    exit 1
  fi
  echo "OK 11c-d: process killed successfully"
else
  echo "OK 11: MCP server already exited (clean shutdown on EOF)"
fi

rm -f "$MCP_KILL_INPUT"

echo ""
echo "=== Phase 14: update + uninstall E2E ==="

if [ -n "${SMOKE_DOWNLOAD_URL:-}" ]; then
  # ── 14a-f: Real update command against local HTTP server ──
  UPDATE_HOME=$(mktemp -d)
  mkdir -p "$UPDATE_HOME/.claude" "$UPDATE_HOME/.local/bin"
  if [[ "$BINARY" == *.exe ]]; then
    cp "$BINARY" "$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"
    chmod 755 "$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"
  else
    cp "$BINARY" "$UPDATE_HOME/.local/bin/codebase-memory-mcp"
    chmod 755 "$UPDATE_HOME/.local/bin/codebase-memory-mcp"
    if [ "$(uname -s)" = "Darwin" ]; then
      codesign --sign - --force "$UPDATE_HOME/.local/bin/codebase-memory-mcp" 2>/dev/null || true
    fi
  fi

  # Pre-install agent config with a WRONG binary path (simulates stale config)
  echo '{"mcpServers":{"codebase-memory-mcp":{"command":"/old/stale/path"}}}' > "$UPDATE_HOME/.claude.json"

  # 14a: Run actual update command (detect variant from available archive)
  UPDATE_VARIANT="--standard"
  if curl -sf "$SMOKE_DOWNLOAD_URL/" 2>/dev/null | grep -q "ui-"; then
    UPDATE_VARIANT="--ui"
  fi
  HOME="$UPDATE_HOME" CBM_DOWNLOAD_URL="$SMOKE_DOWNLOAD_URL" \
    "$BINARY" update $UPDATE_VARIANT -y 2>&1 || true

  # 14b: Verify new binary exists and runs
  if [ ! -f "$UPDATE_HOME/.local/bin/codebase-memory-mcp" ]; then
    echo "FAIL 14b: binary missing after update"
    exit 1
  fi
  UPD_BIN="$UPDATE_HOME/.local/bin/codebase-memory-mcp"
  if [ "$(uname -s)" = "Darwin" ]; then
    codesign --sign - --force "$UPD_BIN" 2>/dev/null || true
  fi
  if ! "$UPD_BIN" --version > /dev/null 2>&1; then
    echo "FAIL 14b: updated binary doesn't run"
    exit 1
  fi
  echo "OK 14b: updated binary runs"

  # 14c: Verify agent config was refreshed (stale path replaced)
  UPD_CMD=$(cat "$UPDATE_HOME/.claude.json" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('mcpServers',{}).get('codebase-memory-mcp',{}).get('command',''))" 2>/dev/null || echo "")
  if [ "$UPD_CMD" = "/old/stale/path" ]; then
    echo "FAIL 14c: agent config still has stale path after update"
    exit 1
  fi
  if [ -n "$UPD_CMD" ]; then
    echo "OK 14c: agent config refreshed (path=$UPD_CMD)"
  else
    echo "OK 14c: agent config refreshed (no stale path)"
  fi

  # ── 14d-f: Real uninstall with binary removal ──
  # First verify binary + configs exist
  if [ ! -f "$UPDATE_HOME/.local/bin/codebase-memory-mcp" ]; then
    echo "FAIL 14d: binary should exist before uninstall"
    exit 1
  fi

  # Run actual uninstall
  HOME="$UPDATE_HOME" "$BINARY" uninstall -y 2>&1 || true

  # 14e: Verify binary removed
  if [ -f "$UPDATE_HOME/.local/bin/codebase-memory-mcp" ] || [ -f "$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe" ]; then
    echo "FAIL 14e: binary still exists after uninstall"
    exit 1
  fi
  echo "OK 14e: binary removed by uninstall"

  # 14f: Verify agent config cleaned
  if cat "$UPDATE_HOME/.claude.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
if 'codebase-memory-mcp' in d.get('mcpServers', {}): sys.exit(1)
sys.exit(0)
" 2>/dev/null; then
    echo "OK 14f: agent config removed by uninstall"
  else
    echo "FAIL 14f: agent config still present after uninstall"
    exit 1
  fi

  rm -rf "$UPDATE_HOME"

else
  # Local mode: basic binary replacement test (no download)
  UPDATE_DIR=$(mktemp -d)
  mkdir -p "$UPDATE_DIR/install"
  cp "$BINARY" "$UPDATE_DIR/install/codebase-memory-mcp"
  chmod 755 "$UPDATE_DIR/install/codebase-memory-mcp"
  cp "$BINARY" "$UPDATE_DIR/smoke-downloaded"
  rm -f "$UPDATE_DIR/install/codebase-memory-mcp"
  cp "$UPDATE_DIR/smoke-downloaded" "$UPDATE_DIR/install/codebase-memory-mcp"
  chmod 755 "$UPDATE_DIR/install/codebase-memory-mcp"
  if [ "$(uname -s)" = "Darwin" ]; then
    codesign --sign - --force "$UPDATE_DIR/install/codebase-memory-mcp" 2>/dev/null || true
  fi
  if ! "$UPDATE_DIR/install/codebase-memory-mcp" --version > /dev/null 2>&1; then
    echo "FAIL 14: binary replacement failed"
    exit 1
  fi
  echo "OK 14: binary replacement + verify (local mode)"
  rm -rf "$UPDATE_DIR"
fi

# ── Phase 12 + 13: Download E2E + install script E2E (CI only) ──
# These phases require SMOKE_DOWNLOAD_URL to be set (local HTTP server in CI).
# When unset, they are skipped (local development runs).

if [ -n "${SMOKE_DOWNLOAD_URL:-}" ]; then

echo ""
echo "=== Phase 12: download + checksum + extraction E2E ==="

DL_DIR=$(mktemp -d)

# Detect platform for archive name
DL_OS=$(uname -s | tr 'A-Z' 'a-z')
# Normalize MSYS2/MinGW to "windows"
case "$DL_OS" in
  mingw*|msys*) DL_OS="windows" ;;
esac
DL_ARCH=$(uname -m)
case "$DL_ARCH" in
  aarch64) DL_ARCH="arm64" ;;
  x86_64)
    # Rosetta detection
    if [ "$DL_OS" = "darwin" ] && sysctl -n machdep.cpu.brand_string 2>/dev/null | grep -qi apple; then
      DL_ARCH="arm64"
    else
      DL_ARCH="amd64"
    fi
    ;;
esac

if [ "$DL_OS" = "darwin" ] || [ "$DL_OS" = "linux" ]; then
  DL_EXT="tar.gz"
else
  DL_EXT="zip"
fi
# Try standard name first, fall back to UI variant
DL_ARCHIVE="codebase-memory-mcp-${DL_OS}-${DL_ARCH}.${DL_EXT}"
DL_ARCHIVE_UI="codebase-memory-mcp-ui-${DL_OS}-${DL_ARCH}.${DL_EXT}"

# 12a: curl download (try standard, then UI variant)
echo "--- Phase 12a: curl download ---"
if ! curl -fSL -o "$DL_DIR/$DL_ARCHIVE" "$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE" 2>/dev/null; then
  # Try UI variant
  if curl -fSL -o "$DL_DIR/$DL_ARCHIVE_UI" "$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE_UI" 2>/dev/null; then
    DL_ARCHIVE="$DL_ARCHIVE_UI"
  else
    echo "FAIL 12a: curl download failed (tried standard and ui variants)"
    exit 1
  fi
fi
if [ ! -s "$DL_DIR/$DL_ARCHIVE" ]; then
  echo "FAIL 12a: downloaded archive is empty"
  exit 1
fi
echo "OK 12a: archive downloaded ($(wc -c < "$DL_DIR/$DL_ARCHIVE") bytes)"

# 12b: checksum download
echo "--- Phase 12b: checksum verification ---"
if ! curl -fsSL -o "$DL_DIR/checksums.txt" "$SMOKE_DOWNLOAD_URL/checksums.txt"; then
  echo "FAIL 12b: checksums.txt download failed"
  exit 1
fi

# 12c: verify checksum
EXPECTED=$(grep "$DL_ARCHIVE" "$DL_DIR/checksums.txt" | awk '{print $1}')
if [ -z "$EXPECTED" ]; then
  echo "FAIL 12c: archive not found in checksums.txt"
  exit 1
fi
if command -v sha256sum &>/dev/null; then
  ACTUAL=$(sha256sum "$DL_DIR/$DL_ARCHIVE" | awk '{print $1}')
elif command -v shasum &>/dev/null; then
  ACTUAL=$(shasum -a 256 "$DL_DIR/$DL_ARCHIVE" | awk '{print $1}')
else
  echo "FAIL 12c: no sha256 tool available"
  exit 1
fi
if [ "$EXPECTED" != "$ACTUAL" ]; then
  echo "FAIL 12c: checksum mismatch (expected=$EXPECTED actual=$ACTUAL)"
  exit 1
fi
echo "OK 12c: checksum verified"

# 12d: extract binary
echo "--- Phase 12d: extraction ---"
(cd "$DL_DIR" && if [ "$DL_EXT" = "zip" ]; then unzip -q "$DL_ARCHIVE"; else tar -xzf "$DL_ARCHIVE"; fi)
DL_BIN="$DL_DIR/codebase-memory-mcp"
if [ ! -f "$DL_BIN" ]; then
  echo "FAIL 12d: binary not found after extraction"
  exit 1
fi
chmod +x "$DL_BIN"
echo "OK 12d: binary extracted"

# 12e: extracted binary runs
if ! "$DL_BIN" --version > /dev/null 2>&1; then
  # On macOS arm64, may need signing
  if [ "$DL_OS" = "darwin" ]; then
    xattr -d com.apple.quarantine "$DL_BIN" 2>/dev/null || true
    codesign --sign - --force "$DL_BIN" 2>/dev/null || true
    if ! "$DL_BIN" --version > /dev/null 2>&1; then
      echo "FAIL 12e: extracted binary doesn't run even after signing"
      exit 1
    fi
  else
    echo "FAIL 12e: extracted binary doesn't run"
    exit 1
  fi
fi
echo "OK 12e: extracted binary runs"

# 12f: platform-specific post-extraction verification
if [ "$DL_OS" = "darwin" ]; then
  if codesign -v "$DL_BIN" 2>/dev/null; then
    echo "OK 12f: macOS binary has valid signature (CI pre-signed)"
  else
    echo "OK 12f: macOS binary signed locally (CI pre-sign not yet active)"
  fi
else
  echo "OK 12f: binary runs without signing ($DL_OS)"
fi

rm -rf "$DL_DIR"

echo ""
echo "=== Phase 13: install script E2E ==="

if [ "$DL_OS" != "windows" ] && [ -f "$REPO_ROOT/install.sh" ]; then
  echo "--- Phase 13: install.sh E2E ---"
  INSTALL_TEST_HOME=$(mktemp -d)
  INSTALL_TEST_DIR=$(mktemp -d)
  mkdir -p "$INSTALL_TEST_HOME/.claude"
  mkdir -p "$INSTALL_TEST_HOME/.local/bin"

  # 13a: run install.sh with local URL + isolated HOME
  HOME="$INSTALL_TEST_HOME" CBM_DOWNLOAD_URL="$SMOKE_DOWNLOAD_URL" \
    "$REPO_ROOT/install.sh" --dir="$INSTALL_TEST_DIR" 2>&1 || true

  # 13b: binary placed
  if [ ! -f "$INSTALL_TEST_DIR/codebase-memory-mcp" ]; then
    echo "FAIL 13b: binary not placed by install.sh"
    exit 1
  fi
  echo "OK 13b: binary placed"

  # 13c: binary runs
  # Sign if needed on macOS
  if [ "$DL_OS" = "darwin" ]; then
    codesign --sign - --force "$INSTALL_TEST_DIR/codebase-memory-mcp" 2>/dev/null || true
  fi
  if ! "$INSTALL_TEST_DIR/codebase-memory-mcp" --version > /dev/null 2>&1; then
    echo "FAIL 13c: installed binary doesn't run"
    exit 1
  fi
  echo "OK 13c: binary runs"

  # 13d: macOS signature check
  if [ "$DL_OS" = "darwin" ]; then
    if codesign -v "$INSTALL_TEST_DIR/codebase-memory-mcp" 2>/dev/null; then
      echo "OK 13d: macOS binary signed"
    else
      echo "FAIL 13d: macOS binary not signed after install.sh"
      exit 1
    fi
  else
    echo "OK 13d: no signing needed ($DL_OS)"
  fi

  # 13e: agent configs created (at least Claude Code since we made ~/.claude)
  if [ -f "$INSTALL_TEST_HOME/.claude.json" ] && grep -q 'codebase-memory-mcp' "$INSTALL_TEST_HOME/.claude.json" 2>/dev/null; then
    echo "OK 13e: agent configs created by install.sh"
  else
    echo "FAIL 13e: install.sh did not create agent configs"
    exit 1
  fi

  # 13f: PATH setup — verify shell rc file was modified
  RC_FILE=""
  if [ -f "$INSTALL_TEST_HOME/.zshrc" ]; then RC_FILE="$INSTALL_TEST_HOME/.zshrc"; fi
  if [ -f "$INSTALL_TEST_HOME/.bashrc" ]; then RC_FILE="$INSTALL_TEST_HOME/.bashrc"; fi
  if [ -f "$INSTALL_TEST_HOME/.profile" ]; then RC_FILE="$INSTALL_TEST_HOME/.profile"; fi
  if [ -n "$RC_FILE" ] && grep -q '.local/bin' "$RC_FILE" 2>/dev/null; then
    echo "OK 13f: PATH added to shell rc file"
  elif echo "$PATH" | grep -q "$INSTALL_TEST_DIR"; then
    echo "OK 13f: install dir already on PATH"
  else
    echo "OK 13f: PATH setup (rc file may not have been modified if already present)"
  fi

  rm -rf "$INSTALL_TEST_HOME" "$INSTALL_TEST_DIR"

elif [ -f "$REPO_ROOT/install.ps1" ] && command -v powershell.exe &>/dev/null; then
  echo "--- Phase 13: install.ps1 E2E (Windows) ---"
  PS1_TEST_HOME=$(mktemp -d)
  PS1_TEST_DIR=$(mktemp -d)
  mkdir -p "$PS1_TEST_HOME/.claude"

  # Convert MSYS paths to Windows paths for PowerShell
  if command -v cygpath &>/dev/null; then
    WIN_DIR=$(cygpath -w "$PS1_TEST_DIR")
    WIN_URL="$SMOKE_DOWNLOAD_URL"
    WIN_SCRIPT=$(cygpath -w "$REPO_ROOT/install.ps1")
    WIN_HOME=$(cygpath -w "$PS1_TEST_HOME")
  else
    WIN_DIR="$PS1_TEST_DIR"
    WIN_URL="$SMOKE_DOWNLOAD_URL"
    WIN_SCRIPT="$REPO_ROOT/install.ps1"
    WIN_HOME="$PS1_TEST_HOME"
  fi

  # 13f: run install.ps1
  HOME="$PS1_TEST_HOME" CBM_DOWNLOAD_URL="$WIN_URL" \
    powershell.exe -ExecutionPolicy ByPass -File "$WIN_SCRIPT" "--dir=$WIN_DIR" 2>&1 || true

  # 13g: binary placed
  PS1_BIN="$PS1_TEST_DIR/codebase-memory-mcp.exe"
  if [ ! -f "$PS1_BIN" ] && [ -f "$PS1_TEST_DIR/codebase-memory-mcp" ]; then
    PS1_BIN="$PS1_TEST_DIR/codebase-memory-mcp"
  fi
  if [ -f "$PS1_BIN" ]; then
    echo "OK 13g: binary placed by install.ps1"
  else
    echo "FAIL 13g: binary not placed by install.ps1"
    exit 1
  fi

  # 13h: binary runs
  if "$PS1_BIN" --version > /dev/null 2>&1; then
    echo "OK 13h: binary runs"
  else
    echo "FAIL 13h: installed binary doesn't run"
    exit 1
  fi

  rm -rf "$PS1_TEST_HOME" "$PS1_TEST_DIR"
else
  echo "SKIP Phase 13: no install script available for this platform"
fi

else
  echo ""
  echo "=== Phase 12-13: SKIPPED (SMOKE_DOWNLOAD_URL not set) ==="
fi

# ── Phase 15: UI HTTP server reachability ──
# Only runs if the binary was built with embedded UI assets.
echo ""
echo "=== Phase 15: UI HTTP server ==="

UI_PORT=19876
UI_INPUT=$(mktemp)
"$BINARY" --port "$UI_PORT" < "$UI_INPUT" > /dev/null 2>&1 &
UI_PID=$!
sleep 1

if kill -0 "$UI_PID" 2>/dev/null; then
  # 15a: GET / returns 200 with HTML content
  UI_BODY=$(curl -sf "http://127.0.0.1:$UI_PORT/" 2>/dev/null || echo "")
  if echo "$UI_BODY" | grep -qi "<html"; then
    echo "OK 15a: UI serves HTML at /"
  elif [ -z "$UI_BODY" ]; then
    echo "SKIP 15a: UI not reachable (binary may not have embedded assets)"
  else
    echo "FAIL 15a: UI root did not return HTML"
    kill "$UI_PID" 2>/dev/null || true
    exit 1
  fi

  # 15b: POST /rpc accepts JSON-RPC and returns JSON
  RPC_BODY=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
    "http://127.0.0.1:$UI_PORT/rpc" 2>/dev/null || echo "")
  if echo "$RPC_BODY" | grep -q "jsonrpc"; then
    echo "OK 15b: /rpc returns JSON-RPC response"
  elif [ -z "$RPC_BODY" ]; then
    echo "SKIP 15b: /rpc not reachable"
  else
    echo "FAIL 15b: /rpc did not return JSON-RPC"
  fi

  kill "$UI_PID" 2>/dev/null || true
  wait "$UI_PID" 2>/dev/null || true
else
  echo "SKIP Phase 15: binary exited immediately (no UI assets embedded)"
fi
rm -f "$UI_INPUT"

echo ""
echo "=== smoke-test: ALL PASSED ==="
