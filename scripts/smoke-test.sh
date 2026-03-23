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

# 3b: trace_call_path — verify compute has callers
TRACE=$(cli trace_call_path "{\"project\":\"$PROJECT\",\"function_name\":\"compute\",\"direction\":\"inbound\",\"max_depth\":1}")
CALLERS=$(echo "$TRACE" | python3 -c "import json,sys; d=json.loads(json.loads(sys.stdin.read())['content'][0]['text']); print(len(d.get('callers',[])))" 2>/dev/null || echo "0")
if [ "$CALLERS" -lt 1 ]; then
  echo "FAIL: trace_call_path found 0 callers for 'compute'"
  exit 1
fi
echo "OK: trace_call_path found $CALLERS caller(s) for 'compute'"

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
cli delete_project "{\"project_name\":\"$PROJECT\"}" > /dev/null

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
for TOOL in index_repository search_graph trace_call_path get_code_snippet search_code; do
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
echo "=== smoke-test: ALL PASSED ==="
