#!/bin/bash
# benchmark-search-graph.sh — Time search_graph name_pattern= queries against the
# running codebase-memory-mcp binary, which must already have the SIS project indexed.
#
# Usage:
#   scripts/benchmark-search-graph.sh [binary-path]
#
# Defaults to /usr/local/bin/codebase-memory-mcp.

set -euo pipefail

BINARY="${1:-/usr/local/bin/codebase-memory-mcp}"
PROJECT="home-ubuntu-dev-sis"

echo "Binary: $BINARY"
echo "Project: $PROJECT"
echo ""

# Each benchmark case: NAME and JSON request body
run_case() {
    local label="$1"
    local request="$2"
    local start end elapsed_ms result

    start=$(date +%s%3N)
    result=$( echo "$request" | "$BINARY" 2>/dev/null || true )
    end=$(date +%s%3N)
    elapsed_ms=$(( end - start ))

    # Extract match count from JSON result (nodes array length) or total field
    local count
    count=$(echo "$result" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    content = d.get('result', {}).get('content', [{}])[0].get('text', '{}')
    obj = json.loads(content)
    print(obj.get('total', obj.get('count', '?')))
except Exception as e:
    print('?')
" 2>/dev/null || echo "?")

    printf "  %-55s %5dms  (total=%s)\n" "$label" "$elapsed_ms" "$count"
}

# Build a minimal JSON-RPC call for search_graph
sg() {
    local project="$1"
    local args="$2"
    printf '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_graph","arguments":{"project":"%s",%s}}}' \
        "$project" "$args"
}

echo "=== search_graph name_pattern= benchmarks ==="
run_case "name_pattern=.*Controller.*"        "$(sg "$PROJECT" '"name_pattern":".*Controller.*","limit":20')"
run_case "name_pattern=.*Service.*"           "$(sg "$PROJECT" '"name_pattern":".*Service.*","limit":20')"
run_case "name_pattern=.*Repository.*"        "$(sg "$PROJECT" '"name_pattern":".*Repository.*","limit":20')"
run_case "name_pattern=specificFuncName"      "$(sg "$PROJECT" '"name_pattern":"specificFuncName","limit":20')"
run_case "label=Method + name_pattern=.*get.*" "$(sg "$PROJECT" '"label":"Method","name_pattern":".*get.*","limit":20')"
run_case "name_pattern=.*Approve.*"           "$(sg "$PROJECT" '"name_pattern":".*Approve.*","limit":20')"
run_case "name_pattern=.*authorize.*"         "$(sg "$PROJECT" '"name_pattern":".*authorize.*","limit":20')"

echo ""
echo "=== search_graph query= benchmarks (BM25 path — for reference) ==="
run_case "query=approve apps authorization school"           "$(sg "$PROJECT" '"query":"approve apps authorization school","limit":20')"
run_case "query=Group User Details Manage All Users"         "$(sg "$PROJECT" '"query":"Group User Details Manage All Users permission","limit":20')"
run_case "query=dev portal approve integration third party"  "$(sg "$PROJECT" '"query":"dev portal approve integration third party apps authorization","limit":20')"
