#!/bin/bash
# benchmark-search-graph.sh — Time search_graph name_pattern= queries against a
# codebase-memory-mcp binary to measure the regex / LIKE pre-filter performance.
#
# Usage:
#   scripts/benchmark-search-graph.sh <binary-path> <project-name>
#
# Example:
#   scripts/benchmark-search-graph.sh ./build/c/codebase-memory-mcp my-project

set -euo pipefail

BINARY="${1:?Usage: $0 <binary-path> <project-name>}"
PROJECT="${2:?Usage: $0 <binary-path> <project-name>}"

echo "Binary:  $BINARY"
echo "Project: $PROJECT"
echo ""

run_case() {
    local label="$1"
    local request="$2"
    local start end elapsed_ms result

    start=$(date +%s%3N)
    result=$(echo "$request" | "$BINARY" 2>/dev/null || true)
    end=$(date +%s%3N)
    elapsed_ms=$(( end - start ))

    local count
    count=$(echo "$result" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    content = d.get('result', {}).get('content', [{}])[0].get('text', '{}')
    obj = json.loads(content)
    print(obj.get('total', obj.get('count', '?')))
except Exception:
    print('?')
" 2>/dev/null || echo "?")

    printf "  %-55s %5dms  (total=%s)\n" "$label" "$elapsed_ms" "$count"
}

sg() {
    local project="$1"
    local args="$2"
    printf '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_graph","arguments":{"project":"%s",%s}}}' \
        "$project" "$args"
}

echo "=== search_graph name_pattern= benchmarks ==="
run_case "name_pattern=.*Controller.*"         "$(sg "$PROJECT" '"name_pattern":".*Controller.*","limit":20')"
run_case "name_pattern=.*Service.*"            "$(sg "$PROJECT" '"name_pattern":".*Service.*","limit":20')"
run_case "name_pattern=.*Repository.*"         "$(sg "$PROJECT" '"name_pattern":".*Repository.*","limit":20')"
run_case "name_pattern=specificFunctionName"   "$(sg "$PROJECT" '"name_pattern":"specificFunctionName","limit":20')"
run_case "label=Method + name_pattern=.*get.*" "$(sg "$PROJECT" '"label":"Method","name_pattern":".*get.*","limit":20')"

echo ""
echo "=== search_graph query= benchmarks (BM25 path — for reference) ==="
run_case "query=controller service handler"    "$(sg "$PROJECT" '"query":"controller service handler","limit":20')"
run_case "query=repository database query"     "$(sg "$PROJECT" '"query":"repository database query","limit":20')"
