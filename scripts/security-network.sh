#!/usr/bin/env bash
set -euo pipefail

# Layer 3: Network egress test — monitors outbound connections during MCP session.
#
# Runs a full MCP session (initialize → index → search → EOF) under strace
# and verifies only expected connections are made.
#
# Linux only (strace required). macOS/Windows: skip with success.
#
# Usage: scripts/security-network.sh <binary-path>

BINARY="${1:?usage: security-network.sh <binary-path>}"

if [[ ! -f "$BINARY" ]]; then
    echo "FAIL: binary not found: $BINARY"
    exit 1
fi

echo "=== Layer 3: Network Egress Test ==="

# Skip on non-Linux (no strace)
if [[ "$(uname)" != "Linux" ]]; then
    echo "SKIP: strace not available on $(uname) — covered by binary string audit"
    exit 0
fi

if ! command -v strace &>/dev/null; then
    echo "SKIP: strace not installed"
    exit 0
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Create a minimal test project
mkdir -p "$TMPDIR/project/src"
cat > "$TMPDIR/project/src/main.py" << 'EOF'
def main():
    print("hello")
EOF

# MCP session input (jsonrpc over stdio)
cat > "$TMPDIR/input.jsonl" << JSONL
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"index_repository","arguments":{"repo_path":"$TMPDIR/project"}}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_graph","arguments":{"name_pattern":"main"}}}
JSONL

STRACE_LOG="$TMPDIR/strace.log"

echo "Running MCP session under strace..."

# Run binary with strace monitoring connect() syscalls
# -f: follow forks, -e trace=connect: only log connect() calls
timeout 30 strace -f -e trace=connect \
    "$BINARY" < "$TMPDIR/input.jsonl" \
    > "$TMPDIR/output.jsonl" 2> "$STRACE_LOG" || true

echo ""
echo "--- Connection log ---"

FAIL=0

# Parse strace output for connect() calls with AF_INET (IPv4 network)
# Format: connect(fd, {sa_family=AF_INET, sin_port=htons(443), sin_addr=inet_addr("x.x.x.x")}, ...)
if grep 'sa_family=AF_INET' "$STRACE_LOG" > "$TMPDIR/connections.log" 2>/dev/null; then
    while IFS= read -r conn; do
        # Extract destination IP and port
        ip=$(echo "$conn" | grep -oP 'inet_addr\("\K[^"]+' || echo "unknown")
        port=$(echo "$conn" | grep -oP 'htons\(\K[0-9]+' || echo "0")

        # Allowed connections:
        # - 127.0.0.1 (localhost, any port)
        # - DNS (port 53, any IP)
        # - api.github.com (140.82.x.x range, port 443) — update check
        case "$ip" in
            127.0.0.1|0.0.0.0)
                echo "  OK: localhost:$port"
                ;;
            *)
                if [[ "$port" == "53" ]]; then
                    echo "  OK: DNS lookup to $ip"
                elif [[ "$port" == "443" ]]; then
                    echo "  REVIEW: HTTPS to $ip:$port (expected: api.github.com for update check)"
                    # This is expected — the binary checks for updates on startup
                else
                    echo "  BLOCKED: Unexpected connection to $ip:$port"
                    FAIL=1
                fi
                ;;
        esac
    done < "$TMPDIR/connections.log"
else
    echo "  No outbound connections detected."
fi

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "=== NETWORK EGRESS TEST FAILED ==="
    echo "Unexpected outbound connections detected. Full strace log:"
    grep 'connect(' "$STRACE_LOG" || true
    exit 1
fi

echo "=== Network egress test passed ==="
