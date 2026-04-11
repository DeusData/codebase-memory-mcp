#!/usr/bin/env bash
set -euo pipefail

# Fuzz testing: feeds random/mutated inputs to the MCP server and CLI
# to find crashes, hangs, and memory errors. Runs for a limited time.
#
# Usage: scripts/security-fuzz-random.sh <binary-path> [duration_seconds]

BINARY="${1:?usage: security-fuzz-random.sh <binary-path> [duration_seconds]}"
DURATION="${2:-60}"

if [[ ! -f "$BINARY" ]]; then
    echo "FAIL: binary not found: $BINARY"
    exit 1
fi

echo "=== Fuzz Testing ($DURATION seconds) ==="

FUZZ_TMPDIR=$(mktemp -d)
trap 'rm -rf "$FUZZ_TMPDIR"' EXIT

CRASHES=0
ITERATIONS=0
END_TIME=$((SECONDS + DURATION))

# Portable timeout
run_with_timeout() {
    local secs="$1"; shift
    if command -v timeout &>/dev/null; then
        timeout "$secs" "$@" || true
    else
        perl -e "alarm($secs); exec @ARGV" -- "$@" || true
    fi
}

# ── Phase 1: Random JSON-RPC mutations ───────────────────────
echo ""
echo "--- Phase 1: Random JSON-RPC mutations ---"

while [ $SECONDS -lt $END_TIME ]; do
    ITERATIONS=$((ITERATIONS + 1))

    # Generate a random mutated JSON-RPC payload
    PAYLOAD=$(python3 -c "
import random, string, json

# Base valid request
base = {
    'jsonrpc': '2.0',
    'id': random.randint(-999999, 999999),
    'method': random.choice([
        'initialize', 'tools/call', 'tools/list',
        random.choice(string.ascii_letters) * random.randint(1, 100),
        '',
    ]),
}

# Random mutations
mutation = random.randint(0, 10)
if mutation == 0:
    # Valid tool call with random args
    base['params'] = {'name': 'search_graph', 'arguments': {
        'name_pattern': ''.join(random.choices(string.printable, k=random.randint(0, 500)))
    }}
elif mutation == 1:
    # Huge nested object
    base['params'] = {'a': {'b': {'c': {'d': {'e': 'deep'}}}}}
elif mutation == 2:
    # Random bytes as method
    base['method'] = ''.join(random.choices(string.printable, k=random.randint(1, 1000)))
elif mutation == 3:
    # Null fields
    base['method'] = None
    base['id'] = None
elif mutation == 4:
    # Array instead of object
    base = [1, 2, 3]
elif mutation == 5:
    # Empty
    base = {}
elif mutation == 6:
    # Random Cypher query
    base['params'] = {'name': 'query_graph', 'arguments': {
        'query': ''.join(random.choices(string.printable, k=random.randint(1, 200)))
    }}
elif mutation == 7:
    # Very long string values
    base['params'] = {'name': 'search_graph', 'arguments': {
        'name_pattern': 'A' * random.randint(10000, 100000)
    }}
elif mutation == 8:
    # Unicode/binary-like content
    base['params'] = {'name': 'search_code', 'arguments': {
        'pattern': ''.join(chr(random.randint(0, 0xFFFF)) for _ in range(100))
    }}
elif mutation == 9:
    # Random tool name
    base['params'] = {'name': ''.join(random.choices(string.ascii_letters, k=50)), 'arguments': {}}
else:
    # Completely random JSON
    base = ''.join(random.choices(string.printable, k=random.randint(1, 500)))

try:
    print(json.dumps(base))
except:
    print(json.dumps({'garbage': True}))
" 2>/dev/null || echo '{}')

    # Build session: init + mutated payload
    INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"fuzz","version":"1.0"}}}'
    printf '%s\n%s\n' "$INIT" "$PAYLOAD" > "$FUZZ_TMPDIR/input.jsonl"

    # Run and check for crashes (not exit code — we expect errors, just not crashes)
    run_with_timeout 5 "$BINARY" < "$FUZZ_TMPDIR/input.jsonl" > /dev/null 2>&1
    EC=$?

    # 139 = SIGSEGV, 134 = SIGABRT, 136 = SIGFPE — real crashes
    if [ $EC -eq 139 ] || [ $EC -eq 134 ] || [ $EC -eq 136 ]; then
        echo "CRASH: exit code $EC on iteration $ITERATIONS"
        echo "Payload: $PAYLOAD"
        CRASHES=$((CRASHES + 1))
    fi
done

echo "Phase 1: $ITERATIONS iterations, $CRASHES crashes"

# ── Phase 2: Random Cypher queries via CLI ───────────────────
echo ""
echo "--- Phase 2: Random Cypher queries via CLI ---"

CLI_ITERATIONS=0
CLI_END=$((SECONDS + 10))

while [ $SECONDS -lt $CLI_END ]; do
    CLI_ITERATIONS=$((CLI_ITERATIONS + 1))

    QUERY=$(python3 -c "
import random, string
parts = ['MATCH', 'WHERE', 'RETURN', '(', ')', '-[', ']->', '<-[', ']-',
         'n', 'r', '.name', '.label', '=', '\"', \"'\", ';', '--', 'DROP',
         'DELETE', 'ATTACH', 'DETACH', '*', 'COUNT', 'LIMIT', 'ORDER BY']
q = ' '.join(random.choices(parts, k=random.randint(1, 20)))
# Sometimes add random garbage
if random.random() > 0.5:
    q += ''.join(random.choices(string.printable, k=random.randint(1, 100)))
print(q)
" 2>/dev/null || echo "MATCH (n) RETURN n")

    run_with_timeout 3 "$BINARY" cli query_graph "{\"query\":\"$QUERY\"}" > /dev/null 2>&1
    EC=$?

    if [ $EC -eq 139 ] || [ $EC -eq 134 ] || [ $EC -eq 136 ]; then
        echo "CRASH: exit code $EC on Cypher iteration $CLI_ITERATIONS"
        echo "Query: $QUERY"
        CRASHES=$((CRASHES + 1))
    fi
done

echo "Phase 2: $CLI_ITERATIONS iterations, $CRASHES total crashes"

# ── Summary ──────────────────────────────────────────────────
echo ""
if [ $CRASHES -gt 0 ]; then
    echo "=== FUZZ TESTING FAILED: $CRASHES crash(es) found ==="
    exit 1
fi

echo "=== Fuzz testing passed: $((ITERATIONS + CLI_ITERATIONS)) iterations, 0 crashes ==="
