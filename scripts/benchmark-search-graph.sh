#!/usr/bin/env bash
# benchmark-search-graph.sh — Time search_graph name_pattern= queries against a
# codebase-memory-mcp binary to measure the regex / LIKE pre-filter performance.
#
# Usage:
#   scripts/benchmark-search-graph.sh <binary-path> <repo-path>
#
# Example:
#   scripts/benchmark-search-graph.sh ./build/c/codebase-memory-mcp ~/src/my-project
#
# The repository is indexed (untimed) into a private runtime and cache first;
# the queries then run against that index through a daemon this run keeps warm,
# so a timing never includes daemon activation and never touches the operator's
# live store (#1696).

set -euo pipefail

BINARY="${1:?Usage: $0 <binary-path> <repo-path>}"
REPO="${2:?Usage: $0 <binary-path> <repo-path>}"
REPO=$(cd "$REPO" && pwd -P)

# shellcheck source=test-runtime.sh
source "$(dirname "${BASH_SOURCE[0]}")/test-runtime.sh"
cbm_test_runtime_init
BENCH_TMP=""
trap 'cbm_test_runtime_cleanup "$BINARY"; [ -z "$BENCH_TMP" ] || rm -rf -- "$BENCH_TMP"' EXIT
BENCH_TMP=$(mktemp -d)
INDEX_ERR="$BENCH_TMP/index-stderr.log"

if ! "$BINARY" daemon start >/dev/null 2>&1; then
    echo "private daemon did not start" >&2
    exit 1
fi
# One pre-escaped spelling of the path, the way the soak harness builds its
# own: a repository path may legitimately contain a quote or a backslash, and
# hand-built JSON turns that into a parse error.
REPO_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$REPO")
# Index and parse keep their stderr instead of discarding it: without it the
# failure below names only its symptom, and the cause — an unreadable
# repository, a refused daemon, a malformed envelope — is unrecoverable.
INDEX_JSON=$("$BINARY" cli index_repository "{\"repo_path\":$REPO_JSON,\"mode\":\"full\"}" \
    2>"$INDEX_ERR" || echo '{}')
PROJECT=$(echo "$INDEX_JSON" | python3 -c "
import json, sys
d = json.load(sys.stdin)
if 'content' in d:
    d = json.loads(d['content'][0]['text'])
print(d.get('project', ''))
" 2>>"$INDEX_ERR" || echo "")
if [ -z "$PROJECT" ]; then
    echo "index of $REPO did not report a project" >&2
    if [ -s "$INDEX_ERR" ]; then
        echo "--- index/parse stderr ---" >&2
        cat "$INDEX_ERR" >&2
    fi
    if [ -n "$INDEX_JSON" ]; then
        echo "--- index response (first 500 bytes) ---" >&2
        printf '%.500s\n' "$INDEX_JSON" >&2
    fi
    exit 1
fi

echo "Binary:  $BINARY"
echo "Project: $PROJECT (indexed from $REPO)"
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
echo "=== search_graph query= benchmarks (BM25 path) ==="
run_case "query=controller service handler"                   "$(sg "$PROJECT" '"query":"controller service handler","limit":20')"
run_case "query=user authentication permission role"          "$(sg "$PROJECT" '"query":"user authentication permission role","limit":20')"
run_case "query=create update delete manage list view admin"  "$(sg "$PROJECT" '"query":"create update delete manage list view admin","limit":20')"
