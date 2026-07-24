#!/usr/bin/env bash
set -euo pipefail

# Smoke test: verify the binary is fully operational.
#
# Phase 1: --version output
# Phase 2: Index a small multi-language project
# Phase 3: Verify node/edge counts, search, and trace
#
# Usage: smoke-test.sh <binary-path> [--agent-config-only]
# The explicit optional mode runs only version + agent config install/uninstall
# checks (useful when validating installer-only changes).

BINARY="${1:?usage: smoke-test.sh <binary-path> [--agent-config-only]}"
SMOKE_MODE="${2:-}"
if [ -n "$SMOKE_MODE" ] && [ "$SMOKE_MODE" != "--agent-config-only" ]; then
  echo "usage: smoke-test.sh <binary-path> [--agent-config-only]" >&2
  exit 2
fi
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"

smoke_mktemp_file() {
  if [ -n "${SMOKE_TEMP_ROOT:-}" ]; then
    mktemp "$SMOKE_TEMP_ROOT/cbm-smoke.XXXXXX"
  else
    mktemp
  fi
}

smoke_mktemp_dir() {
  if [ -n "${SMOKE_TEMP_ROOT:-}" ]; then
    mktemp -d "$SMOKE_TEMP_ROOT/cbm-smoke.XXXXXX"
  else
    mktemp -d
  fi
}

# Windows release archives contain a small permanent launcher plus a portable
# payload. Whenever a smoke fixture copies the launcher, keep the payload next
# to it so the copied fixture remains a complete portable bundle.
WINDOWS_PAYLOAD=""
if [[ "$BINARY" == *.exe ]]; then
  WINDOWS_PAYLOAD="$(cd "$(dirname "$BINARY")" && pwd)/codebase-memory-mcp.payload.exe"
  if [ ! -f "$WINDOWS_PAYLOAD" ]; then
    echo "FAIL: Windows launcher has no adjacent codebase-memory-mcp.payload.exe"
    exit 1
  fi
fi

copy_smoke_binary() {
  local destination="$1"
  cp "$BINARY" "$destination"
  if [ -n "$WINDOWS_PAYLOAD" ]; then
    cp "$WINDOWS_PAYLOAD" "$(dirname "$destination")/codebase-memory-mcp.payload.exe"
  fi
}

# Retire the shared account daemon (if one is running) and wait until it
# reports not-running. Install/uninstall flows leave an ephemeral daemon
# draining asynchronously whose mapped generation backing and open logs
# block rm on Windows (POSIX rm doesn't care) — so every cleanup of a
# fixture HOME that received an install, and the final cache removal, must
# retire it deterministically first. A daemon that will not retire is a
# failure in its own right, never a tolerated race.
retire_account_daemon() {
  local label="$1"
  if ! "$BINARY" daemon status >/dev/null 2>&1; then
    return 0
  fi
  "$BINARY" daemon stop >/dev/null 2>&1 || true
  local gone=0
  for _ in $(seq 1 100); do            # bounded ~20s (100 x 0.2s)
    if ! "$BINARY" daemon status >/dev/null 2>&1; then gone=1; break; fi
    sleep 0.2
  done
  if [ "$gone" -ne 1 ]; then
    echo "FAIL $label: account daemon still active after daemon stop"
    exit 1
  fi
  if [[ "$BINARY" == *.exe ]]; then
    sleep 1
  fi
}

# Tolerate ERRORS, never CRASHES: the adversarial phases assert "doesn't
# crash", and a bare `|| true` discards exactly the crash exit the claim is
# about. rc >= 128 means killed by signal / fatal status (SIGSEGV=139,
# SIGABRT=134 — ASan aborts land here too).
run_no_crash() {
  local label="$1"; shift
  local rc=0
  "$@" > /dev/null 2>&1 || rc=$?
  if [ "$rc" -ge 128 ]; then
    echo "FAIL $label: crashed (rc=$rc)"
    exit 1
  fi
  return 0
}

TMPDIR=$(smoke_mktemp_dir)
DRYRUN_HOME=""
# On MSYS2/Windows, convert POSIX path to native Windows path for the binary
if command -v cygpath &>/dev/null; then
    TMPDIR=$(cygpath -m "$TMPDIR")
fi
trap 'rm -rf "$TMPDIR" "${DRYRUN_HOME:-}"' EXIT

CLI_STDERR=$(smoke_mktemp_file)
cli() { "$BINARY" cli "$@" 2>"$CLI_STDERR"; }

echo "=== Phase 1: version ==="
VERSION_STATUS=0
OUTPUT=$("$BINARY" --version 2>&1) || VERSION_STATUS=$?
echo "$OUTPUT"
if [ "$VERSION_STATUS" -ne 0 ]; then
  echo "FAIL: --version exited with status $VERSION_STATUS"
  exit 1
fi
if ! echo "$OUTPUT" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: unexpected version output"
  exit 1
fi
echo "OK"

if [ "$SMOKE_MODE" != "--agent-config-only" ]; then
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

# C++ crash reproduction (#424): a large, templated C++ header. The vendored
# tree-sitter runtime previously corrupted the heap and SEGV'd mid-parse on
# large templated C++ in the PRODUCTION build (MI_OVERRIDE=1) — most reliably on
# Windows static-MinGW, where ts_malloc/ts_free could resolve to different
# allocators. Generating a header with heavy parse churn exercises that path;
# the prod binary must index it without crashing (status must be "indexed").
python3 - "$TMPDIR/src/big_templated.hpp" << 'GENEOF'
import sys
with open(sys.argv[1], "w") as f:
    f.write("#include <cstddef>\nnamespace repro {\n")
    for i in range(1500):
        f.write(
            "template <typename T> struct Box{0} {{\n"
            "  T value;\n"
            "  bool operator<(const Box{0} &o) const {{ return value < o.value; }}\n"
            "  bool operator==(const Box{0} &o) const {{ return value == o.value; }}\n"
            "  bool operator>(const Box{0} &o) const {{ return o.value < value; }}\n"
            "  T get() const {{ return value; }}\n"
            "}};\n".format(i)
        )
    f.write("}\n")
GENEOF

# Index (flag form: --repo-path -> repo_path)
if ! RESULT=$(cli index_repository --repo-path "$TMPDIR"); then
  echo "FAIL: index_repository (flag form) exited non-zero"
  cat "$CLI_STDERR"
  exit 1
fi
echo "$RESULT"

# Allocator-integrity guard: the prod binary overrides the global allocator with
# mimalloc. A misconfigured override (e.g. compiling alloc-override.c's
# forwarding defs on a platform where system libs keep using the system
# allocator) corrupts free() and mimalloc prints "mimalloc: error: ..." to
# stderr — often WITHOUT a non-zero exit. Treat any such line as a hard failure.
if grep -qiE 'mimalloc: error|mi_free: invalid pointer|mi_assert' "$CLI_STDERR"; then
  echo "FAIL: mimalloc reported an allocator error during indexing"
  echo "--- stderr ---"
  cat "$CLI_STDERR"
  echo "--- end stderr ---"
  exit 1
fi

STATUS=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('status',''))" 2>/dev/null || echo "")
if [ "$STATUS" != "indexed" ]; then
  echo "FAIL: index status is '$STATUS', expected 'indexed'"
  echo "--- stderr ---"
  cat "$CLI_STDERR"
  echo "--- end stderr ---"
  exit 1
fi

NODES=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('nodes',0))" 2>/dev/null || echo "0")
EDGES=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('edges',0))" 2>/dev/null || echo "0")

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
PROJECT=$(echo "$RESULT" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('project',''))" 2>/dev/null || echo "")

if ! SEARCH=$(cli search_graph --project "$PROJECT" --name-pattern compute); then
  echo "FAIL: search_graph (flag form) exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
# search_graph default output is TOON (key: value scalars + header/rows tables)
TOTAL=$(echo "$SEARCH" | sed -n 's/^total: //p' | head -1)
TOTAL=${TOTAL:-0}
if [ "$TOTAL" -lt 1 ]; then
  echo "FAIL: search_graph for 'compute' returned 0 results"
  exit 1
fi
echo "OK: search_graph found $TOTAL result(s) for 'compute'"

# 3b: trace_path — verify compute has callers
if ! TRACE=$(cli trace_path --project "$PROJECT" --function-name compute --direction inbound --depth 1); then
  echo "FAIL: trace_path (flag form) exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
# trace_path default output is the tree format: callers_total carries the
# exact reachable count (test files excluded by default)
CALLERS=$(echo "$TRACE" | sed -n 's/^callers_total: \([0-9]*\).*/\1/p' | head -1)
CALLERS=${CALLERS:-0}
if [ "$CALLERS" -lt 1 ]; then
  echo "FAIL: trace_path found 0 callers for 'compute'"
  exit 1
fi
echo "OK: trace_path found $CALLERS caller(s) for 'compute'"

# 3c: get_graph_schema — verify labels exist
if ! SCHEMA=$(cli get_graph_schema --project "$PROJECT"); then
  echo "FAIL: get_graph_schema (flag form) exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
LABELS=$(echo "$SCHEMA" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(len(d.get('node_labels',[])))" 2>/dev/null || echo "0")
if [ "$LABELS" -lt 3 ]; then
  echo "FAIL: schema has fewer than 3 node labels"
  exit 1
fi
echo "OK: schema has $LABELS node labels"

# 3d: Verify __init__.py didn't clobber Folder node
if ! FOLDERS=$(cli search_graph --project "$PROJECT" --label Folder); then
  echo "FAIL: search_graph --label Folder exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
FOLDER_COUNT=$(echo "$FOLDERS" | sed -n 's/^total: //p' | head -1)
FOLDER_COUNT=${FOLDER_COUNT:-0}
if [ "$FOLDER_COUNT" -lt 2 ]; then
  echo "FAIL: expected at least 2 Folder nodes (src, src/pkg), got $FOLDER_COUNT"
  exit 1
fi
echo "OK: $FOLDER_COUNT Folder nodes (init.py didn't clobber them)"

# 3d-cypher: query_graph Cypher capabilities
# #238 WITH DISTINCT — all functions share label "Function" → collapses to 1 row.
CYPHER_WD=$(cli query_graph --project "$PROJECT" --query "MATCH (f:Function) WITH DISTINCT f.label AS lbl RETURN lbl")
WD_ROWS=$(echo "$CYPHER_WD" | sed -n 's/^total: //p' | head -1)
WD_ROWS=${WD_ROWS:-0}
if [ "$WD_ROWS" -lt 1 ]; then
  echo "FAIL: query_graph WITH DISTINCT returned 0 rows"
  echo "$CYPHER_WD"
  exit 1
fi
echo "OK: query_graph WITH DISTINCT returned $WD_ROWS row(s)"

# #241 WHERE label test — f:Function is true for every Function node.
CYPHER_LBL=$(cli query_graph --project "$PROJECT" --query "MATCH (f:Function) WHERE f:Function RETURN f.name")
LBL_ROWS=$(echo "$CYPHER_LBL" | sed -n 's/^total: //p' | head -1)
LBL_ROWS=${LBL_ROWS:-0}
if [ "$LBL_ROWS" -lt 1 ]; then
  echo "FAIL: query_graph WHERE label-test returned 0 rows"
  echo "$CYPHER_LBL"
  exit 1
fi
echo "OK: query_graph WHERE f:Function returned $LBL_ROWS row(s)"

# #242 label alternation — (n:Function|Module) seeds either label.
CYPHER_ALT=$(cli query_graph --project "$PROJECT" --query "MATCH (n:Function|Module) RETURN n.name")
ALT_ROWS=$(echo "$CYPHER_ALT" | sed -n 's/^total: //p' | head -1)
ALT_ROWS=${ALT_ROWS:-0}
if [ "$ALT_ROWS" -lt 1 ]; then
  echo "FAIL: query_graph label alternation returned 0 rows"
  echo "$CYPHER_ALT"
  exit 1
fi
echo "OK: query_graph (n:Function|Module) returned $ALT_ROWS row(s)"

# #239 count(DISTINCT) — must parse and return a single aggregate row.
CYPHER_CD=$(cli query_graph --project "$PROJECT" --query "MATCH (f:Function) RETURN count(DISTINCT f.label)")
CD_ROWS=$(echo "$CYPHER_CD" | sed -n 's/^total: //p' | head -1)
CD_ROWS=${CD_ROWS:-0}
if [ "$CD_ROWS" -ne 1 ]; then
  echo "FAIL: query_graph count(DISTINCT) expected 1 row, got $CD_ROWS"
  echo "$CYPHER_CD"
  exit 1
fi
echo "OK: query_graph count(DISTINCT f.label) returned 1 aggregate row"

# 3d-funcs: scalar / introspection functions (full Cypher suite, Tier 1)
cyp_first_cell() {
  # $1 = query; echoes rows[0][0] (or empty). Flag form passes the query as ONE
  # argv token, so string-literal args (e.g. replace(f.name,"a","A")) and Cypher
  # metacharacters {}|=~<>" need no JSON escaping.
  cli query_graph --project "$PROJECT" --query "$1" |
    sed -n '/^rows: /{n;p;}' | sed 's/^  //' | sed 's/^"//;s/"$//;s/\\"/"/g'
}

# labels(n) → JSON list like ["Function"]
LBLV=$(cyp_first_cell 'MATCH (f:Function) RETURN labels(f) AS l LIMIT 1')
case "$LBLV" in
  '['*) echo "OK: query_graph labels(f) = $LBLV" ;;
  *) echo "FAIL: query_graph labels(f) returned '$LBLV' (expected a [\"...\"] list)"; exit 1 ;;
esac

# type(r) → relationship type
TYPV=$(cyp_first_cell 'MATCH (f:Function)-[r]->(g) RETURN type(r) AS t LIMIT 1')
if [ -z "$TYPV" ]; then
  echo "FAIL: query_graph type(r) returned empty"; exit 1
fi
echo "OK: query_graph type(r) = $TYPV"

# id(n) → numeric identity
IDV=$(cyp_first_cell 'MATCH (f:Function) RETURN id(f) AS i LIMIT 1')
case "$IDV" in
  ''|*[!0-9]*) echo "FAIL: query_graph id(f) returned non-numeric '$IDV'"; exit 1 ;;
  *) echo "OK: query_graph id(f) = $IDV" ;;
esac

# properties(n) → JSON object
PROPV=$(cyp_first_cell 'MATCH (f:Function) RETURN properties(f) AS p LIMIT 1')
case "$PROPV" in
  '{'*) echo "OK: query_graph properties(f) is a JSON object" ;;
  *) echo "FAIL: query_graph properties(f) returned '$PROPV'"; exit 1 ;;
esac

# toInteger() cast in projection
TIV=$(cyp_first_cell 'MATCH (f:Function) RETURN toInteger(f.start_line) AS n LIMIT 1')
case "$TIV" in
  ''|*[!0-9-]*) echo "FAIL: query_graph toInteger(f.start_line) returned non-integer '$TIV'"; exit 1 ;;
  *) echo "OK: query_graph toInteger(f.start_line) = $TIV" ;;
esac

# size() string-length function in projection
SZV=$(cyp_first_cell 'MATCH (f:Function) RETURN size(f.name) AS s LIMIT 1')
case "$SZV" in
  ''|*[!0-9]*) echo "FAIL: query_graph size(f.name) returned non-integer '$SZV'"; exit 1 ;;
  *) echo "OK: query_graph size(f.name) = $SZV" ;;
esac

# multi-arg functions: substring + coalesce
SUBV=$(cyp_first_cell 'MATCH (f:Function) RETURN substring(f.name, 0, 3) AS s LIMIT 1')
if [ -z "$SUBV" ]; then echo "FAIL: query_graph substring(...) returned empty"; exit 1; fi
echo "OK: query_graph substring(f.name,0,3) = $SUBV"
COALV=$(cyp_first_cell 'MATCH (f:Function) RETURN coalesce(f.nonesuch, f.name) AS c LIMIT 1')
if [ -z "$COALV" ]; then echo "FAIL: query_graph coalesce(...) returned empty"; exit 1; fi
echo "OK: query_graph coalesce(f.nonesuch, f.name) = $COALV"

# EXISTS { } pattern predicate (edge-type-specific existence)
CYPHER_EX=$(cli query_graph --project "$PROJECT" --query "MATCH (f:Function) WHERE EXISTS { (f)-[:CALLS]->() } RETURN f.name")
EX_ROWS=$(echo "$CYPHER_EX" | sed -n 's/^total: //p' | head -1)
EX_ROWS=${EX_ROWS:-0}
if [ "$EX_ROWS" -lt 1 ]; then
  echo "FAIL: query_graph EXISTS{} predicate returned 0 rows"; echo "$CYPHER_EX"; exit 1
fi
echo "OK: query_graph EXISTS { (f)-[:CALLS]->() } returned $EX_ROWS row(s)"

# =~ regex match in WHERE
CYPHER_RX=$(cli query_graph --project "$PROJECT" --query 'MATCH (f:Function) WHERE f.name =~ ".+" RETURN f.name')
RX_ROWS=$(echo "$CYPHER_RX" | sed -n 's/^total: //p' | head -1)
RX_ROWS=${RX_ROWS:-0}
if [ "$RX_ROWS" -lt 1 ]; then
  echo "FAIL: query_graph WHERE =~ regex returned 0 rows"; echo "$CYPHER_RX"; exit 1
fi
echo "OK: query_graph WHERE f.name =~ regex returned $RX_ROWS row(s)"

# keys(n) → JSON list including "name"
KEYSV=$(cyp_first_cell 'MATCH (f:Function) RETURN keys(f) AS k LIMIT 1')
case "$KEYSV" in
  *'"name"'*) echo "OK: query_graph keys(f) = $KEYSV" ;;
  *) echo "FAIL: query_graph keys(f) returned '$KEYSV'"; exit 1 ;;
esac

# reverse() + replace() + left() string functions
REVV=$(cyp_first_cell 'MATCH (f:Function) RETURN reverse(f.name) AS r LIMIT 1')
[ -n "$REVV" ] && echo "OK: query_graph reverse(f.name) = $REVV" || { echo "FAIL: reverse empty"; exit 1; }
REPV=$(cyp_first_cell 'MATCH (f:Function) RETURN replace(f.name, "a", "A") AS r LIMIT 1')
[ -n "$REPV" ] && echo "OK: query_graph replace(...) = $REPV" || { echo "FAIL: replace empty"; exit 1; }
LEFTV=$(cyp_first_cell 'MATCH (f:Function) RETURN left(f.name, 3) AS l LIMIT 1')
[ -n "$LEFTV" ] && echo "OK: query_graph left(f.name,3) = $LEFTV" || { echo "FAIL: left empty"; exit 1; }

# NOT EXISTS dead-code query (functions with no caller)
CYPHER_NX=$(cli query_graph --project "$PROJECT" --query "MATCH (f:Function) WHERE NOT EXISTS { (f)<-[:CALLS]-() } RETURN f.name")
NX_OK=$(echo "$CYPHER_NX" | grep -qE '^rows: [0-9]+' && echo "True" || echo "False")
[ "$NX_OK" = "True" ] && echo "OK: query_graph NOT EXISTS dead-code query executed" || { echo "FAIL: NOT EXISTS query"; echo "$CYPHER_NX" | head -c 300; exit 1; }

# CASE expression in RETURN
CASEV=$(cyp_first_cell 'MATCH (f:Function) RETURN CASE WHEN f.name =~ ".+" THEN "named" ELSE "anon" END AS c LIMIT 1')
[ "$CASEV" = "named" ] && echo "OK: query_graph CASE expression = $CASEV" || { echo "FAIL: CASE returned '$CASEV'"; exit 1; }

# unsupported function must FAIL LOUDLY (not silently return empty). The CLI
# prints the parse error to stderr (captured by cli() into $CLI_STDERR) and exits
# non-zero, leaving stdout empty — so verify the loud failure on that channel.
if cli query_graph --project "$PROJECT" --query "MATCH (f:Function) RETURN nosuchfn(f.name)" >/dev/null; then
  echo "FAIL: unsupported function did not error (exit 0)"; exit 1
fi
ERROUT=$(cat "$CLI_STDERR" 2>/dev/null)
case "$ERROUT" in
  *unsupported*) echo "OK: unsupported function errors loudly" ;;
  *) echo "FAIL: unsupported function did not error: $ERROUT" | head -c 300; exit 1 ;;
esac

# 3f: get_architecture surfaces Leiden community clusters
if ! ARCH=$(cli get_architecture --project "$PROJECT" --aspects clusters); then
  echo "FAIL: get_architecture (flag form) exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
# get_architecture default output is TOON: clusters[N]{...} header carries the count
NCLUST=$(echo "$ARCH" | sed -n 's/^clusters: \([0-9]*\).*/\1/p' | head -1)
NCLUST=${NCLUST:-0}
if [ "$NCLUST" -lt 1 ]; then
  echo "FAIL: get_architecture returned 0 community clusters"; echo "$ARCH" | head -c 400; exit 1
fi
echo "OK: get_architecture returned $NCLUST community cluster(s)"

# 3g: search_code — basic search reports elapsed_ms + matches
SC=$(cli search_code --project "$PROJECT" --pattern cbm_ --mode compact --limit 5)
# compact mode emits TOON scalars: `elapsed_ms: N` + `total_grep_matches: N`
SC_ELAPSED=$(echo "$SC" | sed -n 's/^elapsed_ms: //p' | head -1)
SC_GREPM=$(echo "$SC" | sed -n 's/^total_grep_matches: //p' | head -1)
if [ -n "$SC_ELAPSED" ]; then
  echo "OK: search_code elapsed_ms=$SC_ELAPSED total_grep_matches=${SC_GREPM:-0}"
else
  echo "FAIL: search_code basic / no elapsed_ms"; echo "$SC" | head -c 400; exit 1
fi

# 3g: search_code — literal '|' under regex=false must surface a warning (#282)
SCW=$(cli search_code --project "$PROJECT" --pattern "cbm_init|cbm_nope" --regex false --limit 5)
# TOON scalar `warning: ... regex=true ...`
if echo "$SCW" | grep -q "regex=true"; then
  echo "OK: search_code literal-| warning surfaced"
else
  echo "FAIL: search_code literal-| warning missing"; echo "$SCW" | head -c 400; exit 1
fi

# 3g: search_code — '&' in file_pattern accepted, not rejected as invalid (#272)
SCA=$(cli search_code --project "$PROJECT" --pattern cbm_ --file-pattern "*R&D*.c" --limit 5)
case "$SCA" in
  *"invalid characters"*) echo "FAIL: search_code rejected '&' in file_pattern"; echo "$SCA" | head -c 300; exit 1 ;;
  *) echo "OK: search_code accepts '&' in file_pattern" ;;
esac

echo ""
echo "=== Phase 3h: CLI input-mode guards (flags / stdin / --args-file / --help / deprecation) ==="

# Small helper: assert its stdin is a JSON object (exit non-zero otherwise).
assert_json_obj() { python3 -c "import json,sys; d=json.loads(sys.stdin.read()); sys.exit(0 if isinstance(d,dict) else 1)" 2>/dev/null; }
# search_graph emits TOON by default: a results/semantic table header proves
# the tool parsed its typed flags and produced a well-formed response.
assert_toon_table() { grep -qE '^(results|semantic): [0-9]+'; }

# B1: INTEGER flag — --limit is schema-typed integer; must parse and answer.
if ! IM_INT=$(cli search_graph --project "$PROJECT" --name-pattern compute --limit 5); then
  echo "FAIL B1: search_graph --limit 5 exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
if echo "$IM_INT" | assert_toon_table; then
  echo "OK B1: INTEGER flag (--limit 5) parsed → TOON results table"
else
  echo "FAIL B1: --limit 5 did not produce a TOON results table"; echo "$IM_INT" | head -c 300; exit 1
fi

# B2: BOOLEAN bare flag — --exclude-entry-points with no value → true; must succeed.
if ! IM_BOOL=$(cli search_graph --project "$PROJECT" --exclude-entry-points); then
  echo "FAIL B2: search_graph --exclude-entry-points exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
if echo "$IM_BOOL" | assert_toon_table; then
  echo "OK B2: BOOLEAN bare flag (--exclude-entry-points) → success"
else
  echo "FAIL B2: --exclude-entry-points did not produce a TOON results table"; echo "$IM_BOOL" | head -c 300; exit 1
fi

# B3: ARRAY flag — repeated --semantic-query accumulates into a JSON array.
# Semantic-only calls emit ONLY the semantic table (may be empty, header stays).
if ! IM_ARR=$(cli search_graph --project "$PROJECT" --semantic-query send --semantic-query publish); then
  echo "FAIL B3: search_graph repeated --semantic-query exited non-zero"; cat "$CLI_STDERR"; exit 1
fi
if echo "$IM_ARR" | grep -qE '^semantic: [0-9]+'; then
  echo "OK B3: ARRAY flag (repeated --semantic-query) → semantic TOON table"
else
  echo "FAIL B3: repeated --semantic-query did not produce a semantic table"; echo "$IM_ARR" | head -c 300; exit 1
fi

# B4: STDIN — piped JSON resolves; this path must NOT emit a deprecation warning.
IM_STDIN=$(echo "{\"project\":\"$PROJECT\"}" | "$BINARY" cli get_graph_schema 2>"$CLI_STDERR")
if ! echo "$IM_STDIN" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); sys.exit(0 if 'node_labels' in d else 1)" 2>/dev/null; then
  echo "FAIL B4: stdin get_graph_schema did not resolve"; echo "$IM_STDIN" | head -c 300; cat "$CLI_STDERR"; exit 1
fi
if grep -qi 'deprecated' "$CLI_STDERR"; then
  echo "FAIL B4: stdin path wrongly emitted a deprecation warning"; cat "$CLI_STDERR"; exit 1
fi
echo "OK B4: STDIN input resolves, no deprecation warning"

# B5: --args-file — JSON read from a file resolves; must NOT warn deprecated.
IM_ARGS_FILE=$(smoke_mktemp_file)
echo "{\"project\":\"$PROJECT\"}" > "$IM_ARGS_FILE"
if ! IM_AF=$(cli get_graph_schema --args-file "$IM_ARGS_FILE"); then
  echo "FAIL B5: get_graph_schema --args-file exited non-zero"; cat "$CLI_STDERR"; rm -f "$IM_ARGS_FILE"; exit 1
fi
if ! echo "$IM_AF" | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); sys.exit(0 if 'node_labels' in d else 1)" 2>/dev/null; then
  echo "FAIL B5: --args-file get_graph_schema did not resolve"; echo "$IM_AF" | head -c 300; rm -f "$IM_ARGS_FILE"; exit 1
fi
if grep -qi 'deprecated' "$CLI_STDERR"; then
  echo "FAIL B5: --args-file path wrongly emitted a deprecation warning"; cat "$CLI_STDERR"; rm -f "$IM_ARGS_FILE"; exit 1
fi
rm -f "$IM_ARGS_FILE"
echo "OK B5: --args-file input resolves, no deprecation warning"

# B6: per-tool --help — RC0 with expected flags in stdout; unknown tool errors non-zero.
if ! H_SG=$(cli search_graph --help); then
  echo "FAIL B6a: 'search_graph --help' exited non-zero"; exit 1
fi
if echo "$H_SG" | grep -q -- "--name-pattern"; then
  echo "OK B6a: search_graph --help (RC0) lists --name-pattern"
else
  echo "FAIL B6a: search_graph --help missing --name-pattern"; echo "$H_SG" | head -c 400; exit 1
fi
if ! H_IR=$(cli index_repository --help); then
  echo "FAIL B6b: 'index_repository --help' exited non-zero"; exit 1
fi
if echo "$H_IR" | grep -q -- "--repo-path"; then
  echo "OK B6b: index_repository --help (RC0) lists --repo-path"
else
  echo "FAIL B6b: index_repository --help missing --repo-path"; echo "$H_IR" | head -c 400; exit 1
fi
# Unknown tool: must exit non-zero and report "unknown tool" (on stderr).
if cli notatool --help >/dev/null; then
  echo "FAIL B6c: 'notatool --help' exited 0 (expected non-zero for unknown tool)"; exit 1
fi
if grep -qi 'unknown tool' "$CLI_STDERR"; then
  echo "OK B6c: 'notatool --help' errors non-zero with 'unknown tool'"
else
  echo "FAIL B6c: 'notatool --help' did not report 'unknown tool'"; cat "$CLI_STDERR"; exit 1
fi

# B7: DEPRECATION guard — one raw-JSON call MUST warn on stderr; flag form must NOT.
cli search_graph "{\"project\":\"$PROJECT\",\"name_pattern\":\"compute\"}" >/dev/null || true
if grep -qi 'deprecated' "$CLI_STDERR"; then
  echo "OK B7a: raw-JSON cli emits deprecation warning on stderr"
else
  echo "FAIL B7a: raw-JSON cli did NOT emit deprecation warning"; cat "$CLI_STDERR"; exit 1
fi
cli search_graph --project "$PROJECT" --name-pattern compute >/dev/null || true
if grep -qi 'deprecated' "$CLI_STDERR"; then
  echo "FAIL B7b: flag-form cli wrongly emitted a deprecation warning"; cat "$CLI_STDERR"; exit 1
else
  echo "OK B7b: flag-form cli emits no deprecation warning"
fi

# 3e: delete_project cleanup
if ! cli delete_project --project "$PROJECT" > /dev/null; then
  echo "FAIL: delete_project (flag form) exited non-zero"; cat "$CLI_STDERR"; exit 1
fi

echo ""
echo "=== Phase 4: security checks ==="

# 4a: Clean shutdown — wait through bounded cold bootstrap, then require the
# initialized frontend to exit promptly after EOF.
echo "Testing clean shutdown..."
if ! python3 "$REPO_ROOT/scripts/test_mcp_interactive.py" \
    "$BINARY" --scenario initialize --repo-path "$TMPDIR" \
    --response-timeout 45 --exit-timeout 8 > /dev/null; then
  echo "FAIL: initialized binary did not exit within 8 seconds after EOF"
  exit 1
fi
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
  local input_file="$1" output_file="$2" max_wait="${3:-45}"
  # Keep the frontend's stderr instead of discarding it: with the daemon
  # architecture, a first-session start/connect failure surfaces ONLY on
  # stderr (stdout stays reserved for JSON-RPC), so 2>/dev/null turned every
  # such failure into an undiagnosable "no id:1 response".
  "$BINARY" < "$input_file" > "$output_file" 2>"${output_file}.err" &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$max_wait" ]; do
    sleep 1
    waited=$((waited + 1))
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Dump every daemon-side diagnostic for an MCP failure: the frontend stderr
# and the durable daemon + conflict logs under the cache root.
mcp_dump_diagnostics() {
  local output_file="$1"
  if [ -s "${output_file}.err" ]; then
    echo "--- frontend stderr ---"
    cat "${output_file}.err"
  fi
  local cache="${CBM_CACHE_DIR:-$HOME/.cache/codebase-memory-mcp}"
  for log in cbm-daemon.log daemon-conflicts.ndjson; do
    if [ -s "$cache/logs/$log" ]; then
      echo "--- $log (tail) ---"
      tail -20 "$cache/logs/$log"
    fi
  done
}

MCP_INPUT=$(smoke_mktemp_file)
MCP_OUTPUT=$(smoke_mktemp_file)
cat > "$MCP_INPUT" << 'MCPEOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
MCPEOF

mcp_run "$MCP_INPUT" "$MCP_OUTPUT" 45

# 5a: Verify initialize response (id:1)
if ! grep -q '"id":1' "$MCP_OUTPUT"; then
  echo "FAIL: no initialize response (id:1) in MCP output"
  echo "Output was:"
  cat "$MCP_OUTPUT"
  mcp_dump_diagnostics "$MCP_OUTPUT"
  rm -f "$MCP_INPUT" "$MCP_OUTPUT" "${MCP_OUTPUT}.err"
  exit 1
fi
echo "OK: initialize response received (id:1)"

# 5b: Verify tools/list response (id:2) with tool names
if ! grep -q '"id":2' "$MCP_OUTPUT"; then
  echo "FAIL: no tools/list response (id:2) in MCP output"
  echo "Output was:"
  cat "$MCP_OUTPUT"
  mcp_dump_diagnostics "$MCP_OUTPUT"
  rm -f "$MCP_INPUT" "$MCP_OUTPUT" "${MCP_OUTPUT}.err"
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
MCP_TOOL_OUTPUT=$(smoke_mktemp_file)

if ! python3 "$REPO_ROOT/scripts/test_mcp_interactive.py" \
    "$BINARY" --scenario roundtrip --repo-path "$TMPDIR" \
    > "$MCP_TOOL_OUTPUT"; then
  echo "FAIL: interactive MCP index + search session failed"
  cat "$MCP_TOOL_OUTPUT"
  rm -f "$MCP_TOOL_OUTPUT"
  exit 1
fi

if ! grep -q '"id":2' "$MCP_TOOL_OUTPUT"; then
  echo "FAIL: no index_repository response (id:2)"
  cat "$MCP_TOOL_OUTPUT"
  rm -f "$MCP_TOOL_OUTPUT"
  exit 1
fi

if ! grep -q '"id":3' "$MCP_TOOL_OUTPUT"; then
  echo "FAIL: no search_graph response (id:3)"
  cat "$MCP_TOOL_OUTPUT"
  rm -f "$MCP_TOOL_OUTPUT"
  exit 1
fi
echo "OK: MCP tool call round-trip (index + search) succeeded"

# 5f: Content-Length framing (OpenCode compatibility)
echo ""
echo "--- Phase 5f: Content-Length framing ---"
MCP_CL_INPUT=$(smoke_mktemp_file)
MCP_CL_OUTPUT=$(smoke_mktemp_file)

INIT_MSG='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"cl-test","version":"1.0"}}}'
INIT_LEN=${#INIT_MSG}
printf "Content-Length: %d\r\n\r\n%s" "$INIT_LEN" "$INIT_MSG" > "$MCP_CL_INPUT"

TOOLS_MSG='{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
TOOLS_LEN=${#TOOLS_MSG}
printf "Content-Length: %d\r\n\r\n%s" "$TOOLS_LEN" "$TOOLS_MSG" >> "$MCP_CL_INPUT"

mcp_run "$MCP_CL_INPUT" "$MCP_CL_OUTPUT" 45

if ! grep -q '"id":1' "$MCP_CL_OUTPUT" || ! grep -q '"id":2' "$MCP_CL_OUTPUT"; then
  echo "FAIL: Content-Length framed handshake did not produce both responses"
  cat "$MCP_CL_OUTPUT"
  rm -f "$MCP_CL_INPUT" "$MCP_CL_OUTPUT"
  exit 1
fi
echo "OK: Content-Length framing works (OpenCode compatible)"

rm -f "$MCP_CL_INPUT" "$MCP_CL_OUTPUT" "$MCP_TOOL_OUTPUT"

echo ""
echo "=== Phase 6: CLI subcommands ==="

DRYRUN_HOME=$(smoke_mktemp_dir)
DRYRUN_CACHE="$DRYRUN_HOME/.cache/codebase-memory-mcp"
mkdir -p "$DRYRUN_CACHE" \
  "$DRYRUN_HOME/.local/bin" \
  "$DRYRUN_HOME/.config" \
  "$DRYRUN_HOME/AppData/Roaming" \
  "$DRYRUN_HOME/AppData/Local"

run_dryrun_env() {
  HOME="$DRYRUN_HOME" \
    XDG_CONFIG_HOME="$DRYRUN_HOME/.config" \
    APPDATA="$DRYRUN_HOME/AppData/Roaming" \
    LOCALAPPDATA="$DRYRUN_HOME/AppData/Local" \
    CBM_CACHE_DIR="$DRYRUN_CACHE" \
    PATH="$DRYRUN_HOME/.local/bin:$PATH" \
    "$@"
}

# 6a: install --dry-run -y
echo "--- Phase 6a: install --dry-run ---"
INSTALL_OUT=$(run_dryrun_env "$BINARY" install --dry-run -y 2>&1)
if ! echo "$INSTALL_OUT" | grep -qi 'install\|skill\|mcp\|agent'; then
  echo "FAIL: install --dry-run produced unexpected output"
  echo "$INSTALL_OUT"
  exit 1
fi
if ! echo "$INSTALL_OUT" | grep -qi 'dry-run'; then
  echo "FAIL: install --dry-run did not indicate dry-run mode"
  echo "--- install --dry-run output was: ---"
  echo "$INSTALL_OUT"
  exit 1
fi
echo "OK: install --dry-run completed"

# 6b: uninstall --dry-run -y
echo "--- Phase 6b: uninstall --dry-run ---"
if [[ "$BINARY" == *.exe ]]; then
  if UNINSTALL_OUT=$(run_dryrun_env "$BINARY" uninstall --dry-run -y 2>&1); then
    echo "FAIL: portable Windows bundle accepted uninstall"
    exit 1
  fi
  if ! echo "$UNINSTALL_OUT" | grep -qi 'managed\|install\|package'; then
    echo "FAIL: portable Windows uninstall refusal had no install guidance"
    echo "$UNINSTALL_OUT"
    exit 1
  fi
else
  UNINSTALL_OUT=$(run_dryrun_env "$BINARY" uninstall --dry-run -y 2>&1)
  if ! echo "$UNINSTALL_OUT" | grep -qi 'uninstall\|remov'; then
    echo "FAIL: uninstall --dry-run produced unexpected output"
    echo "$UNINSTALL_OUT"
    exit 1
  fi
fi
echo "OK: uninstall --dry-run completed"

# 6c: update --dry-run --standard -y
echo "--- Phase 6c: update --dry-run ---"
if [[ "$BINARY" == *.exe ]]; then
  if UPDATE_OUT=$(run_dryrun_env "$BINARY" update --dry-run --standard -y 2>&1); then
    echo "FAIL: portable Windows bundle accepted update"
    exit 1
  fi
  if ! echo "$UPDATE_OUT" | grep -qi 'managed\|install\|package'; then
    echo "FAIL: portable Windows update refusal had no install guidance"
    echo "$UPDATE_OUT"
    exit 1
  fi
else
  UPDATE_OUT=$(run_dryrun_env "$BINARY" update --dry-run --standard -y 2>&1)
  if ! echo "$UPDATE_OUT" | grep -qi 'dry-run'; then
    echo "FAIL: update --dry-run did not indicate dry-run mode"
    echo "$UPDATE_OUT"
    exit 1
  fi
  if ! echo "$UPDATE_OUT" | grep -qi 'standard'; then
    echo "FAIL: update --dry-run did not respect --standard flag"
    exit 1
  fi
fi
# On Linux the binary must self-update from the static "-portable" asset: the
# standard linux asset dynamically links glibc 2.38+ and breaks on older distros
# (Debian 11, RHEL 8, Ubuntu 20.04). Guards build_update_url in src/cli/cli.c.
if [ "$(uname -s)" = "Linux" ]; then
  if ! echo "$UPDATE_OUT" | grep -q -- '-portable'; then
    echo "FAIL: linux update --dry-run does not target the -portable asset"
    echo "$UPDATE_OUT"
    exit 1
  fi
  echo "OK: linux update targets the -portable (static) asset"
fi
echo "OK: update --dry-run --standard completed"

# 6d: config set/get/reset round-trip
echo "--- Phase 6d: config set/get/reset ---"
run_dryrun_env "$BINARY" config set auto_index true 2>/dev/null
CONFIG_VAL=$(run_dryrun_env "$BINARY" config get auto_index 2>/dev/null)
if ! echo "$CONFIG_VAL" | grep -q 'true'; then
  echo "FAIL: config get auto_index returned '$CONFIG_VAL', expected 'true'"
  exit 1
fi
run_dryrun_env "$BINARY" config reset auto_index 2>/dev/null
echo "OK: config set/get/reset round-trip"

# 6e: Simulated binary replacement (update flow without network)
# Simulates the update command's Steps 3-6: extract, replace, verify.
# Uses a copy of the test binary as the "downloaded" version.
echo "--- Phase 6e: simulated binary replacement ---"
REPLACE_DIR=$(smoke_mktemp_dir)
INSTALL_DIR="$REPLACE_DIR/install"
mkdir -p "$INSTALL_DIR"

# 1. Copy binary to "install dir" as the "currently installed" version
copy_smoke_binary "$INSTALL_DIR/codebase-memory-mcp"
chmod 755 "$INSTALL_DIR/codebase-memory-mcp"

# Verify installed binary works
INSTALLED_VER=$("$INSTALL_DIR/codebase-memory-mcp" --version 2>&1)
if ! echo "$INSTALLED_VER" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
  echo "FAIL: installed binary --version failed: $INSTALLED_VER"
  rm -rf "$REPLACE_DIR"
  exit 1
fi

# 2. Copy binary as the "downloaded" new version
copy_smoke_binary "$REPLACE_DIR/smoke-codebase-memory-mcp"

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
MCP_SC_OUTPUT=$(smoke_mktemp_file)
if ! python3 "$REPO_ROOT/scripts/test_mcp_interactive.py" \
    "$BINARY" --scenario advanced --repo-path "$TMPDIR" \
    > "$MCP_SC_OUTPUT"; then
  echo "FAIL: interactive MCP advanced-tool session failed"
  cat "$MCP_SC_OUTPUT"
  rm -f "$MCP_SC_OUTPUT"
  exit 1
fi

if ! grep -q '"id":3' "$MCP_SC_OUTPUT"; then
  echo "FAIL: search_code response (id:3) missing"
  exit 1
fi
echo "OK: search_code v2 via MCP"

# 7b: search_graph discovery + get_code_snippet via MCP
if ! grep -q '"id":4' "$MCP_SC_OUTPUT" || ! grep -q '"id":5' "$MCP_SC_OUTPUT"; then
  echo "FAIL: search_graph/get_code_snippet response (id:4 or id:5) missing"
  exit 1
fi
echo "OK: get_code_snippet via MCP"

rm -f "$MCP_SC_OUTPUT"

fi

echo ""
echo "=== Phase 8: agent config install E2E ==="

# Set up an isolated HOME. Directory-only agents get only the root required for
# detection; CLI-detected agents use stubs below so install must create their
# config parents from scratch.
FAKE_HOME=$(smoke_mktemp_dir)
mkdir -p "$FAKE_HOME/.claude"
mkdir -p "$FAKE_HOME/.codex"
mkdir -p "$FAKE_HOME/.gemini/antigravity-cli"
mkdir -p "$FAKE_HOME/.junie"
mkdir -p "$FAKE_HOME/.cursor"
mkdir -p "$FAKE_HOME/.codeium/windsurf"
mkdir -p "$FAKE_HOME/.qoder"
mkdir -p "$FAKE_HOME/.pi/agent"
mkdir -p "$FAKE_HOME/.warp"
mkdir -p "$FAKE_HOME/.cline"
mkdir -p "$FAKE_HOME/.codebuddy"
mkdir -p "$FAKE_HOME/.bob/rules"
mkdir -p "$FAKE_HOME/.pochi"
mkdir -p "$FAKE_HOME/.rovodev"
mkdir -p "$FAKE_HOME/.aws/amazonq"
CUSTOM_KIMI_HOME="$FAKE_HOME/vendor-kimi"
ROO_CFG="$FAKE_HOME/explicit/roo.json"
mkdir -p "$CUSTOM_KIMI_HOME" "$(dirname "$ROO_CFG")"
if [ "$(uname -s)" = "Darwin" ]; then
  mkdir -p "$FAKE_HOME/Library/Application Support/Zed"
  mkdir -p "$FAKE_HOME/Library/Application Support/Code/User/profiles/smoke-profile"
  VSCODE_PROFILE_CFG="$FAKE_HOME/Library/Application Support/Code/User/profiles/smoke-profile/mcp.json"
elif [[ "${BINARY:-}" == *.exe ]]; then
  mkdir -p "$FAKE_HOME/AppData/Roaming/Zed"
  mkdir -p "$FAKE_HOME/AppData/Roaming/Code/User/profiles/smoke-profile"
  VSCODE_PROFILE_CFG="$FAKE_HOME/AppData/Roaming/Code/User/profiles/smoke-profile/mcp.json"
else
  mkdir -p "$FAKE_HOME/.config/zed"
  mkdir -p "$FAKE_HOME/.config/Code/User/profiles/smoke-profile"
  VSCODE_PROFILE_CFG="$FAKE_HOME/.config/Code/User/profiles/smoke-profile/mcp.json"
fi
if [[ "$BINARY" == *.exe ]]; then
  GITLAB_DIR="$FAKE_HOME/AppData/Roaming/GitLab/duo"
  GITLAB_HOOKS="$GITLAB_DIR/hooks.json"
  DEVIN_DIR="$FAKE_HOME/AppData/Roaming/devin"
else
  GITLAB_DIR="$FAKE_HOME/.config/gitlab/duo"
  GITLAB_HOOKS="$FAKE_HOME/.gitlab/duo/hooks.json"
  DEVIN_DIR="$FAKE_HOME/.config/devin"
fi
GITLAB_MCP="$GITLAB_DIR/mcp.json"
DEVIN_CONFIG="$DEVIN_DIR/config.json"
DEVIN_INSTRUCTIONS="$DEVIN_DIR/AGENTS.md"
DEVIN_SKILL="$DEVIN_DIR/skills/codebase-memory/SKILL.md"
CODEBUDDY_MCP="$FAKE_HOME/.codebuddy/.mcp.json"
CODEBUDDY_INSTRUCTIONS="$FAKE_HOME/.codebuddy/CODEBUDDY.md"
CODEBUDDY_SKILL="$FAKE_HOME/.codebuddy/skills/codebase-memory/SKILL.md"
CODEBUDDY_AGENT="$FAKE_HOME/.codebuddy/agents/codebase-memory.md"
CODEBUDDY_SETTINGS="$FAKE_HOME/.codebuddy/settings.json"
BOB_IDE_MCP="$FAKE_HOME/.bob/mcp.json"
BOB_SHELL_MCP="$FAKE_HOME/.bob/mcp_settings.json"
BOB_RULE="$FAKE_HOME/.bob/rules/codebase-memory.md"
BOB_SKILL="$FAKE_HOME/.bob/skills/codebase-memory/SKILL.md"
BOB_AGENT="$FAKE_HOME/.bob/agents/codebase-memory.md"
POCHI_MCP="$FAKE_HOME/.pochi/config.jsonc"
POCHI_INSTRUCTIONS="$FAKE_HOME/.pochi/README.pochi.md"
POCHI_SKILL="$FAKE_HOME/.pochi/skills/codebase-memory/SKILL.md"
POCHI_AGENT="$FAKE_HOME/.pochi/agents/codebase-memory.md"
ROVO_MCP="$FAKE_HOME/.rovodev/mcp.json"
ROVO_INSTRUCTIONS="$FAKE_HOME/.rovodev/AGENTS.md"
ROVO_SKILL="$FAKE_HOME/.rovodev/skills/codebase-memory/SKILL.md"
ROVO_AGENT="$FAKE_HOME/.rovodev/subagents/codebase-memory.md"
AMAZON_Q_MCP="$FAKE_HOME/.aws/amazonq/default.json"
mkdir -p "$GITLAB_DIR" "$(dirname "$GITLAB_HOOKS")" "$DEVIN_DIR"
mkdir -p "$FAKE_HOME/.local/bin"
# A Windows portable pair is the installer source, not a valid managed target.
# Leave the canonical destination absent so install can publish the authenticated
# generation backing + canonical hard-link pair. A one-link copy at that path is
# deliberately rejected as an unknown/conflicting installation.
if [[ "$BINARY" == *.exe ]]; then
  SELF_PATH="$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"
else
  cp "$BINARY" "$FAKE_HOME/.local/bin/codebase-memory-mcp"
  SELF_PATH="$FAKE_HOME/.local/bin/codebase-memory-mcp"
fi
create_agent_stub() {
  local name="$1"
  if [[ "$BINARY" == *.exe ]]; then
    printf '@echo off\r\n' > "$FAKE_HOME/.local/bin/$name.cmd"
  else
    printf '#!/bin/sh\necho stub\n' > "$FAKE_HOME/.local/bin/$name"
    chmod +x "$FAKE_HOME/.local/bin/$name"
  fi
}
for AGENT_CLI in aider opencode kilo openclaw kiro-cli hermes openhands cline qwen droid crush goose vibe auggie bob rovodev; do
  create_agent_stub "$AGENT_CLI"
done

# Pre-existing configs (verify merge, not overwrite)
echo '{"existingKey": true}' > "$FAKE_HOME/.claude.json"
echo '{"existingKey": true}' > "$FAKE_HOME/.gemini/settings.json"
echo '{"theme": "dark"}' > "$FAKE_HOME/.qoder/settings.json"
echo '# Personal Kimi guidance' > "$CUSTOM_KIMI_HOME/AGENTS.md"
printf 'theme = "dark"\n' > "$CUSTOM_KIMI_HOME/config.toml"
echo '{"keep": "roo"}' > "$ROO_CFG"
printf '[existing_section]\nline_from_user = true\n' > "$FAKE_HOME/.codex/config.toml"
echo '{"hooksEnabled": false, "keep": "cline"}' > "$FAKE_HOME/.cline/settings.json"
echo '{"keep": "gitlab-mcp"}' > "$GITLAB_MCP"
echo '{"keep": "gitlab-hooks", "hooks": {"SessionStart": [{"matcher": "startup", "hooks": [{"type": "command", "command": "/usr/bin/user-hook", "timeout": 9}]}]}}' > "$GITLAB_HOOKS"
echo '{"theme_mode": "dark"}' > "$DEVIN_CONFIG"
echo '# Personal Devin guidance' > "$DEVIN_INSTRUCTIONS"
echo '{"keep": "codebuddy"}' > "$CODEBUDDY_MCP"
echo '# Personal CodeBuddy guidance' > "$CODEBUDDY_INSTRUCTIONS"
echo '{"keep": "bob-ide"}' > "$BOB_IDE_MCP"
echo '{"keep": "bob-shell"}' > "$BOB_SHELL_MCP"
echo '# Personal Bob guidance' > "$BOB_RULE"
printf '{\n  // Personal Pochi setting\n  "keep": "pochi"\n}\n' > "$POCHI_MCP"
echo '# Personal Pochi guidance' > "$POCHI_INSTRUCTIONS"
echo '# Personal Rovo guidance' > "$ROVO_INSTRUCTIONS"

# Run install — override platform config dirs so cbm_app_config_dir() and
# cbm_app_local_dir() resolve to FAKE_HOME paths on all platforms.
PHASE8_INSTALL_RC=0
PHASE8_INSTALL_LOG=$(smoke_mktemp_file)
HOME="$FAKE_HOME" \
  XDG_CONFIG_HOME="$FAKE_HOME/.config" \
  APPDATA="$FAKE_HOME/AppData/Roaming" \
  LOCALAPPDATA="$FAKE_HOME/AppData/Local" \
  KIMI_CODE_HOME="$CUSTOM_KIMI_HOME" \
  CBM_ROO_CONFIG_PATH="$ROO_CFG" \
  PATH="$FAKE_HOME/.local/bin:$PATH" \
  "$BINARY" install -y > "$PHASE8_INSTALL_LOG" 2>&1 || PHASE8_INSTALL_RC=$?
cat "$PHASE8_INSTALL_LOG"
if [[ "$BINARY" == *.exe ]]; then
  # The managed install itself must SUCCEED on Windows — an install-time
  # staging/ACL refusal used to scroll past as tolerated noise while the
  # downstream config assertions kept passing against a previous generation
  # ("staging transaction open failed (status -3, os 0)" hid a real install
  # failure class on Administrators-default-owner profiles).
  if [ "$PHASE8_INSTALL_RC" -ne 0 ]; then
    echo "FAIL 8-0: managed install exited rc=$PHASE8_INSTALL_RC"
    exit 1
  fi
  PHASE8_CANONICAL="$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"
  if [ ! -f "$PHASE8_CANONICAL" ]; then
    echo "FAIL 8-0: canonical launcher missing after managed install"
    exit 1
  fi
  PHASE8_LINKS=$(stat -c %h "$PHASE8_CANONICAL" 2>/dev/null || echo 0)
  if [ "$PHASE8_LINKS" != "2" ]; then
    echo "FAIL 8-0: canonical launcher is not an exact two-link file (links=$PHASE8_LINKS)"
    exit 1
  fi
  echo "OK 8-0: managed install committed an exact two-link canonical launcher"
fi

# Helper for JSON validation (pipe file to python — avoids MSYS2 path translation issues)
json_get() { cat "$1" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)" 2>/dev/null || echo ""; }

# Helper: compare exact paths across native Windows and MSYS2 spellings.
exact_path_match() {
  local first="${1%$'\r'}"
  local second="${2%$'\r'}"
  [ "$first" = "$second" ] && return 0
  if command -v cygpath >/dev/null 2>&1; then
    first=$(cygpath -am "$first" 2>/dev/null || true)
    second=$(cygpath -am "$second" 2>/dev/null || true)
    first=$(printf '%s' "$first" | tr '[:upper:]' '[:lower:]')
    second=$(printf '%s' "$second" | tr '[:upper:]' '[:lower:]')
    [ -n "$first" ] && [ "$first" = "$second" ] && return 0
  fi
  return 1
}

# Command paths retain a basename fallback for platforms without a native path
# converter. Configuration references use exact_path_match instead.
path_match() {
  exact_path_match "$1" "$2" && return 0
  [ "$(basename "$1" 2>/dev/null)" = "$(basename "$2" 2>/dev/null)" ] && return 0
  return 1
}

# A Windows path lands in a YAML/TOML config as a double-quoted scalar with
# ESCAPED backslashes ("C:\\Users\\..."), so a raw grep for the msys-form
# $SELF_PATH can never match it. Unquote, unescape, normalize separators,
# then path_match.
quoted_path_value_matches() {
  local value="${1%$'\r'}"
  local expected="$2"
  value="${value#\"}"
  value="${value%\"}"
  value="${value//\\\\/\\}"
  value="${value//\\//}"
  [ -n "$value" ] && path_match "$value" "$expected"
}

json_instructions_contain_path() {
  local config="$1"
  local expected="$2"
  local candidate
  while IFS= read -r -d '' candidate; do
    exact_path_match "$candidate" "$expected" && return 0
  done < <(cat "$config" 2>/dev/null | python3 -c \
    "import json,sys; d=json.load(sys.stdin); sys.stdout.buffer.write(b''.join(p.encode('utf-8') + b'\\0' for p in d.get('instructions', []) if isinstance(p, str)))" \
    2>/dev/null)
  return 1
}

# Validate the common Scout/Verify/Auditor contract once for every documented
# profile dialect. Dialect-specific schema checks remain below where useful.
assert_tier_profile_set() {
  local label="$1"
  local directory="$2"
  local suffix="$3"
  local access="$4"
  local spec slug tier file
  for spec in \
    "codebase-memory-scout|Tier 1" \
    "codebase-memory|Tier 2" \
    "codebase-memory-auditor|Tier 3"; do
    slug=${spec%%|*}
    tier=${spec##*|}
    file="$directory/$slug$suffix"
    if [ ! -f "$file" ]; then
      echo "FAIL 8aw: $label $tier profile missing: $file"
      exit 1
    fi
    if { ! grep -Fq "$tier" "$file" 2>/dev/null && ! grep -Fq "$slug" "$file" 2>/dev/null; } ||
       ! grep -q 'check_index_coverage' "$file" 2>/dev/null ||
       grep -qE '(index_repository|delete_project|manage_adr|ingest_traces)' "$file" 2>/dev/null; then
      echo "FAIL 8aw: $label $tier profile identity, coverage, or mutator contract is wrong"
      exit 1
    fi
    if [ "$access" = "direct" ]; then
      if ! grep -Fq 'source read/grep fallback' "$file" 2>/dev/null; then
        echo "FAIL 8aw: $label $tier direct profile lacks source fallback"
        exit 1
      fi
    elif ! grep -q 'parent agent must supply' "$file" 2>/dev/null ||
         ! grep -q 'must not call or claim access to MCP' "$file" 2>/dev/null ||
         grep -qE '(mcpServers|mcp__codebase-memory-mcp__|mcp_codebase-memory-mcp_|@codebase-memory-mcp/|codebase-memory-mcp/)' "$file" 2>/dev/null; then
      echo "FAIL 8aw: $label $tier handoff profile exposes child MCP or lacks parent evidence"
      exit 1
    fi
  done
}

assert_tier_profile_set_removed() {
  local label="$1"
  local directory="$2"
  local suffix="$3"
  local slug file
  for slug in codebase-memory-scout codebase-memory codebase-memory-auditor; do
    file="$directory/$slug$suffix"
    if [ -e "$file" ]; then
      echo "FAIL 9n-i: owned $label tier profile remains: $file"
      exit 1
    fi
  done
}

assert_tier_prompt_set() {
  local label="$1"
  local directory="$2"
  local suffix="$3"
  local spec slug tier file
  for spec in \
    "codebase-memory-scout|Tier 1" \
    "codebase-memory|Tier 2" \
    "codebase-memory-auditor|Tier 3"; do
    slug=${spec%%|*}
    tier=${spec##*|}
    file="$directory/$slug$suffix"
    if [ ! -f "$file" ] ||
       ! grep -Fq "$tier" "$file" 2>/dev/null ||
       ! grep -q 'check_index_coverage' "$file" 2>/dev/null ||
       ! grep -Fq 'source read/grep fallback' "$file" 2>/dev/null ||
       grep -qE '(index_repository|delete_project|manage_adr|ingest_traces)' "$file" 2>/dev/null; then
      echo "FAIL 8aw: $label $tier prompt contract is wrong"
      exit 1
    fi
  done
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

# 8c: Claude Code must not create the undocumented nested legacy path.
if [ -f "$FAKE_HOME/.claude/.mcp.json" ] && cat "$FAKE_HOME/.claude/.mcp.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if 'codebase-memory-mcp' in d.get('mcpServers', {}) else 1)
" 2>/dev/null; then
  echo "FAIL 8c: install recreated undocumented .claude/.mcp.json entry"
  exit 1
fi
echo "OK 8c: undocumented nested Claude MCP path absent"

# 8c-i: Claude gets a dedicated exact-tool graph subagent in addition to the
# catch-all SubagentStart context hook.
CLAUDE_AGENT="$FAKE_HOME/.claude/agents/codebase-memory.md"
if ! grep -q '^mcpServers: \[codebase-memory-mcp\]$' "$CLAUDE_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__search_graph' "$CLAUDE_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__check_index_coverage' "$CLAUDE_AGENT" 2>/dev/null ||
   ! grep -q '^permissionMode: plan$' "$CLAUDE_AGENT" 2>/dev/null ||
   grep -qE 'mcp__codebase-memory-mcp__(index_repository|delete_project|manage_adr|ingest_traces)' "$CLAUDE_AGENT" 2>/dev/null; then
  echo "FAIL 8c-i: Claude exact-tool graph subagent missing or over-privileged"
  exit 1
fi
echo "OK 8c-i: Claude exact-tool graph subagent"

# 8d: Claude Code hooks keep search augmentation and read-coverage reporting
# separate: PreToolUse matches exactly Grep|Glob, while PostToolUse matches
# exactly Read. Neither hook may grow a Search or catch-all matcher.
if ! cat "$FAKE_HOME/.claude/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
all_hooks = d.get('hooks', {})
pre = all_hooks.get('PreToolUse', [])
post = all_hooks.get('PostToolUse', [])
ok = (any(h.get('matcher') == 'Grep|Glob' for h in pre) and
      any(h.get('matcher') == 'Read' for h in post))
bad = any('Search' in str(h.get('matcher', '')) for h in pre + post)
sys.exit(0 if (ok and not bad) else 1)
" 2>/dev/null; then
  echo "FAIL 8d: Claude search/read hook matchers are not exact"
  exit 1
fi
echo "OK 8d: Claude Code PreToolUse Grep|Glob + PostToolUse Read"

# 8e: Claude Code shim script — must be non-blocking augmenter, not a gate.
# #929: Windows installs a .cmd script (extensionless bash shims triggered the
# Open-With dialog); the old `!= "MINGW64_NT"` gate never matched the real
# uname (MINGW64_NT-10.0-...), so this check silently ran the POSIX branch on
# Windows. Branch by platform prefix instead.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    GATE_SCRIPT="$FAKE_HOME/.claude/hooks/cbm-code-discovery-gate.cmd"
    if [ ! -f "$GATE_SCRIPT" ]; then
      echo "FAIL 8e: .cmd shim missing on Windows"
      exit 1
    fi
    if [ -f "$FAKE_HOME/.claude/hooks/cbm-code-discovery-gate" ]; then
      echo "FAIL 8e: legacy extensionless shim still installed on Windows"
      exit 1
    fi
    ;;
  *)
    GATE_SCRIPT="$FAKE_HOME/.claude/hooks/cbm-code-discovery-gate"
    if [ ! -x "$GATE_SCRIPT" ]; then
      echo "FAIL 8e: shim script not executable or missing"
      exit 1
    fi
    ;;
esac
if grep -q 'exit 2' "$GATE_SCRIPT"; then
  echo "FAIL 8e: shim contains 'exit 2' — must never block"
  exit 1
fi
if ! grep -q 'hook-augment' "$GATE_SCRIPT"; then
  echo "FAIL 8e: shim missing 'hook-augment' delegation"
  exit 1
fi
echo "OK 8e: shim installed, non-blocking, delegates to hook-augment"

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
# Matcher must be exactly 'google_web_search|grep_search' (no read_file). The
# old matcher gated the agent's read tool — consistent with the Claude fix
# we remove it here too.
ok = any(h.get('matcher') == 'google_web_search|grep_search' for h in hooks)
bad = any('read_file' in str(h.get('matcher', '')) for h in hooks)
sys.exit(0 if (ok and not bad) else 1)
" 2>/dev/null; then
  echo "FAIL 8l: Gemini BeforeTool hook matcher must be 'google_web_search|grep_search' (no read_file)"
  exit 1
fi
echo "OK 8l: Gemini BeforeTool hook (matcher=google_web_search|grep_search)"

# 8m: Gemini instructions
if [ ! -f "$FAKE_HOME/.gemini/GEMINI.md" ]; then
  echo "FAIL 8m: Gemini GEMINI.md missing"
  exit 1
fi
echo "OK 8m: Gemini instructions"

# 8m-i: Gemini dedicated graph subagent uses an explicit built-in + MCP tool
# allowlist; omitted tools would inherit every parent tool.
GEMINI_AGENT="$FAKE_HOME/.gemini/agents/codebase-memory.md"
if ! grep -q '^name: codebase-memory$' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q '^kind: local$' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q 'search_graph' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q 'graph project' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q '^tools:' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q 'mcp_codebase-memory-mcp_search_graph' "$GEMINI_AGENT" 2>/dev/null ||
   ! grep -q 'mcp_codebase-memory-mcp_check_index_coverage' "$GEMINI_AGENT" 2>/dev/null ||
   grep -qE 'mcp_codebase-memory-mcp_(index_repository|delete_project|manage_adr|ingest_traces)' "$GEMINI_AGENT" 2>/dev/null; then
  echo "FAIL 8m-i: Gemini dedicated graph subagent is incomplete"
  exit 1
fi
echo "OK 8m-i: Gemini dedicated graph subagent"

# 8n: Zed MCP
if [ "$(uname -s)" = "Darwin" ]; then
  ZED_CFG="$FAKE_HOME/Library/Application Support/Zed/settings.json"
elif [[ "$BINARY" == *.exe ]]; then
  ZED_CFG="$FAKE_HOME/AppData/Roaming/Zed/settings.json"
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
if [[ "$BINARY" == *.exe ]]; then
  ZED_INSTR="$FAKE_HOME/AppData/Roaming/Zed/AGENTS.md"
else
  ZED_INSTR="$FAKE_HOME/.config/zed/AGENTS.md"
fi
if ! grep -q 'Codebase Memory' "$ZED_INSTR" 2>/dev/null ||
   ! grep -q 'search_graph' "$ZED_INSTR" 2>/dev/null; then
  echo "FAIL 8n-i: Zed durable AGENTS.md missing"
  exit 1
fi
echo "OK 8n-i: Zed durable instructions"

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

# 8q-r: Antigravity (shared MCP config and global GEMINI.md instructions).
CMD=$(json_get "$FAKE_HOME/.gemini/config/mcp_config.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8q: Antigravity command='$CMD'"
  exit 1
fi
echo "OK 8q: Antigravity MCP"
if [ ! -f "$FAKE_HOME/.gemini/GEMINI.md" ] ||
   [ -f "$FAKE_HOME/.gemini/antigravity-cli/AGENTS.md" ] ||
   [ -f "$FAKE_HOME/.gemini/antigravity-cli/settings.json" ]; then
  echo "FAIL 8r: Antigravity global instructions or legacy cleanup is wrong"
  exit 1
fi
echo "OK 8r: Antigravity global instructions; undocumented legacy files absent"

# 8s: Aider instructions (detection requires binary on PATH)
if [ -f "$FAKE_HOME/CONVENTIONS.md" ]; then
  if ! grep -q 'codebase-memory-mcp' "$FAKE_HOME/CONVENTIONS.md"; then
    echo "FAIL 8s: Aider CONVENTIONS.md missing content"
    exit 1
  fi
  if ! grep -Fq 'CONVENTIONS.md' "$FAKE_HOME/.aider.conf.yml"; then
    echo "FAIL 8s: .aider.conf.yml does not load installed CONVENTIONS.md"
    exit 1
  fi
  echo "OK 8s: Aider instructions"
else
  echo "SKIP 8s: Aider not detected (binary not on PATH)"
fi

# 8t: KiloCode standalone config (modern JSONC schema).
KILO_CFG="$FAKE_HOME/.config/kilo/kilo.jsonc"
CMD=$(json_get "$KILO_CFG" "d['mcp']['codebase-memory-mcp']['command'][0]")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8t: KiloCode command='$CMD'"
  exit 1
fi
echo "OK 8t: KiloCode MCP"

# 8u: KiloCode rules file and explicit instructions reference.
KILO_RULE="$FAKE_HOME/.config/kilo/rules/codebase-memory-mcp.md"
if [ ! -f "$KILO_RULE" ]; then
  echo "FAIL 8u: KiloCode rules file missing"
  exit 1
fi
if ! json_instructions_contain_path "$KILO_CFG" "$KILO_RULE"; then
  KILO_REFS=$(json_get "$KILO_CFG" "repr(d.get('instructions', []))")
  printf "DEBUG 8u: expected=%q refs=%s\n" "$KILO_RULE" "$KILO_REFS"
  echo "FAIL 8u: KiloCode config does not load its installed rule"
  exit 1
fi
KILO_AGENT="$FAKE_HOME/.config/kilo/agents/codebase-memory.md"
if ! grep -q '^mode: subagent$' "$KILO_AGENT" 2>/dev/null ||
   ! grep -Fq '"*": deny' "$KILO_AGENT" 2>/dev/null ||
   ! grep -Fq '"codebase-memory-mcp_search_graph": allow' "$KILO_AGENT" 2>/dev/null ||
   ! grep -Fq '"codebase-memory-mcp_get_code_snippet": allow' "$KILO_AGENT" 2>/dev/null ||
   ! grep -Fq '"codebase-memory-mcp_check_index_coverage": allow' "$KILO_AGENT" 2>/dev/null ||
   grep -Fq '"codebase-memory-mcp_*": allow' "$KILO_AGENT" 2>/dev/null ||
   grep -qE 'codebase-memory-mcp_(index_repository|delete_project|manage_adr|ingest_traces)' "$KILO_AGENT" 2>/dev/null ||
   grep -qE '^  (edit|bash|shell): allow$' "$KILO_AGENT" 2>/dev/null; then
  echo "FAIL 8u: KiloCode global read-only subagent is missing or over-permissive"
  exit 1
fi
echo "OK 8u: KiloCode instructions + permission-gated global subagent"

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
CMD=$(json_get "$VSCODE_PROFILE_CFG" "d['servers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8v-i: VS Code profile command='$CMD'"
  exit 1
fi
echo "OK 8v-i: VS Code profile MCP"

# 8w: OpenClaw MCP
CMD=$(json_get "$FAKE_HOME/.openclaw/openclaw.json" "d['mcp']['servers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8w: OpenClaw command='$CMD'"
  exit 1
fi
ENABLED=$(json_get "$FAKE_HOME/.openclaw/openclaw.json" "d['mcp']['servers']['codebase-memory-mcp'].get('enabled')")
if [ "$ENABLED" = "False" ]; then
  echo "FAIL 8w: fresh OpenClaw entry is unexpectedly disabled"
  exit 1
fi
echo "OK 8w: OpenClaw MCP (client-default enabled policy preserved)"
for OPENCLAW_CONTEXT in \
  "$FAKE_HOME/.openclaw/workspace/AGENTS.md" \
  "$FAKE_HOME/.openclaw/workspace/TOOLS.md"; do
  if ! grep -q '^## Codebase Knowledge Graph (codebase-memory-mcp)$' "$OPENCLAW_CONTEXT" 2>/dev/null ||
     ! grep -q 'subagent' "$OPENCLAW_CONTEXT" 2>/dev/null; then
    echo "FAIL 8w-i: OpenClaw durable context missing in $OPENCLAW_CONTEXT"
    exit 1
  fi
done
OPENCLAW_COMPACTION=$(json_get "$FAKE_HOME/.openclaw/openclaw.json" "str('Codebase Knowledge Graph (codebase-memory-mcp)' in d['agents']['defaults']['compaction']['postCompactionSections'])")
if [ "$OPENCLAW_COMPACTION" != "True" ]; then
  echo "FAIL 8w-i: OpenClaw compaction reinjection missing"
  exit 1
fi
echo "OK 8w-i: OpenClaw session, compaction, and subagent context"

# 8w-ii: Kiro MCP + always-on steering.
CMD=$(json_get "$FAKE_HOME/.kiro/settings/mcp.json" "d['mcpServers']['codebase-memory-mcp']['command']")
KIRO_AGENT="$FAKE_HOME/.kiro/agents/codebase-memory.json"
KIRO_AGENT_CMD=$(json_get "$KIRO_AGENT" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! path_match "$KIRO_AGENT_CMD" "$SELF_PATH" ||
   ! grep -q 'search_graph' "$FAKE_HOME/.kiro/steering/codebase-memory.md" 2>/dev/null ||
   ! cat "$KIRO_AGENT" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
tools = d.get('tools', [])
server = d.get('mcpServers', {}).get('codebase-memory-mcp', {})
ok = (d.get('name') == 'codebase-memory' and tools[:3] == ['read', 'grep', 'glob'] and
      '@codebase-memory-mcp/search_graph' in tools and
      '@codebase-memory-mcp/check_index_coverage' in tools and
      all('@codebase-memory-mcp/' + name not in tools for name in
          ('index_repository', 'delete_project', 'manage_adr', 'ingest_traces')) and
      '@codebase-memory-mcp' not in tools and d.get('includeMcpJson') is False and
      set(d.get('mcpServers', {})) == {'codebase-memory-mcp'} and
      server.get('args') == ['--tool-profile', 'analysis'] and
      'search_graph' in d.get('prompt', ''))
sys.exit(0 if ok else 1)
" 2>/dev/null; then
  echo "FAIL 8w-ii: Kiro MCP, steering, or isolated graph agent missing"
  exit 1
fi
echo "OK 8w-ii: Kiro MCP + steering + isolated exact-tool graph agent"

# 8x: Hermes Agent YAML MCP mapping
HERMES_CMD=$(sed -n '/^  codebase-memory-mcp:/{n;s/^ *command: *//p;}' \
  "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null | head -1)
if ! grep -q '^mcp_servers:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   ! grep -q '^  codebase-memory-mcp:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   ! quoted_path_value_matches "$HERMES_CMD" "$SELF_PATH"; then
  echo "FAIL 8x: Hermes MCP mapping missing or malformed"
  exit 1
fi
echo "OK 8x: Hermes Agent MCP"
if ! grep -q '^name: codebase-memory$' "$FAKE_HOME/.hermes/skills/codebase-memory/SKILL.md" 2>/dev/null ||
   ! grep -q 'delegate_task' "$FAKE_HOME/.hermes/skills/codebase-memory/SKILL.md" 2>/dev/null ||
   ! grep -q '`context`' "$FAKE_HOME/.hermes/skills/codebase-memory/SKILL.md" 2>/dev/null; then
  echo "FAIL 8x-i: Hermes delegation skill missing"
  exit 1
fi
echo "OK 8x-i: Hermes durable delegation skill"
if ! grep -q '^hooks:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   ! grep -q '^  pre_llm_call:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   ! grep -q 'hook-augment' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   ! grep -q -- '--dialect hermes' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null; then
  echo "FAIL 8x-ii: Hermes pre_llm_call context hook missing"
  exit 1
fi
echo "OK 8x-ii: Hermes pre_llm_call context hook"

# 8y: OpenHands MCP
CMD=$(json_get "$FAKE_HOME/.openhands/mcp.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH"; then
  echo "FAIL 8y: OpenHands command='$CMD'"
  exit 1
fi
echo "OK 8y: OpenHands MCP"
if ! grep -q '^name: codebase-memory$' "$FAKE_HOME/.agents/skills/codebase-memory/SKILL.md" 2>/dev/null ||
   ! grep -q 'trace_path' "$FAKE_HOME/.agents/skills/codebase-memory/SKILL.md" 2>/dev/null; then
  echo "FAIL 8y-i: OpenHands shared skill missing"
  exit 1
fi
echo "OK 8y-i: OpenHands shared skill"

# 8z: Cline CLI + IDE MCP and rules
CLINE_RULE="$FAKE_HOME/.cline/rules/codebase-memory-mcp.md"
for CLINE_CFG in "$FAKE_HOME/.cline/mcp.json" \
                 "$FAKE_HOME/.cline/data/settings/cline_mcp_settings.json"; do
  CMD=$(json_get "$CLINE_CFG" "d['mcpServers']['codebase-memory-mcp']['command']")
  if ! path_match "$CMD" "$SELF_PATH"; then
    echo "FAIL 8z: Cline command='$CMD' in $CLINE_CFG"
    exit 1
  fi
done
if [ ! -f "$CLINE_RULE" ]; then
  echo "FAIL 8z: Cline rule missing"
  exit 1
fi
CLINE_SETTINGS="$FAKE_HOME/.cline/settings.json"
CLINE_ENABLED=$(json_get "$CLINE_SETTINGS" "d.get('hooksEnabled')")
CLINE_KEEP=$(json_get "$CLINE_SETTINGS" "d.get('keep', '')")
if [ "$CLINE_ENABLED" != "False" ] || [ "$CLINE_KEEP" != "cline" ]; then
  echo "FAIL 8z: Cline hook enable state or foreign settings changed"
  exit 1
fi
for CLINE_EVENT in TaskStart TaskResume UserPromptSubmit PreCompact; do
  if [[ "$BINARY" == *.exe ]]; then
    CLINE_HOOK="$FAKE_HOME/.cline/hooks/$CLINE_EVENT.ps1"
  else
    CLINE_HOOK="$FAKE_HOME/.cline/hooks/$CLINE_EVENT"
  fi
  if [ -e "$CLINE_HOOK" ]; then
    echo "FAIL 8z: unsupported Cline $CLINE_EVENT automatic hook was installed"
    exit 1
  fi
done
echo "OK 8z: Cline MCP + instructions; unreliable automatic hooks withheld"

# 8aa: Qwen Code MCP + instructions
CMD=$(json_get "$FAKE_HOME/.qwen/settings.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" || [ ! -f "$FAKE_HOME/.qwen/QWEN.md" ]; then
  echo "FAIL 8aa: Qwen Code integration incomplete"
  exit 1
fi
if ! grep -q 'SessionStart' "$FAKE_HOME/.qwen/settings.json" 2>/dev/null ||
   ! grep -q 'SubagentStart' "$FAKE_HOME/.qwen/settings.json" 2>/dev/null ||
   ! grep -q 'hook-augment' "$FAKE_HOME/.qwen/settings.json" 2>/dev/null; then
  echo "FAIL 8aa: Qwen lifecycle hooks missing"
  exit 1
fi
echo "OK 8aa: Qwen Code MCP + instructions"

# 8ab: VS Code-only installs receive Copilot durable context without inventing
# a Copilot CLI MCP config. No copilot executable is present in the fixture.
if cat "$FAKE_HOME/.copilot/mcp-config.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if 'codebase-memory-mcp' in d.get('mcpServers', {}) else 1)
" 2>/dev/null; then
  echo "FAIL 8ab: VS Code-only install created a Copilot CLI MCP entry"
  exit 1
fi
COPILOT_SKILL="$FAKE_HOME/.copilot/skills/codebase-memory/SKILL.md"
COPILOT_AGENT="$FAKE_HOME/.copilot/agents/codebase-memory.agent.md"
if ! grep -q 'search_graph' "$COPILOT_SKILL" 2>/dev/null ||
   ! grep -q '^tools:' "$COPILOT_AGENT" 2>/dev/null ||
   ! grep -q 'codebase-memory-mcp/trace_path' "$COPILOT_AGENT" 2>/dev/null ||
   grep -qE '^  - (edit|shell|bash|codebase-memory-mcp/(index_repository|delete_project|manage_adr|ingest_traces))$' "$COPILOT_AGENT" 2>/dev/null; then
  echo "FAIL 8ab: VS Code-only durable skill or read-only agent is wrong"
  exit 1
fi
echo "OK 8ab: VS Code-only durable skill + read-only agent; no CLI MCP config"
COPILOT_HOOKS="$FAKE_HOME/.copilot/hooks/codebase-memory-mcp.json"
if ! grep -q 'sessionStart' "$COPILOT_HOOKS" 2>/dev/null ||
   ! grep -q 'subagentStart' "$COPILOT_HOOKS" 2>/dev/null ||
   ! grep -q 'powershell' "$COPILOT_HOOKS" 2>/dev/null ||
   ! grep -q 'timeoutSec' "$COPILOT_HOOKS" 2>/dev/null; then
  echo "FAIL 8ab-i: Copilot lifecycle hook manifest missing"
  exit 1
fi
echo "OK 8ab-i: VS Code SessionStart + SubagentStart hooks"

# 8ac: Factory Droid stdio MCP schema
CMD=$(json_get "$FAKE_HOME/.factory/mcp.json" "d['mcpServers']['codebase-memory-mcp']['command']")
FACTORY_TYPE=$(json_get "$FAKE_HOME/.factory/mcp.json" "d['mcpServers']['codebase-memory-mcp']['type']")
if ! path_match "$CMD" "$SELF_PATH" || [ "$FACTORY_TYPE" != "stdio" ]; then
  echo "FAIL 8ac: Factory Droid MCP schema is wrong"
  exit 1
fi
echo "OK 8ac: Factory Droid MCP (disabled policy left user-controlled)"
if ! grep -q 'search_graph' "$FAKE_HOME/.factory/AGENTS.md" 2>/dev/null; then
  echo "FAIL 8ac-i: Factory durable instructions missing"
  exit 1
fi
FACTORY_MATCHER_COUNT=$(grep -c '"matcher"' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null || true)
if [[ "$BINARY" == *.exe ]]; then
  if grep -q 'hook-augment' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null; then
    echo "FAIL 8ac-i: Factory hook installed on Windows without a documented shell contract"
    exit 1
  fi
  echo "OK 8ac-i: Factory durable instructions; Windows hook withheld"
elif ! grep -q 'SessionStart' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
     ! grep -q 'PostToolUse' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
     ! grep -q 'Read' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
     ! grep -q 'hook-augment' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
     ! grep -q 'timeout' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
     [ "$FACTORY_MATCHER_COUNT" != "1" ]; then
  echo "FAIL 8ac-i: Factory SessionStart/PostToolUse Read hooks missing or malformed"
  exit 1
else
  echo "OK 8ac-i: Factory durable instructions + SessionStart/PostToolUse Read"
fi
FACTORY_AGENT="$FAKE_HOME/.factory/droids/codebase-memory.md"
if ! grep -q '^tools: \["Read", "LS", "Grep", "Glob",' "$FACTORY_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__search_graph' "$FACTORY_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__check_index_coverage' "$FACTORY_AGENT" 2>/dev/null ||
   grep -q '^mcpServers:' "$FACTORY_AGENT" 2>/dev/null ||
   grep -qE 'mcp__codebase-memory-mcp__(index_repository|delete_project|manage_adr|ingest_traces)' "$FACTORY_AGENT" 2>/dev/null; then
  echo "FAIL 8ac-ii: Factory exact-tool Verify droid missing or over-privileged"
  exit 1
fi
echo "OK 8ac-ii: Factory exact-tool Verify droid"

# 8ad: Crush stdio MCP schema + instructions
CMD=$(json_get "$FAKE_HOME/.config/crush/crush.json" "d['mcp']['codebase-memory-mcp']['command']")
CRUSH_TYPE=$(json_get "$FAKE_HOME/.config/crush/crush.json" "d['mcp']['codebase-memory-mcp']['type']")
CRUSH_CONTEXT=$(json_get "$FAKE_HOME/.config/crush/crush.json" "str(any(str(p).endswith('codebase-memory.md') for p in d['options']['context_paths']))")
if ! path_match "$CMD" "$SELF_PATH" || [ "$CRUSH_TYPE" != "stdio" ] ||
   [ "$CRUSH_CONTEXT" != "True" ] ||
   ! grep -q 'does not inherit MCP access' "$FAKE_HOME/.config/crush/codebase-memory.md" 2>/dev/null; then
  echo "FAIL 8ad: Crush integration incomplete"
  exit 1
fi
echo "OK 8ad: Crush MCP + instructions"

# 8ae: Goose YAML extension
if [[ "$BINARY" == *.exe ]]; then
  GOOSE_CFG="$FAKE_HOME/AppData/Roaming/Block/goose/config/config.yaml"
else
  GOOSE_CFG="$FAKE_HOME/.config/goose/config.yaml"
fi
GOOSE_CMD=$(sed -n '/^  codebase-memory-mcp:/,/^  [^ ]/{s/^ *cmd: *//p;}' \
  "$GOOSE_CFG" 2>/dev/null | head -1)
if ! grep -q '^extensions:' "$GOOSE_CFG" 2>/dev/null ||
   ! grep -q '^  codebase-memory-mcp:' "$GOOSE_CFG" 2>/dev/null ||
   ! quoted_path_value_matches "$GOOSE_CMD" "$SELF_PATH"; then
  echo "FAIL 8ae: Goose extension missing or malformed"
  exit 1
fi
echo "OK 8ae: Goose MCP extension"
if ! grep -q 'search_graph' "$FAKE_HOME/.config/goose/.goosehints" 2>/dev/null ||
   ! grep -q 'subagent' "$FAKE_HOME/.config/goose/.goosehints" 2>/dev/null; then
  echo "FAIL 8ae-i: Goose durable hints missing"
  exit 1
fi
echo "OK 8ae-i: Goose durable hints"

# 8af: Mistral Vibe TOML array table
VIBE_CMD=$(sed -n 's/^command *= *//p' "$FAKE_HOME/.vibe/config.toml" 2>/dev/null | head -1)
if ! grep -q '^\[\[mcp_servers\]\]' "$FAKE_HOME/.vibe/config.toml" 2>/dev/null ||
   ! grep -q '^name = "codebase-memory-mcp"' "$FAKE_HOME/.vibe/config.toml" 2>/dev/null ||
   ! quoted_path_value_matches "$VIBE_CMD" "$SELF_PATH"; then
  echo "FAIL 8af: Mistral Vibe MCP table missing or malformed"
  exit 1
fi
echo "OK 8af: Mistral Vibe MCP"
if ! grep -q 'search_graph' "$FAKE_HOME/.vibe/AGENTS.md" 2>/dev/null ||
   ! grep -q 'subagent' "$FAKE_HOME/.vibe/AGENTS.md" 2>/dev/null; then
  echo "FAIL 8af-i: Vibe durable AGENTS.md missing"
  exit 1
fi
VIBE_AGENT="$FAKE_HOME/.vibe/agents/codebase-memory.toml"
VIBE_PROMPT="$FAKE_HOME/.vibe/prompts/codebase-memory.md"
if ! grep -q '^agent_type = "subagent"$' "$VIBE_AGENT" 2>/dev/null ||
   ! grep -Fq 'enabled_tools = ["read_file", "grep_search", "codebase-memory-mcp_search_graph"' "$VIBE_AGENT" 2>/dev/null ||
   ! grep -Fq '"codebase-memory-mcp_get_code_snippet"' "$VIBE_AGENT" 2>/dev/null ||
   ! grep -Fq '"codebase-memory-mcp_check_index_coverage"' "$VIBE_AGENT" 2>/dev/null ||
   grep -Fq '"codebase-memory-mcp_*"' "$VIBE_AGENT" 2>/dev/null ||
   grep -qE 'codebase-memory-mcp_(index_repository|delete_project|manage_adr|ingest_traces)' "$VIBE_AGENT" 2>/dev/null ||
   ! grep -q '^system_prompt_id = "codebase-memory"$' "$VIBE_AGENT" 2>/dev/null ||
   ! grep -q 'search_graph' "$VIBE_PROMPT" 2>/dev/null ||
   ! grep -q 'Never edit files or perform state-changing actions' "$VIBE_PROMPT" 2>/dev/null; then
  echo "FAIL 8af-i: Vibe global subagent or prompt missing"
  exit 1
fi
echo "OK 8af-i: Vibe durable instructions + graph-only subagent and prompt"

# 8ag: Windsurf MCP + always-on global rules.
CMD=$(json_get "$FAKE_HOME/.codeium/windsurf/mcp_config.json" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! grep -q 'search_graph' "$FAKE_HOME/.codeium/windsurf/memories/global_rules.md" 2>/dev/null ||
   ! grep -q 'subagent' "$FAKE_HOME/.codeium/windsurf/memories/global_rules.md" 2>/dev/null; then
  echo "FAIL 8ag: Windsurf MCP or global rules missing"
  exit 1
fi
WINDSURF_RULE_BYTES=$(wc -c < "$FAKE_HOME/.codeium/windsurf/memories/global_rules.md")
if [ "$WINDSURF_RULE_BYTES" -gt 6000 ]; then
  echo "FAIL 8ag: Windsurf global rules exceed the 6000-character limit"
  exit 1
fi
echo "OK 8ag: Windsurf MCP + global rules"

# 8ah: Augment/Auggie MCP, durable rule, dedicated subagent, and matcher-free
# SessionStart hook.
AUGMENT_SETTINGS="$FAKE_HOME/.augment/settings.json"
AUGMENT_RULE="$FAKE_HOME/.augment/rules/codebase-memory.md"
AUGMENT_AGENT="$FAKE_HOME/.augment/agents/codebase-memory.md"
if [[ "$BINARY" == *.exe ]]; then
  AUGMENT_SCRIPT="$FAKE_HOME/.augment/hooks/codebase-memory-session.ps1"
else
  AUGMENT_SCRIPT="$FAKE_HOME/.augment/hooks/codebase-memory-session.sh"
fi
CMD=$(json_get "$AUGMENT_SETTINGS" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! grep -q 'search_graph' "$AUGMENT_RULE" 2>/dev/null ||
   ! grep -q '^name: codebase-memory$' "$AUGMENT_AGENT" 2>/dev/null ||
   ! grep -q 'graph project' "$AUGMENT_AGENT" 2>/dev/null ||
   ! grep -q 'hook-augment' "$AUGMENT_SCRIPT" 2>/dev/null ||
   ! grep -q 'SessionStart' "$AUGMENT_SCRIPT" 2>/dev/null; then
  echo "FAIL 8ah: Augment/Auggie MCP, context, subagent, or wrapper missing"
  exit 1
fi
if ! cat "$AUGMENT_SETTINGS" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
entries = d.get('hooks', {}).get('SessionStart', [])
owned = [e for e in entries if any('codebase-memory-session' in str(h.get('command', '')) for h in e.get('hooks', []))]
ok = owned and all('matcher' not in e for e in owned) and any(h.get('timeout') == 5000 for e in owned for h in e.get('hooks', []))
sys.exit(0 if ok else 1)
" 2>/dev/null; then
  echo "FAIL 8ah: Augment/Auggie SessionStart must be matcher-free with timeout 5000"
  exit 1
fi
if [[ "$BINARY" != *.exe ]] && [ ! -x "$AUGMENT_SCRIPT" ]; then
  echo "FAIL 8ah: Augment/Auggie SessionStart wrapper is not executable"
  exit 1
fi
echo "OK 8ah: Augment/Auggie MCP + SessionStart + subagent"

# 8ai: Consolidated skill installed without recursively deleting legacy content.
SKILL_FILE="$FAKE_HOME/.claude/skills/codebase-memory/SKILL.md"
if [ ! -s "$SKILL_FILE" ]; then
  echo "FAIL 8ai: skill codebase-memory missing or empty"
  exit 1
fi
echo "OK 8ai: skill installed"

# 8aj: Qoder MCP, skill, directly attached read-only graph agent, and current
# lifecycle/read hooks. Legacy UserPromptSubmit is removed during migration.
QODER_SETTINGS="$FAKE_HOME/.qoder/settings.json"
QODER_SKILL="$FAKE_HOME/.qoder/skills/codebase-memory/SKILL.md"
QODER_AGENT="$FAKE_HOME/.qoder/agents/codebase-memory.md"
CMD=$(json_get "$QODER_SETTINGS" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! grep -q 'search_graph' "$QODER_SKILL" 2>/dev/null ||
   ! grep -q '^tools: Read,Grep,Glob,mcp__codebase-memory-mcp__search_graph' "$QODER_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__check_index_coverage' "$QODER_AGENT" 2>/dev/null ||
   ! grep -q '^mcpServers:$' "$QODER_AGENT" 2>/dev/null ||
   ! grep -q '^  - codebase-memory-mcp$' "$QODER_AGENT" 2>/dev/null ||
   grep -qE 'mcp__codebase-memory-mcp__(index_repository|delete_project|manage_adr|ingest_traces)' "$QODER_AGENT" 2>/dev/null ||
   grep -q 'parent agent' "$QODER_AGENT" 2>/dev/null; then
  echo "FAIL 8aj: Qoder MCP, skill, or exact-tool Verify agent missing"
  exit 1
fi
if [[ "$BINARY" == *.exe ]]; then
  QODER_EXPECTED_SHELL="powershell"
else
  QODER_EXPECTED_SHELL=""
fi
if ! cat "$QODER_SETTINGS" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
all_hooks = d.get('hooks', {})
expected = {
    'SessionStart': 'startup|resume|clear|compact|new',
    'SubagentStart': '*',
    'PostToolUse': 'Read',
}
ok = d.get('theme') == 'dark' and 'UserPromptSubmit' not in all_hooks
for event, matcher in expected.items():
    entries = all_hooks.get(event, [])
    ok = ok and len(entries) == 1 and entries[0].get('matcher') == matcher
    hooks = entries[0].get('hooks', []) if entries else []
    ok = ok and len(hooks) == 1
    if hooks:
        hook = hooks[0]
        ok = (ok and '--dialect qoder' in hook.get('command', '') and
              hook.get('timeout') == 5 and
              hook.get('shell', '') == '$QODER_EXPECTED_SHELL')
sys.exit(0 if ok else 1)
" 2>/dev/null; then
  echo "FAIL 8aj: Qoder lifecycle/read hooks missing or malformed"
  exit 1
fi
echo "OK 8aj: Qoder MCP + direct graph agent + lifecycle/read hooks"

# 8ak: Kimi honors KIMI_CODE_HOME for MCP and durable parent/subagent context.
KIMI_MCP="$CUSTOM_KIMI_HOME/mcp.json"
KIMI_SKILL="$CUSTOM_KIMI_HOME/skills/codebase-memory/SKILL.md"
KIMI_CONFIG="$CUSTOM_KIMI_HOME/config.toml"
CMD=$(json_get "$KIMI_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
KIMI_HOOK_COUNT=$(grep -cF '[[hooks]]' "$KIMI_CONFIG" 2>/dev/null || true)
if ! path_match "$CMD" "$SELF_PATH" ||
   ! grep -q '^# Personal Kimi guidance$' "$CUSTOM_KIMI_HOME/AGENTS.md" 2>/dev/null ||
   ! grep -q 'search_graph' "$CUSTOM_KIMI_HOME/AGENTS.md" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$KIMI_SKILL" 2>/dev/null ||
   ! grep -q '^theme = "dark"$' "$KIMI_CONFIG" 2>/dev/null ||
   [ "$KIMI_HOOK_COUNT" != "1" ] ||
   ! grep -q '^event = "UserPromptSubmit"$' "$KIMI_CONFIG" 2>/dev/null ||
   ! grep -q -- '--dialect kimi' "$KIMI_CONFIG" 2>/dev/null ||
   ! grep -q '^timeout = 5$' "$KIMI_CONFIG" 2>/dev/null ||
   grep -q 'SessionStart' "$KIMI_CONFIG" 2>/dev/null ||
   grep -q 'SubagentStart' "$KIMI_CONFIG" 2>/dev/null; then
  echo "FAIL 8ak: custom KIMI_CODE_HOME MCP, durable context, or managed prompt hook missing"
  exit 1
fi
echo "OK 8ak: custom KIMI_CODE_HOME MCP + durable context + UserPromptSubmit hook"

# 8al: Pi has documented instructions and skill, but no invented MCP config.
PI_INSTRUCTIONS="$FAKE_HOME/.pi/agent/AGENTS.md"
PI_SKILL="$FAKE_HOME/.pi/agent/skills/codebase-memory/SKILL.md"
if ! grep -q 'search_graph' "$PI_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$PI_SKILL" 2>/dev/null ||
   [ -e "$FAKE_HOME/.pi/agent/mcp.json" ]; then
  echo "FAIL 8al: Pi durable context missing or unsupported MCP config created"
  exit 1
fi
echo "OK 8al: Pi durable context only (no MCP config)"

# 8am: Warp receives the documented shared skill; MCP remains user/UI-managed.
WARP_SKILL="$FAKE_HOME/.agents/skills/codebase-memory/SKILL.md"
if ! grep -q 'Sessions and Subagents' "$WARP_SKILL" 2>/dev/null ||
   [ -e "$FAKE_HOME/.warp/mcp.json" ] ||
   [ -e "$FAKE_HOME/.config/warp-terminal/mcp.json" ]; then
  echo "FAIL 8am: Warp shared skill missing or unsupported MCP config created"
  exit 1
fi
echo "OK 8am: Warp shared skill only (MCP remains manual)"

# 8an: Junie receives its MCP config, skill, and dedicated restricted-server
# graph agent.
# SessionStart augmentation remains withheld because current EAP docs say its
# additionalContext output is ignored.
JUNIE_MCP="$FAKE_HOME/.junie/mcp/mcp.json"
JUNIE_SKILL="$FAKE_HOME/.junie/skills/codebase-memory/SKILL.md"
JUNIE_AGENT="$FAKE_HOME/.junie/agents/codebase-memory.md"
CMD=$(json_get "$JUNIE_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
JUNIE_SCOUT_CMD=$(json_get "$JUNIE_MCP" "d['mcpServers']['codebase-memory-scout']['command']")
JUNIE_ANALYSIS_CMD=$(json_get "$JUNIE_MCP" "d['mcpServers']['codebase-memory-analysis']['command']")
JUNIE_SCOUT_ARGS=$(json_get "$JUNIE_MCP" "d['mcpServers']['codebase-memory-scout']['args']")
JUNIE_ANALYSIS_ARGS=$(json_get "$JUNIE_MCP" "d['mcpServers']['codebase-memory-analysis']['args']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! path_match "$JUNIE_SCOUT_CMD" "$SELF_PATH" ||
   ! path_match "$JUNIE_ANALYSIS_CMD" "$SELF_PATH" ||
   [ "$JUNIE_SCOUT_ARGS" != "['--tool-profile=scout']" ] ||
   [ "$JUNIE_ANALYSIS_ARGS" != "['--tool-profile=analysis']" ] ||
   ! grep -q 'Sessions and Subagents' "$JUNIE_SKILL" 2>/dev/null ||
   ! grep -q 'description: "Default task-directed graph verification' "$JUNIE_AGENT" 2>/dev/null ||
   ! grep -q 'tools: \["Read", "Grep", "Glob"\]' "$JUNIE_AGENT" 2>/dev/null ||
   ! grep -q 'mcpServers: \["codebase-memory-analysis"\]' "$JUNIE_AGENT" 2>/dev/null ||
   ! grep -q 'hard-enforces the analysis tool profile' "$JUNIE_AGENT" 2>/dev/null ||
   ! grep -q 'check_index_coverage' "$JUNIE_AGENT" 2>/dev/null ||
   grep -qE '(index_repository|delete_project|manage_adr|ingest_traces)' "$JUNIE_AGENT" 2>/dev/null ||
   grep -q '"Bash"' "$JUNIE_AGENT" 2>/dev/null; then
  echo "FAIL 8an: Junie MCP, skill, or restricted-server Verify agent missing"
  exit 1
fi
echo "OK 8an: Junie MCP + skill + restricted-server Verify agent"

# 8ao: Conditional registry clients install only when an explicit config exists.
CMD=$(json_get "$ROO_CFG" "d['mcpServers']['codebase-memory-mcp']['command']")
ROO_KEEP=$(json_get "$ROO_CFG" "d.get('keep', '')")
if ! path_match "$CMD" "$SELF_PATH" || [ "$ROO_KEEP" != "roo" ]; then
  echo "FAIL 8ao: explicit Roo config was not merged safely"
  exit 1
fi
echo "OK 8ao: explicit conditional Roo config merged safely"

# 8ap: GitLab Duo uses its documented MCP path. The optional SessionStart
# augmenter is Unix-only and must preserve a pre-existing user hook.
CMD=$(json_get "$GITLAB_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
GITLAB_KEEP=$(json_get "$GITLAB_MCP" "d.get('keep', '')")
if ! path_match "$CMD" "$SELF_PATH" || [ "$GITLAB_KEEP" != "gitlab-mcp" ]; then
  echo "FAIL 8ap: GitLab Duo MCP is incomplete"
  exit 1
fi
if [[ "$BINARY" == *.exe ]]; then
  if ! cat "$GITLAB_HOOKS" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
entries = d.get('hooks', {}).get('SessionStart', [])
hooks = [h for entry in entries for h in entry.get('hooks', [])]
owned = [h for h in hooks if 'hook-augment' in str(h.get('command', ''))]
user = [h for h in hooks if h.get('command') == '/usr/bin/user-hook']
ok = (d.get('keep') == 'gitlab-hooks' and not owned and len(user) == 1 and
      'enable-project-hooks' not in str(d))
sys.exit(0 if ok else 1)
" 2>/dev/null; then
    echo "FAIL 8ap: GitLab Windows hook was not withheld or user hook changed"
    exit 1
  fi
  echo "OK 8ap: GitLab Duo MCP + preserved user hook; Windows augmentation withheld"
elif ! cat "$GITLAB_HOOKS" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
entries = d.get('hooks', {}).get('SessionStart', [])
hooks = [h for entry in entries for h in entry.get('hooks', [])]
owned = [h for h in hooks if 'hook-augment' in str(h.get('command', ''))]
user = [h for h in hooks if h.get('command') == '/usr/bin/user-hook']
ok = (d.get('keep') == 'gitlab-hooks' and len(owned) == 1 and
      owned[0].get('timeout') == 5 and len(user) == 1 and
      'enable-project-hooks' not in str(d))
sys.exit(0 if ok else 1)
" 2>/dev/null; then
  echo "FAIL 8ap: GitLab Duo SessionStart hook is incomplete"
  exit 1
else
  echo "OK 8ap: GitLab Duo MCP + preserved user hook + SessionStart augmentation"
fi

# 8aq: Devin receives MCP and durable context. On Unix, its prompt/compaction
# augmentations are installed while SessionStart is inherited from Claude in
# this fixture; on Windows all optional hooks are withheld.
CMD=$(json_get "$DEVIN_CONFIG" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$CMD" "$SELF_PATH" ||
   ! grep -q '^# Personal Devin guidance$' "$DEVIN_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'search_graph' "$DEVIN_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$DEVIN_SKILL" 2>/dev/null; then
  echo "FAIL 8aq: Devin MCP, AGENTS.md, or skill missing"
  exit 1
fi
if [[ "$BINARY" == *.exe ]]; then
  if grep -q -- '--dialect devin' "$DEVIN_CONFIG" 2>/dev/null; then
    echo "FAIL 8aq: Devin hook installed on Windows without a documented executor contract"
    exit 1
  fi
  echo "OK 8aq: Devin MCP + durable AGENTS.md/skill; Windows hooks withheld"
elif ! cat "$DEVIN_CONFIG" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
all_hooks = d.get('hooks', {})
ok = d.get('theme_mode') == 'dark' and 'SubagentStart' not in all_hooks
for event in ('UserPromptSubmit', 'PostCompaction'):
    hooks = [h for entry in all_hooks.get(event, []) for h in entry.get('hooks', [])]
    owned = [h for h in hooks if '--dialect devin' in str(h.get('command', ''))]
    ok = ok and len(owned) == 1 and owned[0].get('timeout') == 5
session_hooks = [h for entry in all_hooks.get('SessionStart', []) for h in entry.get('hooks', [])]
ok = ok and not any('--dialect devin' in str(h.get('command', '')) for h in session_hooks)
owned_total = sum(str(all_hooks.get(event, [])).count('--dialect devin')
                  for event in ('SessionStart', 'UserPromptSubmit', 'PostCompaction'))
ok = ok and owned_total == 2
sys.exit(0 if ok else 1)
" 2>/dev/null ||
   ! grep -q 'SessionStart' "$FAKE_HOME/.claude/settings.json" 2>/dev/null ||
   ! grep -q 'cbm-code-discovery-gate' "$FAKE_HOME/.claude/settings.json" 2>/dev/null; then
  echo "FAIL 8aq: Devin hooks are not deduplicated against Claude SessionStart"
  exit 1
else
  echo "OK 8aq: Devin MCP + prompt/compaction hooks + inherited Claude SessionStart"
fi

# 8ar: CodeBuddy Code CLI uses the current .mcp.json and durable skill/agent
# surfaces. Beta settings hooks remain untouched.
CMD=$(json_get "$CODEBUDDY_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
CODEBUDDY_KEEP=$(json_get "$CODEBUDDY_MCP" "d.get('keep', '')")
if ! path_match "$CMD" "$SELF_PATH" || [ "$CODEBUDDY_KEEP" != "codebuddy" ] ||
   ! grep -q '^# Personal CodeBuddy guidance$' "$CODEBUDDY_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'search_graph' "$CODEBUDDY_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$CODEBUDDY_SKILL" 2>/dev/null ||
   ! grep -q '^permissionMode: plan$' "$CODEBUDDY_AGENT" 2>/dev/null ||
   ! grep -q '^tools: Read,Grep,Glob,mcp__codebase-memory-mcp__search_graph,' "$CODEBUDDY_AGENT" 2>/dev/null ||
   ! grep -q 'mcp__codebase-memory-mcp__check_index_coverage' "$CODEBUDDY_AGENT" 2>/dev/null ||
   grep -qE 'mcp__codebase-memory-mcp__(index_repository|delete_project|manage_adr|ingest_traces)' "$CODEBUDDY_AGENT" 2>/dev/null ||
   grep -q '^tools:$' "$CODEBUDDY_AGENT" 2>/dev/null ||
   grep -q 'mcp__codebase-memory__search_graph' "$CODEBUDDY_AGENT" 2>/dev/null ||
   ! grep -q '^skills: codebase-memory$' "$CODEBUDDY_AGENT" 2>/dev/null ||
   [ -e "$CODEBUDDY_SETTINGS" ]; then
  echo "FAIL 8ar: CodeBuddy current MCP, durable context, or read-only agent missing"
  exit 1
fi
echo "OK 8ar: CodeBuddy .mcp.json + CODEBUDDY.md + skill/agent; no beta hooks"

# 8as: Bob IDE is conditional on its existing mcp.json while Bob Shell is
# detected from the bob executable. Both share rules; only the IDE gets a skill.
BOB_IDE_CMD=$(json_get "$BOB_IDE_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
BOB_SHELL_CMD=$(json_get "$BOB_SHELL_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
BOB_IDE_KEEP=$(json_get "$BOB_IDE_MCP" "d.get('keep', '')")
BOB_SHELL_KEEP=$(json_get "$BOB_SHELL_MCP" "d.get('keep', '')")
if ! path_match "$BOB_IDE_CMD" "$SELF_PATH" ||
   ! path_match "$BOB_SHELL_CMD" "$SELF_PATH" ||
   [ "$BOB_IDE_KEEP" != "bob-ide" ] || [ "$BOB_SHELL_KEEP" != "bob-shell" ] ||
   ! grep -q '^# Personal Bob guidance$' "$BOB_RULE" 2>/dev/null ||
   ! grep -q 'search_graph' "$BOB_RULE" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$BOB_SKILL" 2>/dev/null ||
   [ -e "$BOB_AGENT" ]; then
  echo "FAIL 8as: Bob IDE/Shell MCP, shared rules, or IDE skill is wrong"
  exit 1
fi
echo "OK 8as: Bob IDE conditional MCP + Bob Shell MCP + shared rules/IDE skill"

# 8at: Pochi keeps JSONC user content while adding the current mcp root. Its
# handoff agent is intentionally limited to readFile because MCP allowlist names
# are not documented for child agents.
POCHI_CMD=$(sed '/^[[:space:]]*\/\//d' "$POCHI_MCP" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['mcp']['codebase-memory-mcp']['command'])" 2>/dev/null || echo "")
POCHI_TOOL_COUNT=$(grep -c '^  - ' "$POCHI_AGENT" 2>/dev/null || true)
if ! path_match "$POCHI_CMD" "$SELF_PATH" ||
   ! grep -q 'Personal Pochi setting' "$POCHI_MCP" 2>/dev/null ||
   ! grep -q '"keep": "pochi"' "$POCHI_MCP" 2>/dev/null ||
   ! grep -q '^# Personal Pochi guidance$' "$POCHI_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'search_graph' "$POCHI_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$POCHI_SKILL" 2>/dev/null ||
   ! grep -q '^  - readFile$' "$POCHI_AGENT" 2>/dev/null ||
   [ "$POCHI_TOOL_COUNT" != "1" ] ||
   ! grep -q 'parent agent' "$POCHI_AGENT" 2>/dev/null ||
   grep -qE '^  - (writeFile|editFile|execute|bash|shell)$' "$POCHI_AGENT" 2>/dev/null ||
   grep -q 'mcpServers:' "$POCHI_AGENT" 2>/dev/null; then
  echo "FAIL 8at: Pochi JSONC MCP, durable context, or readFile-only agent missing"
  exit 1
fi
echo "OK 8at: Pochi config.jsonc + README/skill + readFile-only handoff agent"

# 8au: Rovo's documented global AGENTS.md memory complements its skill and
# handoff subagent; no undocumented lifecycle hook is invented.
ROVO_CMD=$(json_get "$ROVO_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$ROVO_CMD" "$SELF_PATH" ||
   ! grep -q '^# Personal Rovo guidance$' "$ROVO_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'search_graph' "$ROVO_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q 'Sessions and Subagents' "$ROVO_SKILL" 2>/dev/null ||
   ! grep -q 'parent agent' "$ROVO_AGENT" 2>/dev/null ||
   [ -e "$FAKE_HOME/.rovodev/hooks.json" ]; then
  echo "FAIL 8au: Rovo MCP, global memory, skill, or handoff agent is incomplete"
  exit 1
fi
echo "OK 8au: Rovo MCP + global memory + skill/handoff agent"

# 8av: The dedicated Amazon Q IDE page uses root default.json. Existing
# agents/default.json and mcp.json remain compatibility fallbacks.
AMAZON_Q_CMD=$(json_get "$AMAZON_Q_MCP" "d['mcpServers']['codebase-memory-mcp']['command']")
if ! path_match "$AMAZON_Q_CMD" "$SELF_PATH" ||
   [ -e "$FAKE_HOME/.aws/amazonq/agents/default.json" ] ||
   [ -e "$FAKE_HOME/.aws/amazonq/mcp.json" ]; then
  echo "FAIL 8av: Amazon Q IDE canonical default.json selection is wrong"
  exit 1
fi
echo "OK 8av: Amazon Q IDE canonical default.json"

# 8aw: Every supported profile dialect installs the complete tier matrix. This
# complements the detailed Verify checks above without duplicating each schema.
assert_tier_profile_set "Claude" "$FAKE_HOME/.claude/agents" ".md" "direct"
assert_tier_profile_set "Codex" "$FAKE_HOME/.codex/agents" ".toml" "direct"
assert_tier_profile_set "Gemini" "$FAKE_HOME/.gemini/agents" ".md" "direct"
if [ -f "$FAKE_HOME/.config/opencode/opencode.json" ]; then
  assert_tier_profile_set "OpenCode" "$FAKE_HOME/.config/opencode/agents" ".md" "direct"
fi
assert_tier_profile_set "Kilo" "$FAKE_HOME/.config/kilo/agents" ".md" "direct"
assert_tier_profile_set "Cursor" "$FAKE_HOME/.cursor/agents" ".md" "handoff"
assert_tier_profile_set "Kiro" "$FAKE_HOME/.kiro/agents" ".json" "direct"
assert_tier_profile_set "Junie" "$FAKE_HOME/.junie/agents" ".md" "direct"
assert_tier_profile_set "Augment" "$FAKE_HOME/.augment/agents" ".md" "handoff"
assert_tier_profile_set "Qwen" "$FAKE_HOME/.qwen/agents" ".md" "direct"
assert_tier_profile_set "Factory" "$FAKE_HOME/.factory/droids" ".md" "direct"
assert_tier_profile_set "Vibe" "$FAKE_HOME/.vibe/agents" ".toml" "direct"
assert_tier_prompt_set "Vibe" "$FAKE_HOME/.vibe/prompts" ".md"
for VIBE_SLUG in codebase-memory-scout codebase-memory codebase-memory-auditor; do
  if ! grep -Fq "system_prompt_id = \"$VIBE_SLUG\"" "$FAKE_HOME/.vibe/agents/$VIBE_SLUG.toml" 2>/dev/null; then
    echo "FAIL 8aw: Vibe agent/prompt identifier mismatch for $VIBE_SLUG"
    exit 1
  fi
done
assert_tier_profile_set "Copilot" "$FAKE_HOME/.copilot/agents" ".agent.md" "direct"
assert_tier_profile_set "Qoder" "$FAKE_HOME/.qoder/agents" ".md" "direct"
assert_tier_profile_set "CodeBuddy" "$FAKE_HOME/.codebuddy/agents" ".md" "direct"
assert_tier_profile_set "Pochi" "$FAKE_HOME/.pochi/agents" ".md" "handoff"
assert_tier_profile_set "Rovo" "$FAKE_HOME/.rovodev/subagents" ".md" "handoff"
echo "OK 8aw: all supported Scout/Verify/Auditor profile sets"

echo ""
echo "=== Phase 9: agent config uninstall E2E ==="

# Run uninstall (same FAKE_HOME with all configs present)
UNINSTALL_BINARY="$BINARY"
if [[ "$BINARY" == *.exe ]]; then
  UNINSTALL_BINARY="$SELF_PATH"
fi
HOME="$FAKE_HOME" \
  XDG_CONFIG_HOME="$FAKE_HOME/.config" \
  APPDATA="$FAKE_HOME/AppData/Roaming" \
  LOCALAPPDATA="$FAKE_HOME/AppData/Local" \
  KIMI_CODE_HOME="$CUSTOM_KIMI_HOME" \
  CBM_ROO_CONFIG_PATH="$ROO_CFG" \
  PATH="$FAKE_HOME/.local/bin:$PATH" \
  "$UNINSTALL_BINARY" uninstall -y -n 2>&1 || true

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
if [ ! -f "$FAKE_HOME/.claude/.mcp.json" ] || cat "$FAKE_HOME/.claude/.mcp.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('mcpServers', {}) else 0)
" 2>/dev/null; then
  echo "OK 9c: legacy .mcp.json cleaned"
else
  echo "FAIL 9c: legacy .mcp.json still has entry"
  exit 1
fi

# 9d: All Claude hook registrations and owned scripts removed
if cat "$FAKE_HOME/.claude/settings.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
hooks = d.get('hooks', {})
found = any('cbm-code-discovery-gate' in str(h) or
            'cbm-session-reminder' in str(h) or
            'cbm-subagent-reminder' in str(h)
            for entries in hooks.values() for h in entries)
sys.exit(1 if found else 0)
" 2>/dev/null; then
  for HOOK_SCRIPT in cbm-code-discovery-gate cbm-session-reminder cbm-subagent-reminder; do
    if [ -e "$FAKE_HOME/.claude/hooks/$HOOK_SCRIPT" ]; then
      echo "FAIL 9d: owned hook script still present: $HOOK_SCRIPT"
      exit 1
    fi
  done
  echo "OK 9d: lifecycle/tool hooks and owned scripts removed"
else
  echo "FAIL 9d: Claude hook registration still present"
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
if [ -e "$GEMINI_AGENT" ]; then
  echo "FAIL 9g-ii: Gemini dedicated graph subagent remains"
  exit 1
fi
echo "OK 9g-ii: Gemini dedicated graph subagent removed"

# 9j: VS Code default and profile configs
for VSCODE_CHECK in "$VSCODE_CFG" "$VSCODE_PROFILE_CFG"; do
  if ! cat "$VSCODE_CHECK" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('servers', {}) else 0)
" 2>/dev/null; then
    echo "FAIL 9j: VS Code MCP still present in $VSCODE_CHECK"
    exit 1
  fi
done
echo "OK 9j: VS Code default and profile MCP removed"

# 9k: OpenClaw
if cat "$FAKE_HOME/.openclaw/openclaw.json" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
has_mcp = 'codebase-memory-mcp' in d.get('mcp', {}).get('servers', {})
sections = d.get('agents', {}).get('defaults', {}).get('compaction', {}).get('postCompactionSections', [])
sys.exit(1 if has_mcp or 'Codebase Knowledge Graph (codebase-memory-mcp)' in sections else 0)
" 2>/dev/null; then
  if grep -q 'codebase-memory-mcp:start' "$FAKE_HOME/.openclaw/workspace/AGENTS.md" 2>/dev/null ||
     grep -q 'codebase-memory-mcp:start' "$FAKE_HOME/.openclaw/workspace/TOOLS.md" 2>/dev/null; then
    echo "FAIL 9k: OpenClaw workspace context remains"
    exit 1
  fi
  echo "OK 9k: OpenClaw MCP, compaction, and workspace context removed"
else
  echo "FAIL 9k: OpenClaw MCP still present"
  exit 1
fi

# 9l: JSON-based new agents and modern Kilo are cleaned without deleting files.
for SPEC in \
  "$QODER_SETTINGS|mcpServers" \
  "$KIMI_MCP|mcpServers" \
  "$GITLAB_MCP|mcpServers" \
  "$DEVIN_CONFIG|mcpServers" \
  "$CODEBUDDY_MCP|mcpServers" \
  "$BOB_IDE_MCP|mcpServers" \
  "$BOB_SHELL_MCP|mcpServers" \
  "$JUNIE_MCP|mcpServers" \
  "$ROVO_MCP|mcpServers" \
  "$AMAZON_Q_MCP|mcpServers" \
  "$ROO_CFG|mcpServers" \
  "$FAKE_HOME/.openhands/mcp.json|mcpServers" \
  "$FAKE_HOME/.cline/mcp.json|mcpServers" \
  "$FAKE_HOME/.cline/data/settings/cline_mcp_settings.json|mcpServers" \
  "$FAKE_HOME/.qwen/settings.json|mcpServers" \
  "$FAKE_HOME/.factory/mcp.json|mcpServers" \
  "$AUGMENT_SETTINGS|mcpServers" \
  "$FAKE_HOME/.config/crush/crush.json|mcp" \
  "$FAKE_HOME/.config/kilo/kilo.jsonc|mcp" \
  "$FAKE_HOME/.codeium/windsurf/mcp_config.json|mcpServers"; do
  CFG=${SPEC%%|*}
  ROOT=${SPEC##*|}
  if ! cat "$CFG" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(1 if 'codebase-memory-mcp' in d.get('$ROOT', {}) else 0)
" 2>/dev/null; then
    echo "FAIL 9l: MCP entry remains in $CFG"
    exit 1
  fi
done
if ! cat "$JUNIE_MCP" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
servers = d.get('mcpServers', {})
names = {'codebase-memory-mcp', 'codebase-memory-scout', 'codebase-memory-analysis'}
sys.exit(1 if names.intersection(servers) else 0)
" 2>/dev/null; then
  echo "FAIL 9l: Junie default or restricted MCP alias remains"
  exit 1
fi
POCHI_MCP_AFTER=$(sed '/^[[:space:]]*\/\//d' "$POCHI_MCP" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('present' if 'codebase-memory-mcp' in d.get('mcp', {}) else 'absent')
" 2>/dev/null || echo "invalid")
if [ "$POCHI_MCP_AFTER" != "absent" ]; then
  echo "FAIL 9l: Pochi MCP entry remains or config.jsonc became invalid"
  exit 1
fi
if grep -q 'hook-augment' "$QODER_SETTINGS" 2>/dev/null ||
   grep -q -- '--dialect kimi' "$KIMI_CONFIG" 2>/dev/null ||
   grep -q 'hook-augment' "$GITLAB_HOOKS" 2>/dev/null ||
   grep -q -- '--dialect devin' "$DEVIN_CONFIG" 2>/dev/null ||
   grep -q 'hook-augment' "$FAKE_HOME/.qwen/settings.json" 2>/dev/null ||
   grep -q 'hook-augment' "$FAKE_HOME/.copilot/hooks/codebase-memory-mcp.json" 2>/dev/null ||
   grep -q 'hook-augment' "$FAKE_HOME/.factory/hooks.json" 2>/dev/null ||
   grep -q 'codebase-memory-session' "$AUGMENT_SETTINGS" 2>/dev/null ||
   [ -e "$AUGMENT_AGENT" ] || [ -e "$AUGMENT_SCRIPT" ]; then
  echo "FAIL 9l: lifecycle hook entry remains"
  exit 1
fi
for CLINE_EVENT in TaskStart TaskResume UserPromptSubmit PreCompact; do
  if [[ "$BINARY" == *.exe ]]; then
    CLINE_HOOK="$FAKE_HOME/.cline/hooks/$CLINE_EVENT.ps1"
  else
    CLINE_HOOK="$FAKE_HOME/.cline/hooks/$CLINE_EVENT"
  fi
  if [ -e "$CLINE_HOOK" ]; then
    echo "FAIL 9l: owned Cline $CLINE_EVENT hook remains"
    exit 1
  fi
done
if [ "$(json_get "$ROO_CFG" "d.get('keep', '')")" != "roo" ]; then
  echo "FAIL 9l: explicit Roo config lost its user key"
  exit 1
fi
if CRUSH_CONTEXT=$(json_get "$FAKE_HOME/.config/crush/crush.json" "str(any(str(p).endswith('codebase-memory.md') for p in d.get('options', {}).get('context_paths', [])))") &&
   [ "$CRUSH_CONTEXT" = "True" ]; then
  echo "FAIL 9l: Crush context path remains"
  exit 1
fi
if json_instructions_contain_path "$KILO_CFG" "$KILO_RULE"; then
  echo "FAIL 9l: Kilo instruction reference remains"
  exit 1
fi
if [ "$(json_get "$CLINE_SETTINGS" "d.get('hooksEnabled')")" != "False" ] ||
   [ "$(json_get "$CLINE_SETTINGS" "d.get('keep', '')")" != "cline" ] ||
   [ "$(json_get "$GITLAB_MCP" "d.get('keep', '')")" != "gitlab-mcp" ] ||
   [ "$(json_get "$DEVIN_CONFIG" "d.get('theme_mode', '')")" != "dark" ] ||
   [ "$(json_get "$CODEBUDDY_MCP" "d.get('keep', '')")" != "codebuddy" ] ||
   [ "$(json_get "$BOB_IDE_MCP" "d.get('keep', '')")" != "bob-ide" ] ||
   [ "$(json_get "$BOB_SHELL_MCP" "d.get('keep', '')")" != "bob-shell" ] ||
   ! grep -q '^theme = "dark"$' "$KIMI_CONFIG" 2>/dev/null ||
   grep -qF '[[hooks]]' "$KIMI_CONFIG" 2>/dev/null ||
   ! grep -q 'Personal Pochi setting' "$POCHI_MCP" 2>/dev/null ||
   ! grep -q '"keep": "pochi"' "$POCHI_MCP" 2>/dev/null; then
  echo "FAIL 9l: uninstall changed foreign agent settings"
  exit 1
fi
if ! cat "$GITLAB_HOOKS" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
entries = d.get('hooks', {}).get('SessionStart', [])
hooks = [h for entry in entries for h in entry.get('hooks', [])]
ok = (d.get('keep') == 'gitlab-hooks' and
      any(h.get('command') == '/usr/bin/user-hook' for h in hooks) and
      all('hook-augment' not in str(h.get('command', '')) for h in hooks))
sys.exit(0 if ok else 1)
" 2>/dev/null; then
  echo "FAIL 9l: GitLab uninstall lost or changed the user's SessionStart hook"
  exit 1
fi
echo "OK 9l: JSON agents, lifecycle hooks, and Kilo cleaned; foreign settings preserved"

# 9m: YAML/TOML new agents are cleaned.
if grep -q '^  codebase-memory-mcp:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   grep -q '^  pre_llm_call:' "$FAKE_HOME/.hermes/config.yaml" 2>/dev/null ||
   grep -q '^  codebase-memory-mcp:' "$GOOSE_CFG" 2>/dev/null ||
   grep -q '^name = "codebase-memory-mcp"' "$FAKE_HOME/.vibe/config.toml" 2>/dev/null; then
  echo "FAIL 9m: YAML/TOML MCP entry remains"
  exit 1
fi
if grep -Fq 'CONVENTIONS.md' "$FAKE_HOME/.aider.conf.yml" 2>/dev/null; then
  echo "FAIL 9m: Aider still loads removed conventions"
  exit 1
fi
echo "OK 9m: Hermes, Goose, Vibe, and Aider cleaned"

# 9m-i: Durable managed blocks are removed without deleting user files.
for CONTEXT_FILE in \
  "$ZED_INSTR" \
  "$FAKE_HOME/.kiro/steering/codebase-memory.md" \
  "$FAKE_HOME/.factory/AGENTS.md" \
  "$AUGMENT_RULE" \
  "$FAKE_HOME/.config/crush/codebase-memory.md" \
  "$FAKE_HOME/.config/goose/.goosehints" \
  "$KILO_RULE" \
  "$CLINE_RULE" \
  "$FAKE_HOME/.vibe/AGENTS.md" \
  "$FAKE_HOME/.codeium/windsurf/memories/global_rules.md" \
  "$DEVIN_INSTRUCTIONS" \
  "$CODEBUDDY_INSTRUCTIONS" \
  "$BOB_RULE" \
  "$POCHI_INSTRUCTIONS" \
  "$ROVO_INSTRUCTIONS" \
  "$CUSTOM_KIMI_HOME/AGENTS.md" \
  "$PI_INSTRUCTIONS"; do
  if grep -q 'codebase-memory-mcp:start' "$CONTEXT_FILE" 2>/dev/null; then
    echo "FAIL 9m-i: managed instructions remain in $CONTEXT_FILE"
    exit 1
  fi
done
if ! grep -q '^# Personal Kimi guidance$' "$CUSTOM_KIMI_HOME/AGENTS.md" 2>/dev/null; then
  echo "FAIL 9m-i: uninstall lost custom Kimi instructions"
  exit 1
fi
if ! grep -q '^# Personal Devin guidance$' "$DEVIN_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q '^# Personal CodeBuddy guidance$' "$CODEBUDDY_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q '^# Personal Bob guidance$' "$BOB_RULE" 2>/dev/null ||
   ! grep -q '^# Personal Pochi guidance$' "$POCHI_INSTRUCTIONS" 2>/dev/null ||
   ! grep -q '^# Personal Rovo guidance$' "$ROVO_INSTRUCTIONS" 2>/dev/null; then
  echo "FAIL 9m-i: uninstall lost foreign durable instructions"
  exit 1
fi
echo "OK 9m-i: durable instruction blocks removed"

# 9n: Skills removed (consolidated skill dir)
if [ -d "$FAKE_HOME/.claude/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.hermes/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.agents/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.qoder/skills/codebase-memory" ] ||
   [ -d "$CUSTOM_KIMI_HOME/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.pi/agent/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.junie/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.rovodev/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.copilot/skills/codebase-memory" ] ||
   [ -d "$FAKE_HOME/.vibe/skills/codebase-memory" ] ||
   [ -e "$DEVIN_SKILL" ] ||
   [ -e "$CODEBUDDY_SKILL" ] ||
   [ -e "$BOB_SKILL" ] ||
   [ -e "$POCHI_SKILL" ] ||
   [ -e "$QODER_AGENT" ] ||
   [ -e "$JUNIE_AGENT" ] ||
   [ -e "$ROVO_AGENT" ] ||
   [ -e "$KIRO_AGENT" ] ||
   [ -e "$CLAUDE_AGENT" ] ||
   [ -e "$FACTORY_AGENT" ] ||
   [ -e "$COPILOT_AGENT" ] ||
   [ -e "$KILO_AGENT" ] ||
   [ -e "$VIBE_AGENT" ] ||
   [ -e "$VIBE_PROMPT" ] ||
   [ -e "$CODEBUDDY_AGENT" ] ||
   [ -e "$POCHI_AGENT" ] ||
   [ -e "$BOB_AGENT" ]; then
  echo "FAIL 9n: skills or owned agents not removed"
  exit 1
fi
echo "OK 9n: skills removed"

# 9n-i: Uninstall removes every owned tier sibling, not only the historical
# Verify filename checked by the legacy variables above.
assert_tier_profile_set_removed "Claude" "$FAKE_HOME/.claude/agents" ".md"
assert_tier_profile_set_removed "Codex" "$FAKE_HOME/.codex/agents" ".toml"
assert_tier_profile_set_removed "Gemini" "$FAKE_HOME/.gemini/agents" ".md"
assert_tier_profile_set_removed "OpenCode" "$FAKE_HOME/.config/opencode/agents" ".md"
assert_tier_profile_set_removed "Kilo" "$FAKE_HOME/.config/kilo/agents" ".md"
assert_tier_profile_set_removed "Cursor" "$FAKE_HOME/.cursor/agents" ".md"
assert_tier_profile_set_removed "Kiro" "$FAKE_HOME/.kiro/agents" ".json"
assert_tier_profile_set_removed "Junie" "$FAKE_HOME/.junie/agents" ".md"
assert_tier_profile_set_removed "Augment" "$FAKE_HOME/.augment/agents" ".md"
assert_tier_profile_set_removed "Qwen" "$FAKE_HOME/.qwen/agents" ".md"
assert_tier_profile_set_removed "Factory" "$FAKE_HOME/.factory/droids" ".md"
assert_tier_profile_set_removed "Vibe" "$FAKE_HOME/.vibe/agents" ".toml"
assert_tier_profile_set_removed "Vibe prompt" "$FAKE_HOME/.vibe/prompts" ".md"
assert_tier_profile_set_removed "Copilot" "$FAKE_HOME/.copilot/agents" ".agent.md"
assert_tier_profile_set_removed "Qoder" "$FAKE_HOME/.qoder/agents" ".md"
assert_tier_profile_set_removed "CodeBuddy" "$FAKE_HOME/.codebuddy/agents" ".md"
assert_tier_profile_set_removed "Pochi" "$FAKE_HOME/.pochi/agents" ".md"
assert_tier_profile_set_removed "Rovo" "$FAKE_HOME/.rovodev/subagents" ".md"
echo "OK 9n-i: all owned Scout/Verify/Auditor profile sets removed"

echo ""
echo "--- Phase 9b: adversarial install/uninstall tests ---"

# 9b-1: Install with minimal agents (empty HOME, no agent dirs)
# Note: cbm_find_cli searches hardcoded paths (/usr/local/bin, /opt/homebrew/bin)
# so PATH-based agents like aider may still be detected. We verify the install
# completes without crash and prints "Detected agents:" line.
EMPTY_HOME=$(smoke_mktemp_dir)
mkdir -p "$EMPTY_HOME/.local/bin"
INSTALL_RC=0
INSTALL_OUT=$(HOME="$EMPTY_HOME" LOCALAPPDATA="$EMPTY_HOME/AppData/Local" "$BINARY" install -y 2>&1) || INSTALL_RC=$?
if [ "$INSTALL_RC" -ge 128 ]; then
  echo "FAIL 9b-1: install crashed (rc=$INSTALL_RC)"
  exit 1
fi
if ! echo "$INSTALL_OUT" | grep -qi 'detected agents'; then
  echo "FAIL 9b-1: install output missing 'Detected agents' line"
  exit 1
fi
echo "OK 9b-1: install with minimal agents exits cleanly"
retire_account_daemon "9b-1-cleanup"
rm -rf "$EMPTY_HOME"

# 9b-2: Install twice (idempotent)
IDEM_HOME=$(smoke_mktemp_dir)
mkdir -p "$IDEM_HOME/.claude" "$IDEM_HOME/.local/bin"
copy_smoke_binary "$IDEM_HOME/.local/bin/codebase-memory-mcp"
run_no_crash 9b-2 env HOME="$IDEM_HOME" LOCALAPPDATA="$IDEM_HOME/AppData/Local" "$BINARY" install -y
IDEM_INSTALLER="$BINARY"
if [[ "$BINARY" == *.exe ]]; then
  IDEM_INSTALLER="$IDEM_HOME/.local/bin/codebase-memory-mcp.exe"
fi
run_no_crash 9b-2-second env HOME="$IDEM_HOME" LOCALAPPDATA="$IDEM_HOME/AppData/Local" "$IDEM_INSTALLER" install -y
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
retire_account_daemon "9b-2-cleanup"
rm -rf "$IDEM_HOME"

# 9b-3: Uninstall without prior install
CLEAN_HOME=$(smoke_mktemp_dir)
mkdir -p "$CLEAN_HOME/.claude" "$CLEAN_HOME/.local/bin"
UNINSTALL_RC=0
UNINSTALL_OUT=$(HOME="$CLEAN_HOME" "$BINARY" uninstall -y -n 2>&1) || UNINSTALL_RC=$?
if [ "$UNINSTALL_RC" -ge 128 ]; then
  echo "FAIL 9b-3: uninstall crashed (rc=$UNINSTALL_RC)"
  exit 1
fi
echo "OK 9b-3: uninstall without install doesn't crash"
retire_account_daemon "9b-3-cleanup"
rm -rf "$CLEAN_HOME"

# 9b-4: Install over corrupt JSON
CORRUPT_HOME=$(smoke_mktemp_dir)
mkdir -p "$CORRUPT_HOME/.claude" "$CORRUPT_HOME/.local/bin"
copy_smoke_binary "$CORRUPT_HOME/.local/bin/codebase-memory-mcp"
echo '{invalid json here' > "$CORRUPT_HOME/.claude.json"
run_no_crash 9b-4 env HOME="$CORRUPT_HOME" "$BINARY" install -y
# Should either fix it or handle gracefully — not crash
echo "OK 9b-4: install over corrupt JSON doesn't crash"
retire_account_daemon "9b-4-cleanup"
rm -rf "$CORRUPT_HOME"

# 9b-8: Double uninstall
DBL_HOME=$(smoke_mktemp_dir)
mkdir -p "$DBL_HOME/.claude" "$DBL_HOME/.local/bin"
copy_smoke_binary "$DBL_HOME/.local/bin/codebase-memory-mcp"
run_no_crash 9b-8-install env HOME="$DBL_HOME" "$BINARY" install -y
DBL_UNINSTALLER="$BINARY"
if [[ "$BINARY" == *.exe ]]; then
  DBL_UNINSTALLER="$DBL_HOME/.local/bin/codebase-memory-mcp.exe"
fi
run_no_crash 9b-8-first env HOME="$DBL_HOME" "$DBL_UNINSTALLER" uninstall -y -n
run_no_crash 9b-8-second env HOME="$DBL_HOME" "$BINARY" uninstall -y -n
echo "OK 9b-8: double uninstall doesn't crash"
retire_account_daemon "9b-8-cleanup"
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

retire_account_daemon "9-cleanup"
rm -rf "$FAKE_HOME" "$EMPTY_HOME"

if [ "$SMOKE_MODE" = "--agent-config-only" ]; then
  echo ""
  echo "OK: agent config install/uninstall smoke test passed"
  exit 0
fi

echo ""
echo "=== Phase 10: binary security E2E ==="

SECURITY_DIR=$(smoke_mktemp_dir)
SECURITY_BIN="$SECURITY_DIR/codebase-memory-mcp"
copy_smoke_binary "$SECURITY_BIN"
chmod 755 "$SECURITY_BIN"

if [ "$(uname -s)" = "Darwin" ]; then
  # macOS signing tests
  if codesign -v "$SECURITY_BIN" 2>/dev/null; then
    echo "OK 10a: binary has valid signature"
  else
    echo "FAIL 10a: binary has no valid signature (linker should auto-sign arm64)"
    exit 1
  fi

  # Detect binary architecture (not shell arch — Rosetta reports x86_64 for arm64 binaries)
  BIN_ARCH=$(file "$SECURITY_BIN" | grep -o 'arm64\|x86_64' | head -1)

  if [ "$BIN_ARCH" = "arm64" ]; then
    # arm64 integrity check: tampering the signed code must be DETECTED by the code signature.
    # The original check (run a tampered/unsigned binary and expect SIGKILL 137) is NOT deterministic
    # on current macOS for an ad-hoc-signed CLI binary, which is why this test went flaky then red.
    # Observed on CI:
    #   - `codesign --remove-signature` then run -> macOS 11+ ad-hoc re-signs on exec and RUNS (exit 0)
    #   - garbling only the signature blob then run -> same, re-signed/ignored (exit 0)
    #   - tampering the code then run -> with no CS_KILL/hardened-runtime flag the kernel does NOT
    #     kill the page; it executes the garbage and crashes with SIGILL (exit 132), not 137
    # (runs 28350650225 / 28354735368 / 28360363173 / 28365724001). The deterministic, meaningful
    # invariant is that `codesign --verify` REJECTS a tampered binary -- the CodeDirectory page hashes
    # no longer match the modified code -- while the untampered binary verifies cleanly (10a above).
    # This is a pure userspace hash check (no tampered code is ever executed). Done on a SEPARATE copy
    # so the original $SECURITY_BIN stays intact for the 10e re-sign test.
    # Refs: github.com/garrytan/gstack#997, github.com/nodejs/node#40827.
    TAMPER_BIN="${SECURITY_BIN}.tampered"
    cp "$SECURITY_BIN" "$TAMPER_BIN"
    # Tamper signed code: zero the entry-point instructions (LC_MAIN entryoff = start of __text) plus
    # a span of early __text, leaving the Mach-O header + load commands + signature blob intact so the
    # file still parses and still carries its now-stale signature for --verify to reject.
    ENTRY_OFF=$(otool -l "$SECURITY_BIN" 2>/dev/null | awk '/LC_MAIN/{f=1} f&&/entryoff/{print $2; exit}')
    ENTRY_OFF=${ENTRY_OFF:-2184}
    dd if=/dev/zero of="$TAMPER_BIN" bs=1 seek="$ENTRY_OFF" count=4096 conv=notrunc 2>/dev/null
    dd if=/dev/zero of="$TAMPER_BIN" bs=4096 seek=4 count=512 conv=notrunc 2>/dev/null
    if codesign --verify "$TAMPER_BIN" 2>/dev/null; then
      echo "FAIL 10c: codesign --verify ACCEPTED a tampered arm64 binary (integrity check broken)"
      rm -f "$TAMPER_BIN"
      exit 1
    else
      echo "OK 10c: codesign --verify rejected tampered arm64 binary"
    fi
    rm -f "$TAMPER_BIN"
  else
    # x86_64: signing is not enforced; an unsigned binary should still run
    codesign --remove-signature "$SECURITY_BIN" 2>/dev/null || true
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
MCP_KILL_INPUT=$(smoke_mktemp_file)
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"kill-test","version":"1.0"}}}' > "$MCP_KILL_INPUT"
KILL_OUT=$(smoke_mktemp_file)
"$BINARY" < "$MCP_KILL_INPUT" > "$KILL_OUT" 2>&1 &
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
  # The server ended before the probe: distinguish clean shutdown-on-EOF
  # from an instant crash — the old branch treated both as OK with the
  # output discarded, so a startup segfault read as a clean pass on any
  # fast machine. A clean run must have exited 0 AND produced the
  # initialize response.
  KILL_RC=0
  wait "$KILL_PID" 2>/dev/null || KILL_RC=$?
  if [ "$KILL_RC" -ge 128 ] || ! grep -q '"jsonrpc"' "$KILL_OUT"; then
    echo "FAIL 11: server exited before probe with rc=$KILL_RC and $(wc -c < "$KILL_OUT") bytes of output — crash, not clean EOF shutdown"
    exit 1
  fi
  echo "OK 11: MCP server already exited (clean shutdown on EOF, initialize answered)"
fi
rm -f "$KILL_OUT"

rm -f "$MCP_KILL_INPUT"

echo ""
echo "=== Phase 14: update + uninstall E2E ==="

if [ -n "${SMOKE_DOWNLOAD_URL:-}" ]; then
  # ── 14a-f: Real update command against the local release fixture ──
  # Curl/installer phases below keep using the loopback HTTP artifact server.
  # Native update intentionally accepts only HTTPS, plus an explicit file://
  # CBM_DOWNLOAD_URL test override, so point it directly at the same fixture.
  UPDATE_DOWNLOAD_URL="$SMOKE_DOWNLOAD_URL"
  if [ -n "${SMOKE_UPDATE_FIXTURE_DIR:-}" ]; then
    UPDATE_FIXTURE_DIR="$SMOKE_UPDATE_FIXTURE_DIR"
    if command -v cygpath &>/dev/null; then
      UPDATE_FIXTURE_DIR=$(cygpath -m "$UPDATE_FIXTURE_DIR")
      UPDATE_DOWNLOAD_URL="file:///$UPDATE_FIXTURE_DIR"
    elif [[ "$UPDATE_FIXTURE_DIR" == /* ]]; then
      UPDATE_DOWNLOAD_URL="file://$UPDATE_FIXTURE_DIR"
    else
      echo "FAIL 14a: SMOKE_UPDATE_FIXTURE_DIR must be absolute"
      exit 1
    fi
  fi
  UPDATE_HOME=$(smoke_mktemp_dir)
  mkdir -p "$UPDATE_HOME/.claude" "$UPDATE_HOME/.local/bin"
  if [[ "$BINARY" == *.exe ]]; then
    # Keep the managed canonical absent until WINDOWS_PAYLOAD installs the
    # authenticated two-link launcher layout below.
    :
  else
    cp "$BINARY" "$UPDATE_HOME/.local/bin/codebase-memory-mcp"
    chmod 755 "$UPDATE_HOME/.local/bin/codebase-memory-mcp"
    if [ "$(uname -s)" = "Darwin" ]; then
      codesign --sign - --force "$UPDATE_HOME/.local/bin/codebase-memory-mcp" 2>/dev/null || true
    fi
  fi

  # A portable Windows payload may install a managed launcher, but it must not
  # perform update/uninstall directly. Establish the managed layout first and
  # exercise those mutations through its canonical launcher.
  UPDATE_DRIVER="$BINARY"
  if [[ "$BINARY" == *.exe ]]; then
    HOME="$UPDATE_HOME" "$WINDOWS_PAYLOAD" install -y --force --skip-config \
      "--dir=$UPDATE_HOME/.local/bin"
    UPDATE_DRIVER="$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"
    if [ ! -f "$UPDATE_DRIVER" ]; then
      echo "FAIL 14a: managed Windows launcher missing after install"
      exit 1
    fi
  fi

  # Pre-install agent config with a WRONG binary path (simulates stale config)
  echo '{"mcpServers":{"codebase-memory-mcp":{"command":"/old/stale/path"}}}' > "$UPDATE_HOME/.claude.json"

  # 14a: Run actual update command (detect variant from available archive)
  UPDATE_VARIANT="--standard"
  if curl -sf "$SMOKE_DOWNLOAD_URL/" 2>/dev/null | grep -q "ui-"; then
    UPDATE_VARIANT="--ui"
  fi
  HOME="$UPDATE_HOME" CBM_DOWNLOAD_URL="$UPDATE_DOWNLOAD_URL" \
    "$UPDATE_DRIVER" update $UPDATE_VARIANT -y 2>&1

  # 14b: Verify new binary exists and runs
  if [[ "$BINARY" == *.exe ]]; then
    UPD_BIN="$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"
  else
    UPD_BIN="$UPDATE_HOME/.local/bin/codebase-memory-mcp"
  fi
  if [ ! -f "$UPD_BIN" ]; then
    echo "FAIL 14b: binary missing after update"
    exit 1
  fi
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
  if [ ! -f "$UPD_BIN" ]; then
    echo "FAIL 14d: binary should exist before uninstall"
    exit 1
  fi

  # Run actual uninstall
  HOME="$UPDATE_HOME" "$UPD_BIN" uninstall -y 2>&1

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
  UPDATE_DIR=$(smoke_mktemp_dir)
  mkdir -p "$UPDATE_DIR/install"
  copy_smoke_binary "$UPDATE_DIR/install/codebase-memory-mcp"
  chmod 755 "$UPDATE_DIR/install/codebase-memory-mcp"
  copy_smoke_binary "$UPDATE_DIR/smoke-downloaded"
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

DL_DIR=$(smoke_mktemp_dir)

# Detect platform for archive name
DL_OS=$(uname -s | tr 'A-Z' 'a-z')
# Normalize MSYS2/MinGW to "windows"
case "$DL_OS" in
  mingw*|msys*) DL_OS="windows" ;;
esac
# Prefer a CI-provided SMOKE_ARCH over `uname -m`: on windows-11-arm the base
# MSYS2 `uname` is an emulated x86_64 binary that reports "x86_64", so uname would
# request the amd64 archive and 404. The smoke workflow passes the true matrix
# arch (arm64/amd64). Fall back to uname when SMOKE_ARCH is unset (local runs).
if [ -n "${SMOKE_ARCH:-}" ]; then
  DL_ARCH="$SMOKE_ARCH"
else
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
fi

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
# --noproxy '*': never route the local test server through a proxy — a proxy env
# var present on some runners (notably windows-11-arm) made curl fail to reach
# 127.0.0.1 while the app's own downloader (WinHTTP) bypassed it. On failure,
# surface curl's stderr instead of swallowing it so the reason is visible.
if ! curl -fSL --noproxy '*' -o "$DL_DIR/$DL_ARCHIVE" "$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE" 2>/tmp/cbm-curl12a.err; then
  # Try UI variant
  if curl -fSL --noproxy '*' -o "$DL_DIR/$DL_ARCHIVE_UI" "$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE_UI" 2>>/tmp/cbm-curl12a.err; then
    DL_ARCHIVE="$DL_ARCHIVE_UI"
  else
    echo "FAIL 12a: curl download failed (tried standard and ui variants)"
    echo "--- curl stderr (url: $SMOKE_DOWNLOAD_URL/$DL_ARCHIVE) ---"
    cat /tmp/cbm-curl12a.err 2>/dev/null || true
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
if [ "$DL_OS" = "windows" ]; then
  DL_BIN="$DL_DIR/codebase-memory-mcp.exe"
  DL_PAYLOAD="$DL_DIR/codebase-memory-mcp.payload.exe"
  if [ ! -f "$DL_PAYLOAD" ]; then
    echo "FAIL 12d: Windows payload not found after extraction"
    exit 1
  fi
else
  DL_BIN="$DL_DIR/codebase-memory-mcp"
fi
if [ ! -f "$DL_BIN" ]; then
  echo "FAIL 12d: binary not found after extraction"
  exit 1
fi
chmod +x "$DL_BIN"
echo "OK 12d: binary extracted"

if [ "$DL_OS" = "windows" ] && ! "$DL_PAYLOAD" --version > /dev/null 2>&1; then
  echo "FAIL 12d: extracted Windows payload doesn't run"
  exit 1
fi

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
  INSTALL_TEST_HOME=$(smoke_mktemp_dir)
  INSTALL_TEST_DIR=$(smoke_mktemp_dir)
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
  PS1_TEST_HOME=$(smoke_mktemp_dir)
  PS1_TEST_DIR=$(smoke_mktemp_dir)
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
  # Pass the known-correct arch: powershell runs under x64 emulation on ARM64, so
  # install.ps1's own detection can't tell it's arm64. DL_ARCH is authoritative here.
  HOME="$PS1_TEST_HOME" CBM_DOWNLOAD_URL="$WIN_URL" CBM_ARCH="$DL_ARCH" \
    powershell.exe -NoProfile -ExecutionPolicy ByPass -Command \
      '$env:TEMP=$args[0]; $env:TMP=$args[0]; & $args[1] $args[2]' \
      "$WIN_HOME" "$WIN_SCRIPT" "--dir=$WIN_DIR" 2>&1 || true

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

# A kernel-assigned free port instead of a fixed one: a squatter on a fixed
# port made the whole phase read as SKIP. The tiny TOCTOU window between
# probe and bind is the residual risk, not the common case.
UI_PORT=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
UI_INPUT=$(smoke_mktemp_file)
"$BINARY" --port "$UI_PORT" < "$UI_INPUT" > /dev/null 2>&1 &
UI_PID=$!
# Readiness poll instead of a fixed sleep: SKIP is legitimate ONLY when the
# process exited (the documented no-embedded-assets case); a slow start on a
# loaded runner must not masquerade as it.
UI_READY=0
for _ in $(seq 1 100); do
  if ! kill -0 "$UI_PID" 2>/dev/null; then break; fi
  if curl -sf "http://127.0.0.1:$UI_PORT/" -o /dev/null 2>/dev/null; then UI_READY=1; break; fi
  sleep 0.1
done

if [ "$UI_READY" -eq 1 ] || kill -0 "$UI_PID" 2>/dev/null; then
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
    kill "$UI_PID" 2>/dev/null || true
    exit 1
  fi

  kill "$UI_PID" 2>/dev/null || true
  wait "$UI_PID" 2>/dev/null || true
else
  echo "SKIP Phase 15: binary exited immediately (no UI assets embedded)"
fi
rm -f "$UI_INPUT"

echo ""
echo "=== Phase 16: stdio server leaves no orphan after shutdown ==="
# Regression guard for the orphaned-server failure mode behind #406: a stdio MCP
# server must TERMINATE (not linger as a background process) once its stdin is
# closed. The shutdown trigger is a closed stdin (`< /dev/null`): the server sees
# an immediate, regular EOF on its read loop and exits.
#
# Why not a FIFO writer-close (the previous mechanism)? Closing a FIFO's last
# writer surfaces as POLLHUP rather than a clean POLLIN+EOF; the server's
# poll()-based read loop did not treat that as shutdown, so the FIFO probe left
# the process alive and Phase 16 failed in CI on every platform. A plain
# `< /dev/null` EOF is the simplest reliable trigger and is fully portable
# (POSIX shells and MSYS2 bash alike), so no OS gate is needed here.
"$BINARY" < /dev/null > /dev/null 2>&1 &
SHUT_SRV_PID=$!
SHUT_GONE=0
for _ in $(seq 1 400); do           # bounded ~40s wait (400 × 0.1s)
  if ! kill -0 "$SHUT_SRV_PID" 2>/dev/null; then SHUT_GONE=1; break; fi
  sleep 0.1
done
if [ "$SHUT_GONE" -ne 1 ]; then
  echo "FAIL 16: stdio server still running after stdin closed (orphan process)"
  kill -9 "$SHUT_SRV_PID" 2>/dev/null || true
  wait "$SHUT_SRV_PID" 2>/dev/null || true
  exit 1
fi
wait "$SHUT_SRV_PID" 2>/dev/null || true
echo "OK 16: stdio server terminated after stdin closed, no orphan"

# The account daemon legitimately outlives its last client for a moment while
# it drains, holding its log and config DB open. POSIX rm doesn't care, but on
# Windows an open file blocks deletion — so a caller's cleanup rm of the smoke
# cache races the daemon's asynchronous shutdown and fails with "Device or
# resource busy". Retire the daemon deterministically through its own
# lifecycle command and wait (bounded) until it reports not-running, then give
# Windows one beat for the final handle close.
retire_account_daemon 17
echo "OK 17: account daemon retired; smoke cache is deletable"

echo ""
echo "=== smoke-test: ALL PASSED ==="
