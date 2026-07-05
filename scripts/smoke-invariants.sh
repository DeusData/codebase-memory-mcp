#!/usr/bin/env bash
# smoke-invariants.sh — "the shipped PROD binary does not fail" invariant battery.
#
# A comprehensive, fast, portable smoke battery for the codebase-memory-mcp
# binary. Every invariant prints `PASS: <name>` or `FAIL: <name>: <reason>` and
# accumulates failures. Exit 0 iff ALL invariants pass, 1 if ANY fails.
#
# The binary is BOTH:
#   - a single-tool CLI:  <binary> cli [--json] <tool_name> [json_args]
#   - an MCP stdio server (JSON-RPC 2.0, newline-delimited) on stdin/stdout
#   - plus subcommands: --version --help install/uninstall/update/config
#
# Designed to run IDENTICALLY on Linux / macOS / Windows(msys2 CLANG64).
#
# Usage:
#   scripts/smoke-invariants.sh <binary>        # e.g. build/c/codebase-memory-mcp(.exe)
#
# Portability notes:
#   * set -u (NOT -e): we want every invariant to run even if one fails.
#   * NO `sleep` loops anywhere. All waits are bounded via `read -t` (a bash
#     builtin timeout) against fifos / the server's stdout fd. On msys2 the
#     `coreutils` + `mingw-w64-clang-x86_64-python3` packages (already installed
#     by _smoke.yml) provide everything used here.
#   * MSYS2/Windows: POSIX temp paths are converted to native form with
#     `cygpath -m` before being handed to the binary (mirrors smoke-test.sh).

set -u

# ── Args / setup ──────────────────────────────────────────────────────────
BINARY="${1:-}"
if [ -z "$BINARY" ]; then
    echo "usage: smoke-invariants.sh <binary>" >&2
    exit 2
fi
if [ ! -x "$BINARY" ]; then
    # On some filesystems the +x bit may be missing; tolerate if it is a file.
    if [ ! -f "$BINARY" ]; then
        echo "FAIL: setup: binary not found at '$BINARY'" >&2
        exit 2
    fi
fi
# Absolutise the binary so cwd changes never break invocation.
BINARY="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"

FAILURES=0
PASSES=0

pass() {
    PASSES=$((PASSES + 1))
    echo "PASS: $1"
}
fail() {
    FAILURES=$((FAILURES + 1))
    echo "FAIL: $1: ${2:-}"
}

# Convert a POSIX path to native form for the binary (no-op off msys2).
native_path() {
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -m "$1"
    else
        printf '%s' "$1"
    fi
}

# Per-run scratch root; everything created lives under here for clean teardown.
SCRATCH="$(mktemp -d 2>/dev/null || mktemp -d -t cbmsmoke)"
cleanup() {
    # Best-effort: kill any lingering server, close fds, remove scratch.
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    exec 3>&- 2>/dev/null || true
    exec 4<&- 2>/dev/null || true
    [ -n "${SCRATCH:-}" ] && rm -rf "$SCRATCH" 2>/dev/null || true
}
trap cleanup EXIT

# ── Bounded command runner ────────────────────────────────────────────────
# Run a command with a wall-clock bound WITHOUT `sleep` loops. Prefers the
# `timeout`/`gtimeout` binaries (coreutils, present on Linux + msys2; on macOS
# via `gtimeout`). Falls back to a background-process + bounded `read -t` on a
# fifo that signals completion, so it still works if `timeout` is absent.
#
# Usage: run_bounded <seconds> <cmd...>   → sets RB_OUT / RB_RC
RB_OUT=""
RB_RC=0
run_bounded() {
    local secs="$1"; shift
    local tobin=""
    if command -v timeout >/dev/null 2>&1; then
        tobin="timeout"
    elif command -v gtimeout >/dev/null 2>&1; then
        tobin="gtimeout"
    fi
    local of; of="$SCRATCH/rb_out.$$"
    if [ -n "$tobin" ]; then
        # -s KILL: force-kill at the deadline. The binary catches SIGTERM for a
        # graceful shutdown, so a busy-spin (e.g. a stuck external scanner) would
        # survive plain `timeout`'s TERM and hang the runner forever. timeout still
        # exits 124 on the deadline regardless of the signal used.
        "$tobin" -s KILL "$secs" "$@" >"$of" 2>&1
        RB_RC=$?
    else
        # Fallback (no timeout/gtimeout): background the command, bound the wait via
        # a done-fifo. Open the fifo read-WRITE (exec 9<>) so the open never blocks
        # waiting for a writer — a truly-hanging command never opens the write end,
        # and a blocking open would defeat `read -t`. On the deadline, force-kill
        # the whole command subtree with SIGKILL (children first so no orphan spins
        # on): SIGTERM is caught by the binary and cannot stop a busy-spin.
        local done; done="$SCRATCH/rb_done.$$"
        rm -f "$done"; mkfifo "$done" 2>/dev/null || done=""
        ( "$@" >"$of" 2>&1; echo $? > "$SCRATCH/rb_rc.$$"; [ -n "$done" ] && echo done > "$done" ) &
        local bgpid=$!
        if [ -n "$done" ]; then
            exec 9<>"$done"
            local sig=""
            read -t "$secs" sig <&9
            exec 9<&-
            if [ -z "$sig" ]; then
                pkill -9 -P "$bgpid" 2>/dev/null || true
                kill -9 "$bgpid" 2>/dev/null || true
                RB_RC=124            # mimic timeout's exit code
            else
                RB_RC="$(cat "$SCRATCH/rb_rc.$$" 2>/dev/null || echo 1)"
            fi
            rm -f "$done"
        else
            wait "$bgpid"; RB_RC=$?
        fi
        rm -f "$SCRATCH/rb_rc.$$" 2>/dev/null || true
    fi
    RB_OUT="$(cat "$of" 2>/dev/null)"
    rm -f "$of" 2>/dev/null || true
    return 0
}

# A CLI wrapper: run a single tool call, bounded. Sets CLI_OUT / CLI_RC.
CLI_OUT=""
CLI_RC=0
cli_call() {
    # cli_call <seconds> <tool> [json_args] [--json]
    local secs="$1"; shift
    run_bounded "$secs" "$BINARY" cli "$@"
    CLI_OUT="$RB_OUT"
    CLI_RC="$RB_RC"
}

# ── JSON helpers (python3 — guaranteed present on every smoke runner) ──────
PY="python3"
command -v "$PY" >/dev/null 2>&1 || PY="python"

# Is the argument valid JSON? (reads from stdin)
is_json() {
    "$PY" -c 'import sys,json;
try:
    json.load(sys.stdin); sys.exit(0)
except Exception:
    sys.exit(1)' 2>/dev/null
}

# Extract a top-level field from a JSON-RPC response (reads stdin). Prints the
# repr-ish value or nothing. Used to assert presence of result/error.
jq_has() {
    # jq_has <key>  → exit 0 if top-level key present
    "$PY" -c '
import sys,json
key=sys.argv[1]
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(2)
sys.exit(0 if isinstance(d,dict) and key in d else 1)' "$1" 2>/dev/null
}

# ══════════════════════════════════════════════════════════════════════════
#  CLI-MODE INVARIANTS (process-per-call; no server lifecycle)
# ══════════════════════════════════════════════════════════════════════════

# ── Invariant 1: --version exits 0 and prints a version-looking string ─────
inv_version() {
    run_bounded 30 "$BINARY" --version
    if [ "$RB_RC" -ne 0 ]; then
        fail "version" "--version exited $RB_RC (want 0); out=[$RB_OUT]"
        return
    fi
    if printf '%s' "$RB_OUT" | grep -qE 'v?[0-9]+\.[0-9]+|dev'; then
        pass "version (out=$(printf '%s' "$RB_OUT" | tr '\n' ' '))"
    else
        fail "version" "no version-looking string in [$RB_OUT]"
    fi
}

# ── Invariant 2: --help exits 0 / non-crash and prints usage ───────────────
inv_help() {
    run_bounded 30 "$BINARY" --help
    if [ "$RB_RC" -ne 0 ]; then
        fail "help" "--help exited $RB_RC (want 0)"
        return
    fi
    if printf '%s' "$RB_OUT" | grep -qiE 'usage|codebase-memory-mcp'; then
        pass "help"
    else
        fail "help" "no usage text in --help output"
    fi
    # No-args also must not crash: it starts the server, so we only check that
    # an immediate EOF on stdin gives a clean (non-signal) exit. Bound it.
    run_bounded 15 sh -c "printf '' | '$BINARY' >/dev/null 2>&1"
    # rc 124 = our bound fired (a hang) → that is a real FAIL; >128 = killed by signal.
    if [ "$RB_RC" -eq 124 ]; then
        fail "no-args-eof" "server with empty stdin did not exit within bound (hang)"
    elif [ "$RB_RC" -gt 128 ]; then
        fail "no-args-eof" "server crashed on empty-stdin start (signal $((RB_RC-128)))"
    else
        pass "no-args-eof (clean start+exit on empty stdin, rc=$RB_RC)"
    fi
}

# ── Invariant 10: install --dry-run / --help does not error, no mutation ───
# install supports [-y|-n] [--force] [--dry-run]; -n declines, --dry-run plans
# only. We use --dry-run together with -n to be doubly safe about not touching
# the real user config. (cli.c: g_install_plan path performs no writes.)
inv_install_dryrun() {
    run_bounded 30 "$BINARY" install --dry-run -n
    if [ "$RB_RC" -eq 124 ]; then
        fail "install-dry-run" "install --dry-run hung (no input)"
        return
    fi
    if [ "$RB_RC" -gt 128 ]; then
        fail "install-dry-run" "install --dry-run crashed (signal $((RB_RC-128)))"
        return
    fi
    # We do NOT require exit 0 (a dry-run may report rc!=0 on some states); we
    # require it to RUN without crashing/hanging. Most builds return 0.
    pass "install-dry-run (rc=$RB_RC)"
}

# ══════════════════════════════════════════════════════════════════════════
#  Tiny test repo (shared by index + per-tool invariants)
# ══════════════════════════════════════════════════════════════════════════
TEST_REPO=""
TEST_REPO_NATIVE=""
PROJ_NAME=""
make_test_repo() {
    TEST_REPO="$SCRATCH/repo"
    mkdir -p "$TEST_REPO/src/pkg"
    cat > "$TEST_REPO/src/main.py" <<'PYEOF'
from pkg import helper

def main():
    result = helper.compute(42)
    print(result)

class Config:
    DEBUG = True
PYEOF
    cat > "$TEST_REPO/src/pkg/__init__.py" <<'PYEOF'
from .helper import compute
PYEOF
    cat > "$TEST_REPO/src/pkg/helper.py" <<'PYEOF'
def compute(x):
    return x * 2

def validate(data):
    if not data:
        raise ValueError("empty")
    return True
PYEOF
    cat > "$TEST_REPO/src/server.go" <<'GOEOF'
package main

import "fmt"

func StartServer(port int) {
    fmt.Printf("listening on :%d\n", port)
}

func HandleRequest(path string) string {
    return "ok: " + path
}
GOEOF
    # Make it a git repo (the watcher/index path expects one; harmless if absent).
    git -C "$TEST_REPO" init -q 2>/dev/null || true
    git -C "$TEST_REPO" add -A 2>/dev/null || true
    git -C "$TEST_REPO" -c user.email=smoke@test -c user.name=smoke commit -q -m init 2>/dev/null || true

    TEST_REPO_NATIVE="$(native_path "$TEST_REPO")"
    # Project name derivation mirrors cbm_project_name_from_path: every char not
    # in [A-Za-z0-9._-] → '-', collapse repeats, trim leading/trailing '-'/'.'.
    PROJ_NAME="$("$PY" - "$TEST_REPO_NATIVE" <<'PYEOF'
import sys, re
p = sys.argv[1]
s = re.sub(r'[^A-Za-z0-9._-]', '-', p)
s = re.sub(r'-{2,}', '-', s)
s = re.sub(r'\.{2,}', '.', s)
s = s.strip('-').lstrip('.')
print(s)
PYEOF
)"
}

# ── Invariant 6: index a tiny repo via CLI → nodes>0 and exit 0 ────────────
inv_index_cli() {
    cli_call 90 --json index_repository "{\"repo_path\":\"$TEST_REPO_NATIVE\"}"
    if [ "$CLI_RC" -eq 124 ]; then
        fail "index-cli" "index_repository hung (>90s)"
        return
    fi
    if [ "$CLI_RC" -gt 128 ]; then
        fail "index-cli" "index_repository crashed (signal $((CLI_RC-128)))"
        return
    fi
    # The tool result wraps its payload as a JSON STRING, so the node count appears
    # escaped (\"nodes\":N) and the logs use nodes=N. Strip backslashes + quotes and
    # match either "nodes": / nodes= form; any nodes>0 satisfies "graph non-empty".
    local nodes
    nodes="$(printf '%s' "$CLI_OUT" | "$PY" -c '
import sys,re
t=sys.stdin.read().replace("\\","").replace("\"","")
m=re.findall(r"nodes\s*[:=]\s*(\d+)", t)
print(max((int(x) for x in m), default=0))' 2>/dev/null)"
    if [ "${nodes:-0}" -gt 0 ] 2>/dev/null; then
        pass "index-cli (nodes=$nodes, rc=$CLI_RC)"
    else
        fail "index-cli" "graph empty after index (nodes=${nodes:-0}); out=[$(printf '%s' "$CLI_OUT" | tr '\n' ' ' | cut -c1-300)]"
    fi
}

# ── Invariant: index_status reports a ready, non-empty project ─────────────
inv_index_status_cli() {
    cli_call 30 --json index_status "{\"project\":\"$PROJ_NAME\"}"
    if [ "$CLI_RC" -gt 128 ]; then
        fail "index-status" "crashed (signal $((CLI_RC-128)))"
        return
    fi
    # Result payload is a JSON string with escaped quotes (\"status\":\"ready\"); strip
    # backslashes so the unescaped greps match.
    local st_clean
    st_clean="$(printf '%s' "$CLI_OUT" | tr -d '\\')"
    if printf '%s' "$st_clean" | grep -q '"status":"ready"' && \
       printf '%s' "$st_clean" | grep -qE '"nodes":[1-9]'; then
        pass "index-status (ready, non-empty)"
    else
        fail "index-status" "not ready/non-empty; out=[$(printf '%s' "$CLI_OUT" | tr '\n' ' ' | cut -c1-200)]"
    fi
}

# ══════════════════════════════════════════════════════════════════════════
#  MCP STDIO SERVER LIFECYCLE
# ══════════════════════════════════════════════════════════════════════════
# Fifo-based bidirectional pipe, mirroring soak-test.sh: fd3=server stdin,
# fd4=server stdout. Started ONCE; reused for the handshake + tools/list +
# per-tool invariants. All response reads are bounded with `read -t`.

SERVER_IN=""
SERVER_OUT=""
SERVER_PID=""
MCP_ID=100
SERVER_STDERR=""

mcp_start() {
    SERVER_IN="$SCRATCH/srv.in"
    SERVER_OUT="$SCRATCH/srv.out"
    SERVER_STDERR="$SCRATCH/srv.stderr"
    rm -f "$SERVER_IN" "$SERVER_OUT"
    mkfifo "$SERVER_IN" "$SERVER_OUT" || return 1
    "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" 2>"$SERVER_STDERR" &
    SERVER_PID=$!
    # Open fds AFTER the server starts so the fifos do not block.
    exec 3>"$SERVER_IN"
    exec 4<"$SERVER_OUT"
    return 0
}

# Send one JSON-RPC line and read exactly one response line, bounded.
# Sets MCP_RESP. Returns 0 if a line arrived within the bound, 1 on timeout.
MCP_RESP=""
mcp_send_recv() {
    # mcp_send_recv <request_json> <timeout_secs>
    local req="$1"; local secs="${2:-15}"
    MCP_RESP=""
    # If we already abandoned a wedged server, fail instantly (no wait).
    [ "$SERVER_WEDGED" -eq 1 ] && return 1
    printf '%s\n' "$req" >&3 2>/dev/null || return 1
    # `read -t` is the bounded wait — NO sleep loop.
    if IFS= read -t "$secs" -r MCP_RESP <&4; then
        return 0
    fi
    # Timeout. If the process is still alive it is wedged — abandon it so the
    # rest of the battery does not pay this bound repeatedly.
    if mcp_alive; then
        mcp_mark_wedged
    fi
    return 1
}

mcp_alive() {
    [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null
}

# Set once the server is proven hung/unresponsive (a single bounded read timed
# out while the process is still alive). The downstream server-phase invariants
# short-circuit on this so the WHOLE battery still finishes quickly instead of
# paying a fresh multi-second bounded wait per remaining check against a wedged
# server. We also hard-kill the wedged process immediately so the EOF-exit check
# does not block on a server that will never honour EOF.
SERVER_WEDGED=0
mcp_mark_wedged() {
    SERVER_WEDGED=1
    if [ -n "$SERVER_PID" ]; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    exec 3>&- 2>/dev/null || true
    exec 4<&- 2>/dev/null || true
    SERVER_PID=""
}

# ── Invariant 3: initialize handshake WITHOUT closing stdin (bug #513) ──────
# We must get a JSON-RPC response while stdin remains OPEN. A hang here (no
# response within the bound) is a FAIL — this is exactly the #513 class.
inv_mcp_initialize() {
    if ! mcp_start; then
        fail "mcp-initialize" "could not start server / mkfifo"
        return 1
    fi
    if ! mcp_alive; then
        fail "mcp-initialize" "server did not start (see stderr: $(tr '\n' ' ' < "$SERVER_STDERR" | cut -c1-200))"
        return 1
    fi
    local req='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{}}}'
    if ! mcp_send_recv "$req" 15; then
        fail "mcp-initialize" "no response within 15s with stdin OPEN (hang — #513 class)"
        # A wedged server: abandon it so downstream checks fail fast instead of
        # each paying its own multi-second bounded wait.
        if mcp_alive; then
            mcp_mark_wedged
        fi
        return 1
    fi
    if printf '%s' "$MCP_RESP" | is_json; then
        if printf '%s' "$MCP_RESP" | jq_has result; then
            # Confirm it really is an initialize result (has serverInfo/protocolVersion)
            if printf '%s' "$MCP_RESP" | grep -q 'protocolVersion'; then
                pass "mcp-initialize (response received, stdin still open)"
            else
                pass "mcp-initialize (valid JSON-RPC result; no protocolVersion echoed)"
            fi
        elif printf '%s' "$MCP_RESP" | jq_has error; then
            fail "mcp-initialize" "server returned JSON-RPC error to initialize"
        else
            fail "mcp-initialize" "response has neither result nor error"
        fi
    else
        fail "mcp-initialize" "response not valid JSON: [$(printf '%s' "$MCP_RESP" | cut -c1-200)]"
    fi
    return 0
}

# ── Invariant 4: tools/list returns all expected tools ─────────────────────
# Cross-check against the canonical 14-tool list (TOOLS[] in src/mcp/mcp.c).
EXPECTED_TOOLS="index_repository search_graph query_graph trace_path get_code_snippet get_graph_schema get_architecture search_code list_projects delete_project index_status detect_changes manage_adr ingest_traces"
EXPECTED_TOOL_COUNT=14
inv_tools_list() {
    if ! mcp_alive; then
        fail "tools-list" "server not alive"
        return
    fi
    # tools/list is cursor-paginated (MCP spec): the server returns a page of
    # tools plus result.nextCursor. Follow the cursor across pages and accumulate
    # the advertised tools — exactly what a compliant client does — then assert the
    # union covers all expected tools.
    local got_names="" cursor="" page=0
    while :; do
        page=$((page + 1))
        if [ "$page" -gt 20 ]; then
            fail "tools-list" "pagination did not terminate after 20 pages"
            return
        fi
        local params='{}'
        [ -n "$cursor" ] && params="{\"cursor\":\"$cursor\"}"
        local req="{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":$params}"
        if ! mcp_send_recv "$req" 15; then
            fail "tools-list" "no response within 15s (page $page, hang)"
            return
        fi
        if ! printf '%s' "$MCP_RESP" | is_json; then
            fail "tools-list" "response not valid JSON (page $page)"
            return
        fi
        # Line 1 = space-joined names on this page; line 2 = nextCursor ("" if last).
        local parsed
        parsed="$(printf '%s' "$MCP_RESP" | "$PY" -c '
import sys,json
try:
    d=json.load(sys.stdin)
except Exception:
    print(""); print(""); sys.exit(0)
r=d.get("result") or {}
print(" ".join(t.get("name","") for t in (r.get("tools") or [])))
print(r.get("nextCursor") or "")' 2>/dev/null)"
        got_names="$got_names $(printf '%s' "$parsed" | sed -n '1p')"
        local next; next="$(printf '%s' "$parsed" | sed -n '2p')"
        [ -z "$next" ] && break
        cursor="$next"
    done
    local got_count
    got_count="$(printf '%s' "$got_names" | tr ' ' '\n' | grep -c .)"
    if [ "${got_count:-0}" -ne "$EXPECTED_TOOL_COUNT" ]; then
        fail "tools-list" "got $got_count tools across $page page(s), expected $EXPECTED_TOOL_COUNT; names=[$got_names]"
        return
    fi
    local missing=""
    local t
    for t in $EXPECTED_TOOLS; do
        case " $got_names " in
            *" $t "*) ;;
            *) missing="$missing $t" ;;
        esac
    done
    if [ -n "$missing" ]; then
        fail "tools-list" "missing tools:$missing"
    else
        pass "tools-list (all $EXPECTED_TOOL_COUNT tools present)"
    fi
}

# ── Invariant 5: EVERY MCP tool invocable → valid JSON-RPC, no crash ───────
# Index over the live server first so query tools have a project. Each call must
# return a JSON-RPC response with result OR error and must not crash the server.
inv_every_tool() {
    if [ "$SERVER_WEDGED" -eq 1 ]; then
        fail "every-tool" "skipped — server wedged/unresponsive (see mcp-initialize)"
        return
    fi
    if ! mcp_alive; then
        fail "every-tool" "server not alive before tool sweep"
        return
    fi

    # Index the test repo over the SERVER (so the in-process store is warm for
    # query tools that resolve via the same server instance).
    local idx_req="{\"jsonrpc\":\"2.0\",\"id\":$((MCP_ID++)),\"method\":\"tools/call\",\"params\":{\"name\":\"index_repository\",\"arguments\":{\"repo_path\":\"$TEST_REPO_NATIVE\"}}}"
    if ! mcp_send_recv "$idx_req" 90; then
        # No response: either the server crashed (fd closed → EOF) or it wedged
        # (mcp_send_recv already hard-killed it and set SERVER_WEDGED).
        if [ "$SERVER_WEDGED" -eq 1 ]; then
            fail "every-tool" "index_repository over server hung (>90s, hard-killed)"
        else
            fail "every-tool" "server CRASHED during index_repository (connection closed, no response)"
        fi
        return
    fi
    if printf '%s' "$MCP_RESP" | jq_has result; then
        pass "tool/index_repository (valid response)"
    elif printf '%s' "$MCP_RESP" | jq_has error; then
        pass "tool/index_repository (graceful error response)"
    else
        fail "every-tool" "index_repository response malformed"
    fi
    if ! mcp_alive; then
        fail "every-tool" "server died after index_repository"
        return
    fi

    # name|minimal-args (JSON object) for the remaining 13 tools.
    # Args chosen to be minimally valid per TOOLS[] required fields.
    local p="$PROJ_NAME"
    local -a CALLS
    CALLS=(
        "search_graph|{\"project\":\"$p\",\"name_pattern\":\".*\"}"
        "query_graph|{\"project\":\"$p\",\"query\":\"MATCH (n) RETURN n.name LIMIT 5\"}"
        "trace_path|{\"project\":\"$p\",\"function_name\":\"compute\",\"direction\":\"both\"}"
        "get_code_snippet|{\"project\":\"$p\",\"qualified_name\":\"compute\"}"
        "get_graph_schema|{\"project\":\"$p\"}"
        "get_architecture|{\"project\":\"$p\"}"
        "search_code|{\"project\":\"$p\",\"pattern\":\"def \"}"
        "list_projects|{}"
        "index_status|{\"project\":\"$p\"}"
        "detect_changes|{\"project\":\"$p\"}"
        "manage_adr|{\"project\":\"$p\",\"mode\":\"get\"}"
        "ingest_traces|{\"project\":\"$p\",\"traces\":[]}"
        "delete_project|{\"project\":\"__cbm_smoke_nonexistent__\"}"
    )

    local entry name args
    for entry in "${CALLS[@]}"; do
        name="${entry%%|*}"
        args="${entry#*|}"
        local req="{\"jsonrpc\":\"2.0\",\"id\":$((MCP_ID++)),\"method\":\"tools/call\",\"params\":{\"name\":\"$name\",\"arguments\":$args}}"
        if ! mcp_send_recv "$req" 30; then
            fail "tool/$name" "no response within 30s (hang)"
            # Server may be wedged; stop the sweep to avoid cascade.
            if ! mcp_alive; then
                fail "every-tool" "server died during tool/$name"
                return
            fi
            continue
        fi
        if ! printf '%s' "$MCP_RESP" | is_json; then
            fail "tool/$name" "response not valid JSON: [$(printf '%s' "$MCP_RESP" | cut -c1-160)]"
            continue
        fi
        if printf '%s' "$MCP_RESP" | jq_has result; then
            pass "tool/$name (result)"
        elif printf '%s' "$MCP_RESP" | jq_has error; then
            pass "tool/$name (graceful error)"
        else
            fail "tool/$name" "response has neither result nor error"
        fi
        if ! mcp_alive; then
            fail "tool/$name" "server CRASHED after this call"
            return
        fi
    done

    # Unknown tool must produce a graceful response, not a crash.
    local ureq="{\"jsonrpc\":\"2.0\",\"id\":$((MCP_ID++)),\"method\":\"tools/call\",\"params\":{\"name\":\"__cbm_no_such_tool__\",\"arguments\":{}}}"
    if mcp_send_recv "$ureq" 15 && printf '%s' "$MCP_RESP" | is_json; then
        pass "tool/unknown (graceful response, no crash)"
    else
        fail "tool/unknown" "unknown tool did not produce a bounded valid JSON response"
    fi
    mcp_alive && pass "server-alive-after-sweep" || fail "server-alive-after-sweep" "server not alive after tool sweep"
}

# ── Invariant 7: malformed-input resilience (no crash, graceful error) ─────
# Feed a battery of hostile inputs over the SAME live server and assert it
# neither hangs nor crashes. Each line gets a bounded read; we tolerate either
# a JSON-RPC error response or (for notification-shaped lines) no response, but
# the server must remain alive and responsive afterwards.
inv_malformed_input() {
    if [ "$SERVER_WEDGED" -eq 1 ]; then
        fail "malformed-input" "skipped — server wedged/unresponsive (see mcp-initialize)"
        return
    fi
    if ! mcp_alive; then
        fail "malformed-input" "server not alive at start"
        return
    fi

    local bad
    local long_line
    long_line="$("$PY" -c 'print("x"*200000)')"
    # Each item is a single raw stdin line.
    local -a BADLINES
    BADLINES=(
        'not json at all'
        '{ "jsonrpc": "2.0", broken'
        '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_graph"}}'   # missing required args
        '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"index_repository","arguments":{"repo_path":"/cbm/does/not/exist/xyz"}}}'
        '{"jsonrpc":"2.0","id":1,"method":"no_such_method","params":{}}'
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"query_graph\",\"arguments\":{\"project\":\"$PROJ_NAME\",\"query\":\"$long_line\"}}}"
    )

    local i=0
    for bad in "${BADLINES[@]}"; do
        i=$((i + 1))
        # Send; read at most one response line, bounded. A timeout here is only a
        # problem if the server is ALSO dead — some malformed lines legitimately
        # yield no response. We verify liveness via a follow-up ping. The short
        # bound keeps the well-behaved path instant; the final liveness ping is
        # the real correctness gate, so we tolerate a no-reply here and move on.
        printf '%s\n' "$bad" >&3 2>/dev/null || break
        IFS= read -t 8 -r _discard <&4 || true
        if ! mcp_alive; then
            fail "malformed-input" "server CRASHED on hostile line #$i"
            return
        fi
    done

    # Binary/garbage + non-UTF8 bytes on a single line (printf with octal).
    printf '\001\002\003\377\376\xff\xfe garbage\n' >&3 2>/dev/null || true
    IFS= read -t 8 -r _discard <&4 || true
    if ! mcp_alive; then
        fail "malformed-input" "server CRASHED on binary/non-UTF8 line"
        return
    fi

    # Liveness probe: a well-formed request must still get a valid response.
    local ping="{\"jsonrpc\":\"2.0\",\"id\":$((MCP_ID++)),\"method\":\"tools/list\",\"params\":{}}"
    if mcp_send_recv "$ping" 15 && printf '%s' "$MCP_RESP" | is_json && printf '%s' "$MCP_RESP" | jq_has result; then
        pass "malformed-input (server survived hostile inputs and stayed responsive)"
    else
        fail "malformed-input" "server unresponsive after hostile inputs"
    fi
}

# Index a non-existent repo via CLI → graceful (no crash), as a standalone check.
inv_nonexistent_repo_cli() {
    cli_call 30 --json index_repository '{"repo_path":"/cbm/definitely/not/here/zzz"}'
    if [ "$CLI_RC" -eq 124 ]; then
        fail "nonexistent-repo-cli" "hung on non-existent repo path"
    elif [ "$CLI_RC" -gt 128 ]; then
        fail "nonexistent-repo-cli" "crashed (signal $((CLI_RC-128)))"
    elif printf '%s' "$CLI_OUT" | is_json || printf '%s' "$CLI_OUT" | grep -qiE 'error|not.*found|no such|does not exist|invalid'; then
        pass "nonexistent-repo-cli (graceful, rc=$CLI_RC)"
    else
        # Even a non-JSON, non-error message is acceptable as long as it didn't crash.
        pass "nonexistent-repo-cli (no crash, rc=$CLI_RC)"
    fi
}

# Empty repo dir → index must not crash and should report empty/graceful.
inv_empty_repo_cli() {
    local empty="$SCRATCH/empty_repo"
    mkdir -p "$empty"
    local en; en="$(native_path "$empty")"
    cli_call 30 --json index_repository "{\"repo_path\":\"$en\"}"
    if [ "$CLI_RC" -eq 124 ]; then
        fail "empty-repo-cli" "hung on empty repo"
    elif [ "$CLI_RC" -gt 128 ]; then
        fail "empty-repo-cli" "crashed (signal $((CLI_RC-128)))"
    else
        pass "empty-repo-cli (no crash, rc=$CLI_RC)"
    fi
}

# A binary/garbage file + non-UTF8 + very-long-line in a repo → index no-crash.
inv_garbage_files_cli() {
    local grepo="$SCRATCH/garbage_repo"
    mkdir -p "$grepo"
    # Binary garbage file.
    "$PY" -c 'open("'"$grepo"'/blob.py","wb").write(bytes(range(256))*64)' 2>/dev/null || \
        printf '\000\001\002\377\376 garbage' > "$grepo/blob.py"
    # Non-UTF8 bytes in a source-looking file.
    "$PY" -c 'open("'"$grepo"'/bad.go","wb").write(b"package main\n// \xff\xfe\x80 invalid utf8\nfunc X(){}\n")' 2>/dev/null || true
    # Very long single line.
    "$PY" -c 'open("'"$grepo"'/long.js","w").write("var x = \""+"a"*500000+"\";\n")' 2>/dev/null || true
    git -C "$grepo" init -q 2>/dev/null || true
    local gn; gn="$(native_path "$grepo")"
    cli_call 60 --json index_repository "{\"repo_path\":\"$gn\"}"
    if [ "$CLI_RC" -eq 124 ]; then
        fail "garbage-files-cli" "hung indexing garbage/non-UTF8/long-line repo"
    elif [ "$CLI_RC" -gt 128 ]; then
        fail "garbage-files-cli" "crashed (signal $((CLI_RC-128))) on garbage repo"
    else
        pass "garbage-files-cli (indexed garbage/non-UTF8/long-line without crash, rc=$CLI_RC)"
    fi
}

# ── Invariant: a hard crash on one file is SKIPPED and the rest is indexed ──
# Stage 3c skip-and-continue: a file that hard-crashes the native indexer
# (SIGSEGV/abort/stack-overflow) must be QUARANTINED — the supervisor re-runs the
# worker single-threaded, pins the exact crasher via a per-file marker, adds it to
# a quarantine list, and re-spawns until a clean run indexes the GOOD files while
# reporting the crasher as a phase="crash" skip. Uses the test-only fault injector
# (CBM_TEST_CRASH_ON) so the guard is honest: with the supervisor OFF the crash
# must genuinely escape as a signal (rc>=128, vacuity guard); with it ON (default)
# the run must be contained (rc<128), report status="indexed" + the crasher as
# phase="crash", index the good file (nodes>0), and NOT skip the good file.
inv_crasher_skipped_cli() {
    local crepo="$SCRATCH/crasher_repo"
    mkdir -p "$crepo"
    printf 'def good():\n    return 1\n' > "$crepo/good.py"
    printf 'def boom():\n    return 2\n' > "$crepo/crash_me.py"
    git -C "$crepo" init -q 2>/dev/null || true
    local cn; cn="$(native_path "$crepo")"

    # Honesty baseline: supervisor OFF → the injected fault must escape as a signal.
    export CBM_TEST_CRASH_ON=crash_me
    export CBM_INDEX_SUPERVISOR=0
    cli_call 60 index_repository "{\"repo_path\":\"$cn\"}"
    local base_rc="$CLI_RC"
    unset CBM_INDEX_SUPERVISOR

    # Supervisor ON (default) → the crash must be contained AND skipped-and-continued.
    cli_call 90 index_repository "{\"repo_path\":\"$cn\"}"
    local sup_rc="$CLI_RC"
    local sup_out="$CLI_OUT"
    unset CBM_TEST_CRASH_ON

    # Strip JSON escaping so the assertions are robust to the text-result wrapping.
    local flat
    flat="$(printf '%s' "$sup_out" | tr -d '\\' | tr -d '"')"
    local nodes
    nodes="$(printf '%s' "$sup_out" | "$PY" -c '
import sys,re
t=sys.stdin.read().replace("\\","").replace("\"","")
m=re.findall(r"nodes\s*[:=]\s*(\d+)", t)
print(max((int(x) for x in m), default=0))' 2>/dev/null)"

    if [ "$base_rc" -le 128 ]; then
        fail "crasher-skipped-cli" "baseline did not crash (rc=$base_rc) — injector inactive, guard would be vacuous"
    elif [ "$sup_rc" -eq 124 ]; then
        fail "crasher-skipped-cli" "supervised run hung (rc=124)"
    elif [ "$sup_rc" -gt 128 ]; then
        fail "crasher-skipped-cli" "crash escaped the supervisor (signal $((sup_rc-128)))"
    elif ! printf '%s' "$flat" | grep -q 'status:indexed'; then
        fail "crasher-skipped-cli" "status not indexed after skip-and-continue: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif ! printf '%s' "$flat" | grep -q 'phase:crash'; then
        fail "crasher-skipped-cli" "crasher not reported as phase=crash: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif ! printf '%s' "$flat" | grep -q 'crash_me.py'; then
        fail "crasher-skipped-cli" "crash_me.py not listed as skipped: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif printf '%s' "$flat" | grep -q 'good.py'; then
        fail "crasher-skipped-cli" "good.py was skipped (should have been indexed): $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif [ "${nodes:-0}" -le 0 ] 2>/dev/null; then
        fail "crasher-skipped-cli" "good file not indexed (nodes=${nodes:-0}): $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    else
        pass "crasher-skipped-cli (baseline rc=$base_rc escaped; supervised rc=$sup_rc: status=indexed, crash_me.py phase=crash, good.py indexed nodes=$nodes)"
    fi
}

# ── Invariant: a HANG on one file is SKIPPED and the rest is indexed ─────────
# The hang twin of inv_crasher_skipped_cli. A file that makes the indexer make NO
# progress (external-scanner infinite loop, modelled by CBM_TEST_HANG_ON's busy-
# spin) must be QUARANTINED: the supervisor's quiet-timeout kills the worker,
# classifies it as a HANG, pins the exact file via the marker, quarantines it as
# phase="hang", and re-spawns until a clean run indexes the GOOD files while
# reporting the hanger as a phase="hang" skip. Honest guard: with the supervisor
# OFF the injected hang must genuinely NOT complete within a bound (rc=124, the
# vacuity guard — proving the injector really hangs); with it ON + a SHORT
# CBM_INDEX_WORKER_TIMEOUT_S the run must COMPLETE (rc<128, not 124), report
# status="indexed" + the hanger as phase="hang", index the good file (nodes>0),
# and NOT skip the good file.
inv_hanger_skipped_cli() {
    local hrepo="$SCRATCH/hanger_repo"
    mkdir -p "$hrepo"
    printf 'def good():\n    return 1\n' > "$hrepo/good.py"
    printf 'def slow():\n    return 2\n' > "$hrepo/hang_me.py"
    git -C "$hrepo" init -q 2>/dev/null || true
    local hn; hn="$(native_path "$hrepo")"

    # Honesty baseline: supervisor OFF → the injected hang must NOT complete within
    # the bound. The bounded runner fires (rc=124), proving the injector hangs.
    export CBM_TEST_HANG_ON=hang_me
    export CBM_INDEX_SUPERVISOR=0
    cli_call 12 index_repository "{\"repo_path\":\"$hn\"}"
    local base_rc="$CLI_RC"
    unset CBM_INDEX_SUPERVISOR

    # Supervisor ON (default) + a SHORT no-progress timeout → the hang must be
    # detected fast, contained, and skipped-and-continued. Recovery spends two
    # ~5s timeouts (first parallel run, then the single-threaded recovery run) so
    # this invariant legitimately takes ~10-15s; it MUST still complete.
    export CBM_INDEX_WORKER_TIMEOUT_S=5
    cli_call 90 index_repository "{\"repo_path\":\"$hn\"}"
    local sup_rc="$CLI_RC"
    local sup_out="$CLI_OUT"
    unset CBM_INDEX_WORKER_TIMEOUT_S
    unset CBM_TEST_HANG_ON

    # Strip JSON escaping so the assertions are robust to the text-result wrapping.
    local flat
    flat="$(printf '%s' "$sup_out" | tr -d '\\' | tr -d '"')"
    local nodes
    nodes="$(printf '%s' "$sup_out" | "$PY" -c '
import sys,re
t=sys.stdin.read().replace("\\","").replace("\"","")
m=re.findall(r"nodes\s*[:=]\s*(\d+)", t)
print(max((int(x) for x in m), default=0))' 2>/dev/null)"

    if [ "$base_rc" -ne 124 ]; then
        fail "hanger-skipped-cli" "baseline did not hang (rc=$base_rc, want 124) — injector inactive, guard would be vacuous"
    elif [ "$sup_rc" -eq 124 ]; then
        fail "hanger-skipped-cli" "supervised run hung (rc=124) — hang not contained"
    elif [ "$sup_rc" -gt 128 ]; then
        fail "hanger-skipped-cli" "supervised run died via signal $((sup_rc-128))"
    elif ! printf '%s' "$flat" | grep -q 'status:indexed'; then
        fail "hanger-skipped-cli" "status not indexed after skip-and-continue: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif ! printf '%s' "$flat" | grep -q 'phase:hang'; then
        fail "hanger-skipped-cli" "hanger not reported as phase=hang: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif ! printf '%s' "$flat" | grep -q 'hang_me.py'; then
        fail "hanger-skipped-cli" "hang_me.py not listed as skipped: $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif printf '%s' "$flat" | grep -q 'good.py'; then
        fail "hanger-skipped-cli" "good.py was skipped (should have been indexed): $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    elif [ "${nodes:-0}" -le 0 ] 2>/dev/null; then
        fail "hanger-skipped-cli" "good file not indexed (nodes=${nodes:-0}): $(printf '%s' "$sup_out" | tr '\n' ' ' | cut -c1-300)"
    else
        pass "hanger-skipped-cli (baseline rc=$base_rc hung; supervised rc=$sup_rc: status=indexed, hang_me.py phase=hang, good.py indexed nodes=$nodes)"
    fi
}

# ── Invariant 8: clean exit on stdin EOF within a bounded wait (no hang) ────
# Close the server's stdin (fd3). The server must reach EOF, break its loop, and
# exit cleanly. We bound the wait WITHOUT sleep: closing stdin makes the server
# also close its stdout, so a bounded `read` on fd4 returns EOF promptly. We then
# reap with a bounded `wait`-equivalent and require a non-signal exit code.
inv_clean_eof_exit() {
    if [ "$SERVER_WEDGED" -eq 1 ]; then
        fail "clean-eof-exit" "server was wedged/unresponsive — could not test clean EOF (already hard-killed)"
        return
    fi
    if [ -z "$SERVER_PID" ] || ! mcp_alive; then
        # If the server already exited (e.g. crashed earlier), that is reported
        # elsewhere; here we can only note we could not test a clean EOF.
        fail "clean-eof-exit" "no live server to test EOF shutdown"
        return
    fi
    local pid="$SERVER_PID"
    # Close stdin → EOF. The server must now reach EOF, break its loop, and exit,
    # which closes its stdout (fd4). We read fd4 with a bounded `read -t`: each
    # buffered response line drains instantly; when the server exits, fd4 returns
    # EOF; if the server hangs, the bound fires. The TOTAL wait is bounded by a
    # deadline (SECONDS) so a server that dribbles lines forever still can't run
    # us past the cap. NO sleep, NO busy-spin (read blocks in the kernel).
    exec 3>&-
    local deadline=$((SECONDS + 12))
    local eof_seen=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        if IFS= read -t 5 -r _drain <&4; then
            continue   # drained a buffered line; keep reading toward EOF
        fi
        # read failed: EOF (server closed stdout → exiting) OR 5s timeout.
        # Distinguish by liveness: if the process is gone, it was EOF.
        if ! kill -0 "$pid" 2>/dev/null; then
            eof_seen=1
            break
        fi
        # Still alive but no data for 5s — likely closing down; loop until the
        # deadline gives it a chance to exit, re-checking liveness each pass.
    done
    exec 4<&-

    if [ "$eof_seen" -ne 1 ] && kill -0 "$pid" 2>/dev/null; then
        # Still running at the deadline → did not honour EOF → hang.
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        fail "clean-eof-exit" "server did not exit within ~12s of stdin EOF (hang)"
        SERVER_PID=""
        return
    fi
    # Process has exited (or is exiting): reap it directly. `wait` works because
    # the server is a DIRECT child of this shell — it returns the true status.
    wait "$pid" 2>/dev/null
    local status=$?
    SERVER_PID=""
    # Signal death → status>128. A clean exit should be 0 (or at least not a signal).
    if [ "$status" -gt 128 ]; then
        fail "clean-eof-exit" "server exited via signal $((status-128)) on EOF (want clean exit)"
    elif [ "$status" -eq 0 ]; then
        pass "clean-eof-exit (exit 0 on stdin EOF within bound)"
    else
        # Non-zero, non-signal: not a crash, but flag for visibility.
        pass "clean-eof-exit (exited rc=$status on EOF, non-signal)"
    fi
}

# ── Invariant 9: (Linux/macOS) no missing shared libraries ─────────────────
inv_shared_libs() {
    local uname_s
    uname_s="$(uname -s 2>/dev/null || echo unknown)"
    case "$uname_s" in
        Linux)
            if command -v ldd >/dev/null 2>&1; then
                local out
                out="$(ldd "$BINARY" 2>&1)"
                if printf '%s' "$out" | grep -qE 'not found'; then
                    fail "shared-libs" "ldd reports missing libs:\n$(printf '%s' "$out" | grep 'not found')"
                else
                    pass "shared-libs (ldd: no 'not found')"
                fi
            else
                pass "shared-libs (ldd unavailable — skipped)"
            fi
            ;;
        Darwin)
            if command -v otool >/dev/null 2>&1; then
                local out
                out="$(otool -L "$BINARY" 2>&1)"
                # Verify each non-system dylib path resolves.
                local missing=""
                local line lib
                while IFS= read -r line; do
                    lib="$(printf '%s' "$line" | sed -E 's/^[[:space:]]+//; s/ \(.*$//')"
                    case "$lib" in
                        ""|*"$BINARY"*) continue ;;
                        @rpath/*|@loader_path/*|@executable_path/*) continue ;;  # relocatable; cannot stat
                        /usr/lib/*|/System/*) continue ;;                        # system libs always present
                    esac
                    [ -e "$lib" ] || missing="$missing $lib"
                done <<< "$out"
                if [ -n "$missing" ]; then
                    fail "shared-libs" "otool: unresolved non-system dylibs:$missing"
                else
                    pass "shared-libs (otool: all non-system dylibs resolve)"
                fi
            else
                pass "shared-libs (otool unavailable — skipped)"
            fi
            ;;
        *)
            # Windows/msys2: no ldd/otool equivalent used here; the fact that
            # --version ran at all proves the loader resolved its imports.
            pass "shared-libs (skipped on $uname_s; --version success implies loadable)"
            ;;
    esac
}

# ══════════════════════════════════════════════════════════════════════════
#  RUN ALL INVARIANTS
# ══════════════════════════════════════════════════════════════════════════
echo "=== smoke-invariants: binary=$BINARY ==="
echo "--- platform: $(uname -s 2>/dev/null || echo unknown) ---"

make_test_repo

# CLI-mode invariants (independent processes).
inv_version
inv_help
inv_shared_libs
inv_install_dryrun
inv_index_cli
inv_index_status_cli
inv_nonexistent_repo_cli
inv_empty_repo_cli
inv_garbage_files_cli
inv_crasher_skipped_cli
inv_hanger_skipped_cli

# MCP server-lifecycle invariants (one shared server instance).
inv_mcp_initialize
inv_tools_list
inv_every_tool
inv_malformed_input
inv_clean_eof_exit   # MUST run last — it shuts the server down.

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "=== smoke-invariants summary: $PASSES passed, $FAILURES failed ==="
if [ "$FAILURES" -gt 0 ]; then
    echo "=== smoke-invariants: FAILED ==="
    exit 1
fi
echo "=== smoke-invariants: PASSED ==="
exit 0
