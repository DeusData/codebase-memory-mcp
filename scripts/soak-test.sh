#!/usr/bin/env bash
# soak-test.sh — Endurance test for codebase-memory-mcp.
#
# Runs compressed workload cycles: queries, file mutations, reindexes, idle periods.
# Reads the randomized diagnostics path emitted by the daemon (requires
# CBM_DIAGNOSTICS=1).
# Outputs metrics to soak-results/ and exits 0 (pass) or 1 (fail).
#
# Usage:
#   scripts/soak-test.sh <binary> <duration_minutes> [--skip-crash-test]
#
# Tiers:
#   10 min  = quick soak (CI gate)
#   15 min  = ASan soak (leak detection)
#   240 min = nightly (compressed 4h = ~5 days real usage)

set -euo pipefail

BINARY="${1:?Usage: soak-test.sh <binary> <duration_minutes>}"
DURATION_MIN="${2:?Usage: soak-test.sh <binary> <duration_minutes>}"
SKIP_CRASH="${3:-}"
BINARY=$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")

# Soak mode selector.
#   default     = original mixed workload (queries + mutations + periodic reindex
#                 + crash-recovery). Unchanged from before this env var existed.
#   query-leak  = #581 detector. After the initial index, NEVER reindex and NEVER
#                 mutate files, so the mimalloc page-return path (cbm_mem_collect,
#                 triggered by index_repository) is never invoked and cannot sweep
#                 a query-only leak. Phase 3 then hammers a variety of READ tools
#                 (search_graph / query_graph / trace_path / get_code_snippet /
#                 search_code) to exercise the query-only store-open + WAL + alloc
#                 paths the bug report implicates. The RSS slope/ratio/ceiling
#                 analysis below is the leak detector. The crash-recovery phase is
#                 skipped in this mode because it reindexes (which would mask #581).
CBM_SOAK_MODE="${CBM_SOAK_MODE:-default}"

RESULTS_DIR="${RESULTS_DIR:-soak-results}"
mkdir -p "$RESULTS_DIR"

# Isolate daemon coordination from interactive CBM sessions and give this run
# a deterministic host-side daemon log. Wine needs a Windows-form cache path
# in the child environment while this Bash harness retains the host path.
SOAK_CACHE_DIR_HOST=$(mktemp -d "${TMPDIR:-/tmp}/cbm-soak-cache.XXXXXX")
SOAK_CACHE_DIR_VALUE="$SOAK_CACHE_DIR_HOST"
if [[ "$BINARY" == *.exe ]] && command -v winepath >/dev/null 2>&1; then
    SOAK_CACHE_DIR_VALUE=$(winepath -w "$SOAK_CACHE_DIR_HOST")
elif [[ "$BINARY" == *.exe ]] && command -v cygpath >/dev/null 2>&1; then
    SOAK_CACHE_DIR_VALUE=$(cygpath -w "$SOAK_CACHE_DIR_HOST")
fi
DAEMON_LOG="$SOAK_CACHE_DIR_HOST/logs/cbm-daemon.log"
DIAG_FILE=""

METRICS_CSV="$RESULTS_DIR/metrics.csv"
LATENCY_CSV="$RESULTS_DIR/latency.csv"
SUMMARY="$RESULTS_DIR/summary.txt"

echo "timestamp,uptime_s,rss_bytes,heap_committed,fd_count,query_count,query_max_us" > "$METRICS_CSV"
echo "timestamp,tool,duration_ms,exit_code" > "$LATENCY_CSV"
: > "$SUMMARY"
PASS=true

DURATION_S=$((DURATION_MIN * 60))

echo "=== soak-test: binary=$BINARY duration=${DURATION_MIN}m mode=${CBM_SOAK_MODE} ==="

# ── Helper: generate realistic test project (~200 files) ─────────

SOAK_PROJECT=$(mktemp -d)
SOAK_PROJECT_VALUE="$SOAK_PROJECT"
if [[ "$BINARY" == *.exe ]] && command -v winepath >/dev/null 2>&1; then
    SOAK_PROJECT_VALUE=$(winepath -w "$SOAK_PROJECT")
    SOAK_PROJECT_VALUE=${SOAK_PROJECT_VALUE//\\//}
elif [[ "$BINARY" == *.exe ]] && command -v cygpath >/dev/null 2>&1; then
    SOAK_PROJECT_VALUE=$(cygpath -m "$SOAK_PROJECT")
fi
# Every JSON request uses one pre-escaped spelling of the path. The Bash
# harness keeps SOAK_PROJECT in its host/MSYS form for file and Git operations,
# while a native Windows child receives the drive-letter form above.
SOAK_PROJECT_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$SOAK_PROJECT_VALUE")
SERVER_PID=""
SERVER_IN=""
SERVER_OUT=""
SOAK_CLEANED=false

soak_cleanup() {
    if $SOAK_CLEANED; then
        return
    fi
    SOAK_CLEANED=true
    { exec 3>&-; } 2>/dev/null || true
    { exec 4<&-; } 2>/dev/null || true
    if [[ "${SERVER_PID:-}" =~ ^[0-9]+$ ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
    [ -z "${SERVER_IN:-}" ] || rm -f -- "$SERVER_IN"
    [ -z "${SERVER_OUT:-}" ] || rm -f -- "$SERVER_OUT"
    if [ -f "$DAEMON_LOG" ]; then
        cp "$DAEMON_LOG" "$RESULTS_DIR/cbm-daemon.log" 2>/dev/null || true
    fi
    rm -rf -- "$SOAK_PROJECT" "$SOAK_CACHE_DIR_HOST"
}

trap soak_cleanup EXIT
trap 'exit 130' INT TERM

generate_project() {
    local root="$1"
    # Python package (80 files)
    for i in $(seq 1 20); do
        local pkg="$root/src/pkg_${i}"
        mkdir -p "$pkg"
        cat > "$pkg/__init__.py" << PYEOF
from .handlers import handle_${i}
from .models import Model${i}
PYEOF
        cat > "$pkg/handlers.py" << PYEOF
from .models import Model${i}
from .utils import validate_${i}, transform_${i}

def handle_${i}(request):
    data = Model${i}.from_request(request)
    if not validate_${i}(data):
        return {"error": "invalid"}
    return transform_${i}(data)

def process_batch_${i}(items):
    return [handle_${i}(item) for item in items]
PYEOF
        cat > "$pkg/models.py" << PYEOF
class Model${i}:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    @classmethod
    def from_request(cls, req):
        return cls(req.get("name", ""), req.get("value", 0))

    def to_dict(self):
        return {"name": self.name, "value": self.value}
PYEOF
        cat > "$pkg/utils.py" << PYEOF
def validate_${i}(data):
    return data is not None and hasattr(data, 'name')

def transform_${i}(data):
    return {"result": data.name.upper(), "score": data.value * ${i}}
PYEOF
    done

    # Go package (40 files)
    mkdir -p "$root/internal/api" "$root/internal/store" "$root/cmd"
    for i in $(seq 1 20); do
        cat > "$root/internal/api/handler_${i}.go" << GOEOF
package api

import "fmt"

func HandleRoute${i}(path string) (string, error) {
    result := ProcessData${i}(path)
    return fmt.Sprintf("route_%d: %s", ${i}, result), nil
}

func ProcessData${i}(input string) string {
    return fmt.Sprintf("processed_%d_%s", ${i}, input)
}
GOEOF
        cat > "$root/internal/store/repo_${i}.go" << GOEOF
package store

type Entity${i} struct {
    ID   int
    Name string
    Data map[string]interface{}
}

func FindEntity${i}(id int) (*Entity${i}, error) {
    return &Entity${i}{ID: id, Name: "entity"}, nil
}

func SaveEntity${i}(e *Entity${i}) error {
    return nil
}
GOEOF
    done

    # TypeScript (40 files)
    mkdir -p "$root/frontend/src/components" "$root/frontend/src/hooks"
    for i in $(seq 1 20); do
        cat > "$root/frontend/src/components/Component${i}.tsx" << TSEOF
import React from 'react';
import { useData${i} } from '../hooks/useData${i}';

interface Props${i} { id: number; label: string; }

export const Component${i}: React.FC<Props${i}> = ({ id, label }) => {
    const { data, loading } = useData${i}(id);
    if (loading) return <div>Loading...</div>;
    return <div className="comp-${i}">{label}: {JSON.stringify(data)}</div>;
};
TSEOF
        cat > "$root/frontend/src/hooks/useData${i}.ts" << TSEOF
import { useState, useEffect } from 'react';

export function useData${i}(id: number) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    useEffect(() => {
        fetch('/api/data/${i}/' + id)
            .then(r => r.json())
            .then(d => { setData(d); setLoading(false); });
    }, [id]);
    return { data, loading };
}
TSEOF
    done

    # Config files
    cat > "$root/config.yaml" << 'YAMLEOF'
database:
  host: localhost
  port: 5432
  pool_size: 10
server:
  workers: 4
  timeout: 30
YAMLEOF
    cat > "$root/Dockerfile" << 'DEOF'
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "-m", "src.main"]
DEOF
}

echo "Generating test project (~200 files)..."
generate_project "$SOAK_PROJECT"

# Init git repo (required for watcher)
git -C "$SOAK_PROJECT" init -q 2>/dev/null
git -C "$SOAK_PROJECT" add -A 2>/dev/null
git -C "$SOAK_PROJECT" -c user.email=test@test -c user.name=test commit -q -m "init" 2>/dev/null
FILE_COUNT=$(find "$SOAK_PROJECT" -type f | wc -l | tr -d ' ')
echo "OK: $FILE_COUNT files in test project"

# ── Helper: run CLI tool call and record latency ─────────────────

# Query ID counter
QUERY_ID=1
MCP_LAST_RESPONSE=""

# Send a JSON-RPC tool call to the running server via its stdin pipe.
# Reads response from server stdout. Records latency.
json_rpc_response_ok() {
    local expected_id="$1"
    local response="$2"
    python3 -c '
import json
import sys

try:
    message = json.loads(sys.stdin.read())
    result = message.get("result") if isinstance(message, dict) else None
    ok = (
        isinstance(message, dict)
        and message.get("id") == int(sys.argv[1])
        and "error" not in message
        and "result" in message
        and not (isinstance(result, dict) and result.get("isError") is True)
    )
except (ValueError, TypeError):
    ok = False
sys.exit(0 if ok else 1)
' "$expected_id" <<<"$response"
}

# Read the authoritative project key from index_repository's nested text JSON.
# This mirrors the path canonicalization actually performed by CBM instead of
# guessing from a host/MSYS spelling that can differ on macOS and Windows.
mcp_response_project() {
    local response="$1"
    python3 -c '
import json
import sys

try:
    message = json.loads(sys.stdin.read())
    result = message.get("result", {})
    content = result.get("content", []) if isinstance(result, dict) else []
    project = ""
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            # The summary JSON may be preceded by plain-text banner items
            # (update-available notice); skip anything that is not JSON.
            try:
                payload = json.loads(item.get("text", ""))
            except ValueError:
                continue
            candidate = payload.get("project") if isinstance(payload, dict) else None
            if isinstance(candidate, str) and candidate:
                project = candidate
                break
    if not project:
        raise ValueError("index response has no project")
    print(project)
except (ValueError, TypeError, AttributeError):
    sys.exit(1)
' <<<"$response"
}

mcp_initialize() {
    local response=""
    echo '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"capabilities":{}}}' >&3
    read -r -t 10 response <&4 2>/dev/null && json_rpc_response_ok 0 "$response"
}

mcp_call() {
    local tool="$1"
    local args="$2"
    local id=$QUERY_ID
    QUERY_ID=$((QUERY_ID + 1))

    local req="{\"jsonrpc\":\"2.0\",\"id\":$id,\"method\":\"tools/call\",\"params\":{\"name\":\"$tool\",\"arguments\":$args}}"
    local t0
    t0=$(python3 -c "import time; print(int(time.time()*1000))")

    # Send request to server stdin
    echo "$req" >&3

    # Read and validate one response (wait up to 30s). A JSON-RPC error or
    # tool-level isError is a failed operation, not a successful round-trip.
    local resp=""
    local exit_code=1
    MCP_LAST_RESPONSE=""
    if read -r -t 30 resp <&4 2>/dev/null; then
        MCP_LAST_RESPONSE="$resp"
        if json_rpc_response_ok "$id" "$resp"; then
            exit_code=0
        else
            # A stale (previous, late) response desyncs every later
            # round-trip: drain queued lines until the matching id shows up
            # or the stream runs dry, so one slow reply costs one failed op
            # instead of corrupting the whole run's verdicts.
            local drain=""
            while read -r -t 2 drain <&4 2>/dev/null; do
                MCP_LAST_RESPONSE="$drain"
                if json_rpc_response_ok "$id" "$drain"; then
                    exit_code=0
                    break
                fi
            done
        fi
    fi
    local t1
    t1=$(python3 -c "import time; print(int(time.time()*1000))")
    local dur=$((t1 - t0))
    echo "$(date +%s),$tool,$dur,$exit_code" >> "$LATENCY_CSV"
    return "$exit_code"
}

# ── Helper: collect diagnostics snapshot ─────────────────────────

diagnostics_host_path() {
    local emitted_path="$1"
    if [[ "$emitted_path" =~ ^[A-Za-z]:[/\\] ]] && command -v winepath >/dev/null 2>&1; then
        winepath -u "$emitted_path"
    elif [[ "$emitted_path" =~ ^[A-Za-z]:[/\\] ]] && command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$emitted_path"
    else
        printf '%s\n' "$emitted_path"
    fi
}

refresh_diagnostics_paths() {
    [ -f "$DAEMON_LOG" ] || return 1
    local start_line snapshot_path trajectory_path
    start_line=$(grep '"event":"diagnostics.start"' "$DAEMON_LOG" 2>/dev/null | tail -n 1) || true
    [ -n "$start_line" ] || return 1
    # The discovery record is JSON (a control record survives CBM_LOG_LEVEL
    # suppression); decode the two standard escapes temp paths can carry.
    snapshot_path=$(printf '%s\n' "$start_line" | sed -n 's/.*"snapshot":"\([^"]*\)".*/\1/p' |
        sed 's/\\\([\"\\/]\)/\1/g')
    trajectory_path=$(printf '%s\n' "$start_line" | sed -n 's/.*"trajectory":"\([^"]*\)".*/\1/p' |
        sed 's/\\\([\"\\/]\)/\1/g')
    [ -n "$snapshot_path" ] && [ -n "$trajectory_path" ] || return 1
    DIAG_FILE=$(diagnostics_host_path "$snapshot_path")
}

diagnostics_start_count() {
    if [ -f "$DAEMON_LOG" ]; then
        grep -c '"event":"diagnostics.start"' "$DAEMON_LOG" 2>/dev/null || true
    else
        echo 0
    fi
}

daemon_stop_count() {
    if [ -f "$DAEMON_LOG" ]; then
        grep -c 'msg=daemon.stop' "$DAEMON_LOG" 2>/dev/null || true
    else
        echo 0
    fi
}

wait_for_daemon_stop() {
    local after_count="$1"
    local attempts=300
    while [ "$attempts" -gt 0 ]; do
        local current_count
        current_count=$(daemon_stop_count)
        if [ "${current_count:-0}" -gt "$after_count" ]; then
            return 0
        fi
        attempts=$((attempts - 1))
        sleep 0.1
    done
    return 1
}

wait_for_diagnostics_snapshot() {
    local after_count="${1:-0}"
    local previous_path="${2:-}"
    local attempts=100
    while [ "$attempts" -gt 0 ]; do
        local current_count
        current_count=$(diagnostics_start_count)
        if [ "${current_count:-0}" -gt "$after_count" ] && refresh_diagnostics_paths &&
            [ -f "$DIAG_FILE" ] &&
            { [ -z "$previous_path" ] || [ "$DIAG_FILE" != "$previous_path" ]; }; then
            return 0
        fi
        attempts=$((attempts - 1))
        sleep 0.1
    done
    return 1
}

SNAPSHOT_ATTEMPTS=0
collect_snapshot() {
    SNAPSHOT_ATTEMPTS=$((SNAPSHOT_ATTEMPTS + 1))
    refresh_diagnostics_paths || return 0
    if [ -f "$DIAG_FILE" ]; then
        python3 -c "
import json, time
d = json.load(open('$DIAG_FILE'))
# Use heap_committed if available, otherwise RSS (mimalloc may report 0 for committed)
mem = d.get('heap_committed_bytes', 0)
if mem == 0: mem = d.get('rss_bytes', 0)
print(f\"{int(time.time())},{d.get('uptime_s',0)},{d.get('rss_bytes',0)},{mem},{d.get('fd_count',0)},{d.get('query_count',0)},{d.get('query_max_us',0)}\")
" 2>/dev/null >> "$METRICS_CSV"
    fi
}

diagnostics_json_value() {
    local key="$1"
    refresh_diagnostics_paths || return 1
    python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    value = json.load(stream).get(sys.argv[2], "")
print(value)
' "$DIAG_FILE" "$key" 2>/dev/null
}

# ── Phase 1: Start MCP server with diagnostics ──────────────────

echo "--- Phase 1: start server ---"
# Bidirectional pipes: fd3 = server stdin (write), fd4 = server stdout (read)
SERVER_IN=$(mktemp -u).in
SERVER_OUT=$(mktemp -u).out
mkfifo "$SERVER_IN" "$SERVER_OUT"

CBM_CACHE_DIR="$SOAK_CACHE_DIR_VALUE" CBM_DIAGNOSTICS=1 CBM_LOG_LEVEL=info \
    CBM_LOG_FORMAT=text "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" \
    2>"$RESULTS_DIR/server-stderr.log" &
SERVER_PID=$!

# Open fds AFTER server starts (otherwise fifo blocks)
exec 3>"$SERVER_IN"   # write to server stdin
exec 4<"$SERVER_OUT"  # read from server stdout
sleep 3

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "FAIL: server did not start"
    exec 3>&- 4<&-
    rm -f "$SERVER_IN" "$SERVER_OUT"
    exit 1
fi
echo "OK: server running (pid=$SERVER_PID)"

if ! wait_for_diagnostics_snapshot; then
    echo "FAIL: daemon did not emit a usable diagnostics.start path"
    exec 3>&- 4<&-
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    rm -f "$SERVER_IN" "$SERVER_OUT"
    exit 1
fi

# Send and validate the initialize handshake.
if ! mcp_initialize; then
    echo "FAIL: server did not complete initialize"
    exit 1
fi

# ── Phase 2: Initial index ───────────────────────────────────────

echo "--- Phase 2: initial index ---"
mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}" || PASS=false
if ! PROJ_NAME=$(mcp_response_project "$MCP_LAST_RESPONSE"); then
    echo "FAIL: initial index response did not report its canonical project key"
    exit 1
fi
sleep 6  # wait for diagnostics write
collect_snapshot

BASELINE_RSS=$(cat "$DIAG_FILE" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('rss_bytes',0))" 2>/dev/null || echo "0")
BASELINE_FDS=$(cat "$DIAG_FILE" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('fd_count',0))" 2>/dev/null || echo "0")
echo "OK: baseline RSS=${BASELINE_RSS} FDs=${BASELINE_FDS}"

# ── Phase 3: Compressed workload loop ────────────────────────────

echo "--- Phase 3: workload loop (${DURATION_MIN}m) ---"
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_S))
CYCLE=0
LAST_MUTATE=0
LAST_REINDEX=0

while [ "$(date +%s)" -lt "$END_TIME" ]; do
    NOW=$(date +%s)
    CYCLE=$((CYCLE + 1))

    if [ "$CBM_SOAK_MODE" = "query-leak" ]; then
        # ── #581 query-only leak mode ────────────────────────────────
        # Pure read-query hammering: no mutation, no reindex — so
        # cbm_mem_collect (mimalloc page return) is NEVER triggered and
        # cannot sweep a query-only leak. Hammer a VARIETY of read tools to
        # exercise the store-open + WAL + alloc paths the report implicates.
        mcp_call search_graph "{\"project\":\"$PROJ_NAME\",\"name_pattern\":\".*Handle.*\"}" || PASS=false
        mcp_call query_graph "{\"project\":\"$PROJ_NAME\",\"query\":\"MATCH (n) RETURN n.name LIMIT 25\"}" || PASS=false
        mcp_call trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}" || PASS=false
        mcp_call get_code_snippet "{\"project\":\"$PROJ_NAME\",\"qualified_name\":\"handle_1\"}" || PASS=false
        mcp_call search_code "{\"project\":\"$PROJ_NAME\",\"pattern\":\"def \"}" || PASS=false
    else
        # ── default mode (unchanged) ─────────────────────────────────
        # Queries every 2 seconds
        mcp_call search_graph "{\"project\":\"$PROJ_NAME\",\"name_pattern\":\".*handle_.*\"}" || PASS=false
        mcp_call trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}" || PASS=false

        # File mutation every 2 minutes
        if [ $((NOW - LAST_MUTATE)) -ge 120 ]; then
            echo "# mutation at cycle $CYCLE $(date)" >> "$SOAK_PROJECT/src/main.py"
            git -C "$SOAK_PROJECT" add -A 2>/dev/null
            git -C "$SOAK_PROJECT" -c user.email=test@test -c user.name=test commit -q -m "cycle $CYCLE" 2>/dev/null || true
            LAST_MUTATE=$NOW
        fi

        # Full reindex every 2 minutes (compressed — simulates 15min real interval)
        if [ $((NOW - LAST_REINDEX)) -ge 120 ]; then
            mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}" || PASS=false
            LAST_REINDEX=$NOW
        fi
    fi

    # Collect diagnostics every 10 seconds (5 cycles)
    if [ $((CYCLE % 5)) -eq 0 ]; then
        collect_snapshot
    fi

    sleep 2
done

# ── Phase 4: Idle period + final snapshot ────────────────────────

echo "--- Phase 4: idle (30s) ---"
sleep 30
collect_snapshot

# Check the actual daemon on platforms where its diagnostics PID shares the
# host process namespace. Wine/native Windows use a different PID namespace,
# so reporting the thin frontend as daemon CPU would be false coverage.
DAEMON_PID=$(diagnostics_json_value pid)
IDLE_CPU=""
if [[ "$BINARY" != *.exe ]] && [[ "$DAEMON_PID" =~ ^[0-9]+$ ]]; then
    IDLE_CPU=$(ps -o %cpu= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ' || true)
    echo "OK: idle daemon CPU=${IDLE_CPU:-unavailable}%"
else
    echo "SKIP: idle daemon CPU is unavailable across the Windows PID boundary"
fi

# ── Phase 4b: one-shot CLI admission churn ───────────────────────
# Every `cli` one-shot is a complete daemon client cycle (connect → commit →
# execute → close-intent → drain) against the SAME daemon the metrics sampler
# is watching. The long-lived MCP session above never exercises that path, so
# a per-admission leak in the accept/worker-finish cycle would be invisible
# without this churn — it lands in the same RSS/FD analysis below.

echo "--- Phase 4b: CLI one-shot admission churn ---"
CHURN_CYCLES=${SOAK_CLI_CHURN_CYCLES:-40}
CHURN_FAILS=0
churn_index=0
while [ "$churn_index" -lt "$CHURN_CYCLES" ]; do
    if ! CBM_CACHE_DIR="$SOAK_CACHE_DIR_VALUE" "$BINARY" cli list_projects '{}' \
        >/dev/null 2>>"$RESULTS_DIR/cli-churn-stderr.log"; then
        CHURN_FAILS=$((CHURN_FAILS + 1))
    fi
    churn_index=$((churn_index + 1))
done
if [ "$CHURN_FAILS" -gt 0 ]; then
    echo "FAIL: $CHURN_FAILS of $CHURN_CYCLES one-shot CLI churn cycles failed"
    PASS=false
else
    echo "OK: $CHURN_CYCLES one-shot CLI churn cycles recycled the live daemon"
fi
collect_snapshot

# ── Phase 5: Crash recovery test ────────────────────────────────
# Skipped in query-leak mode: crash recovery re-indexes (Phase 5 calls
# index_repository), which triggers cbm_mem_collect and would mask the #581
# query-only leak the whole run is trying to surface.

if [ "$SKIP_CRASH" != "--skip-crash-test" ] && [ "$CBM_SOAK_MODE" != "query-leak" ]; then
    echo "--- Phase 5: crash recovery ---"

    # Hand an indexing request to the frontend, then kill it without consuming
    # the response. The last-session disconnect must cancel session work and
    # let the account daemon terminate before a clean restart.
    DIAGNOSTICS_START_COUNT=$(diagnostics_start_count)
    DAEMON_STOP_COUNT=$(daemon_stop_count)
    DIAG_FILE_BEFORE_CRASH="$DIAG_FILE"
    crash_id=$QUERY_ID
    QUERY_ID=$((QUERY_ID + 1))
    echo "{\"jsonrpc\":\"2.0\",\"id\":$crash_id,\"method\":\"tools/call\",\"params\":{\"name\":\"index_repository\",\"arguments\":{\"repo_path\":$SOAK_PROJECT_JSON}}}" >&3
    sleep 0.1
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
    exec 3>&- 4<&-
    if ! wait_for_daemon_stop "$DAEMON_STOP_COUNT"; then
        echo "FAIL: last-session crash did not stop the shared daemon"
        exit 1
    fi

    # Restart server
    CBM_CACHE_DIR="$SOAK_CACHE_DIR_VALUE" CBM_DIAGNOSTICS=1 CBM_LOG_LEVEL=info \
        CBM_LOG_FORMAT=text "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" \
        2>>"$RESULTS_DIR/server-stderr.log" &
    SERVER_PID=$!
    exec 3>"$SERVER_IN"
    exec 4<"$SERVER_OUT"
    sleep 3

    if kill -0 "$SERVER_PID" 2>/dev/null; then
        if ! wait_for_diagnostics_snapshot "$DIAGNOSTICS_START_COUNT" "$DIAG_FILE_BEFORE_CRASH"; then
            echo "FAIL: frontend restart did not start a fresh daemon diagnostics generation"
            exit 1
        fi
        echo "OK: server restarted after kill -9"
        if ! mcp_initialize; then
            echo "FAIL: restarted server did not complete initialize"
            PASS=false
        fi

        # Verify clean re-index works
        mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}" || PASS=false
        if $PASS; then
            echo "OK: clean re-index after crash recovery"
        fi
    else
        echo "FAIL: server did not restart after kill -9"
        PASS=false
    fi
fi

# ── Phase 6: Shutdown + analysis ─────────────────────────────────

echo "--- Phase 6: shutdown + analysis ---"
FINAL_DAEMON_STOP_COUNT=$(daemon_stop_count)
exec 3>&-  # close server stdin → EOF → clean exit
sleep 2
exec 4<&-  # close stdout reader
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
if ! wait_for_daemon_stop "$FINAL_DAEMON_STOP_COUNT"; then
    echo "FAIL: final frontend shutdown did not stop the shared daemon"
    PASS=false
fi

# ── Analysis ─────────────────────────────────────────────────────

# Check 0: the analysis must have DATA. Snapshot collection silently
# tolerates transient misses, so a run where diagnostics never materialized
# used to sail through every check vacuously — the leak/FD/latency verdicts
# below are only meaningful if most sampling attempts actually landed.
SNAPSHOT_ROWS=$(($(wc -l < "$METRICS_CSV") - 1))
if [ "$SNAPSHOT_ATTEMPTS" -gt 0 ] && [ $((SNAPSHOT_ROWS * 10)) -lt $((SNAPSHOT_ATTEMPTS * 6)) ]; then
    echo "FAIL: only ${SNAPSHOT_ROWS}/${SNAPSHOT_ATTEMPTS} snapshots collected — analysis would be vacuous" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 1: Memory leak detection via RSS trend
# This is the primary leak detector on ALL platforms (including Windows
# where LeakSanitizer is unavailable). Catches both linear leaks (slope)
# and step-function leaks (first vs last comparison).
TOTAL_SAMPLES=$(awk -F, 'NR>1 && $3>0 { n++ } END { print n+0 }' "$METRICS_CSV")
MAX_RSS=$(awk -F, 'NR>1 && $3>0 { if ($3>max) max=$3 } END { printf "%.0f", max/1024/1024 }' "$METRICS_CSV")
FIRST_RSS=$(awk -F, 'NR==2 && $3>0 { printf "%.0f", $3/1024/1024 }' "$METRICS_CSV")
LAST_RSS=$(awk -F, '$3>0 { last=$3 } END { printf "%.0f", last/1024/1024 }' "$METRICS_CSV")
echo "RSS: first=${FIRST_RSS}MB last=${LAST_RSS}MB max=${MAX_RSS}MB (${TOTAL_SAMPLES} samples)" | tee -a "$SUMMARY"

# Absolute ceiling — catches catastrophic leaks on any run length
if [ "${MAX_RSS:-0}" -gt 200 ] 2>/dev/null; then
    echo "FAIL: RSS ${MAX_RSS}MB > 200MB ceiling" | tee -a "$SUMMARY"
    PASS=false
fi

# Slope — informational for short runs, enforced only for runs >= 30 min
# (10-min runs have too few post-warmup samples for reliable regression)
RSS_SLOPE=$(awk -F, -v skip="$((TOTAL_SAMPLES / 5))" '
NR>1 && $3>0 {
    row++
    if (row <= skip) next
    n++; x=$1; y=$3; sx+=x; sy+=y; sxx+=x*x; sxy+=x*y
}
END {
    if (n<5) { print 0; exit }
    slope = (n*sxy - sx*sy) / (n*sxx - sx*sx)
    printf "%.0f", slope * 3600 / 1024
}' "$METRICS_CSV")
echo "RSS slope (post-warmup): ${RSS_SLOPE} KB/hr" | tee -a "$SUMMARY"
if [ "$DURATION_MIN" -ge 30 ] && [ "${RSS_SLOPE:-0}" -gt 500 ] 2>/dev/null; then
    echo "FAIL: RSS slope ${RSS_SLOPE} KB/hr > 500 KB/hr" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 1b: RSS ratio (last / first) — catches step-function leaks
if [ "${FIRST_RSS:-0}" -gt 0 ] 2>/dev/null; then
    RSS_RATIO=$(awk "BEGIN { printf \"%.1f\", ${LAST_RSS} / ${FIRST_RSS} }")
    echo "RSS ratio (last/first): ${RSS_RATIO}x" | tee -a "$SUMMARY"
    if awk "BEGIN { exit (${LAST_RSS} / ${FIRST_RSS} > 3.0) ? 0 : 1 }" 2>/dev/null; then
        echo "FAIL: RSS grew ${RSS_RATIO}x (last=${LAST_RSS}MB vs first=${FIRST_RSS}MB)" | tee -a "$SUMMARY"
        PASS=false
    fi
fi

# Check 2: FD drift
FD_DRIFT=$(awk -F, 'NR>1 && $5>0 { if (!first) first=$5; last=$5 } END { print last-first }' "$METRICS_CSV")
echo "FD drift: ${FD_DRIFT:-0}" | tee -a "$SUMMARY"
if [ "${FD_DRIFT:-0}" -gt 20 ] 2>/dev/null; then
    echo "FAIL: FD drift ${FD_DRIFT} > 20" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 3: Idle daemon CPU (where the PID is host-addressable)
if [ -n "$IDLE_CPU" ]; then
    # ps %cpu may carry a locale decimal comma; strip either separator.
    IDLE_INT=$(echo "$IDLE_CPU" | cut -d. -f1 | cut -d, -f1)
    echo "Idle daemon CPU: ${IDLE_CPU}%" | tee -a "$SUMMARY"
    if [ "${IDLE_INT:-0}" -gt 5 ] 2>/dev/null; then
        echo "FAIL: idle daemon CPU ${IDLE_CPU}% > 5%" | tee -a "$SUMMARY"
        PASS=false
    fi
else
    echo "Idle daemon CPU: unavailable on Windows (not evaluated)" | tee -a "$SUMMARY"
fi

# Check 4: Max query latency (exclude index_repository — indexing is legitimately slow)
MAX_LATENCY=$(awk -F, 'NR>1 && $2!="index_repository" { if ($3>max) max=$3 } END { print max+0 }' "$LATENCY_CSV")
MAX_INDEX=$(awk -F, 'NR>1 && $2=="index_repository" { if ($3>max) max=$3 } END { print max+0 }' "$LATENCY_CSV")
echo "Max query latency: ${MAX_LATENCY}ms (index: ${MAX_INDEX}ms)" | tee -a "$SUMMARY"
# 60s threshold — MSYS2/Wine adds significant overhead to all operations
if [ "${MAX_LATENCY:-0}" -gt 60000 ] 2>/dev/null; then
    echo "FAIL: max query latency ${MAX_LATENCY}ms > 60s" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 5: Query count (sanity — should have many)
TOTAL_QUERIES=$(awk -F, 'NR>1 { n++ } END { print n+0 }' "$LATENCY_CSV")
echo "Total queries: $TOTAL_QUERIES" | tee -a "$SUMMARY"

# ── Cleanup ──────────────────────────────────────────────────────

soak_cleanup
trap - EXIT INT TERM

echo ""
if $PASS; then
    echo "=== soak-test: PASSED ===" | tee -a "$SUMMARY"
else
    echo "=== soak-test: FAILED ===" | tee -a "$SUMMARY"
    exit 1
fi
