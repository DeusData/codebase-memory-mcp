#!/usr/bin/env bash
# soak-test.sh — Endurance test for codebase-memory-mcp.
#
# Runs compressed workload cycles: queries, file mutations, reindexes, idle periods.
# Reads the randomized diagnostics snapshot path the DAEMON emits in its log as
# a diagnostics.start record (requires CBM_DIAGNOSTICS=1). The path cannot be
# derived from the frontend pid: the daemon host, not the frontend this harness
# spawns, is what writes diagnostics.
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
#   default     = mixed queries, mutations, periodic reindex, and crash recovery.
#   query-leak  = read-only query pressure without reindex/memory collection, so
#                 query-only leaks cannot be hidden by an indexing cleanup pass.
#                 It exercises search_graph, query_graph, trace_path,
#                 get_code_snippet, and search_code without invoking the
#                 mimalloc collection path in index_repository.
CBM_SOAK_MODE="${CBM_SOAK_MODE:-default}"
case "$CBM_SOAK_MODE" in
    default|query-leak) ;;
    *)
        echo "invalid CBM_SOAK_MODE: $CBM_SOAK_MODE" >&2
        exit 2
        ;;
esac

case "$DURATION_MIN" in
    ''|*[!0-9]*)
        echo "duration_minutes must be a non-negative integer" >&2
        exit 2
        ;;
esac

SOAK_RSS_MAX_MB="${CBM_SOAK_RSS_MAX_MB:-0}"
case "$SOAK_RSS_MAX_MB" in
    ''|*[!0-9]*)
        echo "CBM_SOAK_RSS_MAX_MB must be a non-negative integer" >&2
        exit 2
        ;;
esac

SOAK_IDLE_SECONDS="${CBM_SOAK_IDLE_SECONDS:-30}"
SOAK_IDLE_CPU_MAX_PERCENT="${CBM_SOAK_IDLE_CPU_MAX_PERCENT:-5}"
SOAK_RESPONSE_TIMEOUT_SECONDS="${CBM_SOAK_RESPONSE_TIMEOUT_SECONDS:-60}"
for positive_setting in "$SOAK_IDLE_SECONDS" "$SOAK_RESPONSE_TIMEOUT_SECONDS"; do
    case "$positive_setting" in
        ''|*[!0-9]*)
            echo "CBM_SOAK_IDLE_SECONDS and CBM_SOAK_RESPONSE_TIMEOUT_SECONDS must be integers" >&2
            exit 2
            ;;
    esac
done
if [ "$SOAK_IDLE_SECONDS" -eq 0 ] || [ "$SOAK_RESPONSE_TIMEOUT_SECONDS" -eq 0 ]; then
    echo "CBM_SOAK_IDLE_SECONDS and CBM_SOAK_RESPONSE_TIMEOUT_SECONDS must be positive" >&2
    exit 2
fi
case "$SOAK_IDLE_CPU_MAX_PERCENT" in
    ''|*[!0-9]*)
        echo "CBM_SOAK_IDLE_CPU_MAX_PERCENT must be a non-negative integer" >&2
        exit 2
        ;;
esac

DIAGNOSTICS_REFRESH_ATTEMPTS=20
DIAGNOSTICS_REFRESH_POLL_SECONDS=0.5
MILLISECONDS_PER_SECOND=1000
CPU_PERCENT_SCALE=100
WORKLOAD_MUTATION_INTERVAL_SECONDS="${CBM_SOAK_MUTATION_INTERVAL_SECONDS:-120}"
WORKLOAD_REINDEX_INTERVAL_SECONDS="${CBM_SOAK_REINDEX_INTERVAL_SECONDS:-120}"
DIAGNOSTICS_SAMPLE_CYCLE_INTERVAL="${CBM_SOAK_DIAGNOSTICS_SAMPLE_CYCLES:-5}"
WORKLOAD_CYCLE_SLEEP_SECONDS="${CBM_SOAK_WORKLOAD_CYCLE_SLEEP_SECONDS:-2}"
MEMORY_GROWTH_RATIO_MAX="${CBM_SOAK_MEMORY_GROWTH_RATIO_MAX:-3.0}"
RSS_SLOPE_ENFORCEMENT_MINUTES="${CBM_SOAK_RSS_SLOPE_ENFORCEMENT_MINUTES:-30}"
RSS_SLOPE_MAX_KB_PER_HOUR="${CBM_SOAK_RSS_SLOPE_MAX_KB_PER_HOUR:-500}"
FD_DRIFT_MAX="${CBM_SOAK_FD_DRIFT_MAX:-20}"
QUERY_LATENCY_MAX_MS="${CBM_SOAK_QUERY_LATENCY_MAX_MS:-60000}"

for positive_integer_setting in \
    "$WORKLOAD_MUTATION_INTERVAL_SECONDS" \
    "$WORKLOAD_REINDEX_INTERVAL_SECONDS" \
    "$DIAGNOSTICS_SAMPLE_CYCLE_INTERVAL" \
    "$WORKLOAD_CYCLE_SLEEP_SECONDS" \
    "$QUERY_LATENCY_MAX_MS"; do
    case "$positive_integer_setting" in
        ''|*[!0-9]*|0)
            echo "CBM_SOAK_* interval and limit settings must be positive integers" >&2
            exit 2
            ;;
    esac
done
for nonnegative_integer_setting in \
    "$RSS_SLOPE_ENFORCEMENT_MINUTES" \
    "$RSS_SLOPE_MAX_KB_PER_HOUR" \
    "$FD_DRIFT_MAX"; do
    case "$nonnegative_integer_setting" in
        ''|*[!0-9]*)
            echo "CBM_SOAK_* resource limit settings must be non-negative integers" >&2
            exit 2
            ;;
    esac
done
if ! awk -v ratio="$MEMORY_GROWTH_RATIO_MAX" \
    'BEGIN { exit !(ratio ~ /^[0-9]+([.][0-9]+)?$/ && ratio > 0) }'; then
    echo "CBM_SOAK_MEMORY_GROWTH_RATIO_MAX must be a positive number" >&2
    exit 2
fi

# CBM_SOAK_RESULTS_DIR is the canonical namespaced setting. Retain RESULTS_DIR
# as a compatibility fallback for existing CI and direct harness callers.
RESULTS_DIR="${CBM_SOAK_RESULTS_DIR:-${RESULTS_DIR:-soak-results}}"
mkdir -p "$RESULTS_DIR"

METRICS_CSV="$RESULTS_DIR/metrics.csv"
LATENCY_CSV="$RESULTS_DIR/latency.csv"
SUMMARY="$RESULTS_DIR/summary.txt"
SERVER_STDERR="$RESULTS_DIR/server-stderr.log"

echo "timestamp,uptime_s,rss_bytes,heap_committed,fd_count,query_count,query_max_us,cache_bytes,db_bytes,wal_bytes" > "$METRICS_CSV"
echo "timestamp,tool,duration_ms,exit_code" > "$LATENCY_CSV"
: > "$SUMMARY"
: > "$SERVER_STDERR"

DURATION_S=$((DURATION_MIN * 60))
PASS=true

SOAK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/cbm-soak-XXXXXX")
SOAK_PROJECT="$SOAK_ROOT/project"
SERVER_IN="$SOAK_ROOT/server.in"
SERVER_OUT="$SOAK_ROOT/server.out"
SOAK_CACHE="$SOAK_ROOT/cache"
MCP_SOAK_PROJECT="$SOAK_PROJECT"
CBM_CACHE_DIR="$SOAK_CACHE"
mkdir -p "$SOAK_PROJECT" "$SOAK_CACHE"

SERVER_PID=""
DIAG_FILE=""
DIAG_FILES=()
FDS_OPEN=false
SOAK_CLEANED=false

# Child-facing spellings of the two paths. Keyed off the BINARY being a .exe
# rather than off the host OS, so a Windows binary run under Wine on Linux or
# macOS still receives drive-letter paths while this Bash harness keeps the host
# form for its own file and Git operations.
if [[ "$BINARY" == *.exe ]] && command -v winepath >/dev/null 2>&1; then
    MCP_SOAK_PROJECT=$(winepath -w "$SOAK_PROJECT")
    MCP_SOAK_PROJECT=${MCP_SOAK_PROJECT//\\//}
    CBM_CACHE_DIR=$(winepath -w "$SOAK_CACHE")
elif [[ "$BINARY" == *.exe ]] && command -v cygpath >/dev/null 2>&1; then
    MCP_SOAK_PROJECT=$(cygpath -m "$SOAK_PROJECT")
    CBM_CACHE_DIR=$(cygpath -m "$SOAK_CACHE")
else
    case "$(uname -s)" in
        MINGW*|MSYS*|CYGWIN*)
            MCP_SOAK_PROJECT=$(cygpath -m "$SOAK_PROJECT")
            CBM_CACHE_DIR=$(cygpath -m "$SOAK_CACHE")
            ;;
    esac
fi

# Every JSON request uses one pre-escaped spelling of the project path, so a
# path containing a quote or backslash cannot corrupt the request.
SOAK_PROJECT_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$MCP_SOAK_PROJECT")

# Diagnostics are written by the DAEMON HOST, not by the frontend process this
# harness spawns, so the path cannot be derived from $SERVER_PID. The daemon
# emits it in its log as a diagnostics.start control record; refresh_diagnostics_paths()
# below reads it from there.
DAEMON_LOG="$SOAK_CACHE/logs/cbm-daemon.log"
export CBM_CACHE_DIR

close_server_fds() {
    if $FDS_OPEN; then
        exec 3>&- 4<&-
        FDS_OPEN=false
    fi
}

stop_server() {
    close_server_fds
    # Numeric guard: a non-numeric SERVER_PID would make kill target a job spec.
    if [[ ! "${SERVER_PID:-}" =~ ^[0-9]+$ ]]; then
        SERVER_PID=""
        return 0
    fi
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        local waited=0
        # Diagnostics shutdown may wait for its five-second writer interval.
        while kill -0 "$SERVER_PID" 2>/dev/null && [ "$waited" -lt 70 ]; do
            sleep 0.1
            waited=$((waited + 1))
        done
    fi
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "$SERVER_PID" ]; then
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

cleanup_runtime() {
    # Idempotent: EXIT and INT/TERM can both fire, and running the teardown twice
    # would kill a pid this run no longer owns.
    if $SOAK_CLEANED; then
        return 0
    fi
    SOAK_CLEANED=true
    set +e
    stop_server
    rm -f "$SERVER_IN" "$SERVER_OUT"
    for diag_file in "${DIAG_FILES[@]}"; do
        rm -f "$diag_file" "$diag_file.tmp"
    done
    # Preserve the daemon log as run evidence before the cache root goes away;
    # without it a failed soak leaves nothing to diagnose from.
    if [ -f "$DAEMON_LOG" ]; then
        cp "$DAEMON_LOG" "$RESULTS_DIR/cbm-daemon.log" 2>/dev/null || true
    fi
    rm -rf "$SOAK_ROOT"
    return 0
}

trap cleanup_runtime EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Convert writes to a closed server pipe into ordinary command failures. Each
# protocol write is checked below so a dead server is reported without the
# harness itself being terminated by SIGPIPE.
trap '' PIPE
echo "=== soak-test: binary=$BINARY duration=${DURATION_MIN}m mode=${CBM_SOAK_MODE} ==="

# ── Helper: generate realistic test project (~200 files) ─────────

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
LAST_MCP_RESPONSE=""

now_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

# Upstream main's form replaces this harness's response_is_success. Substring
# matching on '"error":' false-negates whenever that text appears inside a
# legitimate result payload (a search hit quoting it, for instance), and it
# cannot tell that a response arrived for a DIFFERENT request -- which matters
# on this FIFO where one dropped reply desynchronizes every later read. Parsing
# the JSON and matching the id fixes both.
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

# Send a JSON-RPC tool call to the running server via its stdin pipe.
# Reads and validates the response from server stdout. Records latency and
# returns nonzero for write/read timeout, JSON-RPC error, or MCP isError.
mcp_call() {
    local tool="$1"
    local args="$2"
    local id=$QUERY_ID
    QUERY_ID=$((QUERY_ID + 1))

    local req="{\"jsonrpc\":\"2.0\",\"id\":$id,\"method\":\"tools/call\",\"params\":{\"name\":\"$tool\",\"arguments\":$args}}"
    local t0
    t0=$(now_ms)

    if ! printf '%s\n' "$req" >&3; then
        echo "$(date +%s),$tool,0,1" >> "$LATENCY_CSV"
        echo "FAIL: $tool request write failed" >&2
        return 1
    fi

    local resp=""
    local status=1
    if read -r -t "$SOAK_RESPONSE_TIMEOUT_SECONDS" resp <&4 2>/dev/null &&
        json_rpc_response_ok "$id" "$resp"; then
        status=0
    fi
    LAST_MCP_RESPONSE="$resp"

    local t1
    t1=$(now_ms)
    local dur=$((t1 - t0))
    echo "$(date +%s),$tool,$dur,$status" >> "$LATENCY_CSV"
    if [ "$status" -ne 0 ]; then
        echo "FAIL: $tool returned no successful MCP response: $(printf '%s' "$resp" | cut -c1-300)" >&2
        return 1
    fi
    return 0
}

run_mcp_call() {
    if ! mcp_call "$@"; then
        PASS=false
    fi
}

require_server_running() {
    local phase_name="$1"
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        return 0
    fi
    echo "FAIL: server exited during $phase_name" >&2
    return 1
}

mcp_initialize() {
    local request
    request='{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"capabilities":{}}}'
    if ! printf '%s\n' "$request" >&3; then
        return 1
    fi
    local response=""
    read -r -t 10 response <&4 2>/dev/null && json_rpc_response_ok 0 "$response"
}

# ── Helper: collect diagnostics snapshot ─────────────────────────

# Diagnostics discovery, carried over from upstream main and REQUIRED by the
# daemon architecture: the daemon host writes the snapshot, not the frontend
# this harness spawns, and its path is randomized. Deriving it from $SERVER_PID
# (as this harness previously did) points at the wrong process, so DIAG_FILE
# never exists, every snapshot records nothing, and the whole soak analysis runs
# on an empty CSV while reporting success.

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
    # Re-resolve the daemon-emitted path on every call: a daemon restart (Phase 5
    # crash recovery) publishes a new randomized snapshot path.
    refresh_diagnostics_paths || return 1
    if [ -n "$DIAG_FILE" ] && [ -f "$DIAG_FILE" ]; then
        local diag_values
        if ! diag_values=$(python3 -c "
import json
d = json.load(__import__('sys').stdin)
# Use heap_committed if available, otherwise RSS (mimalloc may report 0 for committed)
mem = d.get('heap_committed_bytes', 0)
if mem == 0: mem = d.get('rss_bytes', 0)
print(f\"{d.get('uptime_s',0)},{d.get('rss_bytes',0)},{mem},{d.get('fd_count',0)},{d.get('query_count',0)},{d.get('query_max_us',0)}\")
" < "$DIAG_FILE" 2>/dev/null); then
            return 1
        fi

        local cache_kb db_kb wal_kb
        cache_kb=$(du -sk "$SOAK_CACHE" 2>/dev/null | awk '{print $1+0}')
        db_kb=$(find "$SOAK_CACHE" -type f -name '*.db' -exec du -k {} + 2>/dev/null |
            awk '{total += $1} END {print total+0}')
        wal_kb=$(find "$SOAK_CACHE" -type f -name '*.db-wal' -exec du -k {} + 2>/dev/null |
            awk '{total += $1} END {print total+0}')
        echo "$(date +%s),$diag_values,$((cache_kb * 1024)),$((db_kb * 1024)),$((wal_kb * 1024))" \
            >> "$METRICS_CSV"
        return 0
    fi
    # Fail loud. Upstream's version returned 0 when the path could not be
    # resolved, which made every `if ! collect_snapshot` guard dead and let a run
    # that recorded no samples at all report success.
    return 1
}

read_idle_cpu_sample() {
    if [ -z "$DIAG_FILE" ] || [ ! -f "$DIAG_FILE" ]; then
        return 1
    fi
    python3 -c '
import json, sys
d = json.load(sys.stdin)
uptime_s = d.get("uptime_s")
user_cpu_ms = d.get("process_user_cpu_ms")
system_cpu_ms = d.get("process_system_cpu_ms")
if not all(isinstance(value, int) and value >= 0
           for value in (uptime_s, user_cpu_ms, system_cpu_ms)):
    raise SystemExit(1)
print(f"{uptime_s},{user_cpu_ms + system_cpu_ms}")
' < "$DIAG_FILE" 2>/dev/null
}

# Upstream main's form replaces this harness's extract_index_project: it skips
# non-JSON content items (the update-available banner) instead of raising on
# them, and type-checks every level. The old version aborted on a healthy run
# whenever a banner preceded the summary JSON.
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

start_server() {
    # This harness drives explicit index_repository calls against its isolated
    # fixture. Disable the product's configured session-CWD auto-index so an
    # unrelated checkout cannot contend for the global pipeline lock or pollute
    # the soak cache and idle-resource measurements.
    # CBM_LOG_LEVEL/CBM_LOG_FORMAT are required, not cosmetic: the daemon's
    # diagnostics.start record is how this harness discovers the snapshot path,
    # and it must reach $DAEMON_LOG in a form refresh_diagnostics_paths can read.
    local snapshots_before
    snapshots_before=$(diagnostics_start_count)
    CBM_AUTO_INDEX=false CBM_DIAGNOSTICS=1 CBM_LOG_LEVEL=info CBM_LOG_FORMAT=text \
        "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" 2>>"$SERVER_STDERR" &
    SERVER_PID=$!

    exec 3>"$SERVER_IN"
    exec 4<"$SERVER_OUT"
    FDS_OPEN=true
    sleep 3

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "FAIL: server did not start" >&2
        return 1
    fi
    # The daemon owns diagnostics; wait for it to publish a snapshot path rather
    # than guessing one from the frontend's pid.
    # $1, when given, is the snapshot path seen BEFORE a restart: requiring a
    # different one proves a fresh daemon generation rather than a stale reattach.
    if ! wait_for_diagnostics_snapshot "${snapshots_before:-0}" "${1:-}"; then
        echo "FAIL: daemon did not emit a usable diagnostics.start path" >&2
        return 1
    fi
    DIAG_FILES+=("$DIAG_FILE")
    if ! mcp_initialize; then
        echo "FAIL: server initialize did not return a successful response" >&2
        return 1
    fi
    echo "OK: server running and initialized (pid=$SERVER_PID)"
    return 0
}

# ── Phase 1: Start MCP server with diagnostics ──────────────────

echo "--- Phase 1: start server ---"
# Bidirectional pipes: fd3 = server stdin (write), fd4 = server stdout (read)
mkfifo "$SERVER_IN" "$SERVER_OUT"

if ! start_server; then
    exit 1
fi

# ── Phase 2: Initial index ───────────────────────────────────────

echo "--- Phase 2: initial index ---"
if ! mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}"; then
    exit 1
fi
sleep 6  # wait for diagnostics write
if ! require_server_running "initial indexing"; then
    exit 1
fi
if ! collect_snapshot; then
    echo "FAIL: initial diagnostics snapshot was unavailable" >&2
    exit 1
fi

# Use the product's returned project slug rather than duplicating its path
# normalization in this cross-platform harness.
if ! PROJ_NAME=$(mcp_response_project "$LAST_MCP_RESPONSE"); then
    echo "FAIL: initial index response did not contain a project slug" >&2
    exit 1
fi

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
        # Read-only pressure: never mutate or reindex, because indexing invokes
        # memory collection and could hide a query-only leak.
        run_mcp_call search_graph "{\"project\":\"$PROJ_NAME\",\"name_pattern\":\".*Handle.*\"}"
        run_mcp_call query_graph "{\"project\":\"$PROJ_NAME\",\"query\":\"MATCH (n) RETURN n.name LIMIT 25\"}"
        run_mcp_call trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}"
        run_mcp_call get_code_snippet "{\"project\":\"$PROJ_NAME\",\"qualified_name\":\"handle_1\"}"
        run_mcp_call search_code "{\"project\":\"$PROJ_NAME\",\"pattern\":\"def \"}"
    else
        # Trace a symbol generated above. The former "compute" fixture did not
        # exist, so trace_path correctly returned an MCP error and made every
        # soak cycle fail without exercising traversal or its resource lifecycle.
        run_mcp_call search_graph "{\"project\":\"$PROJ_NAME\",\"name_pattern\":\".*handle_1.*\"}"
        run_mcp_call trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}"

        # File mutation every 2 minutes.
        if [ $((NOW - LAST_MUTATE)) -ge "$WORKLOAD_MUTATION_INTERVAL_SECONDS" ]; then
            echo "# mutation at cycle $CYCLE $(date)" >> "$SOAK_PROJECT/src/main.py"
            git -C "$SOAK_PROJECT" add -A 2>/dev/null
            git -C "$SOAK_PROJECT" -c user.email=test@test -c user.name=test \
                commit -q -m "cycle $CYCLE" 2>/dev/null || true
            LAST_MUTATE=$NOW
        fi

        # Full reindex every 2 minutes (compressed — simulates 15min real interval).
        if [ $((NOW - LAST_REINDEX)) -ge "$WORKLOAD_REINDEX_INTERVAL_SECONDS" ]; then
            run_mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}"
            LAST_REINDEX=$NOW
        fi
    fi

    # Collect diagnostics every 10 seconds (5 cycles)
    if [ $((CYCLE % DIAGNOSTICS_SAMPLE_CYCLE_INTERVAL)) -eq 0 ]; then
        if ! collect_snapshot; then
            echo "FAIL: diagnostics snapshot was unavailable" >&2
            PASS=false
        fi
    fi

    sleep "$WORKLOAD_CYCLE_SLEEP_SECONDS"
done

# ── Phase 4: Idle period + final snapshot ────────────────────────

echo "--- Phase 4: idle (${SOAK_IDLE_SECONDS}s) ---"
IDLE_CPU="0"
IDLE_OBSERVED_SECONDS=0
IDLE_START_SAMPLE=""
if ! IDLE_START_SAMPLE=$(read_idle_cpu_sample); then
    echo "FAIL: idle CPU baseline diagnostics were unavailable" >&2
    PASS=false
fi

sleep "$SOAK_IDLE_SECONDS"

IDLE_END_SAMPLE=""
if [ -n "$IDLE_START_SAMPLE" ]; then
    IDLE_START_UPTIME=${IDLE_START_SAMPLE%%,*}
    IDLE_START_CPU_MS=${IDLE_START_SAMPLE#*,}
    IDLE_TARGET_UPTIME=$((IDLE_START_UPTIME + SOAK_IDLE_SECONDS))
    for ((attempt = 0; attempt < DIAGNOSTICS_REFRESH_ATTEMPTS; attempt++)); do
        if candidate_sample=$(read_idle_cpu_sample); then
            candidate_uptime=${candidate_sample%%,*}
            if [ "$candidate_uptime" -ge "$IDLE_TARGET_UPTIME" ]; then
                IDLE_END_SAMPLE=$candidate_sample
                break
            fi
        fi
        sleep "$DIAGNOSTICS_REFRESH_POLL_SECONDS"
    done
fi

if [ -z "$IDLE_END_SAMPLE" ]; then
    echo "FAIL: idle CPU completion diagnostics were unavailable" >&2
    PASS=false
else
    IDLE_END_UPTIME=${IDLE_END_SAMPLE%%,*}
    IDLE_END_CPU_MS=${IDLE_END_SAMPLE#*,}
    IDLE_OBSERVED_SECONDS=$((IDLE_END_UPTIME - IDLE_START_UPTIME))
    IDLE_PROCESS_CPU_MS=$((IDLE_END_CPU_MS - IDLE_START_CPU_MS))
    if [ "$IDLE_OBSERVED_SECONDS" -le 0 ] || [ "$IDLE_PROCESS_CPU_MS" -lt 0 ]; then
        echo "FAIL: idle CPU diagnostics were not monotonic" >&2
        PASS=false
    else
        IDLE_CPU=$(awk -v cpu_ms="$IDLE_PROCESS_CPU_MS" \
            -v wall_s="$IDLE_OBSERVED_SECONDS" \
            -v ms_per_s="$MILLISECONDS_PER_SECOND" \
            -v percent_scale="$CPU_PERCENT_SCALE" \
            'BEGIN { printf "%.1f", (cpu_ms / (wall_s * ms_per_s)) * percent_scale }')
    fi
fi

if ! collect_snapshot; then
    echo "FAIL: final diagnostics snapshot was unavailable" >&2
    PASS=false
fi

echo "OK: idle CPU=${IDLE_CPU}% over ${IDLE_OBSERVED_SECONDS}s"

# ── Phase 4b: one-shot CLI admission churn ───────────────────────
# Carried over from upstream main. Every `cli` one-shot is a complete daemon
# client cycle (connect, commit, execute, close-intent, drain) against the SAME
# daemon the metrics sampler is watching. The long-lived MCP session above never
# exercises that path, so a per-admission leak in the accept/worker-finish cycle
# would be invisible without this churn; it lands in the same RSS/FD analysis.

echo "--- Phase 4b: CLI one-shot admission churn ---"
CHURN_CYCLES="${CBM_SOAK_CLI_CHURN_CYCLES:-${SOAK_CLI_CHURN_CYCLES:-40}}"
case "$CHURN_CYCLES" in
    ''|*[!0-9]*)
        echo "CBM_SOAK_CLI_CHURN_CYCLES must be a non-negative integer" >&2
        exit 2
        ;;
esac
CHURN_FAILS=0
churn_index=0
while [ "$churn_index" -lt "$CHURN_CYCLES" ]; do
    if ! CBM_CACHE_DIR="$CBM_CACHE_DIR" "$BINARY" cli list_projects '{}' \
        >/dev/null 2>>"$RESULTS_DIR/cli-churn-stderr.log"; then
        CHURN_FAILS=$((CHURN_FAILS + 1))
    fi
    churn_index=$((churn_index + 1))
done
if [ "$CHURN_FAILS" -gt 0 ]; then
    echo "FAIL: $CHURN_FAILS of $CHURN_CYCLES one-shot CLI churn cycles failed" | tee -a "$SUMMARY"
    PASS=false
else
    echo "OK: $CHURN_CYCLES one-shot CLI churn cycles recycled the live daemon"
fi
if ! collect_snapshot; then
    echo "FAIL: diagnostics snapshot unavailable after CLI churn" | tee -a "$SUMMARY"
    PASS=false
fi

# ── Phase 5: Crash recovery test ────────────────────────────────
# Skipped in query-leak mode: crash recovery re-indexes (Phase 5 calls
# index_repository), which triggers cbm_mem_collect and would mask the #581
# query-only leak the whole run is trying to surface.

if [ "$SKIP_CRASH" != "--skip-crash-test" ] && [ "$CBM_SOAK_MODE" != "query-leak" ]; then
    echo "--- Phase 5: crash recovery ---"

    # Send an index request without waiting for its response, then kill the
    # process while the request may be active. The post-restart checked reindex
    # is the recovery oracle.
    echo "# crash recovery mutation $(date)" >> "$SOAK_PROJECT/src/main.py"
    # Baselines for the two daemon-lifecycle assertions below, carried over from
    # upstream main. Killing the frontend is only half the contract: the LAST
    # session disconnecting must also let the shared account daemon terminate,
    # and the restart must start a FRESH diagnostics generation rather than
    # reattaching to the old one. Neither is observable without these counters.
    DAEMON_STOP_COUNT=$(daemon_stop_count)
    DIAG_FILE_BEFORE_CRASH="$DIAG_FILE"
    CRASH_REQUEST_ID=$QUERY_ID
    QUERY_ID=$((QUERY_ID + 1))
    CRASH_REQUEST="{\"jsonrpc\":\"2.0\",\"id\":$CRASH_REQUEST_ID,\"method\":\"tools/call\",\"params\":{\"name\":\"index_repository\",\"arguments\":{\"repo_path\":$SOAK_PROJECT_JSON}}}"
    if ! printf '%s\n' "$CRASH_REQUEST" >&3; then
        echo "FAIL: crash-recovery index request write failed" >&2
        PASS=false
    fi
    sleep 0.1
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    close_server_fds
    SERVER_PID=""

    if ! wait_for_daemon_stop "${DAEMON_STOP_COUNT:-0}"; then
        echo "FAIL: last-session crash did not stop the shared daemon" | tee -a "$SUMMARY"
        PASS=false
    fi

    if start_server "$DIAG_FILE_BEFORE_CRASH"; then
        if mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}"; then
            echo "OK: server restarted and checked reindex passed after kill -9"
        else
            echo "FAIL: checked reindex failed after crash recovery"
            PASS=false
        fi
    else
        echo "FAIL: server did not restart and initialize after kill -9"
        PASS=false
    fi
fi

# ── Phase 6: Shutdown + analysis ─────────────────────────────────

echo "--- Phase 6: shutdown + analysis ---"
# The shared account daemon must terminate when its last frontend goes away.
# Without this assertion a daemon that outlives every session -- leaking its
# mapped generations, logs and worker threads -- passes the soak silently.
FINAL_DAEMON_STOP_COUNT=$(daemon_stop_count)
stop_server
if ! wait_for_daemon_stop "${FINAL_DAEMON_STOP_COUNT:-0}"; then
    echo "FAIL: final frontend shutdown did not stop the shared daemon" | tee -a "$SUMMARY"
    PASS=false
fi

# ── Analysis ─────────────────────────────────────────────────────

if [ ! -s "$METRICS_CSV" ] || [ "$(wc -l < "$METRICS_CSV")" -lt 2 ]; then
    echo "FAIL: no diagnostics samples were recorded" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 0 (from upstream main): the analysis must have ENOUGH data, not merely
# some. Snapshot collection tolerates transient misses, so a run where
# diagnostics mostly failed to materialize would sail through every leak, FD and
# latency verdict below vacuously. Require at least 60% of sampling attempts to
# have landed. This is strictly stronger than the "any rows at all" check above,
# which one lucky sample satisfies.
SNAPSHOT_ROWS=$(($(wc -l < "$METRICS_CSV") - 1))
if [ "${SNAPSHOT_ATTEMPTS:-0}" -gt 0 ] && [ $((SNAPSHOT_ROWS * 10)) -lt $((SNAPSHOT_ATTEMPTS * 6)) ]; then
    echo "FAIL: only ${SNAPSHOT_ROWS}/${SNAPSHOT_ATTEMPTS} snapshots collected — analysis would be vacuous" \
        | tee -a "$SUMMARY"
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

# An absolute ceiling is meaningful only when the caller has a measured
# platform/workload budget. Zero (the default) disables this optional policy;
# relative growth and long-run slope remain mandatory below.
if [ "$SOAK_RSS_MAX_MB" -gt 0 ] && [ "${MAX_RSS:-0}" -gt "$SOAK_RSS_MAX_MB" ] \
    2>/dev/null; then
    echo "FAIL: RSS ${MAX_RSS}MB > ${SOAK_RSS_MAX_MB}MB ceiling" | tee -a "$SUMMARY"
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
if [ "$DURATION_MIN" -ge "$RSS_SLOPE_ENFORCEMENT_MINUTES" ] && \
    [ "${RSS_SLOPE:-0}" -gt "$RSS_SLOPE_MAX_KB_PER_HOUR" ] 2>/dev/null; then
    echo "FAIL: RSS slope ${RSS_SLOPE} KB/hr > ${RSS_SLOPE_MAX_KB_PER_HOUR} KB/hr" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 1b: RSS ratio (last / first) — catches step-function leaks
if [ "${FIRST_RSS:-0}" -gt 0 ] 2>/dev/null; then
    RSS_RATIO=$(awk "BEGIN { printf \"%.1f\", ${LAST_RSS} / ${FIRST_RSS} }")
    MAX_RSS_RATIO=$(awk "BEGIN { printf \"%.1f\", ${MAX_RSS} / ${FIRST_RSS} }")
    echo "RSS ratio: last/first=${RSS_RATIO}x max/first=${MAX_RSS_RATIO}x" \
        | tee -a "$SUMMARY"
    if awk -v ceiling="$MEMORY_GROWTH_RATIO_MAX" \
        "BEGIN { exit (${LAST_RSS} / ${FIRST_RSS} > ceiling) ? 0 : 1 }" 2>/dev/null; then
        echo "FAIL: RSS grew ${RSS_RATIO}x (last=${LAST_RSS}MB vs first=${FIRST_RSS}MB)" | tee -a "$SUMMARY"
        PASS=false
    fi
    if awk -v ceiling="$MEMORY_GROWTH_RATIO_MAX" \
        "BEGIN { exit (${MAX_RSS} / ${FIRST_RSS} > ceiling) ? 0 : 1 }" 2>/dev/null; then
        echo "FAIL: peak RSS grew ${MAX_RSS_RATIO}x above post-index baseline" | tee -a "$SUMMARY"
        PASS=false
    fi
fi

# Check 2: cache/database/WAL growth after the initial indexed baseline.
FIRST_CACHE_BYTES=$(awk -F, 'NR==2 {print $8+0}' "$METRICS_CSV")
LAST_CACHE_BYTES=$(awk -F, 'NR>1 {last=$8} END {print last+0}' "$METRICS_CSV")
MAX_CACHE_BYTES=$(awk -F, 'NR>1 && $8>max {max=$8} END {print max+0}' "$METRICS_CSV")
LAST_DB_BYTES=$(awk -F, 'NR>1 {last=$9} END {print last+0}' "$METRICS_CSV")
MAX_WAL_BYTES=$(awk -F, 'NR>1 && $10>max {max=$10} END {print max+0}' "$METRICS_CSV")
echo "Storage: cache first=${FIRST_CACHE_BYTES}B last=${LAST_CACHE_BYTES}B max=${MAX_CACHE_BYTES}B; db last=${LAST_DB_BYTES}B; WAL max=${MAX_WAL_BYTES}B" \
    | tee -a "$SUMMARY"

if [ "${FIRST_CACHE_BYTES:-0}" -gt 0 ] 2>/dev/null; then
    CACHE_RATIO=$(awk "BEGIN { printf \"%.2f\", ${LAST_CACHE_BYTES} / ${FIRST_CACHE_BYTES} }")
    echo "Cache ratio (last/first): ${CACHE_RATIO}x" | tee -a "$SUMMARY"
    if awk -v ceiling="$MEMORY_GROWTH_RATIO_MAX" \
        "BEGIN { exit (${LAST_CACHE_BYTES} / ${FIRST_CACHE_BYTES} > ceiling) ? 0 : 1 }" \
        2>/dev/null; then
        echo "FAIL: cache grew ${CACHE_RATIO}x after initial index" | tee -a "$SUMMARY"
        PASS=false
    fi
fi

CACHE_SLOPE=$(awk -F, -v skip="$((TOTAL_SAMPLES / 5))" '
NR>1 && $8>=0 {
    row++
    if (row <= skip) next
    n++; x=$1; y=$8; sx+=x; sy+=y; sxx+=x*x; sxy+=x*y
}
END {
    if (n<5 || n*sxx == sx*sx) { print 0; exit }
    slope = (n*sxy - sx*sy) / (n*sxx - sx*sx)
    printf "%.0f", slope * 3600 / 1024
}' "$METRICS_CSV")
echo "Cache slope (post-warmup): ${CACHE_SLOPE} KB/hr" | tee -a "$SUMMARY"

# Check 3: FD drift
FD_DRIFT=$(awk -F, 'NR>1 && $5>0 { if (!first) first=$5; last=$5 } END { print last-first }' "$METRICS_CSV")
echo "FD drift: ${FD_DRIFT:-0}" | tee -a "$SUMMARY"
if [ "${FD_DRIFT:-0}" -gt "$FD_DRIFT_MAX" ] 2>/dev/null; then
    echo "FAIL: FD drift ${FD_DRIFT} > ${FD_DRIFT_MAX}" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 4: Idle CPU measured from process CPU-time deltas across the configured
# idle interval. A lifetime ps %cpu sample incorrectly includes initial indexing.
echo "Idle CPU: ${IDLE_CPU}% over ${IDLE_OBSERVED_SECONDS}s" | tee -a "$SUMMARY"
if awk -v measured="$IDLE_CPU" -v ceiling="$SOAK_IDLE_CPU_MAX_PERCENT" \
    'BEGIN { exit measured > ceiling ? 0 : 1 }' 2>/dev/null; then
    echo "FAIL: idle CPU ${IDLE_CPU}% > ${SOAK_IDLE_CPU_MAX_PERCENT}%" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 5: Max query latency (exclude index_repository — indexing is legitimately slow)
MAX_LATENCY=$(awk -F, 'NR>1 && $2!="index_repository" { if ($3>max) max=$3 } END { print max+0 }' "$LATENCY_CSV")
MAX_INDEX=$(awk -F, 'NR>1 && $2=="index_repository" { if ($3>max) max=$3 } END { print max+0 }' "$LATENCY_CSV")
echo "Max query latency: ${MAX_LATENCY}ms (index: ${MAX_INDEX}ms)" | tee -a "$SUMMARY"
# 60s threshold — MSYS2/Wine adds significant overhead to all operations
if [ "${MAX_LATENCY:-0}" -gt "$QUERY_LATENCY_MAX_MS" ] 2>/dev/null; then
    echo "FAIL: max query latency ${MAX_LATENCY}ms > ${QUERY_LATENCY_MAX_MS}ms" | tee -a "$SUMMARY"
    PASS=false
fi

# Check 6: Query count and failures.
TOTAL_QUERIES=$(awk -F, 'NR>1 { n++ } END { print n+0 }' "$LATENCY_CSV")
FAILED_QUERIES=$(awk -F, 'NR>1 && $4!=0 { n++ } END { print n+0 }' "$LATENCY_CSV")
echo "Total queries: $TOTAL_QUERIES (failed: $FAILED_QUERIES)" | tee -a "$SUMMARY"
if [ "$TOTAL_QUERIES" -eq 0 ] || [ "$FAILED_QUERIES" -ne 0 ]; then
    echo "FAIL: MCP workload did not complete successfully" | tee -a "$SUMMARY"
    PASS=false
fi

# ── Cleanup ──────────────────────────────────────────────────────

cleanup_runtime
trap - EXIT INT TERM
if [ -e "$SOAK_ROOT" ]; then
    echo "FAIL: soak work root was not removed: $SOAK_ROOT" | tee -a "$SUMMARY"
    PASS=false
else
    echo "Cleanup: removed isolated project/cache/FIFO root" | tee -a "$SUMMARY"
fi

echo ""
if $PASS; then
    echo "=== soak-test: PASSED ===" | tee -a "$SUMMARY"
else
    echo "=== soak-test: FAILED ===" | tee -a "$SUMMARY"
    exit 1
fi
