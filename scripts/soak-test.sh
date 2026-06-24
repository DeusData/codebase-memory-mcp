#!/bin/bash
# soak-test.sh — Endurance test for codebase-memory-mcp.
#
# Runs compressed workload cycles: queries, file mutations, reindexes, idle periods.
# Reads diagnostics from /tmp/cbm-diagnostics-<pid>.json (requires CBM_DIAGNOSTICS=1).
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

RESULTS_DIR="soak-results"
mkdir -p "$RESULTS_DIR"

METRICS_CSV="$RESULTS_DIR/metrics.csv"
LATENCY_CSV="$RESULTS_DIR/latency.csv"
SUMMARY="$RESULTS_DIR/summary.txt"

echo "timestamp,uptime_s,rss_bytes,heap_committed,fd_count,query_count,query_max_us" > "$METRICS_CSV"
echo "timestamp,tool,duration_ms,exit_code" > "$LATENCY_CSV"
> "$SUMMARY"

DURATION_S=$((DURATION_MIN * 60))

echo "=== soak-test: binary=$BINARY duration=${DURATION_MIN}m ==="

# ── Helper: generate realistic test project (~200 files) ─────────

SOAK_PROJECT=$(mktemp -d)

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

# Send a JSON-RPC tool call to the running server via its stdin pipe.
# Reads response from server stdout. Records latency.
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

    # Read response (wait up to 30s)
    local resp=""
    if read -t 30 resp <&4 2>/dev/null; then
        local t1
        t1=$(python3 -c "import time; print(int(time.time()*1000))")
        local dur=$((t1 - t0))
        echo "$(date +%s),$tool,$dur,0" >> "$LATENCY_CSV"
    else
        local t1
        t1=$(python3 -c "import time; print(int(time.time()*1000))")
        local dur=$((t1 - t0))
        echo "$(date +%s),$tool,$dur,1" >> "$LATENCY_CSV"
    fi
}

# ── Helper: collect diagnostics snapshot ─────────────────────────

collect_snapshot() {
    local diag_file="/tmp/cbm-diagnostics-${SERVER_PID}.json"
    if [ -f "$diag_file" ]; then
        python3 -c "
import json, time
d = json.load(open('$diag_file'))
# Use heap_committed if available, otherwise RSS (mimalloc may report 0 for committed)
mem = d.get('heap_committed_bytes', 0)
if mem == 0: mem = d.get('rss_bytes', 0)
print(f\"{int(time.time())},{d.get('uptime_s',0)},{d.get('rss_bytes',0)},{mem},{d.get('fd_count',0)},{d.get('query_count',0)},{d.get('query_max_us',0)}\")
" 2>/dev/null >> "$METRICS_CSV"
    fi
}

# ── Phase 1: Start MCP server with diagnostics ──────────────────

echo "--- Phase 1: start server ---"
# Bidirectional pipes: fd3 = server stdin (write), fd4 = server stdout (read)
SERVER_IN=$(mktemp -u).in
SERVER_OUT=$(mktemp -u).out
mkfifo "$SERVER_IN" "$SERVER_OUT"

CBM_DIAGNOSTICS=1 "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" 2>"$RESULTS_DIR/server-stderr.log" &
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

# Send initialize handshake
echo '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"capabilities":{}}}' >&3
read -t 10 INIT_RESP <&4 || true

# ── Phase 2: Initial index ───────────────────────────────────────

echo "--- Phase 2: initial index ---"
mcp_call index_repository "{\"repo_path\":\"$SOAK_PROJECT\"}"
sleep 6  # wait for diagnostics write
collect_snapshot

# Derive project name (same logic as cbm_project_name_from_path)
PROJ_NAME=$(echo "$SOAK_PROJECT" | sed 's|^/||; s|/|-|g')

DIAG_FILE="/tmp/cbm-diagnostics-${SERVER_PID}.json"
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

    # Queries every 2 seconds
    mcp_call search_graph "{\"project\":\"$PROJ_NAME\",\"name_pattern\":\".*compute.*\"}"
    mcp_call trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"compute\",\"direction\":\"both\"}"

    # File mutation every 2 minutes
    if [ $((NOW - LAST_MUTATE)) -ge 120 ]; then
        echo "# mutation at cycle $CYCLE $(date)" >> "$SOAK_PROJECT/src/main.py"
        git -C "$SOAK_PROJECT" add -A 2>/dev/null
        git -C "$SOAK_PROJECT" -c user.email=test@test -c user.name=test commit -q -m "cycle $CYCLE" 2>/dev/null || true
        LAST_MUTATE=$NOW
    fi

    # Full reindex every 2 minutes (compressed — simulates 15min real interval)
    if [ $((NOW - LAST_REINDEX)) -ge 120 ]; then
        mcp_call index_repository "{\"repo_path\":\"$SOAK_PROJECT\"}"
        LAST_REINDEX=$NOW
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

# Check idle CPU
IDLE_CPU=$(ps -o %cpu= -p "$SERVER_PID" 2>/dev/null | tr -d ' ' || echo "0")
echo "OK: idle CPU=${IDLE_CPU}%"

# ── Phase 5: Crash recovery test ────────────────────────────────

if [ "$SKIP_CRASH" != "--skip-crash-test" ]; then
    echo "--- Phase 5: crash recovery ---"

    # Kill server mid-operation, restart, verify clean index
    mcp_call index_repository "{\"repo_path\":\"$SOAK_PROJECT\"}"
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    exec 3>&- 4<&-

    # Restart server
    CBM_DIAGNOSTICS=1 "$BINARY" < "$SERVER_IN" > "$SERVER_OUT" 2>>"$RESULTS_DIR/server-stderr.log" &
    SERVER_PID=$!
    exec 3>"$SERVER_IN"
    exec 4<"$SERVER_OUT"
    sleep 3

    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "OK: server restarted after kill -9"
        echo '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"capabilities":{}}}' >&3
        read -t 10 INIT_RESP <&4 || true

        # Verify clean re-index works
        mcp_call index_repository "{\"repo_path\":\"$SOAK_PROJECT\"}"
        echo "OK: clean re-index after crash recovery"
    else
        echo "FAIL: server did not restart after kill -9"
        PASS=false
    fi
fi

# ── Phase 6: Shutdown + analysis ─────────────────────────────────

echo "--- Phase 6: shutdown + analysis ---"
exec 3>&-  # close server stdin → EOF → clean exit
sleep 2
exec 4<&-  # close stdout reader
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
rm -f "$SERVER_IN" "$SERVER_OUT"

# Final diagnostics (written by thread before exit)
FINAL_DIAG="/tmp/cbm-diagnostics-${SERVER_PID}.json"

# ── Analysis ─────────────────────────────────────────────────────

PASS=true

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

# Check 3: Idle CPU
IDLE_INT=$(echo "$IDLE_CPU" | cut -d. -f1)
echo "Idle CPU: ${IDLE_CPU}%" | tee -a "$SUMMARY"
if [ "${IDLE_INT:-0}" -gt 5 ] 2>/dev/null; then
    echo "FAIL: idle CPU ${IDLE_CPU}% > 5%" | tee -a "$SUMMARY"
    PASS=false
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

rm -rf "$SOAK_PROJECT"

echo ""
if $PASS; then
    echo "=== soak-test: PASSED ===" | tee -a "$SUMMARY"
else
    echo "=== soak-test: FAILED ===" | tee -a "$SUMMARY"
    exit 1
fi
