#!/usr/bin/env bash
# Static contract for the daemon crash/restart assertions in the soak harness.
# Source-contract patterns intentionally retain shell variables literally.
# shellcheck disable=SC2016

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
soak="$ROOT/scripts/soak-test.sh"

for required in \
    'json_rpc_response_ok()' \
    'and "result" in message' \
    'diagnostics_start_count()' \
    'read_idle_cpu_sample()' \
    'user_cpu_ms = d.get("process_user_cpu_ms")' \
    'system_cpu_ms = d.get("process_system_cpu_ms")' \
    'Idle CPU: ${IDLE_CPU}% over ${IDLE_OBSERVED_SECONDS}s' \
    'MCP_SOAK_PROJECT="$SOAK_PROJECT"' \
    'MCP_SOAK_PROJECT=$(cygpath -m "$SOAK_PROJECT")' \
    'SOAK_PROJECT_JSON=$(python3 -c' \
    'mcp_response_project()' \
    'PROJ_NAME=$(mcp_response_project "$LAST_MCP_RESPONSE")' \
    'FAIL: soak DACL normalize' \
    'FAIL: soak DACL stamp' \
    'FAIL: soak child DACL reset' \
    'SOAK_NATIVE_WINDOWS=false' \
    "eval 'coproc CBM_SOAK_SERVER {" \
    'SERVER_PID=$CBM_SOAK_SERVER_PID' \
    'CBM_AUTO_INDEX=false' \
    'FDS_OPEN=true' \
    'stderr_mode="truncate"' \
    'stderr_mode="append"' \
    'start_mcp_server "$stderr_mode"' \
    'def handle_${i}(request):' \
    'trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}"' \
    'wait_for_daemon_stop "${DAEMON_STOP_COUNT:-0}"' \
    'wait_for_daemon_stop "${FINAL_DAEMON_STOP_COUNT:-0}"' \
    'wait_for_diagnostics_snapshot "${snapshots_before:-0}" "$previous_snapshot"' \
    'start_server "$DIAG_FILE_BEFORE_CRASH"' \
    'if mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}"; then'; do
    if ! grep -Fq "$required" "$soak"; then
        echo "FAIL: daemon soak recovery contract missing: $required" >&2
        exit 1
    fi
done

if grep -Fq 'WARN: soak DACL stamp failed' "$soak"; then
    echo "FAIL: native-Windows soak must fail closed when its trusted-root DACL cannot be set" >&2
    exit 1
fi

if grep -Fq '"repo_path":"$SOAK_PROJECT"' "$soak"; then
    echo "FAIL: soak must not send an unconverted host/MSYS project path to a Windows binary" >&2
    exit 1
fi

if grep -Fq "json.load(open('\$DIAG_FILE'))" "$soak" ||
    grep -Fq 'with open(sys.argv[1]' "$soak"; then
    echo "FAIL: native Windows Python must consume diagnostics through stdin, not an MSYS path" >&2
    exit 1
fi

if grep -Fq 'ps -o %cpu= -p "$SERVER_PID"' "$soak"; then
    echo "FAIL: soak idle CPU must not measure only the thin frontend" >&2
    exit 1
fi

if grep -Fq 'diagnostics_json_value()' "$soak" || grep -Fq 'DAEMON_PID=' "$soak"; then
    echo "FAIL: soak idle CPU must use daemon process-time deltas, not a sampled pid" >&2
    exit 1
fi

if grep -Fq 'mktemp -u' "$soak"; then
    echo "FAIL: soak transport endpoints must stay under the owned private root" >&2
    exit 1
fi

if grep -Fq 'SOAK_CACHE_DIR_HOST' "$soak" || grep -Fq 'SOAK_WIN_ROOT' "$soak"; then
    echo "FAIL: soak must not split runtime ownership across independent temporary roots" >&2
    exit 1
fi

if ! grep -Fq 'rm -rf -- "$SOAK_ROOT"' "$soak"; then
    echo "FAIL: soak cleanup must remove the one root that owns every runtime artifact" >&2
    exit 1
fi

if [ "$(grep -c '^PASS=true$' "$soak")" -ne 1 ]; then
    echo "FAIL: soak result state must be initialized exactly once" >&2
    exit 1
fi
if ! grep -Fq 'tests/test_soak_daemon_recovery_contract.sh' "$ROOT/scripts/test.sh"; then
    echo "FAIL: daemon soak recovery contract is not wired into the test suite" >&2
    exit 1
fi

echo "Daemon soak recovery contract passed"
