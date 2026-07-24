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
    'DAEMON_PID=$(diagnostics_json_value pid)' \
    'Idle daemon CPU:' \
    'SOAK_PROJECT_VALUE="$SOAK_PROJECT"' \
    'SOAK_PROJECT_VALUE=$(cygpath -m "$SOAK_PROJECT")' \
    'SOAK_PROJECT_JSON=$(python3 -c' \
    'mcp_response_project()' \
    'PROJ_NAME=$(mcp_response_project "$MCP_LAST_RESPONSE")' \
    'def handle_${i}(request):' \
    'trace_path "{\"project\":\"$PROJ_NAME\",\"function_name\":\"handle_1\",\"direction\":\"both\"}"' \
    'wait_for_daemon_stop "$DAEMON_STOP_COUNT"' \
    'wait_for_daemon_stop "$FINAL_DAEMON_STOP_COUNT"' \
    'wait_for_diagnostics_snapshot "$DIAGNOSTICS_START_COUNT" "$DIAG_FILE_BEFORE_CRASH"' \
    'mcp_call index_repository "{\"repo_path\":$SOAK_PROJECT_JSON}" || PASS=false'; do
    if ! grep -Fq "$required" "$soak"; then
        echo "FAIL: daemon soak recovery contract missing: $required" >&2
        exit 1
    fi
done

if grep -Fq '"repo_path":"$SOAK_PROJECT"' "$soak"; then
    echo "FAIL: soak must not send an unconverted host/MSYS project path to a Windows binary" >&2
    exit 1
fi

if grep -Fq 'ps -o %cpu= -p "$SERVER_PID"' "$soak"; then
    echo "FAIL: soak idle CPU must not measure only the thin frontend" >&2
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
