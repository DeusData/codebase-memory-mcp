#!/usr/bin/env bash
# smoke_guard.sh — Smoke test for guard and ghost-file invariants.
#
# Verifies two properties across all 5 guarded query handlers:
#   1. Each handler returns a guard error for unknown/unindexed projects.
#   2. No ghost .db file is created for the unknown project name.
#
# Usage: bash tests/smoke_guard.sh
# Exit 0 on success, non-zero on failure.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="$PROJECT_ROOT/build/c/codebase-memory-mcp"
FAKE_PROJECT="nonexistent_smoke_test_xyz"
CACHE_DIR="${HOME}/.cache/codebase-memory-mcp"
GHOST_FILE="$CACHE_DIR/${FAKE_PROJECT}.db"
FAILURES=0

# ── Step 1: Build ─────────────────────────────────────────────────
echo "[smoke_guard] Building project..."
make -f "$PROJECT_ROOT/Makefile.cbm" cbm -C "$PROJECT_ROOT" --quiet 2>&1
if [ ! -x "$BINARY" ]; then
    echo "[smoke_guard] FAIL: binary not found at $BINARY after build" >&2
    exit 1
fi
echo "[smoke_guard] Build OK: $BINARY"

# ── Step 2: Pre-clean ghost file if somehow present ───────────────
if [ -f "$GHOST_FILE" ]; then
    echo "[smoke_guard] WARNING: ghost file already exists before test; removing: $GHOST_FILE"
    rm -f "$GHOST_FILE"
fi

# ── Helper: assert guard error and no ghost file ──────────────────
check_handler() {
    local handler="$1"
    local args="$2"
    echo "[smoke_guard] Invoking $handler with project='$FAKE_PROJECT'..."
    local response
    response="$("$BINARY" cli "$handler" "$args" 2>/dev/null)"
    echo "[smoke_guard] Response: $response"

    # For a missing .db file, cbm_store_open_path_query returns NULL so
    # REQUIRE_STORE fires ("no project loaded"). For an empty .db,
    # verify_project_indexed fires ("project not indexed"). Both are valid.
    if ! echo "$response" | grep -qE "no project loaded|not indexed"; then
        echo "[smoke_guard] FAIL [$handler]: response does not contain guard error" >&2
        echo "[smoke_guard] Got: $response" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "[smoke_guard] PASS [$handler]: guard error present"
    fi

    if [ -f "$GHOST_FILE" ]; then
        echo "[smoke_guard] FAIL [$handler]: ghost file created at $GHOST_FILE" >&2
        rm -f "$GHOST_FILE"
        FAILURES=$((FAILURES + 1))
    else
        echo "[smoke_guard] PASS [$handler]: no ghost .db file"
    fi
}

# ── Step 3: Test all 5 guarded handlers ───────────────────────────
check_handler "search_graph" "{\"project\":\"$FAKE_PROJECT\",\"name_pattern\":\".*\"}"
check_handler "query_graph"  "{\"project\":\"$FAKE_PROJECT\",\"query\":\"MATCH (n) RETURN n LIMIT 1\"}"
check_handler "get_graph_schema" "{\"project\":\"$FAKE_PROJECT\"}"
check_handler "trace_call_path" "{\"project\":\"$FAKE_PROJECT\",\"function_name\":\"main\",\"direction\":\"both\",\"depth\":1}"
check_handler "get_code_snippet" "{\"project\":\"$FAKE_PROJECT\",\"qualified_name\":\"main\"}"

# ── Step 4: Final result ──────────────────────────────────────────
if [ "$FAILURES" -gt 0 ]; then
    echo "[smoke_guard] FAILED: $FAILURES check(s) failed." >&2
    exit 1
fi

echo "[smoke_guard] All checks passed (5 handlers, guard + ghost-file invariants)."
exit 0
