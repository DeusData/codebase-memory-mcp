#!/usr/bin/env bash
# vm-smoke.sh — run the PR smoke inside the Windows VM exactly as CI does.
#
# Stages and serves a complete Windows release fixture from a profile-rooted
# directory. MSYS2 /tmp and the shared runner workspace are intentionally not
# valid launcher bundle roots.
#
# Run inside the VM's CLANGARM64 shell from the repo root:
#   bash test-infrastructure/vm/vm-smoke.sh
# Or from the host: test-infrastructure/vm/win.sh smoke-install
set -euo pipefail

cd "$(dirname "$0")/../.."
ROOT="$PWD"
for binary in build/c/codebase-memory-mcp-launcher.exe build/c/codebase-memory-mcp.exe; do
    [ -x "$binary" ] || { echo "build first; missing $binary" >&2; exit 2; }
done

SMOKE_ARCH="${SMOKE_ARCH:-arm64}"
SMOKE_VARIANT="${SMOKE_VARIANT:-standard}"
case "$SMOKE_ARCH" in
arm64 | amd64) ;;
*) echo "vm-smoke: SMOKE_ARCH must be arm64 or amd64" >&2; exit 2 ;;
esac
case "$SMOKE_VARIANT" in
standard) SUFFIX="" ;;
ui) SUFFIX="-ui" ;;
*) echo "vm-smoke: SMOKE_VARIANT must be standard or ui" >&2; exit 2 ;;
esac

PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"
SMOKE_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-vm-smoke.XXXXXX")"
FIXTURE_DIR="$SMOKE_DIR/artifacts"
SMOKE_HOME="$SMOKE_DIR/home"
SMOKE_XDG_CONFIG="$SMOKE_DIR/xdg-config"
SMOKE_APPDATA="$SMOKE_DIR/appdata"
SMOKE_LOCALAPPDATA="$SMOKE_DIR/localappdata"
SMOKE_TEMP_DIR="$SMOKE_DIR/temp"
PORT_FILE="$SMOKE_DIR/port"
SERVER_LOG="$SMOKE_DIR/server.log"
SERVER_PID=""
PATH_GUARD="$ROOT/test-infrastructure/vm/windows-user-path-guard.ps1"
PATH_SNAPSHOT="$SMOKE_DIR/user-path-snapshot.json"
PATH_RUN_ID=""
PATH_GUARD_READY=0
cleanup() {
    local status=$?
    trap - EXIT
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [ "$PATH_GUARD_READY" -eq 1 ]; then
        if ! MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile -ExecutionPolicy Bypass \
            -File "$(cygpath -w "$PATH_GUARD")" \
            -Mode cleanup \
            -RunId "$PATH_RUN_ID" \
            -SnapshotPath "$(cygpath -w "$PATH_SNAPSHOT")" \
            -SmokeRoot "$(cygpath -w "$SMOKE_DIR")"; then
            echo "vm-smoke: isolated registry cleanup failed" >&2
            status=1
        fi
    fi
    if [ "$status" -eq 0 ]; then
        rm -rf "$SMOKE_DIR"
    else
        echo "vm-smoke: preserved failed smoke root at $SMOKE_DIR" >&2
    fi
    exit "$status"
}
trap cleanup EXIT

mkdir -p "$FIXTURE_DIR" "$SMOKE_HOME" "$SMOKE_XDG_CONFIG" "$SMOKE_APPDATA" \
    "$SMOKE_LOCALAPPDATA" "$SMOKE_TEMP_DIR"
PATH_RUN_ID=$(
    powershell.exe -NoProfile -Command \
        "[Guid]::NewGuid().ToString('N')" | tr -d '\r\n'
)
if ! printf '%s' "$PATH_RUN_ID" | grep -qE '^[0-9A-Fa-f]{32}$'; then
    echo "vm-smoke: could not generate an isolated registry run ID" >&2
    exit 1
fi
PATH_GUARD_READY=1
MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile -ExecutionPolicy Bypass \
    -File "$(cygpath -w "$PATH_GUARD")" \
    -Mode prepare \
    -RunId "$PATH_RUN_ID" \
    -SnapshotPath "$(cygpath -w "$PATH_SNAPSHOT")" \
    -SmokeRoot "$(cygpath -w "$SMOKE_DIR")"
cp build/c/codebase-memory-mcp-launcher.exe "$SMOKE_DIR/codebase-memory-mcp.exe"
cp build/c/codebase-memory-mcp.exe "$SMOKE_DIR/codebase-memory-mcp.payload.exe"
cp "$SMOKE_DIR/codebase-memory-mcp.exe" \
    "$SMOKE_DIR/codebase-memory-mcp.payload.exe" \
    LICENSE install.ps1 "$FIXTURE_DIR/"
scripts/gen-third-party-notices.sh "$FIXTURE_DIR/THIRD_PARTY_NOTICES.md"

EXPECTED_ARTIFACT="codebase-memory-mcp${SUFFIX}-windows-${SMOKE_ARCH}.zip"
(
    cd "$FIXTURE_DIR"
    zip -q "$EXPECTED_ARTIFACT" \
        codebase-memory-mcp.exe codebase-memory-mcp.payload.exe \
        LICENSE install.ps1 THIRD_PARTY_NOTICES.md
    if [ -n "$SUFFIX" ]; then
        cp "$EXPECTED_ARTIFACT" "codebase-memory-mcp-windows-${SMOKE_ARCH}.zip"
    fi
    sha256sum *.zip > checksums.txt
)

python3 "$ROOT/scripts/smoke-fixture-server.py" \
    --directory "$FIXTURE_DIR" --port-file "$PORT_FILE" \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

PORT=""
READY=0
for _ in $(seq 1 100); do
    if [ -s "$PORT_FILE" ]; then
        PORT=$(tr -d '[:space:]' < "$PORT_FILE")
        if curl --noproxy '*' -fsS \
            "http://127.0.0.1:$PORT/$EXPECTED_ARTIFACT" >/dev/null 2>&1; then
            READY=1
            break
        fi
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [ "$READY" -ne 1 ]; then
    echo "vm-smoke: fixture server did not serve $EXPECTED_ARTIFACT" >&2
    echo "--- fixture server log ---" >&2
    cat "$SERVER_LOG" >&2
    exit 1
fi

# The release smoke performs real agent config mutations. Neutralize every
# supported destination override before entering it; individual phases may set
# an intentional override of their own beneath SMOKE_DIR.
env \
    -u CLAUDE_CONFIG_DIR \
    -u CODEX_HOME \
    -u KIRO_HOME \
    -u HERMES_HOME \
    -u QWEN_HOME \
    -u CLINE_DATA_DIR \
    -u OPENCLAW_HOME \
    -u OPENCLAW_STATE_DIR \
    -u OPENCLAW_PROFILE \
    -u OPENCLAW_CONFIG_PATH \
    -u OPENCLAW_WORKSPACE_DIR \
    -u OPENCODE_CONFIG \
    -u OPENCODE_CONFIG_DIR \
    -u COPILOT_HOME \
    -u CRUSH_GLOBAL_CONFIG \
    -u VIBE_HOME \
    -u GLAB_CONFIG_DIR \
    -u KIMI_CODE_HOME \
    -u CBM_CONTINUE_CONFIG_PATH \
    -u CBM_TRAE_CONFIG_PATH \
    -u CBM_ROO_CONFIG_PATH \
    -u CBM_CODY_CONFIG_PATH \
    -u CBM_TEST_WINDOWS_USER_PATH_RUN_ID \
    HOME="$(cygpath -m "$SMOKE_HOME")" \
    USERPROFILE="$(cygpath -m "$SMOKE_HOME")" \
    XDG_CONFIG_HOME="$(cygpath -m "$SMOKE_XDG_CONFIG")" \
    APPDATA="$(cygpath -m "$SMOKE_APPDATA")" \
    LOCALAPPDATA="$(cygpath -m "$SMOKE_LOCALAPPDATA")" \
    TMPDIR="$SMOKE_TEMP_DIR" \
    TEMP="$(cygpath -m "$SMOKE_TEMP_DIR")" \
    TMP="$(cygpath -m "$SMOKE_TEMP_DIR")" \
    SHELL="/usr/bin/bash" \
    CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")" \
    CBM_TEST_WINDOWS_USER_PATH_RUN_ID="$PATH_RUN_ID" \
    SMOKE_TEMP_ROOT="$SMOKE_DIR" \
    SMOKE_DOWNLOAD_URL="http://127.0.0.1:$PORT" \
    SMOKE_UPDATE_FIXTURE_DIR="$FIXTURE_DIR" \
    SMOKE_ARCH="$SMOKE_ARCH" \
    scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"

MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile -ExecutionPolicy Bypass \
    -File "$(cygpath -w "$PATH_GUARD")" \
    -Mode verify \
    -RunId "$PATH_RUN_ID" \
    -SnapshotPath "$(cygpath -w "$PATH_SNAPSHOT")" \
    -SmokeRoot "$(cygpath -w "$SMOKE_DIR")"
