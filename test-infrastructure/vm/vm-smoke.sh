#!/usr/bin/env bash
# vm-smoke.sh — the ONE Windows smoke harness. Every venue calls this script:
# local CI (win.sh smoke-install), PR CI (pr.yml pr-smoke) and the release
# venues (_smoke.yml). They differ only in host specs, architecture, and which
# binaries they hand it — never in what the smoke does.
#
# Stages and serves a complete Windows release fixture from a profile-rooted
# directory. MSYS2 /tmp and the shared runner workspace are intentionally not
# valid install roots.
#
# Run inside the VM's CLANGARM64 shell from the repo root:
#   bash test-infrastructure/vm/vm-smoke.sh
# Or from the host: test-infrastructure/vm/win.sh smoke-install
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: [env] bash test-infrastructure/vm/vm-smoke.sh

The canonical WINDOWS smoke entry: identical in local CI (win.sh
smoke-install), PR CI (pr.yml) and the release venues (_smoke.yml). Stages a
complete single-binary release fixture under a disposable profile root,
serves it on a kernel-assigned port, prepares/verifies/cleans the user-PATH
registry via windows-user-path-guard.ps1, neutralizes every agent-config
destination override, then runs scripts/smoke-test.sh (all phases).

Run inside the VM's CLANGARM64 shell (or a CI msys2 shell) from the repo root.

Environment:
  SMOKE_ARCH      arm64 (default) | amd64 — selects the served artifact name.
  SMOKE_VARIANT   standard (default) | ui. ui requires the embedded UI:
                  Phase 15's "no assets" SKIP becomes a FAILURE, so a standard
                  binary cannot pass a ui run.
  CBM_SMOKE_ARTIFACT_DIR
                  Release mode: an EXTRACTED windows release artifact
                  (codebase-memory-mcp.exe + LICENSE + install.ps1 +
                  THIRD_PARTY_NOTICES.md). All four are required and served
                  verbatim — an incomplete archive fails the smoke.
                  Unset (default): stages the freshly built binary out of
                  build/c and synthesizes the sidecars (local/PR mode).

On failure the smoke root is preserved for post-mortem (path printed).
EOF
}
case "${1:-}" in
-h|--help) usage; exit 0 ;;
?*) echo "vm-smoke: unexpected argument '$1' (configuration is via environment). Please consult --help." >&2; exit 2 ;;
esac

cd "$(dirname "$0")/../.."
ROOT="$PWD"

# Two staging sources, one smoke. Unset (the local + PR default): stage the
# freshly built binary out of build/c and synthesize the release sidecars. Set: stage an EXTRACTED release artifact verbatim, so the release
# venue smokes the bytes it is about to publish rather than a local rebuild of
# them. Requiring the sidecars here also makes an incomplete archive a smoke
# failure instead of a discovery made after publishing.
ARTIFACT_DIR="${CBM_SMOKE_ARTIFACT_DIR:-}"
if [ -n "$ARTIFACT_DIR" ]; then
    ARTIFACT_DIR="$(cd "$ARTIFACT_DIR" && pwd)"
    for required in codebase-memory-mcp.exe \
        LICENSE install.ps1 THIRD_PARTY_NOTICES.md; do
        [ -s "$ARTIFACT_DIR/$required" ] ||
            { echo "vm-smoke: release artifact is missing $required" >&2; exit 2; }
    done
    BINARY_SRC="$ARTIFACT_DIR/codebase-memory-mcp.exe"
else
    BINARY_SRC="build/c/codebase-memory-mcp.exe"
    [ -x "$BINARY_SRC" ] || { echo "build first; missing $BINARY_SRC" >&2; exit 2; }
fi

SMOKE_ARCH="${SMOKE_ARCH:-arm64}"
SMOKE_VARIANT="${SMOKE_VARIANT:-standard}"
case "$SMOKE_ARCH" in
arm64 | amd64) ;;
*) echo "vm-smoke: SMOKE_ARCH must be arm64 or amd64. Please consult --help." >&2; exit 2 ;;
esac
# A ui run must be handed a binary that actually carries the embedded assets;
# the suffix alone only renames the archive. SMOKE_REQUIRE_UI turns Phase 15's
# documented "no embedded assets" SKIP into a failure, so asking for ui and
# supplying a standard binary can no longer pass quietly.
case "$SMOKE_VARIANT" in
standard) SUFFIX="" ; REQUIRE_UI=0 ;;
ui) SUFFIX="-ui" ; REQUIRE_UI=1 ;;
*) echo "vm-smoke: SMOKE_VARIANT must be standard or ui. Please consult --help." >&2; exit 2 ;;
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
cp "$BINARY_SRC" "$SMOKE_DIR/codebase-memory-mcp.exe"
cp "$SMOKE_DIR/codebase-memory-mcp.exe" "$FIXTURE_DIR/"
# The install/update phases fetch these out of the served archive, so they must
# be the artifact's own copies whenever one was supplied — regenerating them
# here would smoke a sidecar the release never ships.
if [ -n "$ARTIFACT_DIR" ]; then
    cp "$ARTIFACT_DIR/LICENSE" "$ARTIFACT_DIR/install.ps1" \
        "$ARTIFACT_DIR/THIRD_PARTY_NOTICES.md" "$FIXTURE_DIR/"
else
    cp LICENSE install.ps1 "$FIXTURE_DIR/"
    scripts/gen-third-party-notices.sh "$FIXTURE_DIR/THIRD_PARTY_NOTICES.md"
fi

EXPECTED_ARTIFACT="codebase-memory-mcp${SUFFIX}-windows-${SMOKE_ARCH}.zip"
(
    cd "$FIXTURE_DIR"
    zip -q "$EXPECTED_ARTIFACT" \
        codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md
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
        # Header-only readiness: a HEAD proves the server routes the artifact
        # without transferring the archive body on every poll (hosted Windows
        # runners reset mid-body full-GET polls — WinError 10054 on the server,
        # 99 aborted transfers per job — while the same stack passes on the VM;
        # the real download phases still exercise full transfers and report
        # phase-precise if body transport is broken).
        if curl --noproxy '*' -fsSI \
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
    SMOKE_REQUIRE_UI="$REQUIRE_UI" \
    scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"

MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile -ExecutionPolicy Bypass \
    -File "$(cygpath -w "$PATH_GUARD")" \
    -Mode verify \
    -RunId "$PATH_RUN_ID" \
    -SnapshotPath "$(cygpath -w "$PATH_SNAPSHOT")" \
    -SmokeRoot "$(cygpath -w "$SMOKE_DIR")"
