#!/usr/bin/env bash
set -euo pipefail

# Full local smoke, including the release download/install/update phases that
# plain `scripts/smoke-test.sh <binary>` skips without an artifact fixture.
#
# Usage: scripts/smoke-local.sh <binary> [ui]
#   <binary>  product binary to smoke (e.g. build/c/codebase-memory-mcp)
#   ui        optional: mirror the -ui variant asset naming

usage() {
    cat <<'EOF'
Usage: scripts/smoke-local.sh <binary> [standard|ui]

The canonical unix smoke entry: identical in local CI, PR CI, dry run and
release. Stages a complete release fixture (binary + LICENSE + install.sh +
THIRD_PARTY_NOTICES.md), serves it on a kernel-assigned port, then runs
scripts/smoke-test.sh (all phases, including the download/checksum/install/
update E2E) inside a disposable HOME/XDG/TMPDIR sandbox with every agent-config
destination override neutralized.

Arguments:
  <binary>     Product binary to smoke (e.g. build/c/codebase-memory-mcp).
  ui           Mirror the -ui asset naming AND require the embedded UI:
               Phase 15's "no assets" outcome becomes a FAILURE
               (SMOKE_REQUIRE_UI=1), so a standard binary cannot pass a ui run.

Environment:
  CBM_SMOKE_ARTIFACT_DIR   Release mode: an EXTRACTED release artifact
               directory. Sidecars (LICENSE, install.sh, THIRD_PARTY_NOTICES.md)
               are validated and served from THERE instead of regenerated, so
               the release venue smokes exactly the bytes it publishes; an
               incomplete archive fails the smoke. Unset (default): sidecars
               come from this checkout (local/PR mode).

Callers: pr.yml pr-smoke (ubuntu/macos) · _smoke.yml smoke-unix +
smoke-linux-portable (with CBM_SMOKE_ARTIFACT_DIR) · compose smoke services.
Windows uses the sibling test-infrastructure/vm/vm-smoke.sh.
EOF
}
case "${1:-}" in
-h|--help) usage; exit 0 ;;
-*) echo "smoke-local: unknown option '$1'. Please consult --help." >&2; exit 2 ;;
esac
if [ $# -gt 2 ]; then
    echo "smoke-local: too many arguments. Please consult --help." >&2
    exit 2
fi

BINARY="${1:?smoke-local: missing <binary> argument. Please consult --help.}"
VARIANT="${2:-standard}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"

if [ ! -x "$BINARY" ]; then
    echo "smoke-local: binary is not executable: $BINARY" >&2
    exit 2
fi
# A ui run must be handed a binary that actually carries the embedded assets;
# the suffix alone only renames the archive. SMOKE_REQUIRE_UI turns Phase 15's
# documented "no embedded assets" SKIP into a failure, so asking for ui and
# supplying a standard binary can no longer pass quietly.
case "$VARIANT" in
standard) SUFFIX="" ; REQUIRE_UI=0 ;;
ui) SUFFIX="-ui" ; REQUIRE_UI=1 ;;
*) echo "smoke-local: variant must be 'standard' or 'ui'. Please consult --help." >&2; exit 2 ;;
esac

# Unset (the local + PR default): synthesize the release sidecars from this
# checkout. Set: take them from an EXTRACTED release artifact, so the release
# venue serves the files it is about to publish rather than regenerated copies.
ARTIFACT_DIR="${CBM_SMOKE_ARTIFACT_DIR:-}"
if [ -n "$ARTIFACT_DIR" ]; then
    ARTIFACT_DIR="$(cd "$ARTIFACT_DIR" && pwd)"
    for required in LICENSE install.sh THIRD_PARTY_NOTICES.md; do
        [ -s "$ARTIFACT_DIR/$required" ] || {
            echo "smoke-local: release artifact is missing $required" >&2
            exit 2
        }
    done
fi

case "$(uname -s)" in
Darwin) OS=darwin ;;
Linux) OS=linux ;;
*) echo "smoke-local: unsupported host OS $(uname -s)" >&2; exit 2 ;;
esac
case "$(uname -m)" in
arm64 | aarch64) ARCH=arm64 ;;
x86_64)
    # Match install.sh: a Rosetta shell reports x86_64 even though the
    # installer correctly selects the native Apple Silicon artifact.
    if [ "$OS" = "darwin" ] &&
        sysctl -n machdep.cpu.brand_string 2>/dev/null | grep -qi apple; then
        ARCH=arm64
    else
        ARCH=amd64
    fi
    ;;
*) echo "smoke-local: unsupported host arch $(uname -m)" >&2; exit 2 ;;
esac

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cbm-smoke-server-XXXXXX")
FIXTURE_DIR="$WORK_DIR/artifacts"
SMOKE_TEMP_DIR="$WORK_DIR/temp"
SMOKE_HOME="$WORK_DIR/home"
SMOKE_XDG_CONFIG="$WORK_DIR/xdg-config"
SMOKE_APPDATA="$WORK_DIR/appdata"
SMOKE_LOCALAPPDATA="$WORK_DIR/localappdata"
PORT_FILE="$WORK_DIR/port"
SERVER_LOG="$WORK_DIR/server.log"
SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$FIXTURE_DIR" "$SMOKE_TEMP_DIR" "$SMOKE_HOME" "$SMOKE_XDG_CONFIG" \
    "$SMOKE_APPDATA" "$SMOKE_LOCALAPPDATA"
cp "$BINARY" "$FIXTURE_DIR/codebase-memory-mcp"
if [ -n "$ARTIFACT_DIR" ]; then
    cp "$ARTIFACT_DIR/LICENSE" "$ARTIFACT_DIR/install.sh" \
        "$ARTIFACT_DIR/THIRD_PARTY_NOTICES.md" "$FIXTURE_DIR/"
else
    cp "$ROOT/LICENSE" "$ROOT/install.sh" "$FIXTURE_DIR/"
    "$ROOT/scripts/gen-third-party-notices.sh" "$FIXTURE_DIR/THIRD_PARTY_NOTICES.md"
fi

EXPECTED_ARTIFACT="codebase-memory-mcp${SUFFIX}-${OS}-${ARCH}.tar.gz"
tar -czf "$FIXTURE_DIR/$EXPECTED_ARTIFACT" -C "$FIXTURE_DIR" \
    codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md
if [ -n "$SUFFIX" ]; then
    cp "$FIXTURE_DIR/$EXPECTED_ARTIFACT" \
        "$FIXTURE_DIR/codebase-memory-mcp-${OS}-${ARCH}.tar.gz"
fi

# Linux install/update resolves the portable release asset even when this local
# smoke started from the dynamic production binary.
if [ "$OS" = "linux" ]; then
    cp "$FIXTURE_DIR/$EXPECTED_ARTIFACT" \
        "$FIXTURE_DIR/codebase-memory-mcp${SUFFIX}-${OS}-${ARCH}-portable.tar.gz"
    if [ -n "$SUFFIX" ]; then
        cp "$FIXTURE_DIR/codebase-memory-mcp${SUFFIX}-${OS}-${ARCH}-portable.tar.gz" \
            "$FIXTURE_DIR/codebase-memory-mcp-${OS}-${ARCH}-portable.tar.gz"
    fi
fi
(cd "$FIXTURE_DIR" && { sha256sum *.tar.gz > checksums.txt 2>/dev/null ||
    shasum -a 256 *.tar.gz > checksums.txt; })

python3 "$ROOT/scripts/smoke-fixture-server.py" \
    --directory "$FIXTURE_DIR" --port-file "$PORT_FILE" \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

PORT=""
READY=0
for _ in $(seq 1 100); do
    if [ -s "$PORT_FILE" ]; then
        PORT=$(tr -d '[:space:]' < "$PORT_FILE")
        # Header-only readiness (see vm-smoke.sh): proves routing without a
        # full body transfer per poll; the download phases own body transport.
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
    echo "smoke-local: fixture server did not serve $EXPECTED_ARTIFACT" >&2
    echo "--- fixture server log ---" >&2
    cat "$SERVER_LOG" >&2
    exit 1
fi

# Full smoke deliberately runs real install/update/uninstall paths. Keep every
# supported agent destination override inside this disposable root so an
# ambient developer setting cannot redirect those mutations into live config.
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
    HOME="$SMOKE_HOME" \
    USERPROFILE="$SMOKE_HOME" \
    XDG_CONFIG_HOME="$SMOKE_XDG_CONFIG" \
    APPDATA="$SMOKE_APPDATA" \
    LOCALAPPDATA="$SMOKE_LOCALAPPDATA" \
    TMPDIR="$SMOKE_TEMP_DIR" \
    TEMP="$SMOKE_TEMP_DIR" \
    TMP="$SMOKE_TEMP_DIR" \
    SHELL="/bin/sh" \
    CBM_CACHE_DIR="$WORK_DIR/cache" \
    SMOKE_TEMP_ROOT="$SMOKE_TEMP_DIR" \
    SMOKE_DOWNLOAD_URL="http://127.0.0.1:$PORT" \
    SMOKE_UPDATE_FIXTURE_DIR="$FIXTURE_DIR" \
    SMOKE_ARCH="$ARCH" \
    SMOKE_REQUIRE_UI="$REQUIRE_UI" \
    "$ROOT/scripts/smoke-test.sh" "$BINARY"
