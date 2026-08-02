#!/usr/bin/env bash
# smoke-artifact.sh — the local ARTIFACT-FLOW smoke lane. The release venue
# smokes the shipped archive (download → extract → canonical wrapper); before
# this lane existed, local CI only ever smoked source builds, so the whole
# packaging/archive-layout bug class could first appear in a release dry run.
#
# This driver reproduces the release flow end to end with local bytes:
#   scripts/build.sh → scripts/package-release.sh → extract →
#   the SAME canonical wrapper the remote venue runs, in artifact mode
#   (CBM_SMOKE_ARTIFACT_DIR), whose completeness checks make a broken or
#   incomplete archive a loud failure.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/ci/smoke-artifact.sh <goos> <goarch> [--variant standard|ui]
                                    [VAR=VAL ...]

Build → package (scripts/package-release.sh) → extract → smoke the EXTRACTED
artifact through the canonical wrapper, exactly like the release venue:
  unix:    scripts/smoke-local.sh with CBM_SMOKE_ARTIFACT_DIR
  windows: test-infrastructure/vm/vm-smoke.sh with CBM_SMOKE_ARTIFACT_DIR
           (run inside the VM/CI msys2 shell)

  --variant ui builds --with-ui and smokes the ui archive (Phase 15 embedded
  assets become mandatory — a standard binary cannot pass a ui run).

Make passthrough (VAR=VAL): CC= CXX= STATIC=1 ... forwarded to build steps.
Environment: BUILD_DIR (default build/c) — build tree used for the archive.
On failure the work directory is preserved for post-mortem (path printed).
EOF
}

GOOS=""
GOARCH=""
VARIANT="standard"
BUILD_ARGS=()
expect_value=""
for arg in "$@"; do
    if [ "$expect_value" = "variant" ]; then
        VARIANT="$arg"; expect_value=""; continue
    fi
    case "$arg" in
    -h | --help) usage; exit 0 ;;
    --variant) expect_value="variant" ;;
    --variant=*) VARIANT="${arg#--variant=}" ;;
    -*)
        echo "smoke-artifact: unknown option '$arg'. Please consult --help." >&2
        exit 2
        ;;
    *=*) BUILD_ARGS+=("$arg") ;;
    *)
        if [ -z "$GOOS" ]; then GOOS="$arg"
        elif [ -z "$GOARCH" ]; then GOARCH="$arg"
        else
            echo "smoke-artifact: unexpected argument '$arg'. Please consult --help." >&2
            exit 2
        fi
        ;;
    esac
done
[ -n "$GOOS" ] && [ -n "$GOARCH" ] || { usage >&2; exit 2; }
case "$VARIANT" in
standard) SUFFIX=""; UI_FLAG=() ;;
ui) SUFFIX="-ui"; UI_FLAG=(--with-ui) ;;
*) echo "smoke-artifact: variant must be 'standard' or 'ui'. Please consult --help." >&2; exit 2 ;;
esac
case "$GOARCH" in
*-portable) BUILD_ARGS+=("STATIC=1") ;;
esac

BUILD_DIR="${BUILD_DIR:-build/c}"
export BUILD_DIR

scripts/build.sh ${UI_FLAG[@]+"${UI_FLAG[@]}"} \
    BUILD_DIR="$BUILD_DIR" ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}
if [ "$GOOS" = "darwin" ]; then
    codesign --sign - --force "$BUILD_DIR/codebase-memory-mcp"
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cbm-smoke-artifact.XXXXXX")"
cleanup() {
    local status=$?
    trap - EXIT
    if [ "$status" -eq 0 ]; then
        rm -rf "$WORK_DIR"
    else
        echo "smoke-artifact: preserved failed lane root at $WORK_DIR" >&2
    fi
    exit "$status"
}
trap cleanup EXIT

scripts/package-release.sh "$GOOS" "$GOARCH" --variant "$VARIANT" \
    --out-dir "$WORK_DIR" ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}

NAME="codebase-memory-mcp${SUFFIX}-${GOOS}-${GOARCH}"
EXTRACT_DIR="$WORK_DIR/extract"
mkdir -p "$EXTRACT_DIR"
if [ "$GOOS" = "windows" ]; then
    unzip -q -o "$WORK_DIR/$NAME.zip" -d "$EXTRACT_DIR"
    test -s "$EXTRACT_DIR/codebase-memory-mcp.exe"
    # ONE binary per platform: a payload sibling means the AV-flagged launcher
    # stub came back.
    test ! -e "$EXTRACT_DIR/codebase-memory-mcp.payload.exe"
    echo "=== smoke-artifact: smoking EXTRACTED $NAME.zip via vm-smoke.sh ==="
    SMOKE_ARCH="$GOARCH" SMOKE_VARIANT="$VARIANT" \
        CBM_SMOKE_ARTIFACT_DIR="$EXTRACT_DIR" \
        bash test-infrastructure/vm/vm-smoke.sh
else
    tar -xzf "$WORK_DIR/$NAME.tar.gz" -C "$EXTRACT_DIR"
    chmod +x "$EXTRACT_DIR/codebase-memory-mcp"
    echo "=== smoke-artifact: smoking EXTRACTED $NAME.tar.gz via smoke-local.sh ==="
    CBM_SMOKE_ARTIFACT_DIR="$EXTRACT_DIR" \
        scripts/smoke-local.sh "$EXTRACT_DIR/codebase-memory-mcp" "$VARIANT"
fi
echo "=== smoke-artifact: $NAME passed ==="
