#!/usr/bin/env bash
# smoke-artifact.sh — the local ARTIFACT-FLOW smoke lane. The release venue
# smokes the shipped archive (download → extract → canonical wrapper); before
# this lane existed, local CI only ever smoked source builds, so the whole
# packaging/archive-layout bug class could first appear in a release dry run.
#
# This driver reproduces the release flow end to end with local bytes:
#   build → derive stripped/unstripped pair → default-select stripped →
#   byte-preserving package → extract →
#   the SAME canonical wrapper the remote venue runs, in artifact mode
#   (CBM_SMOKE_ARTIFACT_DIR), whose completeness checks make a broken or
#   incomplete archive a loud failure.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/ci/smoke-artifact.sh <goos> <goarch> [VAR=VAL ...]

Build → derive both release candidates → select stripped (local, unscanned) →
package immutable bytes → extract → smoke the EXTRACTED artifact through the
canonical wrapper, exactly like the release venue:
  unix:    scripts/smoke-local.sh with CBM_SMOKE_ARTIFACT_DIR
  windows: test-infrastructure/vm/vm-smoke.sh with CBM_SMOKE_ARTIFACT_DIR
           (run inside the VM/CI msys2 shell)

Make passthrough (VAR=VAL): CC= CXX= STATIC=1 ... forwarded to build steps.
Environment: BUILD_DIR (default build/c) — build tree containing linker output.
On failure the work directory is preserved for post-mortem (path printed).
EOF
}

GOOS=""
GOARCH=""
BUILD_ARGS=()
for arg in "$@"; do
    case "$arg" in
    -h | --help) usage; exit 0 ;;
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
UI_FLAG=(--with-ui)
# This lane builds and packages the SHIPPED composition, so a binary that serves
# no frontend is a defect here, not a documented skip.
export SMOKE_REQUIRE_UI=1
case "$GOARCH" in
*-portable) BUILD_ARGS+=("STATIC=1") ;;
esac

BUILD_DIR="${BUILD_DIR:-build/c}"
export BUILD_DIR

scripts/build.sh ${UI_FLAG[@]+"${UI_FLAG[@]}"} \
    BUILD_DIR="$BUILD_DIR" ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}

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

SOURCE_BINARY="$BUILD_DIR/codebase-memory-mcp"
if [ "$GOOS" = "windows" ] && [ -f "${SOURCE_BINARY}.exe" ]; then
    SOURCE_BINARY="${SOURCE_BINARY}.exe"
fi
[ -f "$SOURCE_BINARY" ] || {
    echo "smoke-artifact: build completed without expected binary: $SOURCE_BINARY" >&2
    exit 2
}

CANDIDATE_ROOT="$WORK_DIR/candidates"
scripts/ci/prepare-release-candidates.sh "$GOOS" "$GOARCH" \
    --binary "$SOURCE_BINARY" --out-dir "$CANDIDATE_ROOT"

SELECTED_NAME="codebase-memory-mcp"
[ "$GOOS" = "windows" ] && SELECTED_NAME="codebase-memory-mcp.exe"
SELECTED_BINARY="$CANDIDATE_ROOT/${GOOS}-${GOARCH}/stripped/$SELECTED_NAME"
[ -f "$SELECTED_BINARY" ] || {
    echo "smoke-artifact: candidate derivation did not produce $SELECTED_BINARY" >&2
    exit 2
}
PROVENANCE="$CANDIDATE_ROOT/${GOOS}-${GOARCH}/candidate-provenance.tsv"
SELECTED_SHA256="$(python3 - "$PROVENANCE" <<'PY'
import csv
import pathlib
import sys

with pathlib.Path(sys.argv[1]).open(encoding="utf-8", newline="") as handle:
    lines = handle.read().splitlines()
rows = list(csv.DictReader(lines[1:], delimiter="\t"))
matches = [row for row in rows if row.get("variant") == "stripped"]
if len(matches) != 1:
    raise SystemExit("smoke-artifact: provenance does not contain exactly one stripped row")
print(matches[0]["sha256"])
PY
)"
echo "=== smoke-artifact: unscanned-local-smoke default selected stripped $GOOS-$GOARCH ($SELECTED_SHA256) ==="

# Generate notices while the build's graph-ui/node_modules tree is available;
# the release build similarly carries this file alongside candidate artifacts.
NOTICES="$WORK_DIR/THIRD_PARTY_NOTICES.md"
scripts/gen-third-party-notices.sh "$NOTICES"
scripts/package-release.sh "$GOOS" "$GOARCH" \
    --selected-binary "$SELECTED_BINARY" \
    --expected-sha256 "$SELECTED_SHA256" \
    --third-party-notices "$NOTICES" \
    --out-dir "$WORK_DIR"

NAME="codebase-memory-mcp-${GOOS}-${GOARCH}"
EXTRACT_DIR="$WORK_DIR/extract"
mkdir -p "$EXTRACT_DIR"
if [ "$GOOS" = "windows" ]; then
    unzip -q -o "$WORK_DIR/$NAME.zip" -d "$EXTRACT_DIR"
    test -s "$EXTRACT_DIR/codebase-memory-mcp.exe"
    # ONE binary per platform: a payload sibling means the AV-flagged launcher
    # stub came back.
    test ! -e "$EXTRACT_DIR/codebase-memory-mcp.payload.exe"
    echo "=== smoke-artifact: smoking EXTRACTED $NAME.zip via vm-smoke.sh ==="
    SMOKE_ARCH="$GOARCH" \
        CBM_SMOKE_ARTIFACT_DIR="$EXTRACT_DIR" \
        bash test-infrastructure/vm/vm-smoke.sh
else
    tar -xzf "$WORK_DIR/$NAME.tar.gz" -C "$EXTRACT_DIR"
    chmod +x "$EXTRACT_DIR/codebase-memory-mcp"
    echo "=== smoke-artifact: smoking EXTRACTED $NAME.tar.gz via smoke-local.sh ==="
    CBM_SMOKE_ARTIFACT_DIR="$EXTRACT_DIR" \
        scripts/smoke-local.sh "$EXTRACT_DIR/codebase-memory-mcp"
fi
echo "=== smoke-artifact: $NAME passed ==="
