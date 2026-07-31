#!/usr/bin/env bash
# package-release.sh — THE canonical release-archive step. Every venue that
# turns built binaries into a release archive (release/_build.yml, the local
# artifact-flow smoke lane) runs this file; workflows provide only
# checkout/toolchain/upload around it. Archive names and contents are defined
# HERE, nowhere else, so a local artifact smoke provably exercises the same
# bytes-layout the release publishes.
#
# This script ARCHIVES what scripts/build.sh already produced — it never
# builds the product itself (the Windows launcher image is the one deliberate
# exception: it is part of the archive, not of the product build).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/package-release.sh <goos> <goarch> [--variant standard|ui]
                                  [--out-dir DIR] [VAR=VAL ...]

The canonical release-archive step: identical in the release build and the
local artifact-flow smoke lane.

  goos       linux | darwin | windows
  goarch     arch label used verbatim in the archive name (amd64, arm64,
             arm64-portable, ...)
  --variant  standard (default) | ui — selects the archive NAME prefix; the
             matching binary must already have been built (--with-ui for ui).
  --out-dir  where to place the archive (default: repository root).

Make passthrough (VAR=VAL, forwarded to the build):
  CC= CXX=   compiler override, e.g. CC=clang CXX=clang++.

Environment:
  BUILD_DIR  build tree to archive from (default build/c).

Archive contents (defined here, canonical) — ONE binary per platform:
  unix:    codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md (.tar.gz)
  windows: codebase-memory-mcp.exe LICENSE install.ps1
           THIRD_PARTY_NOTICES.md (.zip)
EOF
}

GOOS=""
GOARCH=""
VARIANT="standard"
OUT_DIR="$ROOT"
MAKE_ARGS=()
expect_value=""
for arg in "$@"; do
    case "$expect_value" in
    variant) VARIANT="$arg"; expect_value=""; continue ;;
    out-dir) OUT_DIR="$arg"; expect_value=""; continue ;;
    esac
    case "$arg" in
    -h | --help) usage; exit 0 ;;
    --variant) expect_value="variant" ;;
    --variant=*) VARIANT="${arg#--variant=}" ;;
    --out-dir) expect_value="out-dir" ;;
    --out-dir=*) OUT_DIR="${arg#--out-dir=}" ;;
    -*)
        echo "package-release: unknown option '$arg'. Please consult --help." >&2
        exit 2
        ;;
    *=*) MAKE_ARGS+=("$arg") ;;
    *)
        if [ -z "$GOOS" ]; then GOOS="$arg"
        elif [ -z "$GOARCH" ]; then GOARCH="$arg"
        else
            echo "package-release: unexpected argument '$arg'. Please consult --help." >&2
            exit 2
        fi
        ;;
    esac
done
[ -n "$GOOS" ] && [ -n "$GOARCH" ] || { usage >&2; exit 2; }
case "$GOOS" in
linux | darwin | windows) ;;
*) echo "package-release: goos must be linux, darwin or windows." >&2; exit 2 ;;
esac
case "$VARIANT" in
standard) SUFFIX="" ;;
ui) SUFFIX="-ui" ;;
*) echo "package-release: variant must be 'standard' or 'ui'." >&2; exit 2 ;;
esac
[ -n "$expect_value" ] && { echo "package-release: --$expect_value needs a value." >&2; exit 2; }

BUILD_DIR="${BUILD_DIR:-build/c}"
OUT_DIR="$(mkdir -p "$OUT_DIR" && cd "$OUT_DIR" && pwd)"
NAME="codebase-memory-mcp${SUFFIX}-${GOOS}-${GOARCH}"

# Ship every release binary stripped. Production already builds without -g, but
# the linker still keeps a ~536 KB .symtab, so releases carried their full
# symbol table to users: bigger downloads and a free map of the internals, with
# nothing gained. Nothing symbolizes at runtime (mem_profile.c is not in the
# production build and never calls backtrace_symbols), so this costs no
# diagnostics.
#
# It also had a concrete cost. Microsoft's ML scored the unstripped linux-amd64
# binary Trojan:Script/Wacatac.B!ml (1 engine of 62) and blocked release run
# 30398064336 at the VirusTotal gate. That verdict is a decision-boundary
# artifact rather than a property of the code -- the dry-run build two days
# earlier is the same program plus 10 KB and scans clean, and the ui build of
# the same commit was never flagged. Stripping removes the symbol surface those
# models score and cleared BOTH flagged builds (Wacatac.B and Wacatac.C)
# without changing what the program does.
#
# macOS is ad-hoc signed by the build workflow BEFORE this script runs, and
# stripping invalidates that signature, so Mach-O is re-signed here. Skipping
# the re-sign ships a binary the kernel refuses to exec.
strip_release_binary() {
    local binary="$1"
    [ -f "$binary" ] || return 0
    # The right flags differ per format, and the WRONG ones fail silently in
    # the dangerous direction. Measured on the flagged darwin-arm64 artifact:
    #
    #   llvm-strip --strip-all   373 symbols   scanned CLEAN
    #   strip        (no flags)  378 symbols   equivalent
    #   strip -x -S             4058 symbols   the state VirusTotal FLAGGED
    #   strip -X / -u -r        4058 symbols   likewise
    #
    # Apple's strip returns success for `-x -S`, so a helper that just tries
    # candidates until one exits 0 would quietly reship the flagged binary.
    # GNU/LLVM `--strip-all` is not even accepted by Apple's strip, which is why
    # generalising it to every platform broke the macOS build -- loudly, which
    # was the lucky outcome.
    #
    # So: --strip-all where it is understood, plain `strip` for Mach-O, and a
    # hard error when no candidate can do the job. Never a weaker fallback.
    local stripped=""
    for tool in "${STRIP:-}" llvm-strip strip; do
        [ -n "$tool" ] || continue
        command -v "$tool" >/dev/null 2>&1 || continue
        if "$tool" --strip-all "$binary" 2>/dev/null; then
            stripped="$tool --strip-all"
        elif [ "$GOOS" = "darwin" ] && "$tool" "$binary" 2>/dev/null; then
            stripped="$tool"
        fi
        [ -n "$stripped" ] && break
    done
    if [ -z "$stripped" ]; then
        echo "package-release: no working strip for $binary" >&2
        return 1
    fi
    if [ "$GOOS" = "darwin" ]; then
        command -v codesign >/dev/null 2>&1 &&
            codesign --sign - --force "$binary" 2>/dev/null
    fi
    echo "=== package-release: stripped $(basename "$binary") ==="
    return 0
}

if [ "$GOOS" = "windows" ]; then
    # Windows ships ONE binary, exactly like every other platform. There is no
    # launcher stub: a small unsigned PE whose entire job is to verify and
    # execute another binary is statically indistinguishable from a dropper,
    # and Defender's ML scored it Trojan:Win32/Wacatac.B!ml on x64 regardless
    # of what we changed (bcrypt-free, stripped, versioned, and even
    # resource-free builds were all flagged, while the product binary itself
    # scans clean on every platform). Self-update — the launcher's whole reason
    # to exist — moves OUT of the running process into install.ps1: Windows'
    # executable lock only blocks a process from replacing ITSELF.
    PAYLOAD="$BUILD_DIR/codebase-memory-mcp"
    [ -f "${PAYLOAD}.exe" ] && PAYLOAD="${PAYLOAD}.exe"
    [ -f "$PAYLOAD" ] || { echo "package-release: build first; missing $PAYLOAD" >&2; exit 2; }
    PACK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cbm-package.XXXXXX")"
    trap 'rm -rf "$PACK_DIR"' EXIT
    cp "$PAYLOAD" "$PACK_DIR/codebase-memory-mcp.exe"
    strip_release_binary "$PACK_DIR/codebase-memory-mcp.exe" || exit 2
    # Gate the artifact AFTER strip: strip is the last byte-changing step, so
    # this inspects exactly what goes into the archive. Runs here rather than in
    # a workflow step so the local artifact-flow smoke enforces the same thing.
    bash scripts/ci/check-binary-composition.sh --variant="$VARIANT" \
        "$PACK_DIR/codebase-memory-mcp.exe" || exit 2
    cp LICENSE install.ps1 "$PACK_DIR/"
    scripts/gen-third-party-notices.sh "$PACK_DIR/THIRD_PARTY_NOTICES.md"
    (
        cd "$PACK_DIR"
        rm -f "$OUT_DIR/$NAME.zip"
        zip -q "$OUT_DIR/$NAME.zip" \
            codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md
    )
    echo "=== package-release: $OUT_DIR/$NAME.zip ==="
else
    [ -f "$BUILD_DIR/codebase-memory-mcp" ] ||
        { echo "package-release: build first; missing $BUILD_DIR/codebase-memory-mcp" >&2; exit 2; }
    strip_release_binary "$BUILD_DIR/codebase-memory-mcp" || exit 2
    bash scripts/ci/check-binary-composition.sh --variant="$VARIANT" \
        "$BUILD_DIR/codebase-memory-mcp" || exit 2
    cp LICENSE install.sh "$BUILD_DIR/"
    scripts/gen-third-party-notices.sh "$BUILD_DIR/THIRD_PARTY_NOTICES.md"
    tar -czf "$OUT_DIR/$NAME.tar.gz" -C "$BUILD_DIR" \
        codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md
    echo "=== package-release: $OUT_DIR/$NAME.tar.gz ==="
fi
