#!/usr/bin/env bash
# package-release.sh — THE canonical release-archive step. Every venue that
# turns built binaries into a release archive (release/_build.yml, the local
# artifact-flow smoke lane) runs this file; workflows provide only
# checkout/toolchain/upload around it. Archive names and contents are defined
# HERE, nowhere else, so a local artifact smoke provably exercises the same
# bytes-layout the release publishes.
#
# This script ARCHIVES what scripts/build.sh already produced; it never builds
# or synthesizes another executable.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
    cat <<'EOF'
Usage: scripts/package-release.sh <goos> <goarch> [--out-dir DIR] [VAR=VAL ...]

The canonical release-archive step: identical in the release build and the
local artifact-flow smoke lane.

  goos       linux | darwin | windows | freebsd
  goarch     arch label used verbatim in the archive name (amd64, arm64,
             arm64-portable, ...)
  --out-dir  where to place the archive (default: repository root).

Make passthrough (VAR=VAL, forwarded to the build):
  CC= CXX=   compiler override, e.g. CC=clang CXX=clang++.

Environment:
  BUILD_DIR  build tree to archive from (default build/c).

Archive contents (defined here, canonical) — ONE executable, no sidecars:
  unix:    codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md (.tar.gz)
  windows: codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md (.zip)

Only one build variant ships: the binary carries the graph UI and the agent
integration templates inside itself, so an extracted archive is immediately
complete — no adjacent data file has to resolve for `install` to work.
EOF
}

GOOS=""
GOARCH=""
OUT_DIR="$ROOT"
MAKE_ARGS=()
expect_value=""
for arg in "$@"; do
    case "$expect_value" in
    out-dir) OUT_DIR="$arg"; expect_value=""; continue ;;
    esac
    case "$arg" in
    -h | --help) usage; exit 0 ;;
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
linux | darwin | windows | freebsd) ;;
*) echo "package-release: goos must be linux, darwin, windows or freebsd." >&2; exit 2 ;;
esac
[ -n "$expect_value" ] && { echo "package-release: --$expect_value needs a value." >&2; exit 2; }

BUILD_DIR="${BUILD_DIR:-build/c}"
OUT_DIR="$(mkdir -p "$OUT_DIR" && cd "$OUT_DIR" && pwd)"
NAME="codebase-memory-mcp-${GOOS}-${GOARCH}"

# Ship every release binary stripped. Production already builds without -g, but
# the linker still keeps a ~536 KB .symtab, so releases carried their full
# symbol table to users: bigger downloads and a free map of the internals, with
# nothing gained. Nothing symbolizes at runtime (mem_profile.c is not in the
# production build and never calls backtrace_symbols), so this costs no
# diagnostics.
#
# A historical unstripped linux-amd64 artifact was the sole Microsoft detection
# in release run 30398064336, while related stripped artifacts later scanned
# clean. That is useful release evidence but not controlled feature attribution:
# engine state and other bytes can differ between observations. Independently
# of the opaque verdict, stripping removes an unnecessary symbol surface and
# does not change program behavior.
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
    # Windows ships one executable, exactly like every other runtime set. There is no
    # launcher stub: a small unsigned PE whose entire job is to verify and
    # execute another binary adds loader-like behavior and another artifact to
    # audit. Historical launcher builds received Microsoft Wacatac verdicts,
    # but that observation does not identify a stable feature or establish
    # causation. Self-update — the launcher's whole reason to exist — moves OUT
    # of the running process into install.ps1: Windows' executable lock only
    # blocks a process from replacing ITSELF.
    PAYLOAD="$BUILD_DIR/codebase-memory-mcp"
    [ -f "${PAYLOAD}.exe" ] && PAYLOAD="${PAYLOAD}.exe"
    [ -f "$PAYLOAD" ] || { echo "package-release: build first; missing $PAYLOAD" >&2; exit 2; }
    STAGED_BINARY_NAME="codebase-memory-mcp.exe"
    INSTALLER="install.ps1"
else
    PAYLOAD="$BUILD_DIR/codebase-memory-mcp"
    [ -f "$PAYLOAD" ] || { echo "package-release: build first; missing $PAYLOAD" >&2; exit 2; }
    STAGED_BINARY_NAME="codebase-memory-mcp"
    INSTALLER="install.sh"
fi

# Work from one owner-private directory for every target. The binary is copied
# here BEFORE strip/re-sign and the archive is created from this same directory,
# so the executable that validates the adjacent sidecars is byte-for-byte the
# executable users receive. Keeping the stage beside the build also avoids a
# system /tmp mounted noexec: the validation probe must actually execute.
PACK_DIR="$(mktemp -d "$BUILD_DIR/.cbm-package.XXXXXX")"
trap 'rm -rf "$PACK_DIR"' EXIT
STAGED_BINARY="$PACK_DIR/$STAGED_BINARY_NAME"
cp "$PAYLOAD" "$STAGED_BINARY"
strip_release_binary "$STAGED_BINARY" || exit 2

# Gate the artifact AFTER strip/re-sign: this is the final executable image and
# no later step may mutate it.
bash scripts/ci/check-binary-composition.sh "$STAGED_BINARY" || exit 2

cp LICENSE "$INSTALLER" "$PACK_DIR/"
scripts/gen-third-party-notices.sh "$PACK_DIR/THIRD_PARTY_NOTICES.md"

if [ "$GOOS" = "windows" ]; then
    (
        cd "$PACK_DIR"
        rm -f "$OUT_DIR/$NAME.zip"
        zip -q "$OUT_DIR/$NAME.zip" \
            codebase-memory-mcp.exe LICENSE install.ps1 THIRD_PARTY_NOTICES.md
    )
    echo "=== package-release: $OUT_DIR/$NAME.zip ==="
else
    # BSD tar otherwise materializes macOS extended attributes as hidden
    # AppleDouble `._*` members, violating the exact four-file inventory.
    COPYFILE_DISABLE=1 tar -czf "$OUT_DIR/$NAME.tar.gz" -C "$PACK_DIR" \
        codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md
    echo "=== package-release: $OUT_DIR/$NAME.tar.gz ==="
fi
