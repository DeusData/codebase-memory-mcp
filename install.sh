#!/usr/bin/env bash
set -euo pipefail

# install.sh — Build codebase-memory-mcp from THIS repository's sources and
# install the resulting binary. No downloads: everything needed for the
# standard build is vendored in the repo, so the build runs fully offline.
#
# Usage (from the repository root):
#   ./install.sh                    # Build + install the standard variant
#   ./install.sh --ui               # Build + install the UI variant (needs node/npm)
#   ./install.sh --dir /path        # Custom install directory
#   ./install.sh --skip-config      # Skip automatic agent configuration
#
# Requirements: bash, gcc/g++ (or clang), GNU make.
# The --ui variant additionally requires node + npm (graph-ui bundle).

main() {

# Resolve the repository root from this script's location — the sources we
# build are the ones sitting next to this script.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

INSTALL_DIR="$HOME/.local/bin"
VARIANT="standard"
SKIP_CONFIG=false

for arg in "$@"; do
    case "$arg" in
        --ui)           VARIANT="ui" ;;
        --standard)     VARIANT="standard" ;;
        --dir=*)        INSTALL_DIR="${arg#--dir=}" ;;
        --skip-config)  SKIP_CONFIG=true ;;
        --help|-h)
            echo "Usage: install.sh [--ui] [--dir=<path>] [--skip-config]"
            echo "  --ui           Build the UI variant (with graph visualization; needs node/npm)"
            echo "  --standard     Build the standard variant (default; fully offline build)"
            echo "  --dir PATH     Install directory (default: ~/.local/bin)"
            echo "  --skip-config  Skip automatic agent configuration"
            exit 0
            ;;
    esac
done
# Handle --dir <path> (space-separated)
prev=""
for arg in "$@"; do
    if [ "$prev" = "--dir" ]; then
        INSTALL_DIR="$arg"
    fi
    prev="$arg"
done

# ── Toolchain check ─────────────────────────────────────────────
missing=""
if ! command -v make &>/dev/null; then
    missing="$missing make"
fi
if [ -z "${CC:-}" ] && ! command -v gcc &>/dev/null && ! command -v cc &>/dev/null; then
    missing="$missing gcc"
fi
if [ "$VARIANT" = "ui" ]; then
    command -v node &>/dev/null || missing="$missing node"
    command -v npm  &>/dev/null || missing="$missing npm"
fi
if [ -n "$missing" ]; then
    echo "error: missing build tools:$missing" >&2
    echo "  Install them with your system package manager, e.g.:" >&2
    echo "    Debian/Ubuntu: sudo apt install build-essential" >&2
    echo "    RHEL/Fedora:   sudo dnf install gcc gcc-c++ make" >&2
    echo "    Windows:       MSYS2 (pacman -S mingw-w64-x86_64-gcc make)" >&2
    exit 1
fi

echo "codebase-memory-mcp installer (build from source)"
echo "  repo:    $REPO_ROOT"
echo "  variant: $VARIANT"
echo "  target:  $INSTALL_DIR/codebase-memory-mcp"
echo ""

# ── Build ───────────────────────────────────────────────────────
echo "Building..."
if [ "$VARIANT" = "ui" ]; then
    "$REPO_ROOT/scripts/build.sh" --with-ui
else
    "$REPO_ROOT/scripts/build.sh"
fi

BUILT_BIN="$REPO_ROOT/build/c/codebase-memory-mcp"
if [ ! -f "$BUILT_BIN" ] && [ -f "$BUILT_BIN.exe" ]; then
    BUILT_BIN="$BUILT_BIN.exe"
fi
if [ ! -f "$BUILT_BIN" ]; then
    echo "error: build finished but binary not found at $BUILT_BIN" >&2
    exit 1
fi

# ── Install ─────────────────────────────────────────────────────
mkdir -p "$INSTALL_DIR"
DEST="$INSTALL_DIR/codebase-memory-mcp"
case "$BUILT_BIN" in
    *.exe) DEST="$DEST.exe" ;;
esac
if [ -f "$DEST" ]; then
    rm -f "$DEST"
fi
cp "$BUILT_BIN" "$DEST"
chmod 755 "$DEST"

# macOS: ad-hoc signing (required on arm64, harmless on x86_64)
if [ "$(uname -s)" = "Darwin" ]; then
    xattr -d com.apple.quarantine "$DEST" 2>/dev/null || true
    codesign --sign - --force "$DEST" 2>/dev/null || true
fi

# ── Verify ──────────────────────────────────────────────────────
VERSION=$("$DEST" --version 2>&1) || {
    echo "error: installed binary failed to run" >&2
    exit 1
}
echo "Installed: $VERSION"

# ── Configure agents ────────────────────────────────────────────
if [ "$SKIP_CONFIG" = true ]; then
    echo ""
    echo "Skipping agent configuration (--skip-config)"
else
    echo ""
    echo "Configuring coding agents..."
    "$DEST" install -y 2>&1 || {
        echo ""
        echo "Agent configuration failed (non-fatal)."
        echo "Run manually: codebase-memory-mcp install"
    }
fi

# ── PATH check ──────────────────────────────────────────────────
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    echo ""
    echo "NOTE: $INSTALL_DIR is not in your PATH."
    echo "Add it to your shell config:"
    echo ""
    echo "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.zshrc"
fi

echo ""
echo "Done! Restart your coding agent to start using codebase-memory-mcp."

} # end main()

main "$@"
