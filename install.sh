#!/usr/bin/env bash
set -euo pipefail

# install.sh — One-line installer for codebase-memory-mcp.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh | bash
#   curl -fsSL ... | bash -s -- --ui          # Install the UI variant
#   curl -fsSL ... | bash -s -- --dir /path   # Custom install directory
#
# Environment:
#   CBM_DOWNLOAD_URL  Override base URL for downloads (for testing)

REPO="DeusData/codebase-memory-mcp"
INSTALL_DIR="$HOME/.local/bin"
VARIANT="standard"
SKIP_CONFIG=false
CBM_DOWNLOAD_URL="${CBM_DOWNLOAD_URL:-https://github.com/${REPO}/releases/latest/download}"

for arg in "$@"; do
    case "$arg" in
        --ui)           VARIANT="ui" ;;
        --standard)     VARIANT="standard" ;;
        --dir=*)        INSTALL_DIR="${arg#--dir=}" ;;
        --skip-config)  SKIP_CONFIG=true ;;
        --help|-h)
            echo "Usage: install.sh [--ui] [--dir=<path>] [--skip-config]"
            echo "  --ui           Install the UI variant (with graph visualization)"
            echo "  --standard     Install the standard variant (default)"
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

detect_os() {
    case "$(uname -s)" in
        Darwin)               echo "darwin" ;;
        Linux)                echo "linux" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *) echo "error: unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
}

detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        arm64|aarch64) echo "arm64" ;;
        x86_64|amd64)
            # Rosetta detection: shell reports x86_64 but hardware is Apple Silicon
            if [ "$(uname -s)" = "Darwin" ] && sysctl -n machdep.cpu.brand_string 2>/dev/null | grep -qi apple; then
                echo "arm64"
            else
                echo "amd64"
            fi
            ;;
        *) echo "error: unsupported architecture: $arch" >&2; exit 1 ;;
    esac
}

OS=$(detect_os)
ARCH=$(detect_arch)

echo "codebase-memory-mcp installer"
echo "  os:      $OS"
echo "  arch:    $ARCH"
echo "  variant: $VARIANT"
echo "  target:  $INSTALL_DIR/codebase-memory-mcp"
echo ""

# Build download URL
if [ "$OS" = "windows" ]; then
    EXT="zip"
else
    EXT="tar.gz"
fi

if [ "$VARIANT" = "ui" ]; then
    ARCHIVE="codebase-memory-mcp-ui-${OS}-${ARCH}.${EXT}"
else
    ARCHIVE="codebase-memory-mcp-${OS}-${ARCH}.${EXT}"
fi

URL="${CBM_DOWNLOAD_URL}/${ARCHIVE}"

# Download
DLDIR=$(mktemp -d)
trap 'rm -rf "$DLDIR"' EXIT

echo "Downloading ${ARCHIVE}..."
if command -v curl &>/dev/null; then
    curl -fSL --progress-bar -o "$DLDIR/$ARCHIVE" "$URL"
elif command -v wget &>/dev/null; then
    wget -q --show-progress -O "$DLDIR/$ARCHIVE" "$URL"
else
    echo "error: curl or wget required" >&2
    exit 1
fi

# Checksum verification
CHECKSUM_URL="${CBM_DOWNLOAD_URL}/checksums.txt"
if curl -fsSL -o "$DLDIR/checksums.txt" "$CHECKSUM_URL" 2>/dev/null; then
    EXPECTED=$(grep "$ARCHIVE" "$DLDIR/checksums.txt" | awk '{print $1}')
    if [ -n "$EXPECTED" ]; then
        if command -v sha256sum &>/dev/null; then
            ACTUAL=$(sha256sum "$DLDIR/$ARCHIVE" | awk '{print $1}')
        elif command -v shasum &>/dev/null; then
            ACTUAL=$(shasum -a 256 "$DLDIR/$ARCHIVE" | awk '{print $1}')
        else
            ACTUAL=""
        fi
        if [ -n "$ACTUAL" ] && [ "$EXPECTED" != "$ACTUAL" ]; then
            echo "error: CHECKSUM MISMATCH — download may be corrupted!" >&2
            echo "  expected: $EXPECTED" >&2
            echo "  actual:   $ACTUAL" >&2
            exit 1
        elif [ -n "$ACTUAL" ]; then
            echo "Checksum verified."
        fi
    fi
fi

# Extract
echo "Extracting..."
cd "$DLDIR"
if [ "$EXT" = "zip" ]; then
    unzip -q "$ARCHIVE"
else
    tar -xzf "$ARCHIVE"
fi

DLBIN="$DLDIR/codebase-memory-mcp"
if [ ! -f "$DLBIN" ]; then
    echo "error: binary not found after extraction" >&2
    exit 1
fi

# macOS: fix signing
if [ "$OS" = "darwin" ]; then
    echo "Fixing macOS code signing..."
    xattr -d com.apple.quarantine "$DLBIN" 2>/dev/null || true
    codesign --sign - --force "$DLBIN" 2>/dev/null || true
fi

# Install
mkdir -p "$INSTALL_DIR"
DEST="$INSTALL_DIR/codebase-memory-mcp"
if [ -f "$DEST" ]; then
    rm -f "$DEST"
fi
cp "$DLBIN" "$DEST"
chmod 755 "$DEST"

# Verify
VERSION=$("$DEST" --version 2>&1) || {
    echo "error: installed binary failed to run" >&2
    if [ "$OS" = "darwin" ]; then
        echo "  try: xattr -cr $DEST && codesign --force --sign - $DEST" >&2
    fi
    exit 1
}
echo "Installed: $VERSION"

# Configure agents
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

# PATH check
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    echo ""
    echo "NOTE: $INSTALL_DIR is not in your PATH."
    echo "Add it to your shell config:"
    echo ""
    echo "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.zshrc"
fi

echo ""
echo "Done! Restart your coding agent to start using codebase-memory-mcp."
