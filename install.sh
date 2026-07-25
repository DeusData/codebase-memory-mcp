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

# Wrap in main() to prevent partial execution from piped downloads.
# If curl|bash is interrupted mid-transfer, bash would execute the partial
# script. With this wrapper, the function is defined but main() is never
# called because the final line hasn't arrived yet.
main() {

REPO="DeusData/codebase-memory-mcp"
INSTALL_DIR="$HOME/.local/bin"
VARIANT="standard"
SKIP_CONFIG=false
CBM_DOWNLOAD_URL="${CBM_DOWNLOAD_URL:-https://github.com/${REPO}/releases/latest/download}"

# Security: every remote hop must remain HTTPS. Plain HTTP is accepted only
# for an exact loopback authority used by local smoke tests, with redirects
# disabled so a local fixture cannot bounce the installer to the network.
is_loopback_http_url() {
    [[ "$1" =~ ^http://(localhost|127\.0\.0\.1|\[::1\])(:[0-9]+)?([/?\#].*)?$ ]]
}

if [[ "$CBM_DOWNLOAD_URL" == https://* ]]; then
    CBM_DOWNLOAD_LOOPBACK=false
elif is_loopback_http_url "$CBM_DOWNLOAD_URL"; then
    CBM_DOWNLOAD_LOOPBACK=true
else
    echo "error: refusing non-HTTPS download URL: $CBM_DOWNLOAD_URL" >&2
    exit 1
fi

download_file() {
    local url="$1"
    local destination="$2"
    local progress="$3"
    if [ "$CBM_DOWNLOAD_LOOPBACK" = true ]; then
        is_loopback_http_url "$url" || {
            echo "error: loopback download escaped its authority: $url" >&2
            return 1
        }
        if command -v curl &>/dev/null; then
            local curl_args=(-fS --noproxy '*' --proto '=http')
            [ "$progress" = true ] && curl_args+=(--progress-bar) || curl_args+=(-s)
            curl "${curl_args[@]}" -o "$destination" "$url"
        elif command -v wget &>/dev/null; then
            local wget_args=(--no-proxy --max-redirect=0)
            [ "$progress" = true ] && wget_args+=(--show-progress) || wget_args+=(-q)
            wget "${wget_args[@]}" -O "$destination" "$url"
        else
            echo "error: curl or wget required" >&2
            return 1
        fi
        return
    fi

    [[ "$url" == https://* ]] || {
        echo "error: HTTPS download downgraded: $url" >&2
        return 1
    }
    if command -v curl &>/dev/null; then
        local curl_args=(-fSL --max-redirs 5 --proto '=https' --proto-redir '=https')
        [ "$progress" = true ] && curl_args+=(--progress-bar) || curl_args+=(-sS)
        curl "${curl_args[@]}" -o "$destination" "$url"
    elif command -v wget &>/dev/null; then
        local wget_args=(--https-only --max-redirect=5)
        [ "$progress" = true ] && wget_args+=(--show-progress) || wget_args+=(-q)
        wget "${wget_args[@]}" -O "$destination" "$url"
    else
        echo "error: curl or wget required" >&2
        return 1
    fi
}

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

# Linux ships a fully-static "-portable" build; the standard linux binary
# dynamically links glibc 2.38+ and fails on older distros (Debian 11, RHEL 8,
# Ubuntu 20.04). macOS/Windows have no such variant.
PORTABLE=""
[ "$OS" = "linux" ] && PORTABLE="-portable"

if [ "$VARIANT" = "ui" ]; then
    ARCHIVE="codebase-memory-mcp-ui-${OS}-${ARCH}${PORTABLE}.${EXT}"
else
    ARCHIVE="codebase-memory-mcp-${OS}-${ARCH}${PORTABLE}.${EXT}"
fi

URL="${CBM_DOWNLOAD_URL}/${ARCHIVE}"

# Download
DLDIR=$(mktemp -d)
trap 'rm -rf "$DLDIR"' EXIT

echo "Downloading ${ARCHIVE}..."
download_file "$URL" "$DLDIR/$ARCHIVE" true

# Checksum verification is mandatory. Activation must never stop running CBM
# sessions for a candidate whose published digest was not positively verified.
CHECKSUM_URL="${CBM_DOWNLOAD_URL}/checksums.txt"
download_file "$CHECKSUM_URL" "$DLDIR/checksums.txt" false || {
    echo "error: could not download checksums.txt" >&2
    exit 1
}
CHECKSUM_BYTES=$(wc -c < "$DLDIR/checksums.txt" | tr -d '[:space:]')
case "$CHECKSUM_BYTES" in
    ''|*[!0-9]*)
        echo "error: could not determine checksums.txt size" >&2
        exit 1
        ;;
esac
if [ "$CHECKSUM_BYTES" -gt 1048576 ]; then
    echo "error: checksums.txt exceeds the 1 MiB safety limit" >&2
    exit 1
fi
awk -v archive="$ARCHIVE" \
    '$2 == archive || $2 == "*" archive { print $1 }' \
    "$DLDIR/checksums.txt" > "$DLDIR/matching-checksums.txt"
EXPECTED=""
while IFS= read -r digest; do
    case "$digest" in
        ''|*[!0-9A-Fa-f]*)
            echo "error: invalid SHA-256 digest for $ARCHIVE" >&2
            exit 1
            ;;
    esac
    if [ "${#digest}" -ne 64 ]; then
        echo "error: invalid SHA-256 digest length for $ARCHIVE" >&2
        exit 1
    fi
    digest=$(printf '%s' "$digest" | tr 'A-F' 'a-f')
    if [ -n "$EXPECTED" ] && [ "$EXPECTED" != "$digest" ]; then
        echo "error: conflicting SHA-256 digests for $ARCHIVE" >&2
        exit 1
    fi
    EXPECTED="$digest"
done < "$DLDIR/matching-checksums.txt"
if [ -z "$EXPECTED" ]; then
    echo "error: no SHA-256 digest for $ARCHIVE in checksums.txt" >&2
    exit 1
fi
if command -v sha256sum &>/dev/null; then
    ACTUAL=$(sha256sum "$DLDIR/$ARCHIVE" | awk '{print $1}')
elif command -v shasum &>/dev/null; then
    ACTUAL=$(shasum -a 256 "$DLDIR/$ARCHIVE" | awk '{print $1}')
else
    echo "error: sha256sum or shasum is required to verify the download" >&2
    exit 1
fi
ACTUAL=$(printf '%s' "$ACTUAL" | tr 'A-F' 'a-f')
if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "error: CHECKSUM MISMATCH — download may be corrupted!" >&2
    echo "  expected: $EXPECTED" >&2
    echo "  actual:   $ACTUAL" >&2
    exit 1
fi
echo "Checksum verified."

# Extract
echo "Extracting..."
if [ "$EXT" = "zip" ]; then
    unzip -q "$DLDIR/$ARCHIVE" -d "$DLDIR"
else
    tar -xzf "$DLDIR/$ARCHIVE" -C "$DLDIR"
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

# Verify the candidate before it requests account-wide maintenance. The
# candidate itself owns process draining and the transactional target swap.
chmod 755 "$DLBIN"
if ! CANDIDATE_VERSION=$("$DLBIN" --version 2>&1); then
    echo "error: downloaded binary failed to run" >&2
    exit 1
fi
echo "Verified candidate: $CANDIDATE_VERSION"

DEST="$INSTALL_DIR/codebase-memory-mcp"
INSTALL_ARGS=(-y --force "--dir=$INSTALL_DIR")
if [ "$SKIP_CONFIG" = true ]; then
    INSTALL_ARGS+=(--skip-config)
fi
"$DLBIN" install "${INSTALL_ARGS[@]}"

# Verify
VERSION=$("$DEST" --version 2>&1) || {
    echo "error: installed binary failed to run" >&2
    if [ "$OS" = "darwin" ]; then
        echo "  try: xattr -cr $DEST && codesign --force --sign - $DEST" >&2
    fi
    exit 1
}
echo "Installed: $VERSION"

# Agent configuration is part of the candidate-owned activation window.
if [ "$SKIP_CONFIG" = true ]; then
    echo ""
    echo "Skipping agent configuration (--skip-config)"
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

} # end main()

main "$@"
