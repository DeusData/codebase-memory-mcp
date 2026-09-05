#!/usr/bin/env bash
set -euo pipefail

# install.sh — One-line installer for codebase-memory-mcp.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh | bash
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
SKIP_CONFIG=false
CLIENTS_SET=false
CLIENTS=""
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

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dir=*)
            INSTALL_DIR="${1#--dir=}"
            shift
            ;;
        --dir)
            if [ "$#" -lt 2 ] || [[ "$2" == -* ]]; then
                echo "install.sh: '--dir' needs a value. Please consult --help." >&2
                exit 2
            fi
            INSTALL_DIR="$2"
            shift 2
            ;;
        --clients=*)
            CLIENTS_SET=true
            CLIENTS="${1#--clients=}"
            shift
            ;;
        --skip-config)
            SKIP_CONFIG=true
            shift
            ;;
        --help|-h)
            echo "Usage: install.sh [--dir=<path>] [--clients=<list>] [--skip-config]"
            echo "  --dir PATH       Install directory (default: ~/.local/bin)"
            echo "  --clients LIST   Configure only comma-separated clients"
            echo "  --skip-config    Skip automatic agent configuration"
            exit 0
            ;;
        -*)
            echo "install.sh: unknown option '$1'. Please consult --help." >&2
            exit 2
            ;;
        *)
            echo "install.sh: unexpected argument '$1'. Please consult --help." >&2
            exit 2
            ;;
    esac
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

ARCHIVE="codebase-memory-mcp-${OS}-${ARCH}${PORTABLE}.${EXT}"

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

# Validate the complete archive namespace before extraction. Current and legacy
# release assets use the same four-member root layout; anything outside that
# closed set is a release-integrity failure, not a sidecar to ignore.
if [ "$OS" = "windows" ]; then
    ARCHIVE_BINARY="codebase-memory-mcp.exe"
    ARCHIVE_INSTALLER="install.ps1"
else
    ARCHIVE_BINARY="codebase-memory-mcp"
    ARCHIVE_INSTALLER="install.sh"
fi
ARCHIVE_MEMBERS_FILE="$DLDIR/archive-members.txt"
if [ "$EXT" = "zip" ]; then
    if ! unzip -Z1 "$DLDIR/$ARCHIVE" > "$ARCHIVE_MEMBERS_FILE"; then
        echo "error: could not enumerate release archive" >&2
        exit 1
    fi
else
    if ! tar -tzf "$DLDIR/$ARCHIVE" > "$ARCHIVE_MEMBERS_FILE"; then
        echo "error: could not enumerate release archive" >&2
        exit 1
    fi
fi

BINARY_MEMBERS=0
LICENSE_MEMBERS=0
INSTALLER_MEMBERS=0
NOTICE_MEMBERS=0
ARCHIVE_MEMBER_COUNT=0
while IFS= read -r member || [ -n "$member" ]; do
    ARCHIVE_MEMBER_COUNT=$((ARCHIVE_MEMBER_COUNT + 1))
    case "$member" in
        "$ARCHIVE_BINARY") BINARY_MEMBERS=$((BINARY_MEMBERS + 1)) ;;
        LICENSE) LICENSE_MEMBERS=$((LICENSE_MEMBERS + 1)) ;;
        "$ARCHIVE_INSTALLER") INSTALLER_MEMBERS=$((INSTALLER_MEMBERS + 1)) ;;
        THIRD_PARTY_NOTICES.md) NOTICE_MEMBERS=$((NOTICE_MEMBERS + 1)) ;;
        *)
            echo "error: release archive contains unexpected member: $member" >&2
            exit 1
            ;;
    esac
done < "$ARCHIVE_MEMBERS_FILE"

if [ "$BINARY_MEMBERS" -ne 1 ] || [ "$LICENSE_MEMBERS" -ne 1 ] ||
    [ "$INSTALLER_MEMBERS" -ne 1 ] || [ "$NOTICE_MEMBERS" -ne 1 ] ||
    [ "$ARCHIVE_MEMBER_COUNT" -ne 4 ]; then
    echo "error: release archive does not match the exact member set" >&2
    exit 1
fi

# Extract
echo "Extracting..."
if [ "$EXT" = "zip" ]; then
    unzip -q "$DLDIR/$ARCHIVE" -d "$DLDIR"
else
    tar --no-same-owner -xzf "$DLDIR/$ARCHIVE" -C "$DLDIR"
fi

for extracted_member in "$ARCHIVE_BINARY" LICENSE "$ARCHIVE_INSTALLER" \
    THIRD_PARTY_NOTICES.md; do
    if [ ! -f "$DLDIR/$extracted_member" ] || [ -L "$DLDIR/$extracted_member" ]; then
        echo "error: release member is not a regular file: $extracted_member" >&2
        exit 1
    fi
done

DLBIN="$DLDIR/$ARCHIVE_BINARY"
if [ ! -f "$DLBIN" ] || [ -L "$DLBIN" ]; then
    echo "error: binary not found after extraction" >&2
    exit 1
fi

# macOS: fix signing
if [ "$OS" = "darwin" ]; then
    echo "Fixing macOS code signing..."
    # A curl-downloaded archive usually carries no quarantine attribute at all,
    # and xattr then prints "No such xattr: com.apple.quarantine" on stderr.
    # That harmless line was read as the cause of an unrelated install failure
    # and became a bug report's title (#1537) — silence it; nothing here is an
    # error worth showing.
    xattr -d com.apple.quarantine "$DLBIN" >/dev/null 2>&1 || true
    codesign --sign - --force "$DLBIN" >/dev/null 2>&1 || true
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
if [ "$CLIENTS_SET" = true ]; then
    INSTALL_ARGS+=("--clients=$CLIENTS")
fi
if [ "$SKIP_CONFIG" = true ]; then
    INSTALL_ARGS+=(--skip-config)
fi
"$DLBIN" install "${INSTALL_ARGS[@]}"

# Place the installer beside the binary so `update` can point at a local file
# instead of a URL, and so the next update uses THIS release's installer.
#
# Two things make this safe. The source is the copy from the archive we just
# checksum-verified -- never "$0", which does not exist under `curl | bash` and
# would pin us to the OLD installer forever. And it is published by atomic
# rename, never by writing over the live path: bash reads a script incrementally
# by byte offset, so overwriting the file it is executing continues reading the
# NEW bytes at the OLD offset. That fails silently and bizarrely, which is worse
# than failing loudly.
#
# Best effort by design: a user who cannot write to INSTALL_DIR still gets a
# working install, and `update` falls back to explaining where to find it.
DL_INSTALLER="$DLDIR/install.sh"
if [ -f "$DL_INSTALLER" ]; then
    INSTALLER_TMP="$INSTALL_DIR/.install.sh.$$"
    if cp "$DL_INSTALLER" "$INSTALLER_TMP" 2>/dev/null &&
        chmod 755 "$INSTALLER_TMP" 2>/dev/null &&
        mv -f "$INSTALLER_TMP" "$INSTALL_DIR/install.sh" 2>/dev/null; then
        echo "Installed updater -> $INSTALL_DIR/install.sh"
    else
        rm -f "$INSTALLER_TMP" 2>/dev/null || true
        echo "note: could not place install.sh in $INSTALL_DIR (update will explain where to find it)"
    fi
fi

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
