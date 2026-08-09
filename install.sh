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

# Validate the complete archive namespace before extraction. Standard releases
# ship either the current five-file set (with cbm-integrations.json) or the
# legacy four-file set still published as v0.9.0. UI releases add exactly one
# root-level, content-addressed pack on top of either layout. Anything else is
# a release-integrity failure, not a sidecar to ignore.
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
INTEGRATION_MEMBERS=0
LICENSE_MEMBERS=0
INSTALLER_MEMBERS=0
NOTICE_MEMBERS=0
UI_PACK_MEMBERS=0
ARCHIVE_MEMBER_COUNT=0
UI_PACK_NAME=""
while IFS= read -r member || [ -n "$member" ]; do
    ARCHIVE_MEMBER_COUNT=$((ARCHIVE_MEMBER_COUNT + 1))
    case "$member" in
        "$ARCHIVE_BINARY") BINARY_MEMBERS=$((BINARY_MEMBERS + 1)) ;;
        cbm-integrations.json) INTEGRATION_MEMBERS=$((INTEGRATION_MEMBERS + 1)) ;;
        LICENSE) LICENSE_MEMBERS=$((LICENSE_MEMBERS + 1)) ;;
        "$ARCHIVE_INSTALLER") INSTALLER_MEMBERS=$((INSTALLER_MEMBERS + 1)) ;;
        THIRD_PARTY_NOTICES.md) NOTICE_MEMBERS=$((NOTICE_MEMBERS + 1)) ;;
        *)
            if [ "$VARIANT" = "ui" ] &&
                [[ "$member" =~ ^cbm-ui-[0-9a-f]{64}\.pack$ ]]; then
                UI_PACK_MEMBERS=$((UI_PACK_MEMBERS + 1))
                UI_PACK_NAME="$member"
            else
                echo "error: release archive contains unexpected member: $member" >&2
                exit 1
            fi
            ;;
    esac
done < "$ARCHIVE_MEMBERS_FILE"

# Current releases: 5 members (+1 UI pack). Legacy published archives (v0.9.0)
# omit cbm-integrations.json and therefore have 4 core members.
if [ "$INTEGRATION_MEMBERS" -eq 1 ]; then
    EXPECTED_CORE_COUNT=5
elif [ "$INTEGRATION_MEMBERS" -eq 0 ]; then
    EXPECTED_CORE_COUNT=4
else
    echo "error: release archive does not match the exact $VARIANT member set" >&2
    exit 1
fi
EXPECTED_MEMBER_COUNT=$EXPECTED_CORE_COUNT
if [ "$VARIANT" = "ui" ]; then
    EXPECTED_MEMBER_COUNT=$((EXPECTED_CORE_COUNT + 1))
fi
if [ "$BINARY_MEMBERS" -ne 1 ] ||
    [ "$LICENSE_MEMBERS" -ne 1 ] || [ "$INSTALLER_MEMBERS" -ne 1 ] ||
    [ "$NOTICE_MEMBERS" -ne 1 ] ||
    [ "$UI_PACK_MEMBERS" -ne $((EXPECTED_MEMBER_COUNT - EXPECTED_CORE_COUNT)) ] ||
    [ "$ARCHIVE_MEMBER_COUNT" -ne "$EXPECTED_MEMBER_COUNT" ]; then
    echo "error: release archive does not match the exact $VARIANT member set" >&2
    exit 1
fi

# Extract
echo "Extracting..."
if [ "$EXT" = "zip" ]; then
    unzip -q "$DLDIR/$ARCHIVE" -d "$DLDIR"
else
    tar -xzf "$DLDIR/$ARCHIVE" -C "$DLDIR"
fi

REQUIRED_EXTRACTED_MEMBERS=(
    "$ARCHIVE_BINARY"
    LICENSE
    "$ARCHIVE_INSTALLER"
    THIRD_PARTY_NOTICES.md
)
if [ "$INTEGRATION_MEMBERS" -eq 1 ]; then
    REQUIRED_EXTRACTED_MEMBERS+=(cbm-integrations.json)
fi
for extracted_member in "${REQUIRED_EXTRACTED_MEMBERS[@]}"; do
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

DL_UI_PACK=""
if [ "$VARIANT" = "ui" ]; then
    DL_UI_PACK="$DLDIR/$UI_PACK_NAME"
    if [ ! -f "$DL_UI_PACK" ] || [ -L "$DL_UI_PACK" ]; then
        echo "error: UI asset pack not found after extraction" >&2
        exit 1
    fi
    PACK_EXPECTED="${UI_PACK_NAME#cbm-ui-}"
    PACK_EXPECTED="${PACK_EXPECTED%.pack}"
    if command -v sha256sum &>/dev/null; then
        PACK_ACTUAL=$(sha256sum "$DL_UI_PACK" | awk '{print $1}')
    else
        PACK_ACTUAL=$(shasum -a 256 "$DL_UI_PACK" | awk '{print $1}')
    fi
    PACK_ACTUAL=$(printf '%s' "$PACK_ACTUAL" | tr 'A-F' 'a-f')
    if [ "$PACK_EXPECTED" != "$PACK_ACTUAL" ]; then
        echo "error: UI asset pack digest does not match its filename" >&2
        exit 1
    fi
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

# The candidate publishes the runtime set under one native activation guard,
# with sidecars before the executable and retained per-file backups for
# cooperative rollback. The set is intentionally recoverable/fail-closed, not
# claimed to be a crash-atomic multi-file filesystem transaction.
new_exclusive_sibling_temp() {
    local destination="$1"
    local directory basename
    directory="$(dirname "$destination")"
    basename="$(basename "$destination")"
    mktemp "$directory/.${basename}.tmp.XXXXXX"
}

DEST="$INSTALL_DIR/codebase-memory-mcp"
[ "$OS" = "windows" ] && DEST="$INSTALL_DIR/codebase-memory-mcp.exe"
INSTALL_ARGS=(-y --force "--dir=$INSTALL_DIR")
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
    INSTALLER_TMP=""
    if INSTALLER_TMP="$(new_exclusive_sibling_temp "$INSTALL_DIR/install.sh" 2>/dev/null)" &&
        cp "$DL_INSTALLER" "$INSTALLER_TMP" 2>/dev/null &&
        chmod 755 "$INSTALLER_TMP" 2>/dev/null &&
        mv -f "$INSTALLER_TMP" "$INSTALL_DIR/install.sh" 2>/dev/null; then
        echo "Installed updater -> $INSTALL_DIR/install.sh"
    else
        # A non-empty path was exclusively created by mktemp above. If
        # reservation failed, leave every pre-existing sibling untouched.
        [ -z "$INSTALLER_TMP" ] || rm -f "$INSTALLER_TMP" 2>/dev/null || true
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
