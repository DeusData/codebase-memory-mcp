#!/usr/bin/env bash
set -euo pipefail

# codebase-memory-mcp setup script (macOS + Linux)
# Default: install the latest pre-built binary through install.sh
# --from-source: build from source (requires Go + C compiler)

REPO="DeusData/codebase-memory-mcp"
INSTALL_DIR="$HOME/.local/bin"
BINARY_NAME="codebase-memory-mcp"
SOURCE_DIR="$HOME/.local/share/codebase-memory-mcp"
CLEANUP_DIR=""  # set by install_release for EXIT trap

# --- Colors ---

if [ -t 1 ] && command -v tput &>/dev/null; then
    GREEN=$(tput setaf 2)
    RED=$(tput setaf 1)
    YELLOW=$(tput setaf 3)
    BOLD=$(tput bold)
    RESET=$(tput sgr0)
else
    GREEN=""
    RED=""
    YELLOW=""
    BOLD=""
    RESET=""
fi

ok()   { echo "${GREEN}✓${RESET} $*"; }
fail() { echo "${RED}✗${RESET} $*"; }
warn() { echo "${YELLOW}⚠${RESET} $*"; }
info() { echo "  $*"; }

die() { fail "$@"; exit 1; }

# --- Argument parsing ---

FROM_SOURCE=false
for arg in "$@"; do
    case "$arg" in
        --from-source) FROM_SOURCE=true ;;
        --help|-h)
            echo "Usage: $0 [--from-source]"
            echo ""
            echo "  Default:        Download the pre-built binary through install.sh"
            echo "  --from-source:  Clone and build from source (requires Go 1.23+ and a C compiler)"
            exit 0
            ;;
        *) die "Unknown argument: $arg" ;;
    esac
done

# --- Prerequisite checks ---

check_download_tool() {
    if command -v curl &>/dev/null; then
        echo "curl"
    elif command -v wget &>/dev/null; then
        echo "wget"
    else
        die "Neither curl nor wget found. Install one and retry."
    fi
}

check_go_version() {
    if ! command -v go &>/dev/null; then
        die "Go not found. Install Go 1.23+ from https://go.dev/dl/"
    fi

    local version
    version=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | head -1)
    local major minor
    major=$(echo "$version" | grep -oE '[0-9]+' | head -1)
    minor=$(echo "$version" | grep -oE '[0-9]+' | sed -n '2p')

    if [ "$major" -lt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -lt 23 ]; }; then
        die "Go $major.$minor found, but 1.23+ is required. Update from https://go.dev/dl/"
    fi

    ok "Go $major.$minor"
}

check_c_compiler() {
    if command -v cc &>/dev/null || command -v gcc &>/dev/null || command -v clang &>/dev/null; then
        ok "C compiler found"
        return
    fi

    local os
    os=$(uname -s)
    if [ "$os" = "Darwin" ]; then
        die "No C compiler found. Run: xcode-select --install"
    else
        die "No C compiler found. Install build-essential (Debian/Ubuntu) or gcc (Fedora/RHEL)"
    fi
}

check_git() {
    if ! command -v git &>/dev/null; then
        die "Git not found. Install git and retry."
    fi
    ok "Git found"
}

# --- Download + install through install.sh ---
#
# install.sh is the one implementation of "fetch a release and install it": it
# downloads checksums.txt next to the archive, verifies the archive's SHA-256
# against it, checks the archive layout, and only then runs the binary's own
# `install`. This script used to carry a second copy of that download,
# which did not keep up with the installer. It now fetches install.sh from the
# same origin and branch it is itself served from and hands over to it, so
# there is exactly one install path.
#
# CBM_DOWNLOAD_URL (the installers' download-base override, for local testing)
# also moves the installer fetch: install.sh is then taken from
# "$CBM_DOWNLOAD_URL/install.sh".

INSTALLER_URL="https://raw.githubusercontent.com/${REPO}/main/install.sh"
if [ -n "${CBM_DOWNLOAD_URL:-}" ]; then
    INSTALLER_URL="${CBM_DOWNLOAD_URL%/}/install.sh"
fi

# Same transport rule as install.sh: HTTPS everywhere; plain HTTP only for an
# exact loopback authority (the local test fixture), with redirects disabled
# there so a fixture cannot bounce the fetch to the network.
is_loopback_http_url() {
    [[ "$1" =~ ^http://(localhost|127\.0\.0\.1|\[::1\])(:[0-9]+)?([/?\#].*)?$ ]]
}

fetch_installer() {
    local url="$1" destination="$2" tool="$3"
    if is_loopback_http_url "$url"; then
        if [ "$tool" = "curl" ]; then
            curl -fsS --noproxy '*' --proto '=http' -o "$destination" "$url"
        else
            wget -q --no-proxy --max-redirect=0 -O "$destination" "$url"
        fi
    elif [[ "$url" == https://* ]]; then
        if [ "$tool" = "curl" ]; then
            curl -fsSL --max-redirs 5 --proto '=https' --proto-redir '=https' \
                -o "$destination" "$url"
        else
            wget -q --https-only --max-redirect=5 -O "$destination" "$url"
        fi
    else
        die "Refusing non-HTTPS installer URL: $url"
    fi
}

install_release() {
    local tool="$1"

    echo ""
    echo "${BOLD}Fetching install.sh...${RESET}"
    CLEANUP_DIR=$(mktemp -d)
    trap 'rm -rf "$CLEANUP_DIR"' EXIT
    local installer="$CLEANUP_DIR/install.sh"

    fetch_installer "$INSTALLER_URL" "$installer" "$tool" ||
        die "Could not fetch install.sh from ${INSTALLER_URL}"
    [ -s "$installer" ] || die "Fetched an empty install.sh from ${INSTALLER_URL}"
    ok "install.sh fetched"

    echo ""
    echo "${BOLD}Installing through install.sh...${RESET}"
    # The installer configures nothing (--skip-config): agent configuration
    # stays this script's interactive step below. stdin is detached so that,
    # when this script itself arrives through `curl | bash`, nothing the
    # installer runs can read the rest of this script as its input.
    bash "$installer" --dir "$INSTALL_DIR" --skip-config </dev/null ||
        die "install.sh failed (see the messages above)"
}

# --- Build from source ---

build_from_source() {
    echo ""
    echo "${BOLD}Checking prerequisites...${RESET}"
    check_go_version
    check_c_compiler
    check_git

    echo ""
    if [ -d "$SOURCE_DIR/.git" ]; then
        echo "${BOLD}Updating source...${RESET}"
        git -C "$SOURCE_DIR" pull --ff-only
    else
        echo "${BOLD}Cloning repository...${RESET}"
        mkdir -p "$(dirname "$SOURCE_DIR")"
        git clone "https://github.com/${REPO}.git" "$SOURCE_DIR"
    fi
    ok "Source at ${SOURCE_DIR}"

    echo ""
    echo "${BOLD}Building binary (this may take a minute)...${RESET}"
    mkdir -p "$INSTALL_DIR"

    (cd "$SOURCE_DIR" && scripts/build.sh && cp build/c/codebase-memory-mcp "${INSTALL_DIR}/${BINARY_NAME}")

    ok "Built and installed to ${INSTALL_DIR}/${BINARY_NAME}"
}

# --- MCP auto-configuration ---

configure_claude() {
    echo ""
    local binary_path="${INSTALL_DIR}/${BINARY_NAME}"
    local claude_config_dir="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
    local settings_file="${claude_config_dir}/settings.json"

    printf "%s" "${BOLD}Configure Claude Code to use codebase-memory-mcp? [y/N] ${RESET}"
    read -r answer
    if [[ ! "$answer" =~ ^[Yy]$ ]]; then
        echo ""
        info "Add this to your .mcp.json or ${claude_config_dir}/settings.json:"
        echo ""
        echo '  {'
        echo '    "mcpServers": {'
        echo '      "codebase-memory-mcp": {'
        echo '        "type": "stdio",'
        echo "        \"command\": \"${binary_path}\""
        echo '      }'
        echo '    }'
        echo '  }'
        return
    fi

    local mcp_entry
    mcp_entry=$(cat <<JSONEOF
{"type":"stdio","command":"${binary_path}"}
JSONEOF
)

    mkdir -p "$(dirname "$settings_file")"

    if command -v jq &>/dev/null; then
        # Use jq to merge
        if [ -f "$settings_file" ]; then
            local tmp
            tmp=$(mktemp)
            jq --argjson entry "$mcp_entry" '.mcpServers["codebase-memory-mcp"] = $entry' "$settings_file" > "$tmp"
            mv "$tmp" "$settings_file"
        else
            echo "{}" | jq --argjson entry "$mcp_entry" '.mcpServers["codebase-memory-mcp"] = $entry' > "$settings_file"
        fi
        ok "Updated ${settings_file}"
    elif command -v python3 &>/dev/null; then
        python3 -c "
import json, os
path = os.path.expanduser('$settings_file')
data = {}
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
data.setdefault('mcpServers', {})['codebase-memory-mcp'] = json.loads('$mcp_entry')
with open(path, 'w') as f:
    json.dump(data, f, indent=2)
print()
"
        ok "Updated ${settings_file}"
    else
        warn "Neither jq nor python3 found — cannot auto-configure."
        echo ""
        info "Add this to ${settings_file} manually:"
        echo ""
        echo '  "mcpServers": {'
        echo '    "codebase-memory-mcp": {'
        echo '      "type": "stdio",'
        echo "      \"command\": \"${binary_path}\""
        echo '    }'
        echo '  }'
    fi
}

# --- PATH check ---

check_path() {
    if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
        echo ""
        warn "${INSTALL_DIR} is not on your PATH."
        info "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo ""
        echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    fi
}

# --- Main ---

echo ""
echo "${BOLD}codebase-memory-mcp installer${RESET}"
echo ""

if [ "$FROM_SOURCE" = true ]; then
    build_from_source
else
    tool=$(check_download_tool)
    ok "Download tool: ${tool}"
    install_release "$tool"
fi

# Verify binary
if [ ! -x "${INSTALL_DIR}/${BINARY_NAME}" ]; then
    die "Binary at ${INSTALL_DIR}/${BINARY_NAME} is not executable"
fi

ver_output=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>&1) || true
if [ -n "$ver_output" ]; then
    ok "$ver_output"
else
    ok "Binary is executable"
fi

configure_claude
check_path

# --- Git hooks ---
# If run from inside the repo, activate tracked hooks
if [ -d "scripts/hooks" ] && git rev-parse --git-dir &>/dev/null; then
    git config core.hooksPath scripts/hooks
    ok "Git hooks activated (scripts/hooks/)"
fi

echo ""
ok "Done! Restart Claude Code and verify with /mcp"
echo ""
info "To uninstall:"
info "  rm ${INSTALL_DIR}/${BINARY_NAME}"
info "  rm -rf ${SOURCE_DIR}  # if built from source"
info "  rm -rf ~/.cache/codebase-memory-mcp/  # graph database"
