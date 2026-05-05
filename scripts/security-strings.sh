#!/usr/bin/env bash
set -euo pipefail

# Layer 2: Binary string audit — post-build check on the production binary.
#
# Scans extracted strings for:
#   1. Unauthorized URLs (only github.com + localhost allowed)
#   2. Suspiciously long base64-encoded payloads
#   3. Dangerous command names (wget, nc, netcat, telnet, ssh, /dev/tcp)
#   4. Credential patterns (password=, secret=, api_key=)
#
# Usage: scripts/security-strings.sh <binary-path>

BINARY="${1:?usage: security-strings.sh <binary-path>}"

if [[ ! -f "$BINARY" ]]; then
    echo "FAIL: binary not found: $BINARY"
    exit 1
fi

FAIL=0

# Detect file type. Shell scripts and other text files extract poorly via
# `strings` (the entire content becomes the "strings"), and rules tuned for
# compiled binaries (URL allowlist, wget/telnet detection) produce false
# positives — install.sh legitimately uses `wget` as a curl fallback, and
# `case` glob patterns like `https://*` look like unauthorized URLs.
#
# Strategy: for non-binary files we still run credential and base64 audits
# (those are universally meaningful), but skip the URL and dangerous-command
# audits which are designed for compiled artifacts. Script content is
# reviewed in PRs and scanned end-to-end by VirusTotal in the same pipeline.
IS_SCRIPT=false
if command -v file &>/dev/null; then
    FILE_TYPE=$(file -b "$BINARY" 2>/dev/null || echo "")
    case "$FILE_TYPE" in
        *"shell script"*|*"ASCII text"*|*"UTF-8 Unicode text"*|*"Unicode text"*|*"a /usr/bin/env"*)
            IS_SCRIPT=true
            ;;
    esac
fi

if $IS_SCRIPT; then
    echo "=== Layer 2: Script Content Audit ==="
else
    echo "=== Layer 2: Binary String Audit ==="
fi
echo "File: $BINARY"
echo ""

# Check for strings command (needs binutils on some MSYS2 setups)
if ! command -v strings &>/dev/null; then
    echo "SKIP: 'strings' command not available"
    exit 0
fi

# Extract all printable strings (min length 4)
STRINGS_FILE=$(mktemp)
SEC_CMDS=$(mktemp)
SEC_CREDS=$(mktemp)
trap 'rm -f "$STRINGS_FILE" "$SEC_CMDS" "$SEC_CREDS"' EXIT
strings -n 4 "$BINARY" | sort -u > "$STRINGS_FILE"

# ── 1. URL audit (binary only — scripts handled via VT + PR review) ────

if $IS_SCRIPT; then
    echo "--- URL audit (skipped — script file) ---"
else
echo "--- URL audit ---"

# Allowed URL prefixes
ALLOWED_URLS=(
    "https://api.github.com/repos/DeusData/codebase-memory-mcp"
    "https://github.com/DeusData/codebase-memory-mcp"
    "http://127.0.0.1"
    "http://localhost"
    # SQLite internal URLs (part of vendored sqlite3 strings)
    "https://sqlite.org"
    "https://www.sqlite.org"
    # Toolchain URLs embedded by compiler/linker in static builds
    "https://bugs.launchpad.net"
    "https://gcc.gnu.org"
    "https://sourceware.org"
    # W3C XML namespace URIs (SVG, MathML, XLink — used in UI bundle)
    "http://www.w3.org/"
    # UI bundle: React, Three.js, Tailwind, Google Fonts, bundled libraries
    "https://react.dev"
    "https://fonts.googleapis.com"
    "https://fonts.gstatic.com"
    "https://tailwindcss.com"
    "https://cdn.jsdelivr.net"
    "https://docs.pmnd.rs"
    "https://jcgt.org"
    "https://github.com/pmndrs"
    "https://github.com/react-spring"
    "https://github.com/101arrowz"
    "https://github.com/arty-name"
    "https://github.com/fredli74"
    "https://github.com/lojjic"
)

while IFS= read -r url; do
    # Skip short false positives from binary data (e.g. "https://H9")
    if [[ ${#url} -lt 15 ]]; then
        continue
    fi
    allowed=false
    for prefix in "${ALLOWED_URLS[@]}"; do
        if [[ "$url" == "$prefix"* ]]; then
            allowed=true
            break
        fi
    done
    if ! $allowed; then
        echo "BLOCKED: Unauthorized URL in binary: $url"
        FAIL=1
    fi
done < <(grep -oE 'https?://[a-zA-Z0-9._/~:@!$&()*+,;=?#%[-]+' "$STRINGS_FILE" || true)

if [[ $FAIL -eq 0 ]]; then
    echo "OK: All URLs are authorized."
fi
fi  # end !IS_SCRIPT URL audit

# ── 2. Base64 payload detection ──────────────────────────────────

echo ""
echo "--- Base64 payload detection ---"

# Look for base64-like strings longer than 100 chars (potential encoded payloads)
B64_COUNT=$(grep -cE '^[A-Za-z0-9+/]{100,}={0,2}$' "$STRINGS_FILE" || true)
if [[ "$B64_COUNT" -gt 0 ]]; then
    echo "WARNING: Found $B64_COUNT potential base64-encoded strings > 100 chars"
    grep -E '^[A-Za-z0-9+/]{100,}={0,2}$' "$STRINGS_FILE" | head -5 | while IFS= read -r s; do
        echo "  ${s:0:80}..."
    done
    # Warning only — tree-sitter grammar data can look like base64
else
    echo "OK: No suspicious base64 payloads found."
fi

# ── 3. Dangerous command detection (binary only) ─────────────────

echo ""
if $IS_SCRIPT; then
    echo "--- Dangerous command detection (skipped — script file) ---"
    DANGEROUS_CMDS=''
else
echo "--- Dangerous command detection ---"

DANGEROUS_CMDS='wget|netcat|ncat|/dev/tcp|telnet'
# Known-benign matches (vendored grammar URI scheme tables, etc.). Each entry
# is a regex matched against the full line; matches are stripped before
# evaluation. Document the source so reviewers can verify the false positive.
ALLOWED_DANGEROUS=(
    '^telnet$'  # rst tree-sitter grammar: valid_schemas[] in vendored/grammars/rst/tree_sitter_rst/chars.c
)
if grep -wE "$DANGEROUS_CMDS" "$STRINGS_FILE" > "$SEC_CMDS" 2>/dev/null && [ -s "$SEC_CMDS" ]; then
    for allow in "${ALLOWED_DANGEROUS[@]}"; do
        grep -vE "$allow" "$SEC_CMDS" > "${SEC_CMDS}.tmp" || true
        mv "${SEC_CMDS}.tmp" "$SEC_CMDS"
    done
fi
if [ -s "$SEC_CMDS" ]; then
    echo "BLOCKED: Dangerous commands found in binary:"
    cat "$SEC_CMDS"
    FAIL=1
else
    echo "OK: No dangerous commands found."
fi
fi  # end !IS_SCRIPT dangerous-cmd audit

# ── 4. Credential pattern detection ──────────────────────────────

echo ""
echo "--- Credential pattern detection ---"

CRED_PATTERNS='password=|secret=|api_key=|apikey=|auth_token=|private_key='
if grep -iE "$CRED_PATTERNS" "$STRINGS_FILE" > "$SEC_CREDS" 2>/dev/null; then
    echo "BLOCKED: Credential patterns found in binary:"
    cat "$SEC_CREDS"
    FAIL=1
else
    echo "OK: No credential patterns found."
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "=== BINARY STRING AUDIT FAILED ==="
    exit 1
fi

echo "=== Binary string audit passed ==="
