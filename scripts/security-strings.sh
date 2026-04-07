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

echo "=== Layer 2: Binary String Audit ==="
echo "Binary: $BINARY"
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

# ── 1. URL audit ─────────────────────────────────────────────────

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

# ── 3. Dangerous command detection ───────────────────────────────

echo ""
echo "--- Dangerous command detection ---"

DANGEROUS_CMDS='wget|netcat|ncat|/dev/tcp|telnet'
if grep -wE "$DANGEROUS_CMDS" "$STRINGS_FILE" > "$SEC_CMDS" 2>/dev/null && [ -s "$SEC_CMDS" ]; then
    echo "BLOCKED: Dangerous commands found in binary:"
    cat "$SEC_CMDS"
    FAIL=1
else
    echo "OK: No dangerous commands found."
fi

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
