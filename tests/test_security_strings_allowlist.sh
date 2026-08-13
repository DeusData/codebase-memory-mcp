#!/usr/bin/env bash
# Regression guard: scripts/security-strings.sh must allow-list the MSYS2/MinGW
# toolchain URL that the CLANG64 toolchain embeds into the static Windows binary
# (https://github.com/msys2/MINGW-packages).
#
# Reproduces the smoke-windows dry-run failure:
#   BLOCKED: Unauthorized URL in binary: https://github.com/msys2/MINGW-packages
#   === BINARY STRING AUDIT FAILED ===
#
# Root cause: the URL audit's hardcoded ALLOWED_URLS list did not include the
# MSYS2 package-tracker URL. That URL is a toolchain artifact, analogous to the
# gcc.gnu.org / sourceware.org / bugs.launchpad.net entries already allow-listed,
# and only appears in the Windows (.exe) build — hence Linux smoke stayed green.
#
# The negative-control case proves the fix does not weaken the audit: a genuinely
# unauthorized URL must still be BLOCKED.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT/scripts/security-strings.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Build a fixture that `file` classifies as binary "data" (NOT text/script), so
# security-strings.sh runs the URL audit (it intentionally skips the URL audit
# for script/text files). Leading non-printable bytes + NUL separators => data.
make_fixture() {
    local out="$1"; shift
    printf '\x00\x01\x02\x03\x04\x05\x06\x07\xff\xfe\xfd\xfc' > "$out"
    local s
    for s in "$@"; do
        printf '%s\x00' "$s" >> "$out"
    done
    printf '\x00\x00\x00\x00\xff\xff\xff\xff' >> "$out"
}

PASS=0
FAIL=0

# ── Case 1 (the bug): toolchain URL present => audit MUST pass (exit 0) ──
GOOD="$TMP/good.bin"
make_fixture "$GOOD" \
    "https://github.com/DeusData/codebase-memory-mcp" \
    "https://github.com/msys2/MINGW-packages"
if bash "$SCRIPT" "$GOOD" >/dev/null 2>&1; then
    echo "PASS: MSYS2 toolchain URL https://github.com/msys2/MINGW-packages is allow-listed"
    PASS=$((PASS + 1))
else
    echo "FAIL: security-strings.sh blocked the MSYS2 toolchain URL (regression)"
    bash "$SCRIPT" "$GOOD" 2>&1 | grep -i "BLOCKED" || true
    FAIL=$((FAIL + 1))
fi

# ── Case 2 (negative control): unauthorized URL MUST still be blocked ──
BAD="$TMP/bad.bin"
make_fixture "$BAD" "https://evil.example.com/exfil-payload-endpoint"
if bash "$SCRIPT" "$BAD" >/dev/null 2>&1; then
    echo "FAIL: unauthorized URL https://evil.example.com was NOT blocked (audit weakened)"
    FAIL=$((FAIL + 1))
else
    echo "PASS: unauthorized URL https://evil.example.com still blocked"
    PASS=$((PASS + 1))
fi

# ── Case 3: an .mcpb bundle manifest must not be audited as a compiled binary ──
# `file` reports JSON as "JSON data", which matched none of the text patterns, so
# the manifest was run through the URL audit and its own homepage/documentation
# fields were BLOCKED as unauthorized. This blocked the v0.10.3 release — the
# first release to ship .mcpb bundles, hence the first time a manifest reached
# the scanned object set.
MANIFEST="$TMP/manifest.json"
cat > "$MANIFEST" <<'JSON'
{
  "name": "codebase-memory-mcp",
  "homepage": "https://github.com/DeusData/codebase-memory-mcp",
  "documentation": "https://deusdata.github.io/codebase-memory-mcp/",
  "server": { "command": "codebase-memory-mcp", "args": [] }
}
JSON
if bash "$SCRIPT" "$MANIFEST" >/dev/null 2>&1; then
    echo "PASS: .mcpb manifest.json audited as structured text, not as a binary"
    PASS=$((PASS + 1))
else
    echo "FAIL: manifest.json still audited as a compiled binary (regression)"
    bash "$SCRIPT" "$MANIFEST" 2>&1 | grep -i "BLOCKED" || true
    FAIL=$((FAIL + 1))
fi

# ── Case 4 (negative control for case 3): the exemption is by FILE TYPE, not a
# blanket pass. A binary carrying an unauthorized URL must still be blocked even
# though a .json carrying one would not be — that asymmetry is the design, and
# case 2 above proves the binary half still holds. Here we prove the credential
# audit still runs on structured text, so the exemption is narrow. ──
CREDS="$TMP/creds.json"
# The credential audit matches assignment syntax (api_key=, secret=, ...), which
# is what leaks look like in embedded strings — so the fixture uses that form
# inside the JSON value rather than a JSON "key": "value" pair.
printf '{"connection":"host=db user=admin password=hunter2-not-a-real-secret"}' > "$CREDS"
if bash "$SCRIPT" "$CREDS" >/dev/null 2>&1; then
    echo "FAIL: credential pattern in JSON was NOT flagged (exemption too broad)"
    FAIL=$((FAIL + 1))
else
    echo "PASS: credential audit still runs on structured text"
    PASS=$((PASS + 1))
fi

echo "=== security-strings allow-list test: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
