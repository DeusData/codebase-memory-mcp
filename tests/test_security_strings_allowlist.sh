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
# Supply a built UI binary to additionally verify the exact reviewed Monaco
# strings and mutations without checking generated/minified fixtures into git.
# Usage: bash tests/test_security_strings_allowlist.sh [ui-binary-path]
UI_BINARY="${1:-}"

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

# Require the expected detector, not merely a nonzero exit caused by another
# detector or a missing dependency. Retain captured output only on failure.
assert_audit() {
    local expected_status="$1" marker="$2" label="$3" fixture="$4"
    local status=0
    bash "$SCRIPT" "$fixture" > "$TMP/audit.log" 2>&1 || status=$?
    if [[ "$status" -eq "$expected_status" ]] && grep -Fq "$marker" "$TMP/audit.log"; then
        echo "PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $label (exit $status; expected $expected_status and $marker)"
        # Do not dump potentially large embedded JavaScript lines.
        grep -E '^(FAIL|BLOCKED|OK|WARNING|===)' "$TMP/audit.log" || true
        FAIL=$((FAIL + 1))
    fi
}

URL_BLOCKED='BLOCKED: Unauthorized URL in binary:'
COMMAND_BLOCKED='BLOCKED: Dangerous commands found in binary:'
CREDENTIAL_BLOCKED='BLOCKED: Credential patterns found in binary:'
AUDIT_PASSED='=== Binary string audit passed ==='
REFERENCES=(
    'https://web.dev/cross-origin-isolation-guide/'
    'https://code.visualstudio.com/docs/editor/codebasics#_multicursor-modifier).'
    'https://developer.mozilla.org/en-US/docs/Web/API/Cache'
    'https://github.com/huggingface/transformers.js/issues/new/choose'
    'https://github.com/microsoft/vscode/issues/103170'
    'https://github.com/markedjs/marked.'
    'https://tinyurl.com/sudb9s96),'
    'https://ieeexplore.ieee.org/document/1163711'
    'https://code.visualstudio.com/docs/editor/codebasics#_find-and-replace)'
    'https://microsoft.com)'
    'https://github.com/syntax-tree/hast-util-to-jsx-runtime'
    'https://github.com/microsoft/vscode/issues/new'
    'https://github.com/microsoft/vscode/blob/main/LICENSE.txt'
    'https://github.com/microsoft/monaco-editor#faq'
    'https://gist.github.com/hollance/42e32852f24243b748ae6bc1f985b13a'
    'https://github.com/remarkjs/react-markdown/blob/main/changelog.md'
    'https://docs.nvidia.com/cuda/cublas/index.html#cublasLtOrder_t'
    'https://arxiv.org/abs/1502.03167.'
)
for reference in "${REFERENCES[@]}"; do
    make_fixture "$TMP/reference.bin" "$reference"
    assert_audit 0 "$AUDIT_PASSED" "reviewed reference $reference" "$TMP/reference.bin"
    make_fixture "$TMP/reference.bin" "${reference}/unreviewed"
    assert_audit 1 "$URL_BLOCKED" "reference exception has an exact boundary: $reference" "$TMP/reference.bin"
done
for url in \
    'https://web.dev.evil.example/cross-origin-isolation-guide/' \
    'https://developer.mozilla.org.evil.example/en-US/docs/Web/API/Cache' \
    'https://tinyurl.com/another-short-link' \
    'https://huggingface.co.evil.example/model' \
    'https://us.aws.cdn.hf.co.evil.example/model'; do
    make_fixture "$TMP/reference.bin" "$url"
    assert_audit 1 "$URL_BLOCKED" "unreviewed host or path remains blocked: $url" "$TMP/reference.bin"
done

# Similar-looking syntax is insufficient: exemptions bind complete reviewed
# content, not all tokenizer arrays or all boolean password assignments.
make_fixture "$TMP/lookalike.bin" 'const keywords=["wget","telnet"];'
assert_audit 1 "$COMMAND_BLOCKED" 'unreviewed command keyword array is blocked' "$TMP/lookalike.bin"
make_fixture "$TMP/lookalike.bin" 'this.password=!1;'
assert_audit 1 "$CREDENTIAL_BLOCKED" 'unreviewed boolean password assignment is blocked' "$TMP/lookalike.bin"

if [[ -n "$UI_BINARY" ]]; then
    if [[ ! -f "$UI_BINARY" ]] || ! command -v strings >/dev/null; then
        echo 'FAIL: artifact checks require an existing UI binary and strings'
        exit 1
    fi
    assert_audit 0 "$AUDIT_PASSED" 'complete embedded UI binary passes the audit' "$UI_BINARY"

    if command -v sha256sum >/dev/null; then
        HASH_COMMAND=(sha256sum)
    elif command -v shasum >/dev/null; then
        HASH_COMMAND=(shasum -a 256)
    else
        echo 'FAIL: artifact checks require a SHA-256 utility'
        exit 1
    fi
    EXPECTED_HASHES=(
        0d9431238bb1665cc08548a02889077577f6a5e2f7f945ccc8c5128cefa7d688
        91e70ce96dc9392c3091e0e06a838ad43a0c625b007e9e2ff6256a6d4824e9f2
        6e393242a92967b3d1bd03420d0bdfc42b80f18407ef393652580483d2177633
    )
    # Extract from the real artifact. An omitted or changed reviewed line is a
    # failure, never a silently absent fixture. This runs only when requested,
    # so the portable source suite remains usable before building the UI.
    strings -n 4 "$UI_BINARY" | sort -u > "$TMP/artifact.strings"
    grep -iE 'wget|telnet|password=' "$TMP/artifact.strings" > "$TMP/candidates" || true
    while IFS= read -r line; do
        digest=$(printf '%s' "$line" | "${HASH_COMMAND[@]}")
        digest=${digest%% *}
        for index in 0 1 2; do
            if [[ "$digest" == "${EXPECTED_HASHES[$index]}" ]]; then
                printf '%s\n' "$line" > "$TMP/reviewed-$index"
            fi
        done
    done < "$TMP/candidates"

    B64_FIXTURE=$(printf '%0120d' 0 | tr 0 A)
    for index in 0 1 2; do
        if [[ ! -f "$TMP/reviewed-$index" ]]; then
            echo "FAIL: expected reviewed artifact line absent: ${EXPECTED_HASHES[$index]}"
            FAIL=$((FAIL + 1))
            continue
        fi
        IFS= read -r line < "$TMP/reviewed-$index"
        if [[ "$index" -eq 0 ]]; then
            detector="$COMMAND_BLOCKED"
            mutation=';wget synthetic-test-file'
            other_detector="$CREDENTIAL_BLOCKED"
            other_mutation=';api_key=synthetic-test-value'
        else
            detector="$CREDENTIAL_BLOCKED"
            mutation=';api_key=synthetic-test-value'
            other_detector="$COMMAND_BLOCKED"
            other_mutation=';wget synthetic-test-file'
        fi
        make_fixture "$TMP/reviewed.bin" "$line"
        assert_audit 0 "$AUDIT_PASSED" "reviewed artifact line $index is accepted unchanged" "$TMP/reviewed.bin"
        make_fixture "$TMP/reviewed.bin" "$line "
        assert_audit 1 "$detector" "trailing whitespace invalidates reviewed line $index" "$TMP/reviewed.bin"
        make_fixture "$TMP/reviewed.bin" "$line$mutation"
        assert_audit 1 "$detector" "malicious suffix invalidates reviewed line $index" "$TMP/reviewed.bin"
        make_fixture "$TMP/reviewed.bin" "$line$other_mutation"
        assert_audit 1 "$other_detector" "other detector still scans artifact line $index" "$TMP/reviewed.bin"
        make_fixture "$TMP/reviewed.bin" "$line https://evil.example.com/exfil"
        assert_audit 1 "$URL_BLOCKED" "URL detector still scans artifact line $index" "$TMP/reviewed.bin"
        make_fixture "$TMP/reviewed.bin" "$line" "$B64_FIXTURE"
        assert_audit 0 'WARNING: Found 1 potential base64-encoded strings' "base64 scan remains active beside reviewed line $index" "$TMP/reviewed.bin"
    done
fi

echo "=== security-strings allow-list test: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
