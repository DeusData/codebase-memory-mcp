#!/usr/bin/env bash
set -euo pipefail

# CI license-compliance gate (runs in the security workflow for both dry
# runs and releases). Two layers:
#   1) Structural: every vendored component directory must physically carry
#      a license file.
#   2) ScanCode Toolkit license detection over every vendored license text
#      and all first-party sources; ONE detection outside
#      scripts/license-policy.json fails the gate (see license-gate-check.py).
#
# Grammar parser bodies (generated parser.c, no headers) are excluded from
# the ScanCode pass for runtime; their per-directory license files ARE
# scanned, and layer 1 guarantees each grammar carries one.
#
# Requires: scancode (pipx install scancode-toolkit), python3.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --selftest: plant a violation and assert the structural layer catches it.
# Run in CI before the real gate so a silently-broken gate cannot pass.
if [ "${1:-}" = "--selftest" ]; then
    TESTDIR="$ROOT/vendored/.gate-selftest-$$"
    mkdir -p "$TESTDIR"
    echo "int x;" > "$TESTDIR/x.c"
    # The child gate exits 1 on the planted violation (that is the point) —
    # capture its output first so pipefail cannot mask the grep result.
    GATE_OUT="$("$0" 2>/dev/null || true)"
    rm -rf "$TESTDIR"
    if printf '%s' "$GATE_OUT" | grep -q "BLOCKED: vendored code in .*gate-selftest"; then
        echo "OK: gate self-test — planted violation was detected"
        exit 0
    fi
    echo "FAIL: gate self-test — planted unlicensed file was NOT detected"
    exit 1
fi

echo "=== License gate 1/2: structural coverage ==="
# Rule: every directory under either vendored tree that contains source or
# data files must have a license file in itself or an ancestor directory
# WITHIN the vendored tree. No hardcoded component list — newly vendored
# code with no license is flagged immediately.
MISS=0
has_license_dir() {
    ls "$1" 2>/dev/null | grep -qiE '^(LICENSE|LICENCE|COPYING|UNLICENSE|NOTICE)'
}
for root in vendored internal/cbm/vendored; do
    find "$root" -type f \
        \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.hpp' \
        -o -name '*.S' -o -name '*.bin' -o -name '*.txt' \) \
        ! -iname 'LICENSE*' ! -iname 'COPYING*' ! -iname 'NOTICE*' \
        -exec dirname {} \; | LC_ALL=C sort -u | while IFS= read -r d; do
        cur="$d"
        covered=1
        while :; do
            if has_license_dir "$cur"; then
                covered=0
                break
            fi
            [ "$cur" = "$root" ] && break
            cur="$(dirname "$cur")"
        done
        if [ $covered -ne 0 ]; then
            echo "BLOCKED: vendored code in $d has no license file in itself or any ancestor within $root/"
        fi
    done > /tmp/license-gate-structural.$$
    if [ -s /tmp/license-gate-structural.$$ ]; then
        cat /tmp/license-gate-structural.$$
        MISS=1
    fi
    rm -f /tmp/license-gate-structural.$$
done
if [ $MISS -ne 0 ]; then
    echo "=== LICENSE GATE FAILED (structural) ==="
    exit 1
fi
echo "OK: every vendored source directory is covered by a license file"

echo "=== License gate 2/2: ScanCode detection ==="
if ! command -v scancode &>/dev/null; then
    echo "FAIL: scancode not installed (pipx install scancode-toolkit)"
    exit 1
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

# Inputs: all vendored license/notice texts + all first-party sources.
find vendored internal/cbm/vendored -type f \
    \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' -o -iname 'UNLICENSE*' \) \
    > "$STAGE/files.txt"
find src pkg scripts -type f \
    \( -name '*.c' -o -name '*.h' -o -name '*.sh' -o -name '*.js' \
    -o -name '*.py' -o -name '*.rb' -o -name '*.toml' -o -name '*.json' \) \
    >> "$STAGE/files.txt"

mkdir -p "$STAGE/tree"
tar cf - -T "$STAGE/files.txt" | tar xf - -C "$STAGE/tree"

scancode --license --quiet --processes 2 --json-pp "$STAGE/scan.json" "$STAGE/tree" \
    > "$STAGE/scancode.log" 2>&1 || {
    echo "FAIL: scancode run failed:"
    tail -20 "$STAGE/scancode.log"
    exit 1
}

python3 scripts/license-gate-check.py "$STAGE/scan.json" scripts/license-policy.json

echo "=== License gate 3/3: UI npm production tree ==="
# The -ui binaries embed the compiled frontend bundle; its production
# dependency tree must be allow-listed too. --ignore-scripts: no dependency
# postinstall code runs inside the security job.
if command -v npm &>/dev/null && [ -f graph-ui/package-lock.json ]; then
    if [ ! -d graph-ui/node_modules ]; then
        (cd graph-ui && npm ci --ignore-scripts --silent)
    fi
    python3 scripts/license-gate-check-npm.py graph-ui scripts/license-policy.json
else
    echo "FAIL: npm or graph-ui/package-lock.json unavailable — UI tree unchecked"
    exit 1
fi

echo "=== License gate passed ==="
