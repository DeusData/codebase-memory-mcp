#!/usr/bin/env bash
# Contract: the product must not embed integration-script text.
#
# The binary used to carry nine complete shebang'd shell scripts, their
# PowerShell/.cmd twins and the node:child_process client modules as C string
# literals — written to client config dirs at 0755. A block of script-shaped
# bytes inside an unsigned executable is exactly the surface Microsoft
# Defender's ML scored as Trojan:Script/Wacatac.B!ml. Those bodies now live in
# assets/cbm-integrations.json (hash-verified; the binary embeds only the
# SHA-256), and this contract is the proof the removal STAYS removed: one
# revived string literal would silently hand the next release its
# malware-shaped surface back, and nothing else in CI would notice.
#
# Two independent legs:
#   1. SOURCE: no production C source under src/ may contain a shebang or a
#      node:child_process literal. Absence at source level makes absence a
#      BUILD property of every artifact (standard, ui, all platforms) — not a
#      reachability assumption about any one binary.
#   2. ARTIFACT: when build/c/codebase-memory-mcp exists, its bytes are
#      scanned directly. scripts/ci/check-binary-composition.sh (A6) runs the
#      same needles on every RELEASE artifact post-strip, so a missing local
#      build here skips only a redundant leg, never the property.
#
# Comment stripping is mandatory, not tidiness: a contract that a COMMENT can
# satisfy (or trip) is a false guard — see test_spawn_no_window_contract.sh,
# whose first version passed with its fix reverted because prose matched.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 - "$ROOT" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def strip_comments(text):
    """Blank out C comments, preserving line count and column positions."""
    out = []
    i, n = 0, len(text)
    while i < n:
        two = text[i:i + 2]
        if two == "/*":
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append("".join(c if c == "\n" else " " for c in text[i:j]))
            i = j
        elif two == "//":
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


# A file may opt out only with a justified, at-the-case entry (O10): the
# exact repo-relative path plus WHY. Empty on purpose — the templates live in
# assets/cbm-integrations.json, and no production source needs script text.
ALLOWLIST = {
    # "src/foo/bar.c": "why this file legitimately embeds script text",
}

# "#!" alone covers every shebang, including one assembled from adjacent
# string literals ("#!" "/bin/sh"); node:child_process covers the JS bridge.
SOURCE_NEEDLES = ["#!", "node:child_process"]

# Every FIRST-PARTY tree linked into the product binary. Vendored trees are
# deliberately out of scope (pinned upstream code we do not author; the
# artifact leg and the release gate still cover whatever they contribute).
SOURCE_GLOBS = [
    "src/**/*.c",
    "internal/cbm/*.c",
    "internal/cbm/lsp/*.c",
    "internal/cbm/lsp/generated/*.c",
]

failures = []
scanned = 0
sources = sorted(p for g in SOURCE_GLOBS for p in root.glob(g))
for path in sources:
    rel = path.relative_to(root).as_posix()
    scanned += 1
    if rel in ALLOWLIST:
        continue
    text = strip_comments(path.read_text(encoding="utf-8", errors="replace"))
    for needle in SOURCE_NEEDLES:
        for idx, line in enumerate(text.splitlines(), start=1):
            if needle in line:
                failures.append(f"{rel}:{idx} contains '{needle}': {line.strip()[:80]}")

# Anti-vacuity: the files that carried the embedded bodies must still be in
# scope — if they move, this contract must move with them, loudly.
for must_scan in ("src/cli/cli.c", "src/cli/client_adapter.c"):
    if not (root / must_scan).is_file():
        failures.append(f"{must_scan} is gone — re-point this contract at the "
                        "file that now owns integration installs")

if scanned == 0:
    print("FAIL: scanned no production sources — this contract checks nothing")
    sys.exit(1)

if failures:
    print(f"FAIL: {len(failures)} embedded script literal(s) in production sources:\n")
    for f in failures:
        print(f"  - {f}")
    print("\nIntegration bodies belong in assets/cbm-integrations.json (verified by "
          "the embedded SHA-256), never in the binary. A justified exception needs "
          "an ALLOWLIST entry in this file.")
    sys.exit(1)

print(f"OK: {scanned} production source file(s) free of script literals")
PY

# ── Artifact leg ─────────────────────────────────────────────────────
BINARY="build/c/codebase-memory-mcp"
if [ ! -f "$BINARY" ]; then
    # Not a silent green: name exactly why this leg may skip (O10). The source
    # leg above already proved the build property, and the release gate
    # (check-binary-composition.sh A6) scans every shipped artifact.
    echo "skip: $BINARY not built here; artifact leg runs in the release gate (A6)"
    exit 0
fi

fail=0
for needle in '#!/usr/bin/env bash' '#!/bin/bash' '#!/bin/sh' 'node:child_process'; do
    if LC_ALL=C grep -a -q -F -e "$needle" "$BINARY"; then
        echo "FAIL: built binary contains '$needle' — an integration body is embedded again"
        fail=1
    fi
done
# Anti-vacuity canary, same as the release gate: proves the scan reads a real
# artifact rather than a stub/truncated file where absence is meaningless.
if ! LC_ALL=C grep -a -q -F -e 'codebase-memory-mcp' "$BINARY"; then
    echo "FAIL: canary string missing — $BINARY is not a readable product binary"
    fail=1
fi
[ "$fail" -eq 0 ] && echo "OK: built binary carries no shebang or node:child_process bytes"
exit "$fail"
