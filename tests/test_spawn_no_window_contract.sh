#!/usr/bin/env bash
# Contract: every CreateProcessW spawn must suppress its console window.
#
# This exists because the fix for #1427 was applied THREE times and still missed
# a site. The reported symptom is a console window stealing focus mid-typing on a
# stdio MCP session with auto_watch. The flag that prevents it, CREATE_NO_WINDOW,
# has to be set at each spawn independently, and the sites do not look alike:
#
#   daemon/bootstrap.c   had it from the start   (DETACHED_PROCESS | ... | CREATE_NO_WINDOW)
#   subprocess.c         added by #1448          (flags variable)
#   compat_fs.c :313     added by #1448          (inline literal, popen path)
#   compat_fs.c :689     MISSED by #1448         (dwCreationFlags was a bare 0)
#
# The missed one is the helper that runs git / codesign / open, i.e. the one a
# user meets most often. Nobody was wrong to miss it: it is a different function
# from the popen site in the same file, and reviewing "does the PR add the flag"
# says nothing about the sites the PR does not touch. Only a whole-tree property
# catches that, so this asserts the property rather than any one call.
#
# Python, not shell: this needs the 6th argument of a call spanning lines, and
# the enclosing function's flags variable when that argument is not a literal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 - "$ROOT" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])


def strip_comments(text):
    """Blank out C comments, preserving line count and column positions.

    Mandatory, not tidiness: the first version of this contract matched the flag
    name anywhere near the call, so the explanatory COMMENT sitting above the
    fixed call site satisfied it — the test passed with the fix reverted. A
    contract that can be satisfied by prose about the contract is a false guard.
    """
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

# A site may opt out only with a justified, at-the-case entry (O10): the exact
# "file:function" plus WHY. Empty on purpose — every current site sets the flag.
ALLOWLIST = {
    # "src/foo/bar.c:some_function": "why a console window is required here",
}

FLAG = "CREATE_NO_WINDOW"
failures = []
checked = 0

for path in sorted(root.glob("src/**/*.c")):
    raw = path.read_text(encoding="utf-8", errors="replace")
    if "CreateProcessW(" not in raw:
        continue
    # Comments are blanked FIRST, so neither the call scan nor the flag check can
    # be satisfied by prose. Line numbers still map to the real file.
    text = strip_comments(raw)
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if "CreateProcessW(" not in line:
            continue
        checked += 1

        # Enclosing function: nearest preceding line that starts in column 0 and
        # looks like a definition. Good enough — these files are plain C.
        func = "<unknown>"
        for back in range(idx, -1, -1):
            m = re.match(r"^(?:static\s+)?[A-Za-z_][\w \t*]*\b(\w+)\s*\(", lines[back])
            if m and not lines[back].lstrip().startswith(("*", "//", "/*")):
                func = m.group(1)
                break
        rel = path.relative_to(root).as_posix()
        key = f"{rel}:{func}"
        if key in ALLOWLIST:
            continue

        # The call may wrap across lines; take a window around it, plus the
        # enclosing function body, so a `DWORD flags = ...` assignment counts.
        call_window = "\n".join(lines[idx:idx + 4])
        body_start = max(0, idx - 60)
        body_window = "\n".join(lines[body_start:idx + 4])

        if FLAG in call_window or FLAG in body_window:
            continue
        failures.append(
            f"{rel}:{idx + 1} ({func}) spawns without {FLAG}\n"
            f"      {line.strip()}"
        )

if checked == 0:
    print("FAIL: no CreateProcessW call sites found — this contract has stopped "
          "checking anything (did the spawn layer move?)")
    sys.exit(1)

if failures:
    print(f"FAIL: {len(failures)} CreateProcessW site(s) can pop a console window (#1427):\n")
    for f in failures:
        print(f"  - {f}")
    print(f"\nAdd {FLAG} to dwCreationFlags. If a site genuinely needs a console, "
          "add it to ALLOWLIST in this file with the reason.")
    sys.exit(1)

print(f"OK: all {checked} CreateProcessW site(s) set {FLAG}")
PY
