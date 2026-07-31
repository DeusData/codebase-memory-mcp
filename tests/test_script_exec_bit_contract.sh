#!/usr/bin/env bash
# Contract: a shell script invoked as a COMMAND must be executable in git.
#
# This exists because it cost a release build phase. scripts/ci/check-binary-
# composition.sh was committed 100644 while being invoked directly from
# scripts/package-release.sh, so every unix build leg died with:
#
#   scripts/package-release.sh: line 190: .../check-binary-composition.sh: Permission denied
#
# It passed every local check because the WORKING COPY had the bit — only the
# committed mode was wrong, which no amount of running it locally can reveal.
# scripts/ci/append-vt-notes.sh had the identical defect queued up for the verify
# step at the very end of the release.
#
# The rule is about the CALL SITE, not the file: `bash foo.sh` is equally correct
# and mode-independent (and is what most of this repo does). What must not happen
# again is a call site that needs the bit paired with a file that lacks it.
#
# Python, not shell: the analysis is "is this path the first word of a command",
# and expressing that in `case` patterns is how the first version of this file
# got a quoting bug of its own.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 - "$ROOT" <<'PY'
import pathlib
import re
import subprocess
import sys

root = pathlib.Path(sys.argv[1])

def git(*args):
    return subprocess.run(["git", "-C", str(root), *args],
                          capture_output=True, text=True, check=True).stdout

# Tracked .sh files whose COMMITTED mode is non-executable.
non_exec = set()
for line in git("ls-files", "-s", "*.sh").splitlines():
    mode, _, _, path = line.split(maxsplit=3)
    if mode == "100644":
        non_exec.add(path)

# Places that actually execute things.
search_roots = [root / ".github" / "workflows", root / "scripts",
                root / "test-infrastructure"]
search_files = [root / "Makefile.cbm", root / "Makefile"]
for base in search_roots:
    if base.is_dir():
        search_files += [p for p in base.rglob("*")
                         if p.is_file() and p.suffix in {".yml", ".yaml", ".sh", ""}]

# Strip shell/YAML/Make prefixes that mean "not the first word of a command".
INTERPRETED = re.compile(r"^(bash|sh|zsh|source|\.)\s")
LEADERS = re.compile(r"^(?:[-@\t ]*)(?:run:\s*)?(?:then\s+|else\s+|do\s+|&&\s*|\|\|\s*|;\s*)*")

failures = []
for path in search_files:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        continue
    # Join backslash continuations into LOGICAL lines first. Without this,
    #     ... && bash \
    #         test-infrastructure/vm/vm-run-tests.sh --soak
    # reads as a bare script path at the start of a line and reports a false
    # positive — which is how the first draft of this contract flagged a call
    # site that was already correct.
    logical = []
    pending, start = "", 0
    for number, raw in enumerate(text.splitlines(), 1):
        if not pending:
            start = number
        stripped_end = raw.rstrip()
        if stripped_end.endswith("\\"):
            pending += stripped_end[:-1] + " "
            continue
        logical.append((start, pending + raw))
        pending = ""
    if pending:
        logical.append((start, pending))

    for number, raw in logical:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        for script in non_exec:
            if script not in line:
                continue
            # Position of the reference; everything before it must be prefix-y.
            for match in re.finditer(re.escape(script), line):
                head = line[:match.start()]
                head_wo_leaders = LEADERS.sub("", head).lstrip("./")
                if head_wo_leaders.strip():
                    continue          # something real precedes it → an argument
                if INTERPRETED.match(head.strip() + " "):
                    continue          # bash/sh/source foo.sh → mode irrelevant
                if head.strip() in {"", "-", "@"} or LEADERS.fullmatch(head):
                    rel = path.relative_to(root)
                    failures.append(
                        f"{rel}:{number}: executes {script} directly, but its "
                        f"committed mode is 100644\n"
                        f"      fix EITHER side: "
                        f"'git update-index --chmod=+x {script}'  or  'bash {script}'")
                break

if failures:
    for f in sorted(set(failures)):
        print("FAIL: " + f, file=sys.stderr)
    print(f"script exec-bit contract FAILED with {len(set(failures))} violation(s)",
          file=sys.stderr)
    sys.exit(1)
print("PASS: no non-executable script is invoked as a command")
PY
