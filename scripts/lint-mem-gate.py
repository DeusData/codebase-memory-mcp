#!/usr/bin/env python3
"""Gate clang-analyzer memory findings against a change-invalidated whitelist.

Reads analyzer findings on stdin (raw clang-tidy output) and decides the exit
status:

  * a finding with no whitelist entry            -> FAIL
  * a finding whose entry no longer matches the
    code it was written about                    -> FAIL (stale, re-triage)
  * a finding with a matching entry              -> suppressed
  * an entry that matched no finding             -> reported, does not fail

The middle case is the point of the file. A whitelist entry is an argument
about one specific piece of code, so it is pinned to the sha256 of that
function's text. Edit the function and the entry stops counting: the finding
comes back and has to be argued again against the code as it now is. An
entry can never quietly outlive the reasoning that justified it.

Whitelist format (scripts/lint-mem-whitelist.txt), one block per entry:

    ## <path> :: <function> :: <check>
    segment-sha256: <64 hex chars>
    why: |
      Why this finding is not a real defect, argued against the code.
    tried: |
      What was attempted before concluding it is a false positive.

Regenerate a hash after an intentional edit with:

    scripts/lint-mem-gate.py --hash <path> <function>
"""

import hashlib
import re
import sys
from pathlib import Path

import os

WHITELIST = Path(os.environ.get(
    "LINT_MEM_WHITELIST",
    Path(__file__).resolve().parent / "lint-mem-whitelist.txt"))

# A whitelist entry has to carry an argument, not an assertion. These are the
# mechanical part of that -- a floor on substance, checked automatically. The
# rest (is the argument actually correct?) is a review question and stays one.
MIN_WHY = 120
MIN_TRIED = 40

FINDING_RE = re.compile(r"^(?P<file>[^:]+):(?P<line>\d+):\d+:\s+(?:error|warning):\s+(?P<msg>.*?)\s*\[(?P<check>[\w.-]+)\]\s*$")


def strip_noise(text):
    """Blank out string/char literals and comments so brace counting is sane."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            out.append("  ")
            i += 2
        elif c in "\"'":
            quote = c
            out.append(" ")
            i += 1
            while i < n and text[i] != quote:
                if text[i] == "\\":
                    out.append(" ")
                    i += 1
                if i < n:
                    out.append("\n" if text[i] == "\n" else " ")
                    i += 1
            out.append(" ")
            i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def function_spans(path):
    """Yield (name, start_line, end_line) for each function in a C file.

    1-indexed and inclusive. Deliberately simple: this codebase is
    clang-formatted, so a definition starts at column 0 and its body brace
    opens at nesting depth 0. Constructs whose declaration has no '(' before
    the brace (structs, enums, initializers) are skipped.
    """
    src = path.read_text(encoding="utf-8", errors="replace")
    lines = src.splitlines()
    clean = strip_noise(src).splitlines()

    depth = 0
    decl_start = 0
    for idx, line in enumerate(clean):
        if depth == 0 and line[:1] not in ("", " ", "\t", "#"):
            # Candidate start of a top-level declaration.
            if idx == 0 or clean[idx - 1].strip() == "" or clean[idx - 1].rstrip().endswith(("}", ";")):
                decl_start = idx
        opened = line.count("{")
        closed = line.count("}")
        if depth == 0 and opened:
            decl_text = " ".join(clean[decl_start:idx + 1])
            head = decl_text.split("{", 1)[0]
            if "(" in head:
                name_match = None
                for m in re.finditer(r"([A-Za-z_]\w*)\s*\(", head):
                    name_match = m
                    break
                if name_match:
                    body_start = decl_start
                    d = depth + opened - closed
                    if d > 0:
                        end = idx
                        j = idx
                        while j + 1 < len(clean) and d > 0:
                            j += 1
                            d += clean[j].count("{") - clean[j].count("}")
                            end = j
                        yield (name_match.group(1), body_start + 1, end + 1,
                               "\n".join(l.rstrip() for l in lines[body_start:end + 1]))
        depth += opened - closed
        if depth < 0:
            depth = 0


def enclosing(path, line):
    best = None
    for name, start, end, text in function_spans(path):
        if start <= line <= end:
            if best is None or start > best[1]:
                best = (name, start, end, text)
    return best


def segment_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_whitelist():
    entries = {}
    if not WHITELIST.exists():
        return entries
    block_key = None
    field = None
    data = {}

    def flush():
        if block_key:
            entries[block_key] = dict(data)

    for raw in WHITELIST.read_text(encoding="utf-8").splitlines():
        if raw.startswith("## "):
            flush()
            parts = [p.strip() for p in raw[3:].split("::")]
            if len(parts) != 3:
                sys.stderr.write(f"lint-mem-gate: malformed header: {raw}\n")
                sys.exit(2)
            block_key = tuple(parts)
            data = {}
            field = None
        elif block_key is None:
            continue
        elif re.match(r"^\w[\w-]*:", raw):
            key, _, rest = raw.partition(":")
            key = key.strip()
            rest = rest.strip()
            if rest == "|":
                data[key] = ""
                field = key
            else:
                data[key] = rest
                field = None
        elif field is not None:
            data[field] = (data[field] + "\n" + raw.strip()).strip()
    flush()
    return entries


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--hash":
        path, func = Path(sys.argv[2]), sys.argv[3]
        for name, start, end, text in function_spans(path):
            if name == func:
                print(f"## {path} :: {func} :: <check>")
                print(f"segment-sha256: {segment_hash(text)}")
                return 0
        sys.stderr.write(f"lint-mem-gate: no function {func} in {path}\n")
        return 2

    entries = parse_whitelist()
    findings = []
    for raw in sys.stdin:
        m = FINDING_RE.match(raw.rstrip("\n"))
        if not m:
            continue
        f = m.groupdict()
        # Only the analyzer checks this gate is about. clang-tidy also emits
        # clang-diagnostic-* for ordinary compiler warnings; those belong to
        # the build's own -Werror, not here.
        if not f["check"].startswith("clang-analyzer-"):
            continue
        if "/vendored/" in f["file"]:
            continue
        findings.append(f)

    # One diagnostic can be reported once per translation unit that includes it.
    seen = set()
    unique = []
    for f in findings:
        key = (f["file"], f["line"], f["check"])
        if key not in seen:
            seen.add(key)
            unique.append(f)

    failures = []
    used = set()
    for f in unique:
        path = Path(f["file"])
        if not path.exists():
            failures.append((f, "source file not found; cannot triage"))
            continue
        span = enclosing(path, int(f["line"]))
        if span is None:
            failures.append((f, "finding is outside any function; not whitelistable"))
            continue
        name, _, _, text = span
        key = (f["file"], name, f["check"])
        entry = entries.get(key)
        if entry is None:
            failures.append((f, f"no whitelist entry for {name}()"))
            continue
        current = segment_hash(text)
        if entry.get("segment-sha256", "") != current:
            failures.append((f, (
                f"whitelist entry for {name}() is STALE: the function changed since it "
                f"was triaged, so the recorded argument no longer applies to this code.\n"
                f"      re-triage, then update the hash:\n"
                f"        expected {entry.get('segment-sha256', '(missing)')}\n"
                f"        actual   {current}")))
            continue
        why = entry.get("why", "")
        tried = entry.get("tried", "")
        if len(why) < MIN_WHY or len(tried) < MIN_TRIED:
            failures.append((f, (
                f"whitelist entry for {name}() does not argue its case "
                f"(why={len(why)} chars, need >={MIN_WHY}; "
                f"tried={len(tried)} chars, need >={MIN_TRIED})")))
            continue
        used.add(key)

    obsolete = sorted(set(entries) - used)
    if obsolete:
        sys.stderr.write("=== whitelist entries that matched no finding ===\n")
        sys.stderr.write("(not a failure -- analyzer versions differ across platforms --\n")
        sys.stderr.write(" but an entry that never fires is dead weight; drop it.)\n")
        for path, func, check in obsolete:
            sys.stderr.write(f"  {path} :: {func} :: {check}\n")

    if failures:
        sys.stderr.write("=== memory-analyzer findings (gate FAILS) ===\n")
        for f, reason in failures:
            sys.stderr.write(f"  {f['file']}:{f['line']}: {f['msg']} [{f['check']}]\n")
            sys.stderr.write(f"      {reason}\n")
        sys.stderr.write(
            f"\n{len(failures)} finding(s) unaccounted for. Triage each against the code.\n"
            "A genuine false positive gets an entry in scripts/lint-mem-whitelist.txt\n"
            "arguing the case from the code; anything else gets fixed. Never NOLINT.\n")
        return 1

    if used:
        sys.stderr.write(f"=== memory gate clean ({len(used)} argued false positive(s) suppressed) ===\n")
    else:
        sys.stderr.write("=== memory gate clean ===\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
