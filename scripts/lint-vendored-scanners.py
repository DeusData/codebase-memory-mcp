#!/usr/bin/env python3
"""Reject mutable file-scope state in vendored tree-sitter external scanners.

tree-sitter gives every parser its own scanner state: whatever
`tree_sitter_<lang>_external_scanner_create()` returns is that parser's
payload. State kept anywhere else -- a file-scope variable -- is one object for
the whole process, shared by every parser, and the indexer runs one parser per
worker thread. Two vendored scanners did this and both made the graph differ
from run to run: properties (a `static bool`, 2026-09) and jsonnet (three plain
globals, 2026-11). See grammars/MANIFEST.md, "Local source patches".

Scope: internal/cbm/vendored/grammars/*/scanner.c and the headers next to it
(scanners keep helpers in e.g. unicode.h). tree_sitter/ is the runtime's own
header set and is not scanned.

A finding is a top-level declaration of a variable that is not const. The
script understands just enough C for that: comments, string and character
literals and preprocessor lines are blanked, function and type bodies are
skipped, brace initializers are kept with their declaration. It stays
conservative on purpose -- a missed form is acceptable, a flagged clean scanner
is not -- so it does not report:
  * anything declared const (even `const char *p`, whose pointer is mutable),
  * anything with a parenthesis before its initializer (function prototypes,
    function-pointer variables, macro-typed or attributed declarations),
  * initialized arrays (lookup tables; an uninitialized array is reported),
  * typedef and extern declarations.

Exceptions live in scripts/lint-vendored-scanners-whitelist.txt, one
`path :: name :: why` line each (name may be an fnmatch pattern for a family
of generated variables); an entry that matches nothing is itself an error, so
the list cannot outlive the code it excuses.

Usage: scripts/lint-vendored-scanners.py [--root DIR] [FILE...]
Exit 0 clean, 1 findings or stale whitelist entries, 2 usage/IO error.
"""
import argparse
import fnmatch
import os
import re
import sys

GRAMMARS = os.path.join("internal", "cbm", "vendored", "grammars")
WHITELIST = os.path.join("scripts", "lint-vendored-scanners-whitelist.txt")
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def blank_comments_and_literals(text):
    """Replace comments and string/char literal contents with spaces, keeping newlines."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if c == "/" and nxt == "/":
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
        elif c == "/" and nxt == "*":
            out.append("  ")
            i += 2
            while i < n and not (text[i] == "*" and i + 1 < n and text[i + 1] == "/"):
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            out.append("  ")
            i += 2
        elif c in "\"'":
            quote = c
            out.append(c)
            i += 1
            while i < n and text[i] != quote and text[i] != "\n":
                if text[i] == "\\" and i + 1 < n:
                    out.append("  " if text[i + 1] != "\n" else " \n")
                    i += 2
                    continue
                out.append(" ")
                i += 1
            if i < n and text[i] == quote:
                out.append(quote)
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def blank_preprocessor(text):
    """Blank preprocessor lines, including backslash continuations."""
    lines = text.split("\n")
    continued = False
    for idx, line in enumerate(lines):
        if continued or line.lstrip().startswith("#"):
            continued = line.rstrip().endswith("\\")
            lines[idx] = ""
    return "\n".join(lines)


def classify(stmt):
    """Return the variable name if the top-level statement defines mutable state, else None."""
    tokens = IDENT.findall(stmt)
    if not tokens or "typedef" in tokens or "extern" in tokens or "const" in tokens:
        return None
    decl = stmt.split("=", 1)[0]
    if "(" in decl:
        return None
    if "[" in decl and "=" in stmt:
        return None  # initialized array: a table
    if "{...}" in decl:
        decl = decl.split("{...}", 1)[1]  # struct/union/enum body, then its declarators
        names = IDENT.findall(decl)
        return names[0] if names else None
    names = IDENT.findall(decl.split("[", 1)[0])
    if len(names) < 2 or names[0] in ("struct", "union", "enum") and len(names) == 2:
        return None  # a lone macro call, or a forward declaration
    return names[-1]


def findings(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        text = blank_preprocessor(blank_comments_and_literals(fh.read()))
    found = []
    stmt, start_line, line = "", 1, 1
    depth = 0           # depth of a skipped body or initializer
    keep_body = False   # the skipped braces belong to the statement (initializer or type body)
    transparent = 0     # extern "C" { ... } blocks: their braces are not a scope
    for ch in text:
        if ch == "\n":
            line += 1
        if depth > 0:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    if keep_body:
                        stmt += "{...}"
                    else:
                        stmt = ""  # a function body ends its definition
            continue
        if ch == "{":
            head = stmt.strip()
            if head == "extern" or re.fullmatch(r"extern\s+\"\s*\"", head):
                transparent += 1
                stmt = ""
                continue
            # an initializer, or a struct/union/enum body, stays part of the statement
            keep_body = "=" in head or ("(" not in head and bool(head))
            depth = 1
            continue
        if ch == "}" and transparent > 0:
            transparent -= 1
            stmt = ""
            continue
        if ch == ";":
            name = classify(stmt)
            if name:
                found.append((start_line, name, " ".join(stmt.split())))
            stmt = ""
            continue
        if not stmt.strip() and not ch.isspace():
            start_line = line
        stmt += ch
    return found


def load_whitelist(root):
    entries = {}
    path = os.path.join(root, WHITELIST)
    with open(path, encoding="utf-8") as fh:
        for number, raw in enumerate(fh, 1):
            text = raw.strip()
            if not text or text.startswith("#"):
                continue
            parts = [p.strip() for p in text.split("::")]
            if len(parts) != 3 or not all(parts):
                raise ValueError(f"{WHITELIST}:{number}: expected `path :: name :: why`")
            entries[(parts[0], parts[1])] = number
    return entries


def scanner_files(root):
    base = os.path.join(root, GRAMMARS)
    out = []
    for lang in sorted(os.listdir(base)):
        directory = os.path.join(base, lang)
        if not os.path.isfile(os.path.join(directory, "scanner.c")):
            continue
        for name in sorted(os.listdir(directory)):
            if name == "scanner.c" or name.endswith(".h"):
                out.append(os.path.join(directory, name))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--root", default=".")
    parser.add_argument("files", nargs="*", help="check these files instead of every scanner")
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    try:
        whitelist = load_whitelist(root)
        files = [os.path.abspath(f) for f in args.files] or scanner_files(root)
    except (OSError, ValueError) as err:
        print(f"lint-vendored-scanners: {err}", file=sys.stderr)
        return 2
    used, bad = set(), 0
    for path in files:
        rel = os.path.relpath(path, root)
        try:
            hits = findings(path)
        except OSError as err:
            print(f"lint-vendored-scanners: {err}", file=sys.stderr)
            return 2
        for line, name, stmt in hits:
            entry = next((key for key in whitelist
                          if key[0] == rel and fnmatch.fnmatchcase(name, key[1])), None)
            if entry:
                used.add(entry)
                continue
            bad += 1
            print(f"{rel}:{line}: mutable file-scope state `{name}`: {stmt}")
    if not args.files:
        for (rel, name), number in sorted(whitelist.items(), key=lambda kv: kv[1]):
            if (rel, name) not in used:
                bad += 1
                print(f"{WHITELIST}:{number}: stale entry, nothing matches `{rel} :: {name}`")
    print(f"lint-vendored-scanners: {len(files)} files, {bad} problem(s)")
    if bad:
        print("Scanner state belongs in the payload from ..._external_scanner_create(); "
              "see grammars/MANIFEST.md, \"Local source patches\".")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
