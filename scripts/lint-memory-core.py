#!/usr/bin/env python3
"""
lint-memory-core.py — the memory-core linter.

Memory in this project is allocated through ONE core (src/foundation/mem_core.h),
not through scattered raw malloc/calloc/realloc/free/strdup. This linter is the
gate that keeps that true.

WHY A RATCHET, NOT A BAN. An audit on 2026-09-13 found ~800 raw allocation
sites in src/ and none of them can be migrated in one change. So the gate works
like every other honest debt gate: a checked-in baseline records how many raw
sites each file has TODAY, and the build goes red the moment any file has MORE
than its baseline, or a file not in the baseline grows one. Files can only ever
go down. When a file is migrated, its baseline line is lowered (or removed) in
the same change -- and --strict turns "below baseline" into a failure too, so
the baseline cannot silently rot behind the code.

WHAT COUNTS AS RAW. A call to malloc/calloc/realloc/free/strdup/strndup that is
not part of a longer identifier. cbm_alloc, cbm_free, cbm_calloc, mi_malloc,
cbm_arena_alloc and heap_strdup are all NOT matches: the character before the
name is an identifier character. Comments and string literals are stripped
first, so prose that mentions malloc( does not trip the gate -- the security
audit already bit us once on exactly that with fork(.

EXEMPT. The core itself and the allocator plumbing it sits on:
    src/foundation/mem_core.c        the route
    (arena.c allocates its blocks THROUGH the core -- class arena -- and is scanned)
    src/foundation/slab_alloc.c      same
    src/foundation/mem.c             policy/measurement, probes with malloc
    src/foundation/mem_override_*.c  the --wrap / override shims
    src/foundation/compat*.c         libc replacement surface
    internal/**/vendored/**          not ours
    vendored/**                      not ours

SCOPE. Everything under src/ and internal/ -- cli, mcp, daemon, store,
pipeline, the extraction engine -- so the count is for the whole project, not
one subsystem, and a leak shows up wherever it is.

Usage:
    lint-memory-core.py                  check against the baseline (CI)
    lint-memory-core.py --strict         also fail when a file is BELOW baseline
    lint-memory-core.py --write-baseline regenerate scripts/memory-core-baseline.txt
    lint-memory-core.py --list           print every raw site (file:line: call)

Exit 0 = clean. Exit 1 = a file grew. Exit 2 = usage/IO error.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
BASELINE = ROOT / "scripts" / "memory-core-baseline.txt"
SCAN_ROOTS = ("src", "internal")
RAW = re.compile(r"(?<![A-Za-z0-9_])(malloc|calloc|realloc|free|strdup|strndup)\s*\(")

EXEMPT_EXACT = {
    "src/foundation/mem_core.c",
            "src/foundation/mem.c",
}
EXEMPT_PREFIX = (
    "src/foundation/mem_override_",
    "src/foundation/compat",
    "vendored/",
)


def is_vendored(rel: str) -> bool:
    """Any vendored/ segment anywhere under internal/ is not ours."""
    return "/vendored/" in rel or rel.startswith("vendored/")


def is_exempt(rel: str) -> bool:
    return rel in EXEMPT_EXACT or rel.startswith(EXEMPT_PREFIX) or is_vendored(rel)


def strip_comments_and_strings(text: str) -> str:
    """Blank out comments and string literals, preserving line structure so
    reported line numbers stay right. Character-class aware enough for C:
    handles escapes inside strings and does not treat // inside a string as a
    comment."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if c == "/" and nxt == "*":
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append("".join(ch if ch == "\n" else " " for ch in text[i:j]))
            i = j
        elif c == "/" and nxt == "/":
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif c == '"' or c == "'":
            q = c
            j = i + 1
            while j < n and text[j] != q:
                if text[j] == "\\":
                    j += 1
                if j < n and text[j] == "\n":
                    break
                j += 1
            j = min(j + 1, n)
            out.append(q + " " * max(0, j - i - 2) + (q if j - i >= 2 else ""))
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


def scan() -> dict[str, list[tuple[int, str]]]:
    hits: dict[str, list[tuple[int, str]]] = {}
    for root in SCAN_ROOTS:
        base = ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if path.suffix not in (".c", ".h") or not path.is_file():
                continue
            rel = path.relative_to(ROOT).as_posix()
            if is_exempt(rel):
                continue
            text = strip_comments_and_strings(path.read_text(encoding="utf-8", errors="replace"))
            for lineno, line in enumerate(text.splitlines(), 1):
                for m in RAW.finditer(line):
                    hits.setdefault(rel, []).append((lineno, m.group(1)))
    return hits


def read_baseline() -> dict[str, int]:
    if not BASELINE.exists():
        return {}
    out: dict[str, int] = {}
    for raw in BASELINE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        rel, _, count = line.rpartition("\t")
        if not rel:
            continue
        try:
            out[rel] = int(count)
        except ValueError:
            continue
    return out


def write_baseline(hits: dict[str, list]) -> None:
    lines = [
        "# memory-core-baseline.txt -- raw allocator call sites per file.",
        "# Generated by scripts/lint-memory-core.py --write-baseline.",
        "# A file may only ever go DOWN. Lower a line in the same change that",
        "# migrates the file to src/foundation/mem_core.h; never raise one.",
        "",
    ]
    for rel in sorted(hits):
        lines.append(f"{rel}\t{len(hits[rel])}")
    BASELINE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write-baseline", action="store_true")
    ap.add_argument("--strict", action="store_true", help="fail when a file is BELOW its baseline")
    ap.add_argument("--list", action="store_true", help="print every raw site")
    args = ap.parse_args()

    hits = scan()
    total = sum(len(v) for v in hits.values())

    if args.write_baseline:
        write_baseline(hits)
        print(f"memory-core baseline written: {len(hits)} files, {total} raw sites")
        return 0

    if args.list:
        for rel in sorted(hits):
            for lineno, call in hits[rel]:
                print(f"{rel}:{lineno}: {call}(")
        print(f"-- {total} raw sites in {len(hits)} files")
        return 0

    baseline = read_baseline()
    if not baseline:
        print(f"ERROR: no baseline at {BASELINE.relative_to(ROOT)}; run --write-baseline", file=sys.stderr)
        return 2

    grew: list[str] = []
    shrank: list[str] = []
    for rel, sites in sorted(hits.items()):
        now = len(sites)
        allowed = baseline.get(rel, 0)
        if now > allowed:
            # Show the LAST sites: new code is usually appended, so these are the
            # likeliest culprits. The linter cannot know which sites are new
            # without a diff; --list prints them all.
            delta = now - allowed
            tail = ", ".join(f"{ln}:{c}" for ln, c in sites[-max(delta, 1):][-6:])
            grew.append(f"  {rel}: grew by {delta} ({allowed} -> {now}); latest sites: {tail}"
                        f"  [run --list for all]")
        elif now < allowed:
            shrank.append(f"  {rel}: {now} (baseline {allowed}) -- lower the baseline")
    for rel, allowed in sorted(baseline.items()):
        if rel not in hits and allowed > 0:
            shrank.append(f"  {rel}: 0 (baseline {allowed}) -- remove the line")

    if grew:
        print("memory-core linter FAILED: raw allocator use grew. Allocate through")
        print("src/foundation/mem_core.h (cbm_alloc/cbm_calloc/cbm_realloc/cbm_free/")
        print("cbm_mem_strdup) instead of malloc/calloc/realloc/free/strdup.")
        print("\n".join(grew))
    if shrank:
        print("memory-core ratchet: these files improved; tighten scripts/memory-core-baseline.txt:")
        print("\n".join(shrank))
    if grew or (args.strict and shrank):
        return 1
    print(f"memory-core linter passed: {total} raw sites across {len(hits)} files, none grew")
    return 0


if __name__ == "__main__":
    sys.exit(main())
