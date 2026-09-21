#!/usr/bin/env python3
"""
memwaste-padding.py — the layout lane of the waste sanitizer: bytes every
instance of a struct carries for alignment alone.

Padding is waste multiplied by the instance count: 4 bytes in a record the graph
buffer holds 15 million of is 57 MB. The compiler knows every hole; this lane
asks it, for EVERY translation unit of the production build -- vendored code
included (grammars, the tree-sitter runtime, SQLite, mimalloc, tre, lz4, zstd,
the C++ preprocessor) -- by replaying the build's own compile commands with
`-fsyntax-only -Wno-everything -Wpadded`.

Per struct it reports the interior holes (removable by ordering fields from the
largest alignment down), the tail padding (removable only when the interior
reordering frees enough bytes), and how many allocation sites name the type in a
sizeof, as a first hint of instance counts. The event lane's per-site requested
sizes are the runtime evidence to pair it with.

Usage:
    scripts/memwaste-padding.py                  # every TU, top 40 by interior padding
    scripts/memwaste-padding.py --project-only   # skip vendored TUs
    scripts/memwaste-padding.py --json
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import shlex
import subprocess
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_EXT = (".c", ".cc", ".cpp")
KEEP_WITH_ARG = {"-include", "-isystem", "-iquote", "-arch", "-target"}
WARN_RE = re.compile(
    r"^(?P<file>[^:]+):(?P<line>\d+):\d+: warning: padding (?:struct|class|size of) "
    r"'(?P<type>[^']+)' with (?P<bytes>\d+) bytes? to (?P<what>align '(?P<field>[^']*)'|alignment boundary)")


def build_commands(cc: str, cxx: str) -> list[tuple[str, list[str], str]]:
    dry = subprocess.run(["make", "-f", "Makefile.cbm", "-n", "cbm", "BUILD_DIR=build/memwaste-padding-dry",
                          f"CC={cc}", f"CXX={cxx}", "MEMWASTE=1"],
                         cwd=ROOT, capture_output=True, text=True, check=False)
    text = dry.stdout.replace("\\\n", " ")
    jobs = []
    seen = set()
    for line in text.splitlines():
        try:
            argv = shlex.split(line)
        except ValueError:
            continue
        if not argv or os.path.basename(argv[0]) not in (cc, cxx, os.path.basename(cc), os.path.basename(cxx)):
            continue
        flags = []
        sources = []
        i = 1
        while i < len(argv):
            a = argv[i]
            if a in KEEP_WITH_ARG and i + 1 < len(argv):
                flags += [a, argv[i + 1]]
                i += 2
                continue
            if a == "-o":
                i += 2
                continue
            if a.startswith(("-I", "-D", "-U", "-std=")):
                flags.append(a)
            elif a.endswith(SRC_EXT) and os.path.isfile(os.path.join(ROOT, a)):
                sources.append(a)
            i += 1
        for s in sources:
            key = (s, tuple(flags))
            if key not in seen:
                seen.add(key)
                jobs.append((argv[0], flags, s))
    return jobs


def run_one(job: tuple[str, list[str], str]) -> tuple[str, str]:
    compiler, flags, source = job
    res = subprocess.run([compiler, "-fsyntax-only", "-Wno-everything", "-Wpadded"] + flags + [source],
                         cwd=ROOT, capture_output=True, text=True, check=False)
    return source, res.stderr


def is_vendored(path: str) -> bool:
    return "/vendored/" in f"/{path}" or path.startswith("vendored/")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cc", default="clang")
    ap.add_argument("--cxx", default="clang++")
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--project-only", action="store_true")
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    jobs = build_commands(args.cc, args.cxx)
    if args.project_only:
        jobs = [j for j in jobs if not is_vendored(j[2])]
    if not jobs:
        sys.exit("memwaste-padding: no compile commands found (make -n printed none for the compiler)")

    holes: dict[str, dict] = {}
    with cf.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for source, stderr in pool.map(run_one, jobs):
            for line in stderr.splitlines():
                m = WARN_RE.match(line)
                if not m:
                    continue
                file = os.path.relpath(os.path.join(ROOT, m.group("file")), ROOT) \
                    if not os.path.isabs(m.group("file")) else os.path.relpath(m.group("file"), ROOT)
                t = holes.setdefault(m.group("type"), {"type": m.group("type"), "interior": {}, "tail": 0,
                                                       "where": f"{file}:{m.group('line')}",
                                                       "vendored": is_vendored(file)})
                n = int(m.group("bytes"))
                if m.group("field") is not None:
                    t["interior"][m.group("field")] = n  # a header seen by many TUs reports the same hole
                else:
                    t["tail"] = n
                    t["where"] = f"{file}:{m.group('line')}"

    # Allocation hint: how many allocation expressions size by this type.
    sizeof_sites: dict[str, int] = defaultdict(int)
    names = {t.split(" ", 1)[-1] for t in holes}
    alloc_re = re.compile(r"(?:alloc|calloc|realloc|arena_alloc|CBM_DA_PUSH|cbm_da_push)\w*\s*\([^;]*sizeof\s*\(\s*"
                          r"(?:struct\s+)?(\w+)\s*\)")
    for base in ("src", "internal/cbm"):
        for dirpath, _, filenames in os.walk(os.path.join(ROOT, base)):
            if "/vendored" in dirpath:
                continue
            for fn in filenames:
                if not fn.endswith((".c", ".h")):
                    continue
                try:
                    text = open(os.path.join(dirpath, fn), encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                for m in alloc_re.finditer(text):
                    if m.group(1) in names:
                        sizeof_sites[m.group(1)] += 1

    rows = []
    for t in holes.values():
        interior = sum(t["interior"].values())
        short = t["type"].split(" ", 1)[-1]
        rows.append({"type": t["type"], "interior_bytes": interior, "tail_bytes": t["tail"],
                     "holes": len(t["interior"]), "alloc_sites": sizeof_sites.get(short, 0),
                     "where": t["where"], "vendored": t["vendored"]})
    rows.sort(key=lambda r: (-(r["alloc_sites"] > 0), -r["interior_bytes"], r["type"]))

    if args.json:
        json.dump({"translation_units": len(jobs), "structs": rows}, sys.stdout, indent=1)
        print()
        return 0
    print(f"translation units checked: {len(jobs)}  structs with padding: {len(rows)}  "
          f"(vendored: {sum(1 for r in rows if r['vendored'])})")
    print()
    print(f"  {'interior B':>10} {'tail B':>6} {'holes':>5} {'alloc sites':>11}  struct  (declared at)")
    for r in rows[: args.top]:
        tag = " [vendored]" if r["vendored"] else ""
        print(f"  {r['interior_bytes']:>10} {r['tail_bytes']:>6} {r['holes']:>5} {r['alloc_sites']:>11}  "
              f"{r['type']}{tag}  ({r['where']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
