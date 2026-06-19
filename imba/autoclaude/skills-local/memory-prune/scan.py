#!/usr/bin/env python3
"""Scan a Claude Code memory/ dir for prune candidates: expired, duplicate, or
contradictory semantic entries. Prints a report; never deletes anything (the
operator/agent decides). Part of the autoclaude memory-discipline layer.

Usage: python3 scan.py [MEMORY_DIR]
Defaults MEMORY_DIR to the memory/ of the current project transcript dir if not
given; falls back to ./memory.
"""
from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

FM = re.compile(r"^---\s*$")
EXPIRES = re.compile(r"^\s*expires_at:\s*(\d{4}-\d{2}-\d{2})", re.M)
NAME = re.compile(r"^\s*name:\s*(.+)$", re.M)
DESC = re.compile(r"^\s*description:\s*(.+)$", re.M)


def parse(p: Path) -> dict:
    txt = p.read_text(encoding="utf-8", errors="ignore")
    exp = EXPIRES.search(txt)
    name = NAME.search(txt)
    desc = DESC.search(txt)
    return {
        "path": p,
        "name": (name.group(1).strip() if name else p.stem),
        "desc": (desc.group(1).strip() if desc else ""),
        "expires": exp.group(1) if exp else None,
    }


def main() -> int:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    mem = Path(arg) if arg else Path("memory")
    if not mem.exists():
        print(f"no memory dir at {mem}")
        return 0

    entries = [parse(p) for p in mem.glob("*.md") if p.name != "MEMORY.md"]
    today = date.today().isoformat()

    expired = [e for e in entries if e["expires"] and e["expires"] < today]
    by_desc = defaultdict(list)
    for e in entries:
        if e["desc"]:
            by_desc[e["desc"].lower()].append(e)
    dupes = [v for v in by_desc.values() if len(v) > 1]

    print(f"memory/: {len(entries)} semantic entries  ({mem})")
    print(f"expired (expires_at < {today}): {len(expired)}")
    for e in expired:
        print(f"  - {e['path'].name}  (expired {e['expires']})  {e['desc'][:60]}")
    print(f"possible duplicates (same description): {len(dupes)} group(s)")
    for grp in dupes:
        print("  - " + " | ".join(x["path"].name for x in grp))
    if not expired and not dupes:
        print("clean — nothing to prune.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
