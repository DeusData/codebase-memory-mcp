#!/usr/bin/env python3
"""
memwaste-ratchet.py — hold the waste numbers where they are, lower them as they fall.

Same shape as the memory-core linter's ratchet: a checked-in baseline per venue
(scripts/memwaste-baseline.json), every waste class normalised PER THOUSAND GRAPH
NODES so a corpus that grows does not look like a regression. A class that grows
past its baseline by more than --tolerance fails under --enforce; without it the
comparison is printed and the exit is 0 (report-only rollout). A class that
FALLS is reported so the baseline can be lowered with --record.

The inputs are what scripts/memwaste.sh leaves in its --out directory:
memwaste.json (the report's --json) and index.json (the index result, for the
node count).

Usage:
    memwaste-ratchet.py --out DIR --venue pr-linux              # compare, report-only
    memwaste-ratchet.py --out DIR --venue pr-linux --enforce    # compare, gate
    memwaste-ratchet.py --out DIR --venue pr-linux --record     # write the baseline
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "scripts", "memwaste-baseline.json")

# Classes the ratchet holds. Left out on purpose: allocator-internal byte work
# (it moves with allocator settings), the access-lane classes (one lane only),
# and contended mutex acquisitions and pool imbalance -- those depend on thread
# scheduling, and a gate on them would be a lottery (O9). They stay in the report.
CLASSES = [
    "slack_bytes", "never_written_bytes", "over_requested_bytes", "realloc_copy_bytes",
    "short_lived", "dup_bytes_max", "retained_bytes_max", "raw_bytes",
    "container_idle_peak_bytes", "byte_work_bytes", "byte_work_repeats", "io_tiny_calls",
    "path_repeats", "ht_repeat_lookups", "ht_growths",
]


def current(out_dir: str) -> dict:
    report = json.load(open(os.path.join(out_dir, "memwaste.json")))
    index = json.load(open(os.path.join(out_dir, "index.json")))
    nodes = int(index.get("nodes") or 0)
    if nodes <= 0:
        sys.exit("memwaste-ratchet: index.json has no node count")
    # The index worker is the process that allocated the most.
    worker = max(report["processes"].values(), key=lambda p: p["header"].get("usable_bytes", 0))
    summary = worker["summary"]
    return {
        "nodes": nodes,
        "per_1k_nodes": {c: round(summary.get(c, 0) * 1000.0 / nodes, 3) for c in CLASSES},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="memwaste.sh --out directory")
    ap.add_argument("--venue", required=True, help="baseline key, e.g. pr-linux, dryrun-macos")
    ap.add_argument("--tolerance", type=float, default=0.05, help="allowed growth before a class fails")
    ap.add_argument("--enforce", action="store_true")
    ap.add_argument("--record", action="store_true")
    args = ap.parse_args()

    cur = current(args.out)
    base_all = json.load(open(BASELINE)) if os.path.exists(BASELINE) else {}
    if args.record:
        base_all[args.venue] = cur
        with open(BASELINE, "w") as fh:
            json.dump(base_all, fh, indent=1, sort_keys=True)
            fh.write("\n")
        print(f"baseline recorded for {args.venue} ({cur['nodes']:,} nodes)")
        return 0
    base = base_all.get(args.venue)
    if not base:
        print(f"no baseline for {args.venue}: nothing to hold yet (record one with --record)")
        return 0

    worse, better = [], []
    print(f"{'class':<28} {'baseline/1k':>14} {'now/1k':>14} {'delta':>8}")
    for c in CLASSES:
        b = base["per_1k_nodes"].get(c, 0.0)
        n = cur["per_1k_nodes"].get(c, 0.0)
        delta = (n - b) / b if b else (0.0 if n == 0 else float("inf"))
        mark = ""
        if delta > args.tolerance:
            worse.append(c)
            mark = "  WORSE"
        elif delta < -args.tolerance:
            better.append(c)
            mark = "  better"
        shown = "new" if delta == float("inf") else f"{delta * 100:+.1f}%"
        print(f"{c:<28} {b:>14,.3f} {n:>14,.3f} {shown:>8}{mark}")
    if better:
        print(f"\n{len(better)} class(es) improved beyond tolerance: lower the baseline with --record")
    if worse:
        print(f"\n{len(worse)} class(es) grew beyond {args.tolerance * 100:.0f}%: {', '.join(worse)}")
        return 1 if args.enforce else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
