#!/usr/bin/env python3
"""
memwaste-report.py — turn waste-sanitizer dumps into ranked, symbolised evidence.

The sanitizer (src/foundation/mem_events.h) appends JSON lines at every phase
mark: a header per dump, then one object per allocation site and one per CPU
work row, all with RAW return addresses. Symbolisation is offline so the hot
path never loads a symbol handler. This tool reads every dump, keeps the LAST
dump of each process for the cumulative counters, the MAXIMUM over its dumps for
the phase snapshots (retained, duplicates), resolves the addresses against the
binary that produced them, and prints:

  1. a waste summary per process — one line per waste class, in bytes/calls;
  2. the memory table — allocation sites ranked by a chosen waste metric;
  3. containers — arenas, dynamic arrays, hash tables: capacity against use;
  4. CPU work — byte work, I/O, contention, pool imbalance, repeated work.

Usage:
    memwaste-report.py DUMP.jsonl --binary build/memwaste/codebase-memory-mcp
    memwaste-report.py DUMP.jsonl --binary ... --sort never_written --top 30
    memwaste-report.py DUMP.jsonl --binary ... --json   (for the ratchet)
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from collections import defaultdict

MB = 1024.0 * 1024.0

SNAPSHOT_FIELDS = ("retained_bytes", "dup_bytes", "dup_blocks")

WASTE_CLASSES = [
    # (key, label, unit)
    ("slack_bytes", "allocator slack (usable - requested)", "bytes"),
    ("never_written_bytes", "never-written bytes (fill pattern intact at free)", "bytes"),
    ("over_requested_bytes", "requested beyond the last written byte", "bytes"),
    ("zero_untouched_bytes", "calloc tail still zero at free (upper bound)", "bytes"),
    ("realloc_copy_bytes", "bytes copied by realloc growth", "bytes"),
    ("short_lived", "short-lived allocations (churn)", "calls"),
    ("dup_bytes_max", "duplicate small blocks (peak over phases)", "bytes"),
    ("retained_bytes_max", "live across a phase boundary (peak)", "bytes"),
    ("raw_bytes", "bytes that never went through the memory core", "bytes"),
    ("acc_untouched_bytes", "blocks never read or written (access lane)", "bytes"),
    ("acc_dead_bytes", "blocks written, never read by instrumented code (access lane)", "bytes"),
    ("acc_opaque_blocks", "blocks used only by code the access lane cannot see (no verdict)", "blocks"),
    ("acc_idle_bytes", "blocks idle for at least one phase before free (access lane)", "bytes"),
    ("acc_uninit_reads", "reads before any write, not calloc (access lane)", "blocks"),
    ("container_idle_peak_bytes", "container capacity left unused (largest instance, per site)", "bytes"),
    ("byte_work_bytes", "bytes moved/compared by mem*/str* calls in program code", "bytes"),
    ("allocator_byte_work_bytes", "bytes moved/compared inside the allocator (mimalloc)", "bytes"),
    ("byte_work_repeats", "strlen of a string just measured", "calls"),
    ("io_tiny_calls", "I/O calls under 512 bytes", "calls"),
    ("path_repeats", "open/stat of a path already opened/statted", "calls"),
    ("mutex_contended", "contended mutex acquisitions", "calls"),
    ("pool_imbalance_items", "work items an even pool would have spread", "items"),
    ("pool_ops_imbalance", "worker events an even pool would have spread (work done)", "events"),
    ("ht_repeat_lookups", "hash lookups of the key object just looked up", "calls"),
    ("ht_growths", "hash-table growths", "calls"),
]

BYTE_KINDS = {"memcpy", "memmove", "memset", "memcmp", "strlen", "strcmp", "strncmp"}
IO_KINDS = {"read", "write", "pread", "pwrite", "fread", "fwrite"}
PATH_KINDS = {"open", "stat", "fopen", "opendir"}
CONTAINER_KINDS = {"ct_arena", "ct_dyn_array", "ct_hash_table"}
# Symbols of the vendored allocator. Its byte work is real CPU, but the fix is in
# how the allocator is built or configured, not at a program call site.
ALLOCATOR_SYMBOL = re.compile(r"^_?mi_")


def load(path: str):
    """{pid: {"header": last header, "headers": [...], "sites": {addr: row}, "snap_max": {addr: {...}},
    "work": {(kind, addr): row}}} — sites/work from the LAST dump of the process."""
    procs: dict[int, dict] = {}
    cur = None
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue  # a torn line from a process that died mid-write
            if "memwaste" in obj:
                pid = int(obj.get("pid", 0))
                p = procs.setdefault(pid, {"headers": [], "snap_max": defaultdict(dict)})
                p["headers"].append(obj)
                p["header"] = obj
                p["sites"] = {}
                p["work"] = {}
                cur = p
            elif cur is None:
                continue
            elif "work" in obj:
                cur["work"][(obj["work"], obj["site"])] = obj
            elif "site" in obj:
                cur["sites"][obj["site"]] = obj
                snap = cur["snap_max"][obj["site"]]
                for f in SNAPSHOT_FIELDS:
                    v = obj.get(f, 0)
                    if v > snap.get(f, 0):
                        snap[f] = v
                        snap[f + "_phase"] = cur["header"].get("phase_label", "?")
    return procs


def pe_image_base(binary: str) -> int | None:
    """The preferred ImageBase of a PE file, or None for anything else. A PE
    symboliser wants VA = ImageBase + RVA, and the dump gives RVA + runtime base."""
    try:
        with open(binary, "rb") as fh:
            head = fh.read(4096)
    except OSError:
        return None
    if head[:2] != b"MZ" or len(head) < 0x40:
        return None
    pe = int.from_bytes(head[0x3C:0x40], "little")
    if head[pe:pe + 4] != b"PE\0\0":
        return None
    opt = pe + 24
    magic = int.from_bytes(head[opt:opt + 2], "little")
    if magic == 0x20B:  # PE32+
        return int.from_bytes(head[opt + 24:opt + 32], "little")
    if magic == 0x10B:  # PE32
        return int.from_bytes(head[opt + 28:opt + 32], "little")
    return None


def symbolise(binary: str | None, base: str, addrs: list[str]) -> dict[str, str]:
    if not binary or not addrs:
        return {}
    if not os.path.exists(binary) and os.path.exists(binary + ".exe"):
        binary = binary + ".exe"
    out: dict[str, str] = {}
    uniq = sorted(set(addrs))
    if platform.system() == "Darwin" and shutil.which("atos"):
        for i in range(0, len(uniq), 2000):
            chunk = uniq[i:i + 2000]
            res = subprocess.run(["atos", "-o", binary, "-l", base] + chunk,
                                 capture_output=True, text=True, check=False)
            for addr, name in zip(chunk, res.stdout.splitlines()):
                out[addr] = name.strip()
    elif shutil.which("addr2line") or shutil.which("llvm-addr2line"):
        tool = shutil.which("llvm-addr2line") or shutil.which("addr2line")
        b = int(base, 16)
        preferred = pe_image_base(binary) or 0
        for i in range(0, len(uniq), 2000):
            chunk = uniq[i:i + 2000]
            rel = [hex(int(a, 16) - b + preferred) if b else a for a in chunk]
            res = subprocess.run([tool, "-f", "-C", "-p", "-e", binary] + rel,
                                 capture_output=True, text=True, check=False)
            for addr, name in zip(chunk, res.stdout.splitlines()):
                out[addr] = name.strip()
    return out


def pct(part: float, whole: float) -> float:
    return 100.0 * part / whole if whole else 0.0


def summarise(p: dict, names: dict[str, str]) -> dict:
    s = defaultdict(int)
    for addr, r in p["sites"].items():
        s["slack_bytes"] += max(r["usable_bytes"] - r["requested_bytes"], 0)
        for k in ("never_written_bytes", "over_requested_bytes", "zero_untouched_bytes",
                  "realloc_copy_bytes", "short_lived", "raw_bytes", "acc_untouched_bytes",
                  "acc_dead_bytes", "acc_idle_bytes", "acc_uninit_reads", "acc_opaque_blocks"):
            s[k] += r.get(k, 0)
        snap = p["snap_max"].get(addr, {})
        s["dup_bytes_max"] += snap.get("dup_bytes", 0)
        s["retained_bytes_max"] += snap.get("retained_bytes", 0)
    for (kind, addr), w in p["work"].items():
        if kind in CONTAINER_KINDS:
            s["container_idle_peak_bytes"] += w.get("peak", max(w["bytes"] - w["aux1"], 0))
            if kind == "ct_hash_table":
                s["ht_growths"] += w["aux2"]
        elif kind in BYTE_KINDS:
            if ALLOCATOR_SYMBOL.match(names.get(addr, "")):
                s["allocator_byte_work_bytes"] += w["bytes"]
            else:
                s["byte_work_bytes"] += w["bytes"]
            s["byte_work_repeats"] += w["repeats"]
        elif kind in IO_KINDS:
            s["io_tiny_calls"] += w["aux1"]
        elif kind in PATH_KINDS:
            s["path_repeats"] += w["repeats"]
        elif kind == "mutex":
            s["mutex_contended"] += w["aux1"]
        elif kind == "parallel_for":
            s["pool_imbalance_items"] += w["aux1"]
        elif kind == "pool_ops":
            s["pool_ops_imbalance"] += w["aux1"]
        elif kind == "ht_get":
            s["ht_repeat_lookups"] += w["repeats"]
    return dict(s)


def fmt(v: float, unit: str) -> str:
    if unit == "bytes":
        return f"{v / MB:,.1f} MB"
    return f"{v:,.0f} {unit}"


SITE_SORTS = {
    "bytes": lambda r, s: r["usable_bytes"],
    "slack": lambda r, s: r["usable_bytes"] - r["requested_bytes"],
    "never_written": lambda r, s: r.get("never_written_bytes", 0),
    "over_requested": lambda r, s: r.get("over_requested_bytes", 0),
    "churn": lambda r, s: r["short_lived"],
    "copy": lambda r, s: r["realloc_copy_bytes"],
    "live": lambda r, s: r["live_bytes"],
    "allocs": lambda r, s: r["allocs"],
    "dups": lambda r, s: s.get("dup_bytes", 0),
    "retained": lambda r, s: s.get("retained_bytes", 0),
    "dead": lambda r, s: r.get("acc_dead_bytes", 0),
    "untouched": lambda r, s: r.get("acc_untouched_bytes", 0),
    "idle": lambda r, s: r.get("acc_idle_bytes", 0),
    "uninit": lambda r, s: r.get("acc_uninit_reads", 0),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dump")
    ap.add_argument("--binary", help="the binary that produced the dump, for symbolisation")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--sort", choices=sorted(SITE_SORTS), default="bytes")
    ap.add_argument("--json", action="store_true", help="emit the merged tables as JSON")
    args = ap.parse_args()

    procs = load(args.dump)
    if not procs:
        print("no waste-sanitizer dump found in", args.dump, file=sys.stderr)
        return 2

    names_by_pid = {}
    for pid, p in procs.items():
        addrs = list(p["sites"].keys()) + [a for (_, a) in p["work"].keys()]
        names_by_pid[pid] = symbolise(args.binary, p["header"].get("image_base", "0x0"), addrs)

    if args.json:
        out = {"processes": {}}
        for pid, p in procs.items():
            names = names_by_pid[pid]
            out["processes"][str(pid)] = {
                "header": p["header"],
                "phases": [h.get("phase_label") for h in p["headers"]],
                "summary": summarise(p, names),
                "sites": [dict(r, name=names.get(a, a),
                               **{(k if k.endswith("_phase") else k + "_max"): v
                                  for k, v in p["snap_max"].get(a, {}).items()})
                          for a, r in p["sites"].items()],
                "work": [dict(w, name=names.get(a, a)) for (_, a), w in p["work"].items()],
            }
        json.dump(out, sys.stdout, indent=1)
        print()
        return 0

    # Work-heavy processes first: the index worker is the one that matters.
    order = sorted(procs.items(), key=lambda kv: -kv[1]["header"].get("usable_bytes", 0))
    for pid, p in order:
        h = p["header"]
        names = names_by_pid[pid]
        s = summarise(p, names)
        print("=" * 110)
        print(f"process {pid}  last dump={h.get('why')}  phases={len(p['headers'])}  allocs={h['allocs']:,}  "
              f"cum={h['usable_bytes'] / MB:,.0f} MB  live={h['live_bytes'] / MB:,.0f} MB  "
              f"rss={h.get('rss_bytes', 0) / MB:,.0f} MB  sites={h['sites']:,}  fill={h.get('fill')}  access={h.get('access')}  "
              f"layer={h.get('layer_vm_bytes', 0) / MB:,.0f} MB/{h.get('layer_threads', 0)} threads")
        blind = {k: h.get(k, 0) for k in ("site_table_full", "pointer_table_full", "work_table_full", "untracked_frees")}
        if any(blind.values()):
            print("  blind spots: " + "  ".join(f"{k}={v:,}" for k, v in blind.items()))
        if h.get("foreign_frees", 0):
            print(f"  outside the shim: {h['foreign_frees']:,} frees of blocks another allocator made "
                  "(C runtime / system DLL; their allocations are invisible to the layer)")
        print()
        print("  WASTE SUMMARY")
        for key, label, unit in WASTE_CLASSES:
            v = s.get(key, 0)
            if v:
                print(f"    {fmt(v, unit):>18}  {label}")
        if len(p["sites"]) == 0 and len(p["work"]) == 0:
            continue

        print()
        print(f"  MEMORY — top {args.top} sites by {args.sort}")
        print(f"    {'cum MB':>9} {'allocs':>12} {'slack%':>6} {'churn%':>6} {'copy MB':>8} {'unwrit MB':>9} "
              f"{'overreq MB':>10} {'dup MB':>7} {'retain MB':>9} {'raw%':>5} {'live MB':>8}  site")
        key = SITE_SORTS[args.sort]
        rows = sorted(p["sites"].items(), key=lambda kv: (-key(kv[1], p["snap_max"].get(kv[0], {})), kv[0]))
        for addr, r in rows[:args.top]:
            snap = p["snap_max"].get(addr, {})
            print(f"    {r['usable_bytes'] / MB:>9,.1f} {r['allocs']:>12,} "
                  f"{pct(r['usable_bytes'] - r['requested_bytes'], r['usable_bytes']):>6.1f} "
                  f"{pct(r['short_lived'], r['allocs']):>6.1f} {r['realloc_copy_bytes'] / MB:>8,.1f} "
                  f"{r.get('never_written_bytes', 0) / MB:>9,.1f} {r.get('over_requested_bytes', 0) / MB:>10,.1f} "
                  f"{snap.get('dup_bytes', 0) / MB:>7,.1f} {snap.get('retained_bytes', 0) / MB:>9,.1f} "
                  f"{pct(r['raw_bytes'], r['usable_bytes']):>5.0f} {r['live_bytes'] / MB:>8,.1f}  "
                  f"{names.get(addr, addr)[:90]}")

        if h.get("access"):
            # The access lane's verdicts per allocation site. Library-owned blocks (a FILE
            # buffer) are not tracked; bytes a library wrote without a store callback are
            # recognised through the fill pattern.
            acc_cols = [("untouched", "acc_untouched_bytes", "never read or written"),
                        ("dead", "acc_dead_bytes", "written, never read"),
                        ("idle", "acc_idle_bytes", "idle for a phase before free")]
            for sort_key, field, label in acc_cols:
                rows = [(a, r) for a, r in p["sites"].items() if r.get(field, 0) > 0]
                if not rows:
                    continue
                print()
                print(f"  ACCESS — top {args.top} sites by bytes {label}")
                print(f"    {'MB':>9} {'blocks':>11} {'allocs':>12} {'uninit rd':>10}  site")
                blocks_field = field.replace("_bytes", "_blocks")
                for a, r in sorted(rows, key=lambda t: (-t[1][field], t[0]))[:args.top]:
                    print(f"    {r[field] / MB:>9,.1f} {r.get(blocks_field, 0):>11,} {r['allocs']:>12,} "
                          f"{r.get('acc_uninit_reads', 0):>10,}  {names.get(a, a)[:90]}")
            rows = [(a, r) for a, r in p["sites"].items() if r.get("acc_uninit_reads", 0) > 0]
            if rows:
                print()
                print(f"  ACCESS — top {args.top} sites by blocks read before any write (not calloc)")
                print(f"    {'blocks':>11} {'allocs':>12}  site")
                for a, r in sorted(rows, key=lambda t: (-t[1]["acc_uninit_reads"], t[0]))[:args.top]:
                    print(f"    {r['acc_uninit_reads']:>11,} {r['allocs']:>12,}  {names.get(a, a)[:90]}")

        ct = [(k, a, w) for (k, a), w in p["work"].items() if k in CONTAINER_KINDS]
        if ct:
            print()
            # A container reports at every reset: calls are USES, cap/used sum over uses.
            # "peak idle" is the largest capacity a single instance left unused.
            print(f"  CONTAINERS — top {args.top} by largest unused capacity of one instance")
            print(f"    {'kind':<10} {'uses':>9} {'avg cap MB':>10} {'avg used MB':>11} {'unused%':>7} "
                  f"{'peak idle MB':>12} {'growths':>9}  created at")
            for k, a, w in sorted(ct, key=lambda t: (-t[2].get("peak", 0), t[1]))[:args.top]:
                uses = max(w["calls"], 1)
                print(f"    {k[3:]:<10} {w['calls']:>9,} {w['bytes'] / uses / MB:>10,.2f} {w['aux1'] / uses / MB:>11,.2f} "
                      f"{pct(w['bytes'] - w['aux1'], w['bytes']):>7.1f} {w.get('peak', 0) / MB:>12,.1f} "
                      f"{w['aux2']:>9,}  {names.get(a, a)[:90]}")

        def section(title, kinds, sort_key, cols):
            rows = [(k, a, w) for (k, a), w in p["work"].items() if k in kinds]
            if not rows:
                return
            print()
            print(f"  CPU — {title}")
            print("    " + "  ".join(f"{c[0]:>{c[1]}}" for c in cols) + "  site")
            for k, a, w in sorted(rows, key=lambda t: (-sort_key(t[2]), t[1]))[:args.top]:
                cells = []
                for c in cols:
                    v = c[2](k, w)
                    cells.append(f"{v:>{c[1]}}" if isinstance(v, str) else f"{v:>{c[1]},.0f}")
                print("    " + "  ".join(cells) + f"  {names.get(a, a)[:90]}")

        section("byte work (mem*/str*) by bytes", BYTE_KINDS, lambda w: w["bytes"],
                [("kind", 8, lambda k, w: k), ("calls", 12, lambda k, w: w["calls"]),
                 ("MB", 10, lambda k, w: w["bytes"] / MB), ("repeats", 10, lambda k, w: w["repeats"]),
                 ("max call", 10, lambda k, w: w.get("peak", 0))])
        section("I/O by calls", IO_KINDS, lambda w: w["calls"],
                [("kind", 8, lambda k, w: k), ("calls", 12, lambda k, w: w["calls"]),
                 ("MB", 10, lambda k, w: w["bytes"] / MB), ("tiny", 10, lambda k, w: w["aux1"])])
        section("paths opened/statted again", PATH_KINDS, lambda w: w["repeats"],
                [("kind", 8, lambda k, w: k), ("calls", 12, lambda k, w: w["calls"]),
                 ("repeats", 10, lambda k, w: w["repeats"]), ("failed", 10, lambda k, w: w["aux1"])])
        section("mutex contention", {"mutex"}, lambda w: w["aux1"],
                [("calls", 12, lambda k, w: w["calls"]), ("contended", 10, lambda k, w: w["aux1"])])
        section("parallel_for imbalance", {"parallel_for"}, lambda w: w["aux1"],
                [("calls", 8, lambda k, w: w["calls"]), ("items", 10, lambda k, w: w["bytes"]),
                 ("imbalance", 10, lambda k, w: w["aux1"])])
        # imbalance / (work + imbalance): the share of the pool's capacity that sat idle
        # while the busiest worker finished (0% = perfectly even).
        section("pool balance by work done (per-worker events)", {"pool_ops"}, lambda w: w["aux1"],
                [("calls", 8, lambda k, w: w["calls"]), ("events", 14, lambda k, w: w["bytes"]),
                 ("idle%", 6, lambda k, w: f"{pct(w['aux1'], w['bytes'] + w['aux1']):.1f}"),
                 ("workers", 8, lambda k, w: w["aux2"] / max(w["calls"], 1))])
        section("hash-table lookups", {"ht_get"}, lambda w: w["repeats"],
                [("gets", 12, lambda k, w: w["calls"]), ("misses", 12, lambda k, w: w["aux1"]),
                 ("repeats", 12, lambda k, w: w["repeats"])])
        section("dead writes (access lane)", {"dead_write"}, lambda w: w["bytes"],
                [("blocks", 10, lambda k, w: w["aux1"]), ("MB", 10, lambda k, w: w["bytes"] / MB)])
    return 0


if __name__ == "__main__":
    sys.exit(main())
