#!/usr/bin/env python3
"""Analyse #581 memory-lab output: attribute growth to allocation sites.

Reads the waste layer's dumps (CBM_MEMWASTE_OUT, one at every request-stage phase
mark; see scripts/memlab.sh) and the mem.census lines from a daemon log, and
prints:

  * retention per request, as a linear fit rather than first-vs-last, so noise
    does not masquerade as a trend
  * the call sites whose live bytes grow across the run, ranked by growth
  * an explicit unattributed remainder: process growth the layer could not
    account for (allocator metadata, fragmentation, memory outside malloc).
    That number is the honesty check.

Usage:
  memlab-report.py <dump.jsonl> [--census <daemon.log>] [--binary BIN] [--top N]
  memlab-report.py <a.jsonl> --diff <b.jsonl> [--binary BIN]     # platform comparison
"""
import argparse
import json
import re
import sys
from collections import defaultdict


def load_profile(path):
    """Return (summaries, site_series) for the process with the most dumps.
    Every dump lists every site, so the Nth occurrence of a site is its Nth
    sample; a site first seen late is padded with zeros so series align."""
    procs = defaultdict(lambda: {"summaries": [], "series": defaultdict(list)})
    cur = None
    with open(path, "r", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "memwaste" in record:
                cur = procs[record.get("pid", 0)]
                cur["summaries"].append(record)
            elif cur is not None and "site" in record and "work" not in record:
                samples = cur["series"][record["site"]]
                while len(samples) < len(cur["summaries"]) - 1:
                    samples.append({"live_bytes": 0})
                samples.append(record)
    if not procs:
        return [], {}
    best = max(procs.values(), key=lambda p: len(p["summaries"]))
    return best["summaries"], best["series"]


def load_symbols(binary, summaries, sites):
    """Symbolise site addresses with the waste report's own symboliser."""
    if not binary or not summaries:
        return {}
    import importlib.util
    import os
    spec = importlib.util.spec_from_file_location(
        "memwaste_report", os.path.join(os.path.dirname(os.path.abspath(__file__)), "memwaste-report.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.symbolise(binary, summaries[-1].get("image_base", "0x0"), list(sites))


def linear_slope(values):
    """Least-squares slope over sample index. Resistant to the single-outlier
    reading that a first-vs-last delta would treat as the whole trend."""
    n = len(values)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2.0
    mean_y = sum(values) / n
    num = sum((i - mean_x) * (v - mean_y) for i, v in enumerate(values))
    den = sum((i - mean_x) ** 2 for i in range(n))
    return num / den if den else 0.0


def parse_census(path):
    """Pull the pool totals out of mem.census log lines."""
    fields = ("priv_kb", "rss_kb", "crt_kb", "biggest_kb")
    rows = []
    pattern = re.compile(r"(\w+)=(\S+)")
    with open(path, "r", errors="replace") as handle:
        for line in handle:
            if "mem.census" not in line:
                continue
            kv = dict(pattern.findall(line))
            if not any(f in kv for f in fields):
                continue
            rows.append({f: int(kv[f]) for f in fields if f in kv and kv[f].isdigit()})
    return rows


def report(path, census_path, top, binary=None):
    summaries, series = load_profile(path)
    if not summaries and not series:
        print(f"{path}: no waste-layer records -- was the memwaste flavour run with CBM_MEMWASTE=1?")
        return
    names = load_symbols(binary, summaries, series.keys())
    last = summaries[-1] if summaries else {}
    print(f"=== {path} ===")
    if last:
        print(f"samples        : {len(summaries)} dumps (every allocation size is tracked)")
        print(f"sites seen     : {last.get('sites', 0)}")
        print(f"attributed live: {last.get('live_bytes', 0)/1048576:.1f} MB "
              f"in {last.get('live_blocks', 0)} blocks")
        lost = last.get("site_table_full", 0) + last.get("pointer_table_full", 0)
        if lost:
            print(f"RECORDS LOST   : {lost} — tables exhausted, treat totals as a floor")

    growth = []
    for site, samples in series.items():
        live = [s["live_bytes"] for s in samples]
        slope = linear_slope(live)
        if slope <= 0 and live[-1] <= live[0]:
            continue
        growth.append((slope, live[-1] - live[0], live[-1], names.get(site, site)))
    growth.sort(key=lambda row: row[0], reverse=True)

    print(f"\n--- sites whose retained bytes GROW (top {top}) ---")
    if not growth:
        print("none — no tracked site retains more over time.")
    for slope, delta, live, name in growth[:top]:
        print(f"  +{slope/1024:8.1f} KB/sample  total +{delta/1048576:6.2f} MB  "
              f"live {live/1048576:6.2f} MB")
        print(f"      {name}")

    if census_path:
        rows = parse_census(census_path)
        if rows:
            priv = [r["priv_kb"] for r in rows if "priv_kb" in r]
            # attributed = what the waste layer holds live, sampled at its own dumps
            prof = [int(h.get("live_bytes", 0)) // 1024 for h in summaries]
            print(f"\n--- pools vs attribution ({len(rows)} samples) ---")
            if priv:
                print(f"  private committed : {priv[0]/1024:.1f} -> {priv[-1]/1024:.1f} MB "
                      f"({linear_slope(priv):+.1f} KB/sample)")
            if prof and priv:
                unattributed = (priv[-1] - priv[0]) - (prof[-1] - prof[0])
                print(f"  attributed growth : {(prof[-1]-prof[0])/1024:+.1f} MB")
                print(f"  UNATTRIBUTED      : {unattributed/1024:+.1f} MB "
                      "<- allocator metadata, fragmentation, or memory outside malloc (mmap, stacks)")


def diff(path_a, path_b, top, binary=None):
    """Same sites, two platforms: what does one retain that the other does not?
    Raw addresses differ between binaries, so sites are matched by symbol name
    when --binary is given (the same binary for both runs) and by address otherwise."""
    summaries_a, series_a = load_profile(path_a)
    _, series_b = load_profile(path_b)
    names = load_symbols(binary, summaries_a, set(series_a) | set(series_b))
    rows = []
    for site, samples in series_a.items():
        a_live = samples[-1]["live_bytes"]
        b_live = series_b[site][-1]["live_bytes"] if site in series_b else 0
        if a_live - b_live > 0:
            rows.append((a_live - b_live, a_live, b_live, site))
    rows.sort(key=lambda row: row[0], reverse=True)
    print(f"=== {path_a} minus {path_b} (top {top}) ===")
    for delta, a_live, b_live, site in rows[:top]:
        print(f"  +{delta/1048576:7.2f} MB   A={a_live/1048576:.2f} MB  B={b_live/1048576:.2f} MB")
        print(f"      {names.get(site, site)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile")
    parser.add_argument("--census")
    parser.add_argument("--diff")
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--binary", help="the memwaste binary, for symbol names")
    args = parser.parse_args()
    if args.diff:
        diff(args.profile, args.diff, args.top, args.binary)
    else:
        report(args.profile, args.census, args.top, args.binary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
