#!/usr/bin/env python3
"""Summarize result JSONL files into one Markdown scorecard.

    python3 evals/harness/report.py evals/results/**/*.jsonl --out evals/results/SCORECARD.md

Per file: suite, backend, pass/fail/skip, tier-1 failures, slowest steps.
Per failed step: id, title, first failure reason, time.
"""
import argparse
import glob
import json
import os
import sys


def load(path):
    header, summary, steps = {}, {}, []
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "_header" in r:
                header = r["_header"]
            elif "_summary" in r:
                summary = r["_summary"]
            elif "id" in r:
                steps.append(r)
    return header, summary, steps


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--out")
    ap.add_argument("--slowest", type=int, default=5)
    a = ap.parse_args()
    paths = []
    for pat in a.files:
        paths.extend(glob.glob(pat, recursive=True))
    paths = sorted(set(p for p in paths if p.endswith(".jsonl")))
    lines = ["# Eval scorecard", "", "| suite | backend | binary | pass | fail | tier-1 fail | skip | file |",
             "|---|---|---|---|---|---|---|---|"]
    details = []
    tot = {"pass": 0, "fail": 0, "t1": 0, "skip": 0}
    for p in paths:
        h, s, steps = load(p)
        if not steps:
            continue
        npass = sum(1 for x in steps if x.get("pass") and x.get("status") != "skipped")
        nfail = sum(1 for x in steps if not x.get("pass") and x.get("status") != "skipped")
        nskip = sum(1 for x in steps if x.get("status") == "skipped")
        t1 = sum(1 for x in steps if not x.get("pass") and x.get("status") != "skipped" and x.get("tier", 2) == 1)
        tot["pass"] += npass
        tot["fail"] += nfail
        tot["t1"] += t1
        tot["skip"] += nskip
        lines.append(f"| {h.get('suite')} | {h.get('backend')} | {os.path.basename(str(h.get('binary')))} | "
                     f"{npass} | {nfail} | {t1} | {nskip} | {p} |")
        failed = [x for x in steps if not x.get("pass") and x.get("status") != "skipped"]
        slow = sorted((x for x in steps if x.get("kind") == "tool"), key=lambda x: -x.get("ms", 0))[: a.slowest]
        if failed or slow:
            details.append(f"\n## {h.get('suite')} on {h.get('backend')} ({p})\n")
        if failed:
            details.append("| id | tier | title | why | ms |\n|---|---|---|---|---|")
            for x in failed:
                why = "; ".join(x.get("fails") or []) or x.get("status", "")
                details.append(f"| {x.get('id')} | {x.get('tier', 2)} | {x.get('title', '')} | "
                               f"{why.replace('|', '/')[:200]} | {x.get('ms', 0)} |")
        if slow:
            details.append("\nSlowest tool calls: " + ", ".join(f"{x.get('id')} {x.get('ms', 0)} ms" for x in slow))
    lines += ["", f"**Total: {tot['pass']} pass, {tot['fail']} fail ({tot['t1']} tier-1), {tot['skip']} skipped**"]
    text = "\n".join(lines + details) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(text)
    return 1 if tot["t1"] else 0


if __name__ == "__main__":
    sys.exit(main())
