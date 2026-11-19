#!/usr/bin/env python3
"""Index-coverage recall of an indexed `eval-small` against its ground truth.

    python3 evals/harness/coverage_check.py --binary BIN --project eval-small \\
        --expected evals/fixtures/expected/eval-small.json [--report out.json]

Reads the whole graph through query_graph (names, labels, files, CALLS pairs
and File paths) and scores:
  files_tier1      every tier-1 source file has a File node          (must be 100%)
  files_tier2      tier-2 files (docs, infra, __init__)              (report)
  never_indexed    no File node / symbol from vendor/ or generated/  (must be 100%)
  symbols_tier1    every tier-1 (name, label, file) exists           (must be 100%)
  symbols_tier2    tier-2 symbols                                    (report)
  calls_tier1      every tier-1 CALLS pair exists                    (must be 100%)
  calls_tier2      tier-2 pairs (resolver-dependent)                 (report)
  hub_inbound      exact inbound CALLS count of hub                  (must be exact)
  dead_code        unused_helper has 0 inbound CALLS                 (must)
Exit 1 when any "must" line is not met. The report JSON lists every miss.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_client import Mcp, reply_structured, reply_text  # noqa: E402

LONG_NAME = ("very_long_function_name_" + "0" * 200)[:200]


def q(m, project, query, max_rows=99998):
    status, reply = m.call("query_graph", {"project": project, "query": query, "format": "json",
                                           "max_rows": max_rows}, 600)
    if status != "ok":
        raise SystemExit(f"query failed: {reply_text(reply)[:300]}")
    s = reply_structured(reply) or {}
    if s.get("has_more"):
        print(f"warning: query truncated at {s.get('returned')} rows: {query[:60]}", file=sys.stderr)
    return s.get("rows") or []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--expected", required=True)
    ap.add_argument("--report")
    a = ap.parse_args()
    with open(a.expected, encoding="utf-8") as f:
        exp = json.load(f)
    m = Mcp(a.binary)
    nodes = q(m, a.project, "MATCH (n) RETURN n.name, n.label, n.file_path")
    calls = q(m, a.project, "MATCH (x)-[:CALLS]->(y) RETURN x.name, y.name, x.file_path")
    files = {r[0] for r in q(m, a.project, "MATCH (f:File) RETURN f.file_path")}
    m.close()
    have = {(str(n), str(l), str(fp or "")) for n, l, fp in nodes}
    names = {(str(n), str(l)) for n, l, _ in nodes}
    name_files = {(str(n), str(fp or "")) for n, _, fp in nodes}
    pairs = {(str(x), str(y)) for x, y, _ in calls}
    report = {"project": a.project, "nodes": len(nodes), "calls": len(calls), "files": len(files)}
    musts_ok = True

    def score(key, items, pred, must):
        nonlocal musts_ok
        miss = [i for i in items if not pred(i)]
        rate = 1.0 if not items else (len(items) - len(miss)) / len(items)
        report[key] = {"total": len(items), "missing": miss, "recall": round(rate, 4), "must": must}
        if must and miss:
            musts_ok = False
        print(f"{key:16} {len(items) - len(miss):>4}/{len(items):<4} recall={rate:.3f} {'MUST' if must else ''} "
              f"{('missing: ' + json.dumps(miss[:5], ensure_ascii=False)) if miss else ''}")

    score("files_tier1", exp["source_files_tier1"], lambda p: p in files, True)
    score("files_tier2", exp["source_files_tier2"], lambda p: p in files, False)
    bad_files = sorted(p for p in files if p.startswith(("vendor/", "generated/")))
    bad_syms = sorted(n for n, _ in names if n in exp["never_present_symbols"])
    report["never_indexed"] = {"files": bad_files, "symbols": bad_syms, "must": True}
    if bad_files or bad_syms:
        musts_ok = False
    print(f"{'never_indexed':16} files={bad_files} symbols={bad_syms} MUST")

    def sym_pred(s):
        name = s["name"].replace("{{LONG_NAME}}", LONG_NAME)
        return (name, s["label"], s["file"]) in have or (
            (name, s["label"]) in names and (name, s["file"]) in name_files)

    score("symbols_tier1", exp["symbols_tier1"], sym_pred, True)
    score("symbols_tier2", exp["symbols_tier2"], sym_pred, False)
    score("calls_tier1", exp["calls_tier1"], lambda c: (c["from"], c["to"]) in pairs, True)
    score("calls_tier2", exp["calls_tier2"], lambda c: (c["from"], c["to"]) in pairs, False)
    hub = exp["hub"]
    inbound = sorted(x for x, y, _ in calls if y == hub["name"])
    report["hub_inbound"] = {"count": len(inbound), "expected": hub["inbound_calls_exact"], "callers": inbound,
                             "must": True}
    if len(inbound) != hub["inbound_calls_exact"]:
        musts_ok = False
    print(f"{'hub_inbound':16} {len(inbound)} (expected {hub['inbound_calls_exact']}) MUST")
    dead = {n: sorted(x for x, y, _ in calls if y == n) for n in exp["dead_code"]["zero_inbound_calls"]}
    report["dead_code"] = {"inbound": dead, "must": True}
    if any(dead.values()):
        musts_ok = False
    print(f"{'dead_code':16} inbound={dead} MUST")
    dups = exp["duplicates"]["shared_name"]
    found = sorted(fp for n, fp in name_files if n == "shared_name")
    report["duplicates"] = {"expected": dups, "found": found, "must": True}
    if found != sorted(dups):
        musts_ok = False
    print(f"{'duplicates':16} {found} MUST")
    report["musts_ok"] = musts_ok
    if a.report:
        os.makedirs(os.path.dirname(os.path.abspath(a.report)) or ".", exist_ok=True)
        with open(a.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    print("coverage_ok" if musts_ok else "coverage_bad")
    return 0 if musts_ok else 1


if __name__ == "__main__":
    sys.exit(main())
