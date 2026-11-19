#!/usr/bin/env python3
"""Run every suite of one profile in the documented order, then write the scorecard.

    python3 evals/harness/run_all.py --profile evals/profiles/sqlite.local.json [--binary BIN]
    python3 evals/harness/run_all.py --profile evals/profiles/neo4j.local.json --with-scale
    python3 evals/harness/run_all.py --profile ... --only common          # just the common files
    python3 evals/harness/run_all.py --profile ... --skip N05_unavailable  # skip a suite by name

Keeps going after failures; exits 1 when any suite had a tier-1 failure.
Results: evals/results/<backend>/<case-file>.jsonl, evals/results/SCORECARD.md.
"""
import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
EVALS = os.path.dirname(HERE)
COMMON = ["00_schema_and_status", "10_search_graph_structural", "11_search_graph_bm25_semantic",
          "12_search_graph_paging", "20_search_code", "30_query_graph", "40_trace_path",
          "50_snippets_outline", "60_coverage", "70_detect_changes", "80_architecture"]
ORDER = {
    "sqlite": [("sqlite", "S01_index_create"), ("sqlite", "S02_sqlite_schema")] + [("common", c) for c in COMMON]
              + [("sqlite", "S03_fts_bm25"), ("sqlite", "S99_cleanup")],
    "neo4j": [("neo4j", "N01_index_create"), ("neo4j", "N02_neo4j_indexes")] + [("common", c) for c in COMMON]
             + [("sqlite", "S03_fts_bm25"), ("neo4j", "N03_delta_sync"), ("neo4j", "N04_generations"),
                ("neo4j", "N06_native_query"), ("neo4j", "N07_security"), ("neo4j", "N05_unavailable"),
                ("neo4j", "N08_parity"), ("neo4j", "N09_scale"), ("neo4j", "N99_cleanup")],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--binary")
    ap.add_argument("--only", help="common | a suite name substring")
    ap.add_argument("--skip", action="append", default=[], help="suite name substring to skip (repeatable)")
    ap.add_argument("--with-scale", action="store_true", help="include N09_scale (needs eval-medium)")
    ap.add_argument("--no-cleanup", action="store_true", help="leave the projects indexed (skip S99/N99)")
    ap.add_argument("--var", action="append", default=[])
    a = ap.parse_args()
    with open(a.profile, encoding="utf-8") as f:
        backend = json.load(f)["backend"]
    plan = ORDER[backend]
    rc = 0
    ran = []
    for folder, name in plan:
        if a.only and a.only != "common" and a.only not in name:
            continue
        if a.only == "common" and folder != "common":
            continue
        if any(s in name for s in a.skip):
            continue
        if name == "N09_scale" and not a.with_scale:
            continue
        if a.no_cleanup and name.endswith("_cleanup"):
            continue
        cases = os.path.join(EVALS, "cases", folder, name + ".json")
        out = os.path.join(EVALS, "results", backend, name + ".jsonl")
        cmd = [sys.executable, os.path.join(HERE, "run_cases.py"), "--profile", a.profile, "--cases", cases, "--out", out]
        if a.binary:
            cmd += ["--binary", a.binary]
        for v in a.var:
            cmd += ["--var", v]
        print(f"\n=== {backend}: {folder}/{name} ===", flush=True)
        r = subprocess.run(cmd)
        ran.append((name, r.returncode))
        if r.returncode == 2:
            print("MCP session could not start; stopping.")
            rc = 2
            break
        if r.returncode != 0:
            rc = rc or 1
    print("\nSuites:", ", ".join(f"{n}={'ok' if c == 0 else 'FAIL'}" for n, c in ran))
    subprocess.run([sys.executable, os.path.join(HERE, "report.py"), os.path.join(EVALS, "results", "**", "*.jsonl"),
                    "--out", os.path.join(EVALS, "results", "SCORECARD.md")])
    return rc


if __name__ == "__main__":
    sys.exit(main())
