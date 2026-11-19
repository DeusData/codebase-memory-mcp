#!/usr/bin/env python3
"""Compare two result files of the same case file run on two backends
(or on two binaries), ignoring volatile fields.

    python3 evals/harness/compare_backends.py \\
        evals/results/sqlite/10_search_graph_structural.jsonl \\
        evals/results/neo4j/10_search_graph_structural.jsonl \\
        [--report evals/results/parity/10_search_graph_structural.md]

Parity rule (spec 09): for every step id present in both files, the
structured output must be identical after
  - dropping graph_backend, every *_ms / ms timing field, warnings that only
    mention timing, and cursors / next_cursor tokens (opaque per backend);
  - replacing the project name with <project> (the sqlite twin is a
    different project name);
  - sorting rows when the step says "unordered": true.
Exit code 1 when any compared step differs.
"""
import argparse
import copy
import os
import json
import re
import sys

VOLATILE_KEYS = {"graph_backend", "ms", "scope_ms", "scan_ms", "enrich_ms", "elapsed_ms",
                 "cursor", "next_cursor", "next", "generation", "expected_generation",
                 "indexed_at", "last_scan_at", "root_path", "size_bytes", "database_size",
                 # state left by other suites or by the run environment, not by the backend
                 "adr_present", "adr_hint", "available_projects", "recorded_at", "canonical_root",
                 "worktree_root", "local_generation", "synced_generation"}
# Steps whose answer is a listing of the whole environment (other projects exist on one side only).
ENVIRONMENT_STEPS = {
    "C00-01": "lists every project in the cache",
    "C00-02": "lists every project in the cache",
    "C00-09": "lists every project in the cache",
    "DC-18": "compares with a different second project on each side",
    "AR-14": "reads ADR state left by earlier runs",
    "AR-15": "reads ADR state left by earlier runs",
}
FLOAT_DIGITS = 2  # semantic scores: compare to 2 decimals
TIMING_RX = re.compile(r"\b\d+(\.\d+)?\s*(ms|s|seconds)\b")


def load(path):
    steps = {}
    header = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if "_header" in r:
                header = r["_header"]
            elif "id" in r:
                steps[r["id"]] = r
    return header, steps


def normalize(obj, project):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in VOLATILE_KEYS:
                continue
            if k in ("warning", "warnings", "notes") and isinstance(v, (str, list)):
                items = v if isinstance(v, list) else [v]
                items = [i for i in items if not TIMING_RX.search(str(i))]
                if not items:
                    continue
                v = items if isinstance(v, list) else items[0]
            out[k] = normalize(v, project)
        return out
    if isinstance(obj, list):
        return [normalize(x, project) for x in obj]
    if isinstance(obj, float):
        return round(obj, FLOAT_DIGITS)
    if isinstance(obj, str) and project:
        names = project if isinstance(project, list) else [project]
        for name in sorted(names, key=len, reverse=True):
            obj = obj.replace(name, "<project>")
        return obj
    return obj


def drop_cols(obj, names):
    """Blank the named columns of every {cols|columns, rows} table."""
    if isinstance(obj, dict):
        cols = obj.get("cols") or obj.get("columns")
        out = {k: drop_cols(v, names) for k, v in obj.items()}
        if isinstance(cols, list) and isinstance(out.get("rows"), list):
            idx = [i for i, c in enumerate(cols) if c in names]
            out["rows"] = [[None if i in idx else v for i, v in enumerate(r)] if isinstance(r, list) else r
                           for r in out["rows"]]
        return out
    if isinstance(obj, list):
        return [drop_cols(x, names) for x in obj]
    return obj


def sort_rows(obj):
    if isinstance(obj, dict):
        d = {k: sort_rows(v) for k, v in obj.items()}
        for key in ("rows", "groups", "impacted", "callers", "projects", "suggestions"):
            if key in d and isinstance(d[key], list):
                d[key] = sorted(d[key], key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
        return d
    if isinstance(obj, list):
        return [sort_rows(x) for x in obj]
    return obj


def diff(a, b, path=""):
    """First few differences between two normalized JSON values."""
    out = []
    if type(a) is not type(b):
        return [f"{path}: {type(a).__name__} vs {type(b).__name__}"]
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: only in B")
            elif k not in b:
                out.append(f"{path}.{k}: only in A")
            else:
                out.extend(diff(a[k], b[k], f"{path}.{k}"))
            if len(out) > 12:
                break
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append(f"{path}: {len(a)} vs {len(b)} items")
        for i, (x, y) in enumerate(zip(a, b)):
            out.extend(diff(x, y, f"{path}[{i}]"))
            if len(out) > 12:
                break
    elif a != b:
        out.append(f"{path}: {json.dumps(a, ensure_ascii=False)[:80]} vs {json.dumps(b, ensure_ascii=False)[:80]}")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a")
    ap.add_argument("b")
    ap.add_argument("--report")
    ap.add_argument("--unordered", action="store_true", help="sort rows before comparing every step")
    ap.add_argument("--drop-cols", default="", help="comma-separated table columns to ignore (e.g. rank,score)")
    args = ap.parse_args()
    ha, sa = load(args.a)
    hb, sb = load(args.b)
    pa = (ha.get("vars") or {}).get("project")
    pb = (hb.get("vars") or {}).get("project")
    lines = [f"# Parity: {args.a} vs {args.b}", "",
             f"A: backend={ha.get('backend')} project={pa}  B: backend={hb.get('backend')} project={pb}", "",
             "| id | result | detail |", "|---|---|---|"]
    bad = 0
    for sid in sorted(set(sa) & set(sb)):
        ra, rb = sa[sid], sb[sid]
        if ra.get("kind") != "tool" or rb.get("kind") != "tool":
            continue
        if ra.get("status") == "skipped" or rb.get("status") == "skipped":
            lines.append(f"| {sid} | skipped | |")
            continue
        if sid in ENVIRONMENT_STEPS:
            lines.append(f"| {sid} | not compared | {ENVIRONMENT_STEPS[sid]} |")
            continue
        if ra.get("status") != rb.get("status"):
            bad += 1
            lines.append(f"| {sid} | DIFF | status {ra.get('status')} vs {rb.get('status')} |")
            continue
        # every project name either side uses, plus the repo folder name (the Project node's name)
        repo = os.path.basename(str((ha.get("vars") or {}).get("repo") or "")) or None
        names = [n for n in (pa, pb, repo) if n]
        na = normalize(copy.deepcopy(ra.get("structured")), names)
        nb = normalize(copy.deepcopy(rb.get("structured")), names)
        drop = [c for c in args.drop_cols.split(",") if c]
        if drop:
            na, nb = drop_cols(na, drop), drop_cols(nb, drop)
        if args.unordered or (ra.get("unordered") or rb.get("unordered")):
            na, nb = sort_rows(na), sort_rows(nb)
        d = diff(na, nb)
        if d:
            bad += 1
            lines.append(f"| {sid} | DIFF | {'; '.join(d[:4]).replace('|', '/')} |")
        else:
            lines.append(f"| {sid} | same | |")
    only_a = sorted(set(sa) - set(sb))
    only_b = sorted(set(sb) - set(sa))
    if only_a or only_b:
        lines += ["", f"Only in A: {only_a}", f"Only in B: {only_b}"]
    lines += ["", f"**{bad} differing step(s)**"]
    text = "\n".join(lines)
    print(text)
    if args.report:
        os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
