#!/usr/bin/env python3
"""predict.py — map a detect_changes impact set to the C test suites it can
affect: the PR test-impact SHADOW (scripts/ci/test-impact-shadow.sh).
Report-only; nothing here decides a merge.

The mapping is the one the September 2026 test-impact replay measured
(detect_changes at depth 15 plus rule fallbacks, "dcF@15"):
  - impacted test FUNCTIONS (graph rows in tests/*.c) map to the suites that
    RUN_TEST them, resolved in their own file first; a nested suite (RUN_SUITE
    inside another) resolves to the registered suite that runs it; only suites
    registered in tests/test_main.c count, perf suites excluded;
  - a changed tests/*.c file always selects its whole suite;
  - a changed fixture (tests/fixtures/<key>/...) selects the suites whose test
    files name <key>; a changed grammar selects the grammar suites;
  - run-all: the lane selector's full, a changed test header or
    test-infrastructure file, a saturated or truncated impact set, or a C
    change the graph was not asked about -> every suite.

Usage: predict.py --root DIR --changed FILE --selection FILE [--detect FILE]
                  [--depth N] [--meta JSON] --out FILE [--summary FILE]
       predict.py --needs-graph --changed FILE    (prints yes|no)
"""
import argparse
import json
import os
import re
import sys

GRAMMAR_SUITES = re.compile(r"^(grammar_|lang_contract|parse_coverage|extraction|matrix_|"
                            r"edge_types_probe|convergence_probe|ac$|language$)")
C_SOURCE = re.compile(r"\.(c|h|cc|cpp|inc)$")
SUITE_DEF = re.compile(r"^\s*(?:SUITE\((\w+)\)|void\s+suite_(\w+)\s*\(\s*void\s*\))\s*\{", re.M)
RUN_TEST = re.compile(r"\bRUN_TEST\((\w+)\)")
RUN_SUITE = re.compile(r"\bRUN_SUITE\((\w+)\)")


def needs_graph(changed):
    """A C source change the graph can place (prod or test code)."""
    return any(C_SOURCE.search(p) and (p.startswith(("src/", "tests/")) or (
        p.startswith("internal/cbm/") and not p.startswith("internal/cbm/vendored/")))
        for p in changed)


def suitemap(root):
    """file -> suites defined there, test -> suites running it, suite universe,
    and the resolver from any suite to the registered suites that run it."""
    file_suites, test_suites, sources, parents = {}, {}, {}, {}
    for directory, dirs, names in os.walk(os.path.join(root, "tests")):
        dirs[:] = sorted(d for d in dirs if not (directory == os.path.join(root, "tests")
                                                  and d == "fixtures"))
        for name in sorted(names):
            if not name.endswith(".c"):
                continue
            path = os.path.join(directory, name)
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            with open(path, encoding="utf-8", errors="replace") as fh:
                src = fh.read()
            sources[rel] = src
            for m in SUITE_DEF.finditer(src):
                suite = m.group(1) or m.group(2)
                start = depth = m.end() - 1
                end = start
                depth = 0
                while end < len(src):   # the body is the balanced { } block
                    if src[end] == "{":
                        depth += 1
                    elif src[end] == "}":
                        depth -= 1
                        if depth == 0:
                            break
                    end += 1
                file_suites.setdefault(rel, []).append(suite)
                for test in RUN_TEST.findall(src[start:end]):
                    test_suites.setdefault(test, [])
                    if suite not in test_suites[test]:
                        test_suites[test].append(suite)
                for child in RUN_SUITE.findall(src[start:end]):
                    parents.setdefault(child, set()).add(suite)
    main = sources.get("tests/test_main.c", "")
    registered = set(re.findall(r"RUN_SELECTED_SUITE(?:_PERF)?\((\w+)\)", main))
    universe = registered - set(re.findall(r"RUN_SELECTED_SUITE_PERF\((\w+)\)", main))

    def runnable(suite, seen=frozenset()):
        """A registered suite runs itself; a nested one runs inside its parents."""
        if suite in registered:
            return {suite} & universe
        out = set()
        for parent in parents.get(suite, ()):
            if parent not in seen:
                out |= runnable(parent, seen | {suite})
        return out
    return file_suites, test_suites, universe, sources, runnable


def predict(root, changed, selection, detect, depth):
    file_suites, test_suites, universe, sources, runnable = suitemap(root)
    reasons, run_all = [], False
    if selection.get("full"):
        run_all = True
        reasons.append(f"run-all: the lane selector ran everything ({selection.get('tier')})")
    for p in changed:
        if p.startswith("tests/") and not p.startswith("tests/fixtures/") and p.endswith(".h"):
            run_all = True
            reasons.append(f"run-all: test header {p}")
        elif p.startswith("test-infrastructure/"):
            run_all = True
            reasons.append(f"run-all: test infrastructure {p}")

    graph = {"consulted": detect is not None}
    picked = {}   # suite -> "ALL" | set(tests)
    if detect is not None:
        rows = detect.get("impacted", [])
        total = detect.get("impacted_total", len(rows))
        shown = detect.get("impacted_shown", len(rows))
        graph.update(impacted_total=total, impacted_shown=shown,
                     saturated=bool(detect.get("engine_saturated")))
        if graph["saturated"]:
            run_all = True
            reasons.append("run-all: detect_changes saturated")
        if total > len(rows) or shown > len(rows):
            run_all = True
            reasons.append(f"run-all: impact set truncated ({len(rows)} of {total} rows)")
        tests = 0
        for r in rows:
            fp, name = r.get("file", ""), r.get("qn", "").rsplit(".", 1)[-1]
            if not fp.startswith("tests/") or r.get("label") != "Function" or r.get("hop", 0) > depth:
                continue
            local = file_suites.get(fp, [])
            if name in local:
                continue                       # the SUITE function itself
            candidates = test_suites.get(name, [])
            for suite in [s for s in candidates if s in local] or candidates:
                for runner in runnable(suite):
                    picked.setdefault(runner, set()).add(name)
                    tests += 1
        graph["test_rows"] = tests
    elif needs_graph(changed):
        run_all = True
        reasons.append("run-all: C change but no impact set (graph not consulted)")

    for p in changed:
        if p.startswith("tests/fixtures/"):          # fixture sources are data, not tests
            key = p.split("/")[2] if p.count("/") >= 2 else p
            for f, src in sources.items():
                if key in src:
                    for suite in file_suites.get(f, []):
                        for runner in runnable(suite):
                            picked[runner] = "ALL"
            reasons.append(f"changed fixture {p}: suites whose tests name '{key}'")
        elif p.startswith("tests/") and p.endswith(".c"):
            for suite in file_suites.get(p, []):
                for runner in runnable(suite):
                    picked[runner] = "ALL"
                    reasons.append(f"changed test file {p}: suite {runner}")
        elif p.startswith("internal/cbm/vendored/grammars/"):
            for suite in universe:
                if GRAMMAR_SUITES.match(suite):
                    picked[suite] = "ALL"
            reasons.append(f"changed grammar {p}: grammar suites")

    if run_all:
        mode, suites = "all", {s: "ALL" for s in sorted(universe)}
    else:
        suites = {s: v if v == "ALL" else sorted(v) for s, v in sorted(picked.items())}
        mode = "graph" if (graph["consulted"] or suites) else "none"
    return {"version": 1, "mode": mode, "depth": depth,
            "selector": {"tier": selection.get("tier"), "full": bool(selection.get("full"))},
            "universe": len(universe), "selected_suites": len(suites), "suites": suites,
            "graph": graph, "reasons": sorted(set(reasons))}


def summary(result, meta):
    g = result["graph"]
    whole = sum(1 for v in result["suites"].values() if v == "ALL")
    tests = sum(len(v) for v in result["suites"].values() if v != "ALL")
    lines = ["### Test-impact shadow (report-only, never gates)", "",
             "| | |", "|---|---|",
             f"| selector | {result['selector']['tier']} (full: {'yes' if result['selector']['full'] else 'no'}) |",
             f"| prediction | **{result['mode']}**: {result['selected_suites']} of {result['universe']} "
             f"suites ({whole} whole, {tests} tests in the rest) |"]
    if g.get("consulted"):
        lines.append(f"| graph | {g.get('impacted_shown')} of {g.get('impacted_total')} impacted rows at "
                     f"depth {result['depth']}, {g.get('test_rows')} test rows"
                     f"{', saturated' if g.get('saturated') else ''} |")
    for key in sorted(meta):
        lines.append(f"| {key} | {meta[key]} |")
    if result["reasons"]:
        lines += ["", "Reasons: " + "; ".join(result["reasons"][:12])]
    if result["mode"] == "graph" and result["suites"]:
        lines += ["", "Suites: " + ", ".join(result["suites"])[:1500]]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--needs-graph", action="store_true")
    ap.add_argument("--root")
    ap.add_argument("--changed", required=True)
    ap.add_argument("--selection")
    ap.add_argument("--detect")
    ap.add_argument("--depth", type=int, default=15)
    ap.add_argument("--meta", default="{}")
    ap.add_argument("--out")
    ap.add_argument("--summary")
    a = ap.parse_args()
    with open(a.changed, encoding="utf-8") as fh:
        changed = sorted({line.strip() for line in fh if line.strip()})
    if a.needs_graph:
        print("yes" if needs_graph(changed) else "no")
        return 0
    if not (a.root and a.selection and a.out):
        ap.error("--root, --selection and --out are required")
    with open(a.selection, encoding="utf-8") as fh:
        selection = json.load(fh)
    detect = None
    if a.detect:
        with open(a.detect, encoding="utf-8") as fh:
            detect = json.load(fh)
    result = predict(a.root, changed, selection, detect, a.depth)
    meta = json.loads(a.meta)
    result["meta"] = meta
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1, sort_keys=True)
        fh.write("\n")
    if a.summary:
        with open(a.summary, "w", encoding="utf-8") as fh:
            fh.write(summary(result, meta))
    return 0


if __name__ == "__main__":
    sys.exit(main())
