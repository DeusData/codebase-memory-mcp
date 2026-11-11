#!/usr/bin/env bash
# Test-impact prediction contract: scripts/test-impact/predict.py turns a
# detect_changes impact set into the C test suites a change can affect. It
# feeds the PR test-impact SHADOW job -- report-only, never a gate -- but a
# shadow that silently under-predicts would make its own evaluation lie, so
# the mapping and its fallbacks are pinned on a synthetic tree:
#   - impacted test functions map to their registered suites (perf excluded;
#     a nested suite resolves to the registered suite that runs it);
#   - a changed test file always selects its whole suite;
#   - a changed fixture selects the suites whose tests name it;
#   - run-all: the selector's full, a test header, a saturated or truncated
#     impact set -> every suite;
#   - no C change -> the graph is not consulted, nothing is predicted.
#
# Usage: tests/test_test_impact_predict.sh [repo-root]

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
PREDICT="$ROOT/scripts/test-impact/predict.py"
if [ ! -f "$PREDICT" ]; then
    echo "FAIL: $PREDICT not found" >&2
    exit 1
fi
WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-impact-predict.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

python3 - "$PREDICT" "$WORK" <<'PY'
import json
import pathlib
import subprocess
import sys

predict, work = sys.argv[1], pathlib.Path(sys.argv[2])
tree = work / "tree"
(tree / "tests").mkdir(parents=True)
(tree / "tests" / "test_main.c").write_text(
    "RUN_SELECTED_SUITE(alpha);\nRUN_SELECTED_SUITE(beta);\n"
    "RUN_SELECTED_SUITE(gamma);\nRUN_SELECTED_SUITE_PERF(delta);\n")
(tree / "tests" / "test_alpha.c").write_text(
    "TEST(a1) { }\nTEST(a2) { }\nSUITE(alpha) {\n    RUN_TEST(a1);\n    RUN_TEST(a2);\n}\n")
(tree / "tests" / "test_beta.c").write_text(
    'TEST(b1) { load("tests/fixtures/kx/in.c"); }\nSUITE(beta) {\n    RUN_TEST(b1);\n}\n')
(tree / "tests" / "test_gamma.c").write_text(
    "TEST(g1) { }\nvoid suite_gamma(void) {\n    RUN_TEST(g1);\n}\n")
(tree / "tests" / "test_delta.c").write_text("TEST(d1) { }\nSUITE(delta) {\n    RUN_TEST(d1);\n}\n")
# A nested suite runs only inside a registered parent (RUN_SUITE in its body).
(tree / "tests" / "test_nested.c").write_text(
    "TEST(n1) { }\nSUITE(inner) {\n    RUN_TEST(n1);\n}\nSUITE(outer) {\n    RUN_SUITE(inner);\n}\n")
(tree / "tests" / "test_main.c").write_text(
    (tree / "tests" / "test_main.c").read_text() + "RUN_SELECTED_SUITE(outer);\n")
failures = []


def row(qn, label, file, hop):
    return {"qn": qn, "label": label, "file": file, "hop": hop}


def predict_for(name, changed, selection, detect=None):
    case = work / name
    case.mkdir()
    (case / "changed.txt").write_text("".join(f + "\n" for f in changed))
    (case / "selection.json").write_text(json.dumps(selection))
    args = [sys.executable, predict, "--root", str(tree), "--changed", str(case / "changed.txt"),
            "--selection", str(case / "selection.json"), "--out", str(case / "p.json"),
            "--summary", str(case / "s.md")]
    if detect is not None:
        (case / "detect.json").write_text(json.dumps(detect))
        args += ["--detect", str(case / "detect.json")]
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        failures.append(f"{name}: predict.py exited {proc.returncode}: {proc.stderr.strip()}")
        return None
    return json.loads((case / "p.json").read_text())


def expect(name, got, mode, suites):
    if got is None:
        return
    if got["mode"] != mode:
        failures.append(f"{name}: mode {got['mode']}, expected {mode}")
    if got["suites"] != suites:
        failures.append(f"{name}: suites {got['suites']}, expected {suites}")


NARROW = {"tier": "T3-mid", "full": False}
graph = {"impacted_total": 3, "impacted_shown": 3, "impacted": [
    row("p.src.store.store_open", "Function", "src/store/store.c", 1),
    row("p.tests.test_alpha.a1", "Function", "tests/test_alpha.c", 2),
    row("p.tests.test_alpha.alpha", "Function", "tests/test_alpha.c", 3),
    row("p.tests.test_delta.d1", "Function", "tests/test_delta.c", 2)]}

expect("graph rows map to suites, perf excluded",
       predict_for("graph", ["src/store/store.c"], NARROW, graph), "graph", {"alpha": ["a1"]})
expect("a test in a nested suite maps to its registered parent",
       predict_for("nested", ["src/store/store.c"], NARROW,
                   {"impacted_total": 1, "impacted_shown": 1,
                    "impacted": [row("p.tests.test_nested.n1", "Function", "tests/test_nested.c", 2)]}),
       "graph", {"outer": ["n1"]})
expect("a changed test file selects its whole suite",
       predict_for("changed-test", ["src/store/store.c", "tests/test_gamma.c"], NARROW, graph),
       "graph", {"alpha": ["a1"], "gamma": "ALL"})
expect("a changed fixture selects the suites that name it",
       predict_for("fixture", ["tests/fixtures/kx/in.c"], {"tier": "T1-tests", "full": False},
                   {"impacted_total": 0, "impacted_shown": 0, "impacted": []}),
       "graph", {"beta": "ALL"})
ALL = {"alpha": "ALL", "beta": "ALL", "gamma": "ALL", "outer": "ALL"}
expect("the selector's full runs every suite",
       predict_for("full", ["Makefile.cbm", "src/store/store.c"], {"tier": "T5-core", "full": True}, graph),
       "all", ALL)
expect("a changed test header runs every suite",
       predict_for("test-header", ["tests/test_helpers.h"], {"tier": "T1-tests", "full": False}, graph),
       "all", ALL)
expect("a saturated impact set runs every suite",
       predict_for("saturated", ["src/store/store.c"], NARROW, dict(graph, engine_saturated=True)),
       "all", ALL)
expect("a truncated impact set runs every suite",
       predict_for("truncated", ["src/store/store.c"], NARROW, dict(graph, impacted_total=9000)),
       "all", ALL)
expect("no C change predicts nothing",
       predict_for("docs", ["README.md"], {"tier": "T0a-docs", "full": False}), "none", {})
expect("a C change without a graph is not a narrow prediction",
       predict_for("no-graph", ["src/store/store.c"], NARROW), "all", ALL)

for changed, want in ((["README.md"], "no"), (["src/store/store.c"], "yes"),
                      (["tests/test_alpha.c"], "yes")):
    (work / "needs.txt").write_text("".join(f + "\n" for f in changed))
    out = subprocess.run([sys.executable, predict, "--needs-graph", "--changed",
                          str(work / "needs.txt")], capture_output=True, text=True).stdout.strip()
    if out != want:
        failures.append(f"--needs-graph for {changed}: {out!r}, expected {want!r}")

if failures:
    print("TEST-IMPACT PREDICTION CONTRACT VIOLATED:")
    for failure in failures:
        print(f"  {failure}")
    sys.exit(1)
print("test-impact prediction contract OK (mapping, fallbacks, run-all triggers)")
PY
