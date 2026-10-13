#!/usr/bin/env bash
# Lane-wiring contract: the workflows honour the lane selection, and only PRs
# are selected at all.
#
#   1. dry-run.yml and release.yml pass no `lanes` (and no shard profile) to
#      _test.yml / _lint.yml / _security.yml, whose inputs default to "all"
#      (and "standard"): those runs execute every job, sharded, exactly as
#      before lane selection. Only pr.yml asks for the PR shard profile.
#   2. pr.yml feeds the selector's lanes to every reusable workflow, runs the
#      T0a/T0b contracts job, and hands ci-ok both the selection and the
#      run's job list (actions: read) -- ci-ok needs `contracts` but never the
#      report-only memwaste / shadow jobs, and the shadow is continue-on-error.
#   3. every lane of scripts/ci/select-lanes.sh --list-lanes is served by a
#      job: matrix lanes as a "lane":"<name>" tag in the leg data (the tag
#      ci-ok reads back from the job name), single lanes by an `if:` that
#      names them. A lane nothing serves would be selected and never run.
#   4. the test selection (smart CI) is PR-only and fails open: _test.yml's
#      test_selection defaults to empty and only test-unix / test-windows
#      read it; dry runs and releases never pass it; pr.yml's select-tests
#      runs only under vars.TEST_IMPACT_MODE == 'gate', ci-ok does not need it,
#      and `test` passes a selection only from a successful narrowed run.
# Text-level checks (no YAML library on every leg), like the other workflow
# contracts.
#
# Usage: tests/test_lane_wiring_contract.sh [repo-root]

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
LANES=$(bash "$ROOT/scripts/ci/select-lanes.sh" --list-lanes)

python3 - "$ROOT" "$LANES" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
lanes = sys.argv[2].split()
wf = root / ".github" / "workflows"
failures = []


def text(name):
    return (wf / name).read_text(encoding="utf-8")


def jobs(source):
    """job id -> body (the lines indented under `  <id>:`)."""
    out = {}
    for m in re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):\s*$", source):
        rest = source[m.end():]
        end = re.search(r"(?m)^  [A-Za-z0-9_#-]", rest)
        out[m.group(1)] = rest[:end.start()] if end else rest
    return out


def require(cond, message):
    if not cond:
        failures.append(message)


# 1. dry run and release: no selection, so every job runs.
REUSABLE = ("_test.yml", "_lint.yml", "_security.yml")
for caller in ("dry-run.yml", "release.yml"):
    for job, body in jobs(text(caller)).items():
        called = re.search(r"uses: \./\.github/workflows/(_\w+\.yml)", body)
        if called and called.group(1) in REUSABLE:
            require(not re.search(r"(?m)^\s+(lanes|shard_profile|test_selection):", body),
                    f"{caller} job {job} passes a lane or test selection to {called.group(1)}; "
                    f"dry runs and releases must run every lane and every test")
m = re.search(r"(?ms)^      shard_profile:\n(.*?)(?=^      \S|^\S|\Z)", text("_test.yml"))
require(m is not None and re.search(r"(?m)^        default: standard$", m.group(1)) is not None,
        "_test.yml must declare a shard_profile input defaulting to standard")
for callee in REUSABLE:
    src = text(callee)
    m = re.search(r"(?ms)^      lanes:\n(.*?)(?=^      \S|^\S|\Z)", src)
    require(m is not None, f"{callee} must declare a `lanes` input")
    if m:
        require(re.search(r"(?m)^        type: string$", m.group(1)) is not None,
                f"{callee} lanes input must be a string")
        require(re.search(r"(?m)^        default: all$", m.group(1)) is not None,
                f"{callee} lanes input must default to all")

# 2. pr.yml wiring.
pr = jobs(text("pr.yml"))
changes = pr.get("changes", "")
require("scripts/ci/select-lanes.sh --format github-output" in changes,
        "pr.yml changes must run scripts/ci/select-lanes.sh --format github-output")
require("--expect-files" in changes and "github.event.pull_request.changed_files" in changes,
        "pr.yml changes must pass the PR's changed_files to the selector (--expect-files), "
        "so a truncated file list selects everything")
for key in ("tier", "full", "lanes"):
    require(re.search(rf"(?m)^      {key}: \$\{{\{{ steps\.select\.outputs\.{key} \}}\}}$", changes),
            f"pr.yml changes must output {key}")
for job in ("security", "lint", "test"):
    require("lanes: ${{ needs.changes.outputs.lanes }}" in pr.get(job, ""),
            f"pr.yml {job} must pass the selected lanes")
require(re.search(r"(?m)^      shard_profile: pr$", pr.get("test", "")) is not None,
        "pr.yml test must use the PR shard profile")
require("scripts/test.sh --contracts-only" in pr.get("contracts", ""),
        "pr.yml contracts job must run scripts/test.sh --contracts-only")
ci_ok = pr.get("ci-ok", "")
needs = re.search(r"needs: \[([^\]]*)\]", ci_ok)
needs = {n.strip() for n in needs.group(1).split(",")} if needs else set()
require({"changes", "security", "lint", "test", "pr-smoke", "contracts"} <= needs,
        f"ci-ok must need every gating stage incl. contracts, has {sorted(needs)}")
require(not needs & {"memwaste", "test-impact-shadow"},
        "ci-ok must not need the report-only memwaste / shadow jobs")
for needle, why in (("LANES: ${{ needs.changes.outputs.lanes }}", "the selection"),
                    ("JOBS: ", "the run's job list"),
                    ("actions: read", "actions: read to list jobs"),
                    ("jobs?filter=latest", "the latest attempt of every job"),
                    ("scripts/ci/require-all-green.sh", "the canonical gate")):
    require(needle in ci_ok, f"ci-ok must use {why} ({needle})")
shadow = pr.get("test-impact-shadow", "")
require("scripts/ci/test-impact-shadow.sh --binary" in shadow
        and re.search(r"(?m)^    continue-on-error: true$", shadow) is not None,
        "pr.yml test-impact-shadow must exist, run the canonical script and be "
        "continue-on-error (report-only)")
test_wf = jobs(text("_test.yml"))
require('--lanes "$LANES"' in test_wf.get("shard-completeness", ""),
        "shard-completeness must pass the selection to verify-shard-union.sh")

# 4. the test selection: PR-only, fails open.
m = re.search(r"(?ms)^      test_selection:\n(.*?)(?=^      \S|^\S|\Z)", text("_test.yml"))
require(m is not None and re.search(r"(?m)^        default: ''$", m.group(1)) is not None,
        "_test.yml must declare a test_selection input defaulting to '' (every test)")
readers = sorted(job for job, body in test_wf.items() if "inputs.test_selection" in body)
require(readers == ["test-unix", "test-windows"],
        f"only test-unix and test-windows may read test_selection, found {readers}")
for job in ("test-unix", "test-windows"):
    require("CBM_TEST_SELECTION_DIR: ${{ inputs.test_selection != '' && " in test_wf.get(job, ""),
            f"_test.yml {job} must hand the selection to scripts/test.sh as CBM_TEST_SELECTION_DIR "
            f"only when one was passed")
select = pr.get("select-tests", "")
require(re.search(r"(?m)^    if: \$\{\{ vars\.TEST_IMPACT_MODE == 'gate' \}\}$", select) is not None,
        "pr.yml select-tests must run only when vars.TEST_IMPACT_MODE == 'gate'")
require("scripts/ci/test-impact-select.sh" in select and "--gate" in select,
        "pr.yml select-tests must run scripts/ci/test-impact-select.sh --gate")
require("select-tests" not in needs,
        "ci-ok must not need select-tests: a broken selection runs every test, never blocks")
test_job = pr.get("test", "")
require(re.search(r"(?m)^    needs: \[changes, lint, select-tests\]$", test_job) is not None
        and "!cancelled()" in test_job,
        "pr.yml test must need select-tests and still run when it is skipped or failed")
require("test_selection: ${{ needs.select-tests.result == 'success' && "
        "needs.select-tests.outputs.selection == 'narrowed' && 'test-selection' || '' }}" in test_job,
        "pr.yml test must pass a selection only from a successful, narrowed select-tests run")

# 3. every lane is served by a job.
MATRIX = {"unix-x86": "_test.yml", "unix-arm64": "_test.yml", "unix-macos14": "_test.yml",
          "unix-macos-intel": "_test.yml", "windows": "_test.yml", "tsan-x86": "_test.yml",
          "tsan-arm": "_test.yml", "tsan-mac": "_test.yml", "smoke-ubuntu": "pr.yml",
          "smoke-mac": "pr.yml", "smoke-win": "pr.yml"}
SINGLE = {"lint": ("_lint.yml", "lint"), "lint-mem": ("_lint.yml", "lint-mem"),
          "license-gate": ("_security.yml", "license-gate"),
          "codeql-gate": ("_security.yml", "codeql-gate"),
          "pkg-wrappers": ("_test.yml", "test-package-wrappers"),
          "diag": ("_test.yml", "test-diag"), "msan": ("_test.yml", "test-msan"),
          "lsan-macos": ("_test.yml", "test-lsan-macos"),
          "windows-guards": ("_test.yml", "test-windows-guards"),
          "memwaste": ("pr.yml", "memwaste"), "contracts": ("pr.yml", "contracts")}
ALWAYS = {"security-static": ("_security.yml", "security-static"),
          "shard-completeness": ("_test.yml", "shard-completeness")}
for lane in lanes:
    if lane in MATRIX:
        require(f'"lane":"{lane}"' in text(MATRIX[lane]),
                f"lane {lane}: no leg in {MATRIX[lane]} is tagged with it")
    elif lane in SINGLE:
        name, job = SINGLE[lane]
        body = jobs(text(name)).get(job)
        cond = re.search(r"(?m)^    if: (.*)$", body or "")
        require(body is not None and cond is not None and f"'{lane}'" in cond.group(1),
                f"lane {lane}: {name} job {job} must exist and run only when selected")
    elif lane in ALWAYS:
        name, job = ALWAYS[lane]
        body = jobs(text(name)).get(job)
        require(body is not None, f"lane {lane}: {name} job {job} must exist")
    else:
        failures.append(f"lane {lane}: served by no job this contract knows")

if failures:
    print("LANE WIRING CONTRACT VIOLATED:")
    for failure in failures:
        print(f"  {failure}")
    sys.exit(1)
print(f"lane-wiring contract OK ({len(lanes)} lanes served; dry run and release unselected)")
PY
