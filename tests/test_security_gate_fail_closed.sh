#!/usr/bin/env bash
# The code-scanning gate must FAIL CLOSED -- and CodeQL must run where it gates.
#
# History. `codeql-gate` in .github/workflows/_security.yml (since moved into
# .github/workflows/codeql.yml as the analyze job's alert check) counted open
# alerts with:
#
#   ALERTS=$(gh api '.../code-scanning/alerts?state=open' --jq 'length' 2>/dev/null || echo "0")
#
# That reads as defensive and is the exact opposite. The job declared no
# `permissions:` block, so it inherited the workflow's `contents: read` and the
# alert API answered 403. `gh` writes the API error BODY to stdout, so `ALERTS`
# became the string
#
#   {"message":"Resource not accessible by integration",...,"status":"403"}0
#
# every `[ ... -gt ... ]` then failed as a non-integer comparison, the `if` took
# its false branch, and the step printed "CodeQL gate passed (0 alerts)" and
# exited 0. Observed live in a GREEN run (34700514102 / job 103571322660), while
# the repository had an open high-severity alert.
#
# A security gate that cannot read its input must stop the build, not wave it
# through. This test pins both halves of that: the permission that makes the
# read possible, and the fail-closed handling for when it still is not.
#
# It also pins where CodeQL runs. The former PR gate polled up to 150 min per
# PR for a separate CodeQL run and starved PR CI of runners; now:
#   - no pull_request trigger and no job that polls for a CodeQL run;
#   - PR CI calls codeql.yml as its own job (needed by ci-ok), one leg per
#     selected codeql-* lane; each leg analyses its language's whole
#     first-party scope (never narrowed by the diff); c-cpp compiles the
#     vendored objects before tracing, then traces the first-party build plus
#     the vendored files the PR changes;
#   - push to main, the dry run and the release run the full scan, and the
#     release's verify needs it;
#   - superseded push and PR runs are cancelled, dispatched ones never.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


workflow_dir = root / ".github" / "workflows"


def text(name: str) -> str:
    return (workflow_dir / name).read_text(encoding="utf-8")


def job_body(source: str, job: str) -> str | None:
    m = re.search(rf"\n  {re.escape(job)}:\n(?P<body>(?:    .*\n|\n)*)", source)
    return m.group("body") if m else None


def code_only(source: str) -> str:
    return "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))


def steps(body: str) -> list[tuple[str, str]]:
    """(name, step text) per `- name:`/`- uses:` step of a job body."""
    found = []
    for m in re.finditer(r"(?ms)^      - (?P<step>.*?)(?=^      - |\Z)", body):
        step = m.group("step")
        name = re.search(r"(?m)^(?:name: |\s+name: )(.*)$", step)
        found.append((name.group(1).strip() if name else step.splitlines()[0], step))
    return found


workflow = text("codeql.yml")
gate = job_body(workflow, "analyze")
require(gate is not None, "codeql.yml no longer defines an analyze job")
body = gate or ""

# 1. The read has to be possible at all.
require(
    re.search(r"^    permissions:\s*$", body, re.M) is not None,
    "analyze must declare its own permissions block -- inheriting the "
    "workflow default leaves it without security-events access and the alert "
    "API answers 403",
)
require(
    re.search(r"^      security-events:\s*(read|write)\s*$", body, re.M) is not None,
    "analyze must hold security-events (write implies read), or the "
    "code-scanning alert query 403s and the gate cannot see any alert",
)

# 2. The failure direction. `|| echo "0"` on the alert query is the specific
#    construct that turned an API error into a clean pass.
require(
    re.search(r"code-scanning/alerts[^\n]*\|\|\s*echo", body) is None,
    "the alert count must not fall back to a literal on error -- an unreadable "
    "alert list is not zero alerts",
)
require(
    "BLOCKED: cannot read code scanning alerts" in body,
    "the alert check must fail with a named error when the alert API cannot be read",
)
require(
    re.search(r"alert count was not a number", body) is not None,
    "the alert check must reject a non-numeric alert count instead of comparing it",
)

# 3. The count is produced in a command substitution, so a bare `exit` inside
#    the helper would end only the subshell and let the caller proceed with an
#    empty value -- the same class of silent pass. The callers must check.
assignments = re.findall(r"^\s*ALERTS2?=\$\(count_open_alerts\)(.*)$", body, re.M)
require(
    len(assignments) >= 2,
    "expected both alert samples to go through the checked helper",
)
require(
    all("|| exit 1" in tail for tail in assignments),
    "every count_open_alerts call must be followed by || exit 1 -- a failure "
    "inside a command substitution does not stop the caller on its own",
)

# 4. Triggers: never per pull request (PR CI calls the workflow instead), on
#    push to main, callable by PR CI, the dry run and the release.
on = re.search(r"(?ms)^on:\n(?P<body>.*?)(?=^\S)", workflow)
triggers = set(re.findall(r"(?m)^  ([a-z_]+):", on.group("body"))) if on else set()
require("pull_request" not in triggers and "pull_request_target" not in triggers,
        "codeql.yml must not trigger on pull requests (pr.yml calls it as a job)")
require({"push", "workflow_call"} <= triggers,
        f"codeql.yml must run on push to main and be callable, has {sorted(triggers)}")
require(re.search(r"(?m)^  push:\n    branches: \[main\]$", workflow) is not None,
        "codeql.yml's push trigger must be main")

# 5. Nothing waits for a separate CodeQL run, and _security.yml has no CodeQL.
for path in sorted(workflow_dir.glob("*.yml")):
    require("workflows/codeql.yml/runs" not in code_only(path.read_text(encoding="utf-8")),
            f"{path.name} polls for a CodeQL run -- CodeQL runs as a job of the caller")
require("codeql" not in code_only(text("_security.yml")).lower(),
        "_security.yml must not run or wait for CodeQL")

# 6. Superseded runs are cancelled (push to main, a PR), dispatched never;
#    legs of one run never cancel each other.
concurrency = re.search(r"(?ms)^    concurrency:\n(.*?)(?=^    \S)", body)
conc = concurrency.group(1) if concurrency else ""
require("matrix.language" in conc, "the concurrency group must be per language")
require("github.event_name == 'push' && github.ref" in conc,
        "push runs must share a ref-scoped group (only the newest main commit is scanned)")
require("github.event.pull_request.number" in conc,
        "PR scans must share a per-PR group")
require("cancel-in-progress: ${{ github.event_name == 'push' || github.event_name == 'pull_request' }}" in conc,
        "only push and PR scans may be cancelled -- a dry run or release tests one commit")

# 7. The PR scan: analysis never narrowed by the diff; c-cpp prebuilds the
#    vendored objects untraced and traces first-party + changed vendored.
names = [name for name, _ in steps(body)]
by_name = dict(steps(body))


def index(fragment: str) -> int:
    hits = [i for i, n in enumerate(names) if fragment in n]
    return hits[0] if hits else -1


init, analyze = index("Initialize CodeQL"), index("Perform CodeQL Analysis")
prebuild, retrace = index("Build third-party objects"), index("Vendored files this PR changes")
light_build, full_build = index("PR scan, first-party"), index("Build for CodeQL analysis (full)")
require(min(init, analyze, prebuild, retrace, light_build, full_build) >= 0,
        f"codeql.yml analyze lost a step, has {names}")
require(-1 < prebuild < init and -1 < retrace < init,
        "the vendored prebuild and the retrace list must run BEFORE Initialize CodeQL (untraced)")
require(init < light_build < analyze and init < full_build < analyze,
        "the traced builds must run between Initialize CodeQL and the analysis")
for name in names:
    if "Initialize CodeQL" in name or "Perform CodeQL Analysis" in name or "open code scanning alerts" in name:
        require(not re.search(r"(?m)^\s+if:", by_name[name]),
                f"step '{name}' must never be skipped -- a selected leg always analyses")
light_step = by_name.get(names[light_build], "") if light_build >= 0 else ""
require("scripts/build.sh --reuse-vendored --retrace-from" in light_step,
        "the PR c-cpp build must reuse the untraced vendored objects and retrace the changed ones")
require("scripts/ci/codeql-retrace-vendored.sh" in (by_name.get(names[retrace], "") if retrace >= 0 else ""),
        "the retrace list must come from scripts/ci/codeql-retrace-vendored.sh")
require("scripts/build.sh --vendored-only" in (by_name.get(names[prebuild], "") if prebuild >= 0 else ""),
        "the PR c-cpp prebuild must be scripts/build.sh --vendored-only")
require(re.search(r"(?m)^\s+run: scripts/build\.sh\s*$", by_name.get(names[full_build], "") if full_build >= 0 else "") is not None,
        "the full c-cpp scan must trace the whole production build (vendored C included)")
require(re.search(r'(?m)^      CBM_NO_CCACHE: "1"$', body) is not None,
        "the CodeQL job must disable the compiler cache -- a cache hit compiles nothing to trace")
require("config-file: ./.github/codeql/codeql-config.yml" in body,
        "every scan must use the first-party scope in .github/codeql/codeql-config.yml")

# 8. One configuration per language: the full matrix and PR CI's legs agree.
default = re.search(r"fromJSON\(inputs\.matrix \|\| '(?P<json>\{.*?\})'\)", body)
pr = text("pr.yml")
pr_legs = re.search(r"CODEQL='(?P<json>\[.*?\])'", pr)
full_legs = json.loads(default.group("json"))["include"] if default else []
require({leg.get("language") for leg in full_legs} ==
        {"c-cpp", "javascript-typescript", "python", "actions"},
        "the full scan must cover c-cpp, javascript-typescript, python and actions")
require(pr_legs is not None and json.loads(pr_legs.group("json")) == full_legs,
        "pr.yml's CodeQL legs must equal codeql.yml's full matrix (one configuration per language)")

# 9. Every caller GRANTS what codeql.yml requests (a called workflow cannot
#    request more than its caller holds: startup failure, not a downgrade).
#    The caller list is DERIVED, never hardcoded: an earlier revision listed
#    callers by hand and stayed green while a third caller nobody had listed
#    failed every run at startup.
callers = sorted(
    path.name
    for path in workflow_dir.glob("*.yml")
    if "uses: ./.github/workflows/codeql.yml" in path.read_text(encoding="utf-8")
)
require({"pr.yml", "dry-run.yml", "release.yml"} <= set(callers),
        "PR CI, the dry run and the release must call codeql.yml, callers: "
        + (", ".join(callers) or "none"))
for caller_name in callers:
    call_body = job_body(text(caller_name), "codeql") or ""
    require("uses: ./.github/workflows/codeql.yml" in call_body,
            f"{caller_name} must call codeql.yml from a job named codeql")
    for perm, level in (("security-events", "write"), ("actions", "read"), ("contents", "read")):
        require(re.search(rf"^      {perm}:\s*{level}\s*$", call_body, re.M) is not None,
                f"{caller_name} must grant {perm}: {level} to the codeql job")
    light = re.search(r"(?m)^      light: true$", call_body) is not None
    require(light == (caller_name == "pr.yml"),
            f"{caller_name}: only PR CI runs the PR scan (light: true); "
            "the dry run and the release run the full scan")

# 10. PR CI: the codeql job runs for any selected codeql-* lane and ci-ok
#     needs it; the release publishes nothing without the CodeQL pass.
pr_codeql = job_body(pr, "codeql") or ""
cond = re.search(r"(?m)^    if: (.*)$", pr_codeql)
for lane in ("codeql-cpp", "codeql-js", "codeql-py", "codeql-actions"):
    require(cond is not None and f"'{lane}'" in cond.group(1),
            f"pr.yml codeql must run when lane {lane} is selected")
require("matrix: ${{ needs.changes.outputs.codeql }}" in pr_codeql,
        "pr.yml codeql must run the legs the changes job selected")
ci_ok = job_body(pr, "ci-ok") or ""
require(re.search(r"(?m)^    needs: \[[^\]]*\bcodeql\b[^\]]*\]$", ci_ok) is not None,
        "ci-ok must need the codeql job")
verify = job_body(text("release.yml"), "verify") or ""
require(re.search(r"(?m)^    needs: \[[^\]]*\bcodeql\b[^\]]*\]$", verify) is not None,
        "release.yml verify must need the codeql job")
require("needs.codeql.result == 'success'" in verify,
        "release.yml verify must require needs.codeql.result == 'success'")

if failures:
    print("Security gate fail-closed contract FAILED:")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)

print("Security gate fail-closed contract passed")
PY
