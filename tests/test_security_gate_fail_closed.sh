#!/usr/bin/env bash
# The code-scanning gate must FAIL CLOSED.
#
# History. `codeql-gate` in .github/workflows/_security.yml counted open alerts
# with:
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
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


workflow = (root / ".github" / "workflows" / "_security.yml").read_text(encoding="utf-8")

# Isolate the codeql-gate job so a permission granted to some OTHER job cannot
# satisfy this test.
gate = re.search(r"\n  codeql-gate:\n(?P<body>(?:    .*\n|\n)*)", workflow)
require(gate is not None, "_security.yml no longer defines a codeql-gate job")
body = gate.group("body") if gate else ""

# 1. The read has to be possible at all.
require(
    re.search(r"^    permissions:\s*$", body, re.M) is not None,
    "codeql-gate must declare its own permissions block -- inheriting the "
    "workflow default leaves it without security-events access and the alert "
    "API answers 403",
)
require(
    re.search(r"^      security-events:\s*read\s*$", body, re.M) is not None,
    "codeql-gate must request security-events: read, or the code-scanning "
    "alert query 403s and the gate cannot see any alert",
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
    "codeql-gate must fail with a named error when the alert API cannot be read",
)
require(
    re.search(r"alert count was not a number", body) is not None,
    "codeql-gate must reject a non-numeric alert count instead of comparing it",
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

# 4. Every caller must GRANT what the job requests. A reusable workflow cannot
#    request more permission than its caller holds: asking for
#    security-events:read from a caller that grants only contents:read does not
#    downgrade, it fails the entire run with a startup_failure before a single
#    job executes. Checking only the called workflow is how this test passed
#    while the PR pipeline never started.
for caller_name in ("pr.yml", "release.yml"):
    caller = (root / ".github" / "workflows" / caller_name).read_text(encoding="utf-8")
    # The next sibling may be a comment line, not a key, so the lookahead has to
    # accept any 2-space-indented non-space -- matching only `  \w` silently
    # failed to find release.yml's job at all.
    call = re.search(
        r"\n  security:\n(?P<body>(?:    .*\n|\n)*?)(?=\n?  \S|\Z)", caller
    )
    require(call is not None, f"{caller_name} no longer has a security: job")
    call_body = call.group("body") if call else ""
    require(
        "uses: ./.github/workflows/_security.yml" in call_body,
        f"{caller_name}'s security job must call _security.yml",
    )
    require(
        re.search(r"^      security-events:\s*read\s*$", call_body, re.M) is not None,
        f"{caller_name} must grant security-events: read to the security job -- "
        "a called workflow cannot request more than its caller grants, and the "
        "run fails at startup rather than degrading",
    )
    require(
        re.search(r"^      actions:\s*read\s*$", call_body, re.M) is not None,
        f"{caller_name} must grant actions: read to the security job",
    )

if failures:
    print("Security gate fail-closed contract FAILED:")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)

print("Security gate fail-closed contract passed")
PY
