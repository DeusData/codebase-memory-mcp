#!/usr/bin/env bash
# Contract: making a release phase optional must not silently make the phases
# AFTER it optional too, and a draft must require smoke to have actually run.
#
# This exists because it nearly shipped untested binaries. `skip_tests: true`
# skipped `test`, and GitHub propagates "skipped" TRANSITIVELY down the needs
# graph: `build` overrode the condition and ran, but `smoke` and `soak` had no
# override and were skipped. Nothing failed. Nothing said so. `release-draft`
# then ran anyway, because `!cancelled() && !failure()` is fail-OPEN — a skipped
# job is neither cancelled nor failed — so the pipeline was one gate away from
# publishing artifacts nobody had smoke-tested or soaked.
#
# Two properties are pinned:
#   1. every job downstream of an optional phase carries the
#      `!cancelled() && !failure()` override, so it runs when an ancestor was
#      deliberately skipped;
#   2. release-draft requires `needs.smoke.result == 'success'` explicitly, so a
#      skipped smoke BLOCKS the draft instead of sailing past it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WF="$ROOT/.github/workflows/release.yml"
[ -f "$WF" ] || { echo "FAIL: $WF not found" >&2; exit 2; }

python3 - "$WF" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()

# Slice the file into top-level job blocks: two-space indented "name:".
blocks, current, name = {}, [], None
for line in text.splitlines():
    m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
    if m:
        if name:
            blocks[name] = "\n".join(current)
        name, current = m.group(1), []
        continue
    if name is not None:
        current.append(line)
if name:
    blocks[name] = "\n".join(current)

failures = []

def cond(job):
    """The job's `if:` expression, block scalars folded onto one line."""
    body = blocks.get(job, "")
    m = re.search(r"^    if:\s*(>-|>|\|-|\|)?\s*(.*?)(?=^    [a-z_-]+:|\Z)",
                  body, re.S | re.M)
    if not m:
        return ""
    return " ".join(m.group(2).split())

# 1. Downstream-of-optional jobs must tolerate a deliberately skipped ancestor.
#    `test` is the optional phase (if: !inputs.skip_tests); everything after it
#    in the chain has to survive that.
TOLERATE = ["build", "smoke", "soak", "release-draft"]
for job in TOLERATE:
    if job not in blocks:
        failures.append(f"{job}: job missing from release.yml — update this contract")
        continue
    c = cond(job)
    if "!cancelled()" not in c or "!failure()" not in c:
        failures.append(
            f"{job}: `if:` lacks `!cancelled() && !failure()` (got: {c or '<none>'}).\n"
            f"      With skip_tests=true a skipped ancestor SKIPS this job silently.")

# 2. The draft must require smoke to have genuinely succeeded.
draft = cond("release-draft")
if "needs.smoke.result == 'success'" not in draft:
    failures.append(
        "release-draft: `if:` must require needs.smoke.result == 'success'.\n"
        "      `!cancelled() && !failure()` alone is fail-open: a SKIPPED smoke\n"
        "      passes it, and the draft gets cut from unsmoked binaries.")

# soak is allowed to be skipped (soak_level=none) but must be enumerated, not
# ignored — otherwise 'none' and 'silently never ran' are indistinguishable.
if "needs.soak.result" not in draft:
    failures.append(
        "release-draft: `if:` must mention needs.soak.result explicitly so a\n"
        "      legitimately skipped soak (soak_level=none) is distinguishable\n"
        "      from a soak that never ran.")

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    print(f"release gate-chain contract FAILED with {len(failures)} violation(s)",
          file=sys.stderr)
    sys.exit(1)
print("PASS: optional phases cannot silently disable the phases after them")
PY
