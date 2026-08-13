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

# The MCP registry validates every package URL it is handed by FETCHING it, and
# a DRAFT release's assets are not publicly readable. publish-final is the job
# that un-drafts. Both jobs used to need only publish-registries, so they raced
# and the registry was told its own .mcpb URL was a 404 (v0.10.3). The registry
# must therefore run AFTER the release is public — while still never gating it.
def needs(job):
    """The job's `needs:` list as a single line."""
    m = re.search(r"^    needs:\s*(.*)$", blocks.get(job, ""), re.M)
    return m.group(1).strip() if m else ""

if "publish-mcp-registry" not in blocks:
    failures.append("publish-mcp-registry: job missing from release.yml — update this contract")
elif "publish-final" not in needs("publish-mcp-registry"):
    failures.append(
        "publish-mcp-registry: must `needs:` publish-final. The registry fetches\n"
        "      the .mcpb URLs it validates, and a draft release's assets 404 —\n"
        "      running it in parallel with the un-draft is a race it loses.")

# ...and the reverse must NEVER hold: a registry outage must not block shipping.
if "publish-final" in blocks and "publish-mcp-registry" in needs("publish-final"):
    failures.append(
        "publish-final: must NOT depend on publish-mcp-registry. The binary\n"
        "      release is the product and the registry entry is metadata; a\n"
        "      registry-preview outage must never hold up a release.")

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    print(f"release gate-chain contract FAILED with {len(failures)} violation(s)",
          file=sys.stderr)
    sys.exit(1)
print("PASS: optional phases cannot silently disable the phases after them")
PY
