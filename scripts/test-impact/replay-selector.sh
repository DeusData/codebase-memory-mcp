#!/usr/bin/env bash
# replay-selector.sh — replay every September 2026 PR push through the lane
# selector (scripts/ci/select-lanes.sh) and hold it to the measured history.
#
# The rule set was chosen by replaying this history offline; this keeps the
# committed selector honest against the same data, as a contract step:
#   1. every push (767) gets exactly the tier recorded in fixtures/2026-09-
#      pushes.tsv. Where that differs from the analysis tier the row must name
#      its documented deviation, and a deviation may only ever raise the tier
#      (the fail-safe direction) -- never quietly select less than was measured;
#   2. every real failure of the month (22, fixtures/2026-09-real-failures.tsv)
#      had its catching lane selected for its push;
#   3. every push runs the static contract steps (the contracts lane or any
#      test leg) -- they police docs, test-infrastructure/ and lint config.
# Pure data, one selector process (--batch over the distinct file sets).
#
# Usage: scripts/test-impact/replay-selector.sh [repo-root]
set -euo pipefail

case "${1:-}" in
-h | --help)
    sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
-*)
    echo "replay-selector.sh: unknown option '$1'. Please consult --help." >&2
    exit 2
    ;;
esac

ROOT="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
FIXTURES="$ROOT/scripts/test-impact/fixtures"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-replay-selector.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

python3 - "$FIXTURES" "$WORK/batch.jsonl" <<'PY'
import json
import sys

fixtures, batch = sys.argv[1:3]


def rows(name):
    with open(f"{fixtures}/{name}", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if line and not line.startswith("#"):
                yield line.split("\t")


with open(batch, "w", encoding="utf-8") as out:
    for fileset, paths in rows("2026-09-filesets.tsv"):
        out.write(json.dumps({"id": fileset, "files": paths.split(" ")}) + "\n")
PY

bash "$ROOT/scripts/ci/select-lanes.sh" --batch "$WORK/batch.jsonl" >"$WORK/decisions.jsonl"

python3 - "$FIXTURES" "$WORK/decisions.jsonl" <<'PY'
import collections
import json
import sys

fixtures, decisions_path = sys.argv[1:3]
TIERS = ["T0a-docs", "T0b-nonproduct", "T1-tests", "T2-extract", "T3-mid",
         "T4-platform", "T5-core"]
DEVIATIONS = {"makefile-by-name"}


def rows(name, width):
    with open(f"{fixtures}/{name}", encoding="utf-8") as fh:
        for number, line in enumerate(fh, 1):
            line = line.rstrip("\r\n")
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) != width:
                sys.exit(f"FAIL: {name}:{number}: expected {width} columns, got {len(cols)}")
            yield cols


decisions = {}
with open(decisions_path, encoding="utf-8") as fh:
    for line in fh:
        row = json.loads(line)
        decisions[row["id"]] = row

failures = []
pushes = {}
matched = deviated = 0
tiers = collections.Counter()
for run_id, pr, sha, fileset, analysis, expected, note in rows("2026-09-pushes.tsv", 7):
    decision = decisions.get(fileset)
    if decision is None:
        failures.append(f"run {run_id}: fileset {fileset} missing")
        continue
    pushes[run_id] = decision
    tiers[decision["tier"]] += 1
    if decision["tier"] != expected:
        failures.append(f"run {run_id} (PR #{pr}, {sha[:10]}): selector tier "
                        f"{decision['tier']}, expected {expected} -- reasons: "
                        f"{'; '.join(decision['reasons'])}")
        continue
    if analysis == expected:
        if note:
            failures.append(f"run {run_id}: note '{note}' on a row with no deviation")
        matched += 1
    elif note not in DEVIATIONS:
        failures.append(f"run {run_id}: tier differs from the analysis ({analysis}) "
                        f"without a documented deviation")
    elif TIERS.index(expected) < TIERS.index(analysis):
        failures.append(f"run {run_id}: deviation '{note}' LOWERS the tier "
                        f"{analysis} -> {expected}")
    else:
        deviated += 1

# The static contract steps police docs, test-infrastructure/ and lint config,
# so every push runs them: in the contracts job or inside any test leg.
CONTRACT_CARRIERS = {"contracts", "unix-x86", "unix-arm64", "unix-macos14",
                     "unix-macos-intel", "windows"}
uncovered = collections.Counter(d["tier"] for d in pushes.values()
                                if not set(d["lanes"]) & CONTRACT_CARRIERS)
for tier, count in sorted(uncovered.items()):
    failures.append(f"{count} {tier} push(es) run no contract step (neither the "
                    f"contracts lane nor a test leg)")

caught = 0
for run_id, attempt, pr, sha, lane, why in rows("2026-09-real-failures.tsv", 6):
    decision = pushes.get(run_id)
    if decision is None:
        failures.append(f"real failure run {run_id}: no such push in the replay")
    elif lane not in decision["lanes"]:
        failures.append(f"real failure run {run_id} (PR #{pr}): lane {lane} NOT selected "
                        f"(tier {decision['tier']}) -- {why}")
    else:
        caught += 1

if failures:
    print("SELECTOR HISTORY REPLAY FAILED:")
    for failure in failures:
        print(f"  {failure}")
    sys.exit(1)
print(f"selector history replay OK: {len(pushes)} pushes, tier as expected for all "
      f"({matched} equal to the analysis, {deviated} documented fail-safe deviations); "
      f"{caught} real failures, every catching lane selected; every push runs the "
      f"contract steps")
print("  pushes per tier: " + ", ".join(f"{t} {tiers[t]}" for t in TIERS))
print(f"  full runs: {sum(1 for d in pushes.values() if d['full'])}")
PY
