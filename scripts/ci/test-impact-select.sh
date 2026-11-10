#!/usr/bin/env bash
# test-impact-select.sh — the PR selection with the team artifact: what
# `codebase-memory-mcp test-impact select` runs for this change.
#
# Shadow (default): REPORT-ONLY; pr.yml's test-impact-shadow job runs it
# continue-on-error and ci-ok does not need it.
# --gate DIR (pr.yml's select-tests job, only when the repository variable
# TEST_IMPACT_MODE is `gate`): also turns the answer into what the test legs
# run (scripts/test-impact/selection.py) — DIR/test-only.txt and
# DIR/optional-suites.txt for a narrowed selection — and writes
# `selection=narrowed` or `selection=all` to $GITHUB_OUTPUT. Anything but a
# well-formed narrowed answer, a lane that selects nothing included, is `all`.
#
# The base is the base side of the PR merge commit (HEAD^1 by default). Its
# bundle is fetched with scripts/ci/test-impact-bundle.sh, which only accepts
# the main-branch producer's own run for exactly that commit; only then does
# the selection pass --artifact-verified. Without a bundle the engine builds
# the graph itself and admits no coverage: every suite runs whole (the lane
# then reports run_all; the summary shows why each suite runs whole).
#
# Usage: scripts/ci/test-impact-select.sh --binary PATH --out DIR [--base REV]
#          [--platform LABEL] [--gate DIR]
#        Writes DIR/selection-engine.json and DIR/summary-engine.md (appended
#        to $GITHUB_STEP_SUMMARY when set).
# Exit: 0 = selection written · 1 = the tooling broke · 2 = usage error.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="" OUT="" BASE="HEAD^1" PLATFORM="linux-x86_64-clang21" GATE=""
ANCESTOR_MAX=20 # first-parent commits before the merge base whose bundle may stand in
while [ $# -gt 0 ]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --base) BASE="$2"; shift 2 ;;
        --platform) PLATFORM="$2"; shift 2 ;;
        --gate) GATE="$2"; shift 2 ;;
        *) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done
[ -n "$BINARY" ] && [ -n "$OUT" ] || exit 2

mkdir -p "$OUT"
WORK="$(mktemp -d)"
CBM_RUNTIME_DIR="$(mktemp -d /tmp/cbm-rt.XXXXXX)" # sun_path must fit
export CBM_RUNTIME_DIR
trap 'rm -rf "$WORK" "$CBM_RUNTIME_DIR"' EXIT
export CBM_CACHE_DIR="$WORK/cache"
mkdir -p "$WORK/engine" && chmod 700 "$WORK/engine"
base="$(git -C "$ROOT" rev-parse "$BASE")"

artifact=()
bundle_rc=0
select_base="$base" # the commit the selection diffs against: the bundle's own
ancestor_note=""
"$ROOT/scripts/ci/test-impact-bundle.sh" --fetch "$base" "$WORK/bundle" || bundle_rc=$?
if [ "$bundle_rc" -eq 3 ]; then
    # The producer skips main commits in merge bursts (GitHub keeps one pending
    # run per concurrency group). Use the nearest first-parent ancestor A with
    # a bundle instead, and diff against A: A..HEAD carries main's own commits
    # since A as well as the PR, so A's coverage map stays exact for its base.
    for candidate in $(git -C "$ROOT" rev-list --first-parent --max-count="$((ANCESTOR_MAX + 1))" "$base" | tail -n +2); do
        rm -rf "$WORK/bundle"
        bundle_rc=0
        "$ROOT/scripts/ci/test-impact-bundle.sh" --fetch "$candidate" "$WORK/bundle" || bundle_rc=$?
        if [ "$bundle_rc" -ne 3 ]; then
            [ "$bundle_rc" -eq 0 ] && select_base="$candidate"
            break
        fi
    done
    if [ "$select_base" != "$base" ]; then
        # Exact-revert guard: a file main changed after A that the PR changes
        # back drops out of A..HEAD. Then A's map cannot speak for it: no coverage.
        # A diff that fails proves nothing either: no coverage.
        if git -C "$ROOT" diff --no-renames --name-only "$base" HEAD > "$WORK/pr-files.txt" &&
            git -C "$ROOT" diff --no-renames --name-only "$select_base" HEAD > "$WORK/ancestor-files.txt"; then
            missing=$(comm -23 <(sort -u "$WORK/pr-files.txt") <(sort -u "$WORK/ancestor-files.txt"))
        else
            missing="(git diff failed)"
        fi
        distance=$(git -C "$ROOT" rev-list --first-parent --count "$select_base..$base")
        if [ -n "$missing" ]; then
            echo "::warning::test-impact: ancestor bundle $select_base unusable (the change reverts main's own change in: $(echo "$missing" | head -3 | tr '\n' ' ')): selecting without coverage"
            ancestor_note="ancestor $select_base rejected by the revert guard"
            select_base="$base"
            bundle_rc=3
        else
            ancestor_note="ancestor $select_base, $distance first-parent commits before the merge base"
        fi
    fi
fi
if [ "$bundle_rc" -eq 0 ]; then
    artifact=(--artifact "$WORK/bundle" --artifact-commit "$select_base" --artifact-verified)
elif [ "$bundle_rc" -ne 3 ]; then
    # No bundle means no coverage: more tests run, never fewer. Say so.
    echo "::warning::test-impact bundle fetch failed (exit $bundle_rc): selecting without coverage"
fi
started=$(date +%s)
rm -f "$OUT/selection-vetoed.json"
select_rc=0
"$BINARY" test-impact select --repo "$ROOT" --base "$select_base" --work "$WORK/engine" \
    --platform "$PLATFORM" ${artifact[@]+"${artifact[@]}"} \
    --vetoed-out "$OUT/selection-vetoed.json" > "$OUT/selection-engine.json" \
    2> "$WORK/select-stderr.txt" || select_rc=$?
cat "$WORK/select-stderr.txt" >&2 # the engine's diagnostic stays in the log too
[ "$select_rc" -eq 0 ] || exit 1
elapsed=$(( $(date +%s) - started ))

python3 - "$OUT/selection-engine.json" "$elapsed" "${#artifact[@]}" "$OUT/selection-vetoed.json" \
    "$ancestor_note" "$WORK/select-stderr.txt" > "$OUT/summary-engine.md" <<'PY'
import collections, json, os, sys
answer = json.load(open(sys.argv[1]))
coverage = answer.get("receipt", {}).get("coverage", {})


def lane_lines(doc, prefix):
    out = []
    for lane in doc.get("lanes") or []:
        suites = lane.get("suites") or []
        whole = [s for s in suites if s.get("mode") == "whole"]
        narrowed = {s["suite"]: len(s.get("tests") or []) for s in suites if s.get("mode") == "tests"}
        why = collections.Counter(r for s in whole for r in (s.get("reasons") or []))
        hist = ", ".join(f"{name} {n}" for name, n in why.most_common())
        because = (lane.get("because") or {}).get("reasons") or []
        out.append(f"{prefix}lane `{lane.get('lane')}` ({lane.get('decision')}): {len(whole)} whole"
                   f"{f' [{hist}]' if hist else ''}, {len(narrowed)} narrowed "
                   f"({sum(narrowed.values())} tests)"
                   f"{f'; lane reasons {because}' if lane.get('decision') == 'run_all' and because else ''}")
    return out


run_all_reasons = answer.get("run_all_reasons") or []
run_all_rules = answer.get("run_all_rules") or []
reasons_text = f"; run-all reasons {run_all_reasons}" if run_all_reasons else ""
if run_all_rules:
    reasons_text += f"; decided by rule(s) {run_all_rules}"
lines = ["### test-impact engine", "",
         f"- decision: `{answer.get('decision')}` in {sys.argv[2]} s{reasons_text}",
         f"- team artifact: {'fetched' if sys.argv[3] != '0' else 'none'}"
         f"{f' ({sys.argv[5]})' if sys.argv[5] else ''}; coverage "
         f"`{coverage.get('state')}` {coverage.get('rejection_reasons') or ''}"]
note = open(sys.argv[6]).read().strip() if os.path.exists(sys.argv[6]) else ""
if note:
    lines.append(f"- engine note: `{note.splitlines()[0][:300]}`")
lines += lane_lines(answer, "- ")
if os.path.exists(sys.argv[4]):
    lines.append("- the runner-main veto (RUNNER_REACHED) discarded this selection:")
    lines += lane_lines(json.load(open(sys.argv[4])), "  - ")
print("\n".join(lines))
PY

if [ -n "$GATE" ]; then
    mkdir -p "$GATE"
    selection=all
    filter_rc=0
    python3 "$ROOT/scripts/test-impact/selection.py" filter "$OUT/selection-engine.json" \
        --lane unit --out "$GATE/test-only.txt" --optional-out "$GATE/optional-suites.txt" ||
        filter_rc=$?
    if [ "$filter_rc" -eq 0 ]; then
        selection=narrowed
        echo "- PR CI runs $(wc -l < "$GATE/test-only.txt" | tr -d ' ') selection tokens" \
            >> "$OUT/summary-engine.md"
    else
        rm -f "$GATE/test-only.txt" "$GATE/optional-suites.txt"
        echo "- PR CI runs every test (selection exit $filter_rc)" >> "$OUT/summary-engine.md"
    fi
    if [ -n "${GITHUB_OUTPUT:-}" ]; then
        echo "selection=$selection" >> "$GITHUB_OUTPUT"
    fi
fi
cat "$OUT/summary-engine.md"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat "$OUT/summary-engine.md" >> "$GITHUB_STEP_SUMMARY"
fi
