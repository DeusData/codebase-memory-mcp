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
# the graph itself and admits no coverage: every reached suite runs whole.
#
# Usage: scripts/ci/test-impact-select.sh --binary PATH --out DIR [--base REV]
#          [--platform LABEL] [--gate DIR]
#        Writes DIR/selection-engine.json and DIR/summary-engine.md (appended
#        to $GITHUB_STEP_SUMMARY when set).
# Exit: 0 = selection written · 1 = the tooling broke · 2 = usage error.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="" OUT="" BASE="HEAD^1" PLATFORM="linux-x86_64-clang21" GATE=""
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
trap 'rm -rf "$WORK"' EXIT
export CBM_RUNTIME_DIR="$(mktemp -d /tmp/cbm-rt.XXXXXX)" # sun_path must fit
export CBM_CACHE_DIR="$WORK/cache"
mkdir -p "$WORK/engine" && chmod 700 "$WORK/engine"
base="$(git -C "$ROOT" rev-parse "$BASE")"

artifact=()
bundle_rc=0
"$ROOT/scripts/ci/test-impact-bundle.sh" --fetch "$base" "$WORK/bundle" || bundle_rc=$?
if [ "$bundle_rc" -eq 0 ]; then
    artifact=(--artifact "$WORK/bundle" --artifact-commit "$base" --artifact-verified)
elif [ "$bundle_rc" -ne 3 ]; then
    # No bundle means no coverage: more tests run, never fewer. Say so.
    echo "::warning::test-impact bundle fetch failed (exit $bundle_rc): selecting without coverage"
fi
started=$(date +%s)
"$BINARY" test-impact select --repo "$ROOT" --base "$base" --work "$WORK/engine" \
    --platform "$PLATFORM" ${artifact[@]+"${artifact[@]}"} > "$OUT/selection-engine.json"
elapsed=$(( $(date +%s) - started ))

python3 - "$OUT/selection-engine.json" "$elapsed" "${#artifact[@]}" > "$OUT/summary-engine.md" <<'PY'
import json, sys
answer = json.load(open(sys.argv[1]))
coverage = answer.get("receipt", {}).get("coverage", {})
lines = ["### test-impact engine", "",
         f"- decision: `{answer.get('decision')}` in {sys.argv[2]} s",
         f"- team artifact: {'fetched' if sys.argv[3] != '0' else 'none'}; coverage "
         f"`{coverage.get('state')}` {coverage.get('rejection_reasons') or ''}"]
for lane in answer.get("lanes") or []:
    suites = lane.get("suites") or []
    whole = [s["suite"] for s in suites if s.get("mode") == "whole"]
    narrowed = {s["suite"]: len(s.get("tests") or []) for s in suites if s.get("mode") == "tests"}
    lines.append(f"- lane `{lane.get('lane')}` ({lane.get('decision')}): {len(whole)} whole, "
                 f"{len(narrowed)} narrowed ({sum(narrowed.values())} tests)")
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
