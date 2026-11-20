#!/usr/bin/env bash
# test-impact-bundle.sh — fetch the team artifact bundle of one main commit.
#
# The bundle (codebase-memory-mcp test-impact publish; src/mcp/test_impact_artifact.h)
# is published by .github/workflows/test-impact-artifact.yml as the Actions
# artifact `test-impact-<sha>`. A selection may pass --artifact-verified ONLY
# for a bundle this script fetched: it accepts nothing but a SUCCESSFUL run of
# that workflow, in THIS repository, on the main branch, whose head is exactly
# the requested commit — the provenance the receipt itself can never assert.
#
# Usage: scripts/ci/test-impact-bundle.sh --fetch SHA DIR
#          Download the bundle of SHA into DIR (created). Needs GH_TOKEN with
#          actions:read; $GH overrides the gh command, $GITHUB_REPOSITORY the
#          repository (OWNER/NAME).
# Exit: 0 = fetched and verified · 3 = no admissible bundle for SHA ·
#       1 = the tooling broke · 2 = usage error.
set -euo pipefail

GH="${GH:-gh}"
REPO="${GITHUB_REPOSITORY:-DeusData/codebase-memory-mcp}"
WORKFLOW="test-impact-artifact.yml"

usage() { sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; }

[ $# -eq 3 ] && [ "$1" = "--fetch" ] || { usage >&2; exit 2; }
SHA="$2"
DIR="$3"
case "$SHA" in
    *[!0-9a-f]* | "") echo "test-impact-bundle: not a lowercase commit id: $SHA" >&2; exit 2 ;;
esac
[ ${#SHA} -eq 40 ] || [ ${#SHA} -eq 64 ] || { echo "test-impact-bundle: bad id length" >&2; exit 2; }

# The newest successful main-branch run of the producer for exactly this head.
run=$("$GH" api -X GET "repos/$REPO/actions/workflows/$WORKFLOW/runs" \
        -f branch=main -f status=success -f head_sha="$SHA" -f per_page=20 \
        --jq '[.workflow_runs[]
               | select(.head_sha == "'"$SHA"'" and .head_branch == "main"
                        and .repository.full_name == "'"$REPO"'"
                        and .head_repository.full_name == "'"$REPO"'"
                        and (.event == "push" or .event == "schedule"
                             or .event == "workflow_dispatch"))]
              | sort_by(.run_started_at) | last | .id // empty') || exit 1
if [ -z "$run" ]; then
    echo "test-impact-bundle: no successful producer run for $SHA" >&2
    exit 3
fi
mkdir -p "$DIR"
if ! "$GH" run download "$run" --repo "$REPO" --name "test-impact-$SHA" --dir "$DIR"; then
    echo "test-impact-bundle: run $run has no artifact test-impact-$SHA" >&2
    exit 3
fi
# The receipt must name the commit the run was for; the engine checks the rest.
if ! python3 - "$DIR/receipt.json" "$SHA" <<'PY'
import json, sys
receipt = json.load(open(sys.argv[1]))
sys.exit(0 if receipt.get("commit") == sys.argv[2] else 1)
PY
then
    echo "test-impact-bundle: the receipt does not name $SHA" >&2
    exit 3
fi
echo "test-impact-bundle: run $run, bundle of $SHA in $DIR"
