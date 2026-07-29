#!/usr/bin/env bash
# require-all-green.sh — the aggregate gate step: fail unless every needed job
# succeeded (or was legitimately skipped).
#
# Canonical CI step (called by pr.yml ci-ok with RESULTS = toJSON(needs)).
# Lived inline in workflow YAML until 2026-07-26; the venue-parity contract
# forbids logic in workflow run-blocks. `skipped` counts as OK because gated
# jobs (e.g. pr-smoke on docs-only PRs) skip by design — a matrix rename can
# still never silently deadlock a merge, which is this gate's purpose.
#
# Usage: RESULTS='<json of the needs context>' scripts/ci/require-all-green.sh
set -euo pipefail

case "${1:-}" in
-h | --help)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

: "${RESULTS:?set RESULTS to the toJSON(needs) context}"

printf '%s' "$RESULTS" | python3 -c "
import json, sys
needs = json.load(sys.stdin)
bad = {k: v['result'] for k, v in needs.items() if v['result'] not in ('success', 'skipped')}
if bad:
    print('CI NOT OK:', bad)
    sys.exit(1)
print('CI OK:', ', '.join(needs))
"
