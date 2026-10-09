#!/usr/bin/env bash
# require-all-green.sh — the aggregate gate step: fail unless every needed job
# succeeded (or was legitimately skipped) and every SELECTED lane ran green.
#
# Canonical CI step (called by pr.yml ci-ok with RESULTS = toJSON(needs)).
# Lived inline in workflow YAML until 2026-07-26; the venue-parity contract
# forbids logic in workflow run-blocks. `skipped` counts as OK at the needs
# level because gated jobs (e.g. pr-smoke on docs-only PRs) skip by design — a
# matrix rename can still never silently deadlock a merge, which is this
# gate's purpose.
#
# Lane selection (LANES + JOBS, both set by pr.yml): a PR runs only the lanes
# scripts/ci/select-lanes.sh selected, and there `skipped` is no longer a free
# pass — a selected lane whose jobs were skipped, cancelled, never created or
# are anything but success is a silent gate loss, so it fails. Lanes nobody
# selected may be skipped or absent (but may not fail); memwaste is
# report-only and never gates. The selection itself must be trustworthy: a
# list of known lanes that holds lint or contracts (every tier runs one).
#
# Usage: RESULTS='<json of the needs context>' \
#        [LANES='<selector lanes, JSON list>' JOBS=<file>] \
#        scripts/ci/require-all-green.sh
#   JOBS: this run's jobs as JSON lines {"name","status","conclusion"}
#   (gh api .../actions/runs/<id>/jobs?filter=latest --jq '.jobs[] | ...').
set -euo pipefail

case "${1:-}" in
-h | --help)
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

: "${RESULTS:?set RESULTS to the toJSON(needs) context}"

UNIVERSE=""
if [ -n "${LANES+set}" ]; then
    UNIVERSE=$(bash "$(dirname "$0")/select-lanes.sh" --list-lanes)
fi

printf '%s' "$RESULTS" | python3 -c "
import json, os, re, sys

needs = json.load(sys.stdin)
bad = {k: v['result'] for k, v in needs.items() if v['result'] not in ('success', 'skipped')}
problems = [f'needed job {k}: {v}' for k, v in sorted(bad.items())]

if 'LANES' in os.environ:
    universe = set(sys.argv[1].split())
    lanes = None
    try:
        lanes = json.loads(os.environ['LANES'])
    except ValueError:
        pass
    if not isinstance(lanes, list) or not all(isinstance(l, str) for l in lanes):
        problems.append('LANES is not a JSON list of lane names: ' + repr(os.environ['LANES']))
        lanes = []
    selected = set(lanes)
    if selected - universe:
        problems.append('unknown lane(s) selected: ' + ', '.join(sorted(selected - universe)))
    if not selected & {'lint', 'contracts'}:
        problems.append('selection has neither lint nor contracts -- refusing to trust it')

    # job name -> lane. Matrix jobs carry their lane among the values in
    # parentheses; everything else is named by its (caller /) job id.
    single = {
        'lint / lint': 'lint', 'lint / lint-mem': 'lint-mem',
        'security / security-static': 'security-static',
        'security / license-gate': 'license-gate', 'security / codeql-gate': 'codeql-gate',
        'test / test-diag': 'diag', 'test / test-msan': 'msan',
        'test / test-lsan-macos': 'lsan-macos', 'test / test-windows-guards': 'windows-guards',
        'test / shard-completeness': 'shard-completeness', 'contracts': 'contracts',
    }
    matrix = ('test / test-unix (', 'test / test-windows (', 'test / test-tsan (', 'pr-smoke (')
    def lane_of(name):
        if name in single:
            return single[name]
        if name.startswith('test / test-package-wrappers'):
            return 'pkg-wrappers'
        if name == 'memwaste' or name.startswith('memwaste / '):
            return 'memwaste'
        if name.startswith(matrix) and name.endswith(')'):
            values = name[name.index('(') + 1:-1].split(', ')
            hits = [v for v in values if v in universe]
            return hits[0] if len(hits) == 1 else None
        return None

    jobs_by_lane = {}
    path = os.environ.get('JOBS', '')
    if not path or not os.path.isfile(path):
        problems.append('JOBS (the run\'s job list) is missing: cannot verify the selected lanes')
    else:
        for line in open(path, encoding='utf-8'):
            if line.strip():
                job = json.loads(line)
                lane = lane_of(job.get('name', ''))
                if lane:
                    jobs_by_lane.setdefault(lane, []).append(job)
        for lane in sorted(universe - {'memwaste'}):
            jobs = jobs_by_lane.get(lane, [])
            if lane in selected:
                if not jobs:
                    problems.append(f'selected lane {lane}: no job ran')
                for job in jobs:
                    state = job.get('conclusion') if job.get('status') == 'completed' else job.get('status')
                    if state != 'success':
                        problems.append(f'selected lane {lane}: {job[\"name\"]} is {state}')
            else:
                for job in jobs:
                    if job.get('status') == 'completed' and job.get('conclusion') not in ('success', 'skipped'):
                        problems.append(f'unselected lane {lane}: {job[\"name\"]} is {job[\"conclusion\"]}')
    if not problems:
        print('selected lanes green:', ', '.join(sorted(selected)))

if problems:
    print('CI NOT OK:')
    for p in problems:
        print('  ' + p)
    sys.exit(1)
print('CI OK:', ', '.join(needs))
" "$UNIVERSE"
