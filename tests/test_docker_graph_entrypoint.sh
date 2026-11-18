#!/usr/bin/env bash
# Exercise the real graph entrypoint under a Docker-like permissive umask.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/cbm-graph-entrypoint.XXXXXX")"
trap 'rm -rf "$FIXTURE"' EXIT
mkdir -p "$FIXTURE/bin" "$FIXTURE/input/repo"
echo 'def compute(): return 42' > "$FIXTURE/input/repo/sample.py"
python3 - "$ROOT/docker/graph/cbm-graph.sh" "$FIXTURE" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[2])
script = Path(sys.argv[1]).read_text().replace('/data', '"${CBM_ENTRYPOINT_DATA}"').replace('/input', '"${CBM_ENTRYPOINT_INPUT}"')
(root / 'entrypoint.sh').write_text(script)
PY
cat > "$FIXTURE/bin/rsync" <<'SH'
#!/usr/bin/env bash
args=("$@")
src="${args[${#args[@]}-2]}"
dst="${args[${#args[@]}-1]}"
mkdir -p "$dst"
cp -R "${src%/}/." "$dst/"
SH
cat > "$FIXTURE/bin/codebase-memory-mcp-neo4j" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == cli ]]
shift
if [[ "${1:-}" == --json ]]; then raw=1; shift; else raw=0; fi
case "$1" in
index_repository)
    python3 - <<'PY'
import os, stat
if os.name != 'nt':
    assert stat.S_IMODE(os.stat(os.environ['HOME']).st_mode) & 0o077 == 0, 'insecure test HOME'
PY
    if [[ "${CBM_ENTRYPOINT_FAIL:-0}" == 1 ]]; then
        echo 'synthetic indexing failure' >&2
        exit 7
    fi
    [[ "$raw" == 1 ]] || { echo 'machine-readable output was not requested' >&2; exit 8; }
    echo '{"structuredContent":{"status":"indexed","graph_backend":{"status":"serving"}},"isError":false}'
    ;;
index_status) printf 'project: fixture\nnodes: 2\nedges: 1\nstatus: indexed\n' ;;
*) exit 9 ;;
esac
SH
if ! command -v jq >/dev/null; then
    # CI hosts need not install Docker runtime packages. Model only jq's
    # documented JSON-selection interface; the real jq runs in Docker E2E.
    cat > "$FIXTURE/bin/jq" <<'SH'
#!/usr/bin/env python3
import json, sys
assert sys.argv[1:] == ['-er', '.structuredContent.status // empty']
value = json.load(sys.stdin).get('structuredContent', {}).get('status')
if not value:
    sys.exit(1)
print(value)
SH
    chmod +x "$FIXTURE/bin/jq"
fi
chmod +x "$FIXTURE/bin/rsync" "$FIXTURE/bin/codebase-memory-mcp-neo4j"
export PATH="$FIXTURE/bin:$PATH" HOME="$FIXTURE/home" CBM_NAME=fixture NEO4J_PASSWORD=fixture-only
export CBM_ENTRYPOINT_DATA="$FIXTURE/data" CBM_ENTRYPOINT_INPUT="$FIXTURE/input"
unset CBM_NO_COPY CBM_FRESH
if ! (umask 000; bash "$FIXTURE/entrypoint.sh") > "$FIXTURE/pass.log" 2>&1; then
    cat "$FIXTURE/pass.log" >&2
    echo 'FAIL: entrypoint did not create private state and request machine-readable indexing' >&2
    exit 1
fi
if CBM_ENTRYPOINT_FAIL=1 bash "$FIXTURE/entrypoint.sh" > "$FIXTURE/fail.log" 2>&1; then
    echo 'FAIL: entrypoint accepted a failed index' >&2
    exit 1
fi
if ! grep -q 'synthetic indexing failure' "$FIXTURE/fail.log"; then
    cat "$FIXTURE/fail.log" >&2
    echo 'FAIL: entrypoint hid the indexing diagnostic' >&2
    exit 1
fi
echo 'PASS: private Docker HOME, explicit JSON indexing, visible index failures'
