#!/usr/bin/env bash
set -euo pipefail

# Regression tests for scripts/security-fuzz.sh itself.  A fuzz case is green
# only after the target proves it consumed the adversarial request; merely
# surviving initialization and then exiting cleanly at EOF is insufficient.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

INIT_ONLY="$WORKDIR/init-only-mcp"
cat > "$INIT_ONLY" <<'EOF'
#!/usr/bin/env bash
IFS= read -r _initialize || exit 0
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}'
exit 0
EOF
chmod +x "$INIT_ONLY"

if "$ROOT/scripts/security-fuzz.sh" "$INIT_ONLY" \
    > "$WORKDIR/init-only.out" 2>&1; then
    echo "FAIL: security-fuzz accepted a process that never consumed the adversarial payload"
    exit 1
fi

ECHO_ONLY="$WORKDIR/echo-only-mcp"
cat > "$ECHO_ONLY" <<'EOF'
#!/usr/bin/env bash
# Seeing the acknowledgement id in echoed input is not proof that the request
# reached JSON-RPC dispatch; no response object is ever produced here.
while IFS= read -r line; do
    printf '%s\n' "$line"
done
EOF
chmod +x "$ECHO_ONLY"

if "$ROOT/scripts/security-fuzz.sh" "$ECHO_ONLY" \
    > "$WORKDIR/echo-only.out" 2>&1; then
    echo "FAIL: security-fuzz accepted an echoed request as an acknowledgement"
    exit 1
fi

# One python3 process answers the requests; the harness needs python3 for its
# interactive case anyway.  A shell `read` of the 1 MB request costs seconds
# where the shell runs emulated, and the harness's 10-second hang limit then
# decides this test instead of its assertions.
ENV_RESPONDER="$WORKDIR/environment-probe-responder.py"
cat > "$ENV_RESPONDER" <<'EOF'
import re
import sys

# Echo a JSON-RPC result for every request with a numeric id.  This keeps the
# fixture compatible with both the current fixed ids and a future per-case
# acknowledgement id without depending on the malformed payload itself: the
# last id of a line is the one that counts.
REQUEST_ID = re.compile(rb'"id"\s*:\s*([0-9]+)')

for line in sys.stdin.buffer:
    ids = REQUEST_ID.findall(line)
    if not ids:
        continue
    result = b'{"isError":true}' if b'"name":"index_repository"' in line else b'{}'
    sys.stdout.buffer.write(b'{"jsonrpc":"2.0","id":' + ids[-1] + b',"result":' + result + b'}\n')
    sys.stdout.buffer.flush()
EOF

ENV_PROBE="$WORKDIR/environment-probe-mcp"
cat > "$ENV_PROBE" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\t%s\n' "${HOME-}" "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_FUZZ_ENV_PROBE"
exec python3 "${BASH_SOURCE[0]%/*}/environment-probe-responder.py"
EOF
chmod +x "$ENV_PROBE"

CALLER_HOME="$WORKDIR/caller-home"
CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_HOME" "$CALLER_CACHE" "$CALLER_RUNTIME"

if ! HOME="$CALLER_HOME" \
    CBM_CACHE_DIR="$CALLER_CACHE" \
    CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
    CBM_FUZZ_ENV_PROBE="$ENV_LOG" \
    "$ROOT/scripts/security-fuzz.sh" "$ENV_PROBE" \
    > "$WORKDIR/environment.out" 2>&1; then
    echo "FAIL: environment-probe fixture was rejected"
    cat "$WORKDIR/environment.out"
    exit 1
fi

if [[ ! -s "$ENV_LOG" ]]; then
    echo "FAIL: security-fuzz did not execute the environment-probe fixture"
    exit 1
fi

normalize_path() {
    local path=${1%$'\r'}
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$path" 2>/dev/null && return 0
    fi
    printf '%s\n' "${path//\\//}"
}

CALLER_HOME_NORMALIZED=$(normalize_path "$CALLER_HOME")
CALLER_CACHE_NORMALIZED=$(normalize_path "$CALLER_CACHE")
CALLER_RUNTIME_NORMALIZED=$(normalize_path "$CALLER_RUNTIME")

while IFS=$'\t' read -r child_home_raw child_cache_raw child_runtime_raw; do
    child_home=$(normalize_path "$child_home_raw")
    child_cache=$(normalize_path "$child_cache_raw")
    child_runtime=$(normalize_path "$child_runtime_raw")
    if [[ -z "$child_home" || "$child_home" == "$CALLER_HOME_NORMALIZED" ]]; then
        echo "FAIL: security-fuzz exposed the caller HOME to a fuzz target"
        exit 1
    fi
    if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
        echo "FAIL: security-fuzz exposed the caller CBM_CACHE_DIR to a fuzz target"
        exit 1
    fi
    if [[ -z "$child_runtime" || "$child_runtime" == "$CALLER_RUNTIME_NORMALIZED" ]]; then
        echo "FAIL: security-fuzz exposed the caller CBM_RUNTIME_DIR to a fuzz target"
        exit 1
    fi
    child_home_parent=${child_home%/*}
    child_cache_parent=${child_cache%/*}
    child_runtime_parent=${child_runtime%/*}
    if [[ "$child_home_parent" != "$child_cache_parent" ||
          "$child_home_parent" != "$child_runtime_parent" ||
          "${child_home##*/}" != "home" ||
          "${child_cache##*/}" != "cache" ||
          "${child_runtime##*/}" != "runtime" ]]; then
        echo "FAIL: fuzz HOME/cache/runtime were not isolated beneath one harness temp directory"
        exit 1
    fi
done < "$ENV_LOG"

echo "PASS: security fuzz harness requires payload progress and isolates HOME/cache/runtime"
