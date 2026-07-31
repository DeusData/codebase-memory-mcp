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

ENV_PROBE="$WORKDIR/environment-probe-mcp"
cat > "$ENV_PROBE" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\n' "${HOME-}" "${CBM_CACHE_DIR-}" >> "$CBM_FUZZ_ENV_PROBE"

# Echo a JSON-RPC result for every request with a numeric id.  This keeps the
# fixture compatible with both the current fixed ids and a future per-case
# acknowledgement id without depending on the malformed payload itself.
while IFS= read -r line; do
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p')
    if [[ -n "$id" ]]; then
        if [[ "$line" == *'"name":"index_repository"'* ]]; then
            printf '{"jsonrpc":"2.0","id":%s,"result":{"isError":true}}\n' "$id"
        else
            printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
        fi
    fi
done
EOF
chmod +x "$ENV_PROBE"

CALLER_HOME="$WORKDIR/caller-home"
CALLER_CACHE="$WORKDIR/caller-cache"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_HOME" "$CALLER_CACHE"

if ! HOME="$CALLER_HOME" \
    CBM_CACHE_DIR="$CALLER_CACHE" \
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

while IFS=$'\t' read -r child_home_raw child_cache_raw; do
    child_home=$(normalize_path "$child_home_raw")
    child_cache=$(normalize_path "$child_cache_raw")
    if [[ -z "$child_home" || "$child_home" == "$CALLER_HOME_NORMALIZED" ]]; then
        echo "FAIL: security-fuzz exposed the caller HOME to a fuzz target"
        exit 1
    fi
    if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
        echo "FAIL: security-fuzz exposed the caller CBM_CACHE_DIR to a fuzz target"
        exit 1
    fi
    child_home_parent=${child_home%/*}
    child_cache_parent=${child_cache%/*}
    if [[ "$child_home_parent" != "$child_cache_parent" ||
          "${child_home##*/}" != "home" ||
          "${child_cache##*/}" != "cache" ]]; then
        echo "FAIL: fuzz HOME/cache were not isolated beneath one harness temp directory"
        exit 1
    fi
done < "$ENV_LOG"

echo "PASS: security fuzz harness requires payload progress and isolates HOME/cache"
