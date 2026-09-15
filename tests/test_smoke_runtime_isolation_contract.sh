#!/usr/bin/env bash
set -euo pipefail

# Runtime-isolation contract for the smoke harness (#1696, follow-up to #1691).
#
# scripts/smoke-test.sh is the process that actually starts the product during
# a smoke run, and it retires "the account daemon" seven times through
# `daemon stop`. Its wrappers sandbox HOME/XDG/TMPDIR and CBM_CACHE_DIR, but
# only CBM_RUNTIME_DIR moves the daemon rendezvous (docs/CONFIGURATION.md), so
# without a private runtime every one of those stops lands on the operator's
# live daemon. Drive the harness with an environment-probe fixture and require
# that no product process ever receives the caller's runtime or cache.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

normalize_path() {
    local path=${1%$'\r'}
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$path" 2>/dev/null && return 0
    fi
    printf '%s\n' "${path//\\//}"
}

ENV_PROBE="$WORKDIR/environment-probe"
cat > "$ENV_PROBE" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\n' "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_SMOKE_ENV_PROBE"
case "${1-}" in
    --version) echo "v0.0.0-probe"; exit 0 ;;
    daemon) [[ "${2-}" == status ]] && exit 1; exit 0 ;;
esac
echo '{}'
exit 0
EOF
chmod +x "$ENV_PROBE"

CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_CACHE" "$CALLER_RUNTIME"

# The fixture answers nothing beyond --version, so the smoke fails early; only
# the environment it handed to the product is under test here.
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_SMOKE_ENV_PROBE="$ENV_LOG" \
    "$ROOT/scripts/smoke-test.sh" "$ENV_PROBE" > "$WORKDIR/smoke.out" 2>&1 || true

[[ -s "$ENV_LOG" ]] || fail "smoke-test did not execute the environment-probe fixture"

CALLER_CACHE_NORMALIZED=$(normalize_path "$CALLER_CACHE")
CALLER_RUNTIME_NORMALIZED=$(normalize_path "$CALLER_RUNTIME")
private_root=""
while IFS=$'\t' read -r child_cache_raw child_runtime_raw; do
    child_cache=$(normalize_path "$child_cache_raw")
    child_runtime=$(normalize_path "$child_runtime_raw")
    if [[ -z "$child_runtime" || "$child_runtime" == "$CALLER_RUNTIME_NORMALIZED" ]]; then
        fail "smoke-test exposed the caller CBM_RUNTIME_DIR to a product process"
    fi
    if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
        fail "smoke-test exposed the caller CBM_CACHE_DIR to a product process"
    fi
    if [[ "${child_runtime%/*}" != "${child_cache%/*}" ||
          "${child_runtime##*/}" != "runtime" || "${child_cache##*/}" != "cache" ]]; then
        fail "smoke runtime/cache were not isolated beneath one private root"
    fi
    if [[ -n "$private_root" && "$private_root" != "${child_runtime%/*}" ]]; then
        fail "smoke-test switched private roots mid-run"
    fi
    private_root="${child_runtime%/*}"
done < "$ENV_LOG"

[[ ! -e "$private_root" ]] || fail "smoke-test left its private root behind: $private_root"

echo "PASS: smoke harness isolates its daemon runtime and cache from the caller"
