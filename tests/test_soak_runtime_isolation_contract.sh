#!/usr/bin/env bash
set -euo pipefail

# Runtime-isolation contract for the soak harness (#1696, follow-up to #1691).
#
# scripts/soak-test.sh must be the only client of the daemon it measures: it
# asserts that a session crash and the final shutdown each stop "the daemon".
# It used to claim isolation from interactive sessions through a private
# CBM_CACHE_DIR, but only CBM_RUNTIME_DIR moves the daemon rendezvous
# (docs/CONFIGURATION.md), so the soak shared the operator's account daemon
# and either stopped it or was refused with a cache-root conflict. Drive the
# harness with an environment-probe fixture and require that no product
# process ever receives the caller's runtime or cache.

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
printf '%s\t%s\n' "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_SOAK_ENV_PROBE"
[[ "${1-} ${2-}" == "daemon status" ]] && exit 1
exit 0
EOF
chmod +x "$ENV_PROBE"

CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_CACHE" "$CALLER_RUNTIME"

# The fixture exits as soon as it has recorded its environment, so the soak
# fails at "server did not start"; only the environment it handed to the
# product is under test here. RESULTS_DIR keeps the soak's metrics out of cwd.
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_SOAK_ENV_PROBE="$ENV_LOG" \
RESULTS_DIR="$WORKDIR/results" \
    "$ROOT/scripts/soak-test.sh" "$ENV_PROBE" 1 --skip-crash-test \
    > "$WORKDIR/soak.out" 2>&1 || true

[[ -s "$ENV_LOG" ]] || fail "soak-test did not execute the environment-probe fixture"

CALLER_CACHE_NORMALIZED=$(normalize_path "$CALLER_CACHE")
CALLER_RUNTIME_NORMALIZED=$(normalize_path "$CALLER_RUNTIME")
private_root=""
while IFS=$'\t' read -r child_cache_raw child_runtime_raw; do
    child_cache=$(normalize_path "$child_cache_raw")
    child_runtime=$(normalize_path "$child_runtime_raw")
    if [[ -z "$child_runtime" || "$child_runtime" == "$CALLER_RUNTIME_NORMALIZED" ]]; then
        fail "soak-test exposed the caller CBM_RUNTIME_DIR to a product process"
    fi
    if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
        fail "soak-test exposed the caller CBM_CACHE_DIR to a product process"
    fi
    if [[ "${child_runtime%/*}" != "${child_cache%/*}" ||
          "${child_runtime##*/}" != "runtime" || "${child_cache##*/}" != "cache" ]]; then
        fail "soak runtime/cache were not isolated beneath one private root"
    fi
    if [[ -n "$private_root" && "$private_root" != "${child_runtime%/*}" ]]; then
        fail "soak-test switched private roots mid-run"
    fi
    private_root="${child_runtime%/*}"
done < "$ENV_LOG"

[[ ! -e "$private_root" ]] || fail "soak-test left its private root behind: $private_root"

echo "PASS: soak harness isolates its daemon runtime and cache from the caller"
