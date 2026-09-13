#!/usr/bin/env bash
set -euo pipefail

# Runtime-isolation contract for the benchmark harnesses (#1696, follow-up to
# #1691).
#
# scripts/benchmark-index.sh and scripts/benchmark-search-graph.sh ran the
# product with no runtime or cache of their own: the index landed in the
# operator's live store, every one-shot joined the operator's account daemon,
# and the timings depended on whatever that daemon was doing. Drive both with
# an environment-probe fixture and require that no product process ever
# receives the caller's runtime or cache, and that the index benchmark records
# the setup cost it now pays explicitly.

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
printf '%s\t%s\n' "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_BENCH_ENV_PROBE"
[[ "${1-} ${2-}" == "daemon status" ]] && exit 1
exit 0
EOF
chmod +x "$ENV_PROBE"

CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
REPO="$WORKDIR/repo"
mkdir -p "$CALLER_CACHE" "$CALLER_RUNTIME" "$REPO"
echo 'def bench(): return 1' > "$REPO/bench.py"
CALLER_CACHE_NORMALIZED=$(normalize_path "$CALLER_CACHE")
CALLER_RUNTIME_NORMALIZED=$(normalize_path "$CALLER_RUNTIME")

# The fixture answers nothing, so the search benchmark stops once the index
# reports no project; only the environment handed to the product is under test.
assert_isolated() {
    local harness="$1" env_log="$2" private_root=""
    [[ -s "$env_log" ]] || fail "$harness did not execute the environment-probe fixture"
    while IFS=$'\t' read -r child_cache_raw child_runtime_raw; do
        local child_cache child_runtime
        child_cache=$(normalize_path "$child_cache_raw")
        child_runtime=$(normalize_path "$child_runtime_raw")
        if [[ -z "$child_runtime" || "$child_runtime" == "$CALLER_RUNTIME_NORMALIZED" ]]; then
            fail "$harness exposed the caller CBM_RUNTIME_DIR to a product process"
        fi
        if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
            fail "$harness exposed the caller CBM_CACHE_DIR to a product process"
        fi
        if [[ "${child_runtime%/*}" != "${child_cache%/*}" ||
              "${child_runtime##*/}" != "runtime" || "${child_cache##*/}" != "cache" ]]; then
            fail "$harness runtime/cache were not isolated beneath one private root"
        fi
        if [[ -n "$private_root" && "$private_root" != "${child_runtime%/*}" ]]; then
            fail "$harness switched private roots mid-run"
        fi
        private_root="${child_runtime%/*}"
    done < "$env_log"
    [[ ! -e "$private_root" ]] || fail "$harness left its private root behind: $private_root"
}

INDEX_LOG="$WORKDIR/index-environment.log"
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_BENCH_ENV_PROBE="$INDEX_LOG" \
    "$ROOT/scripts/benchmark-index.sh" "$ENV_PROBE" probe "$REPO" "$WORKDIR/results" \
    > "$WORKDIR/index.out" 2>&1 || true
assert_isolated "benchmark-index" "$INDEX_LOG"
for metric in setup-time total-time index-time; do
    [[ -s "$WORKDIR/results/probe/$metric.txt" ]] ||
        fail "benchmark-index did not record $metric.txt"
done

SEARCH_LOG="$WORKDIR/search-environment.log"
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_BENCH_ENV_PROBE="$SEARCH_LOG" \
    "$ROOT/scripts/benchmark-search-graph.sh" "$ENV_PROBE" "$REPO" \
    > "$WORKDIR/search.out" 2>&1 || true
assert_isolated "benchmark-search-graph" "$SEARCH_LOG"

echo "PASS: benchmark harnesses isolate their daemon runtime and cache from the caller"
