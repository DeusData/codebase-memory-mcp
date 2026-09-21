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
if [[ "${1-} ${2-}" == "cli index_repository" ]]; then
    [[ -z "${CBM_BENCH_REQUEST_LOG-}" ]] || printf '%s\n' "${3-}" >> "$CBM_BENCH_REQUEST_LOG"
    echo "probe: index refused" >&2
    exit 1
fi
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

# The evaluation plan indexes a language and then reads that index from its own
# MCP session (docs/EVALUATION_PLAN.md §7). Asked to keep the runtime, a
# successful run must leave its root behind and record, sourceably, the paths
# that reach it — and they must be the paths the product processes actually
# used. Unasked, the root is gone (asserted above).
KEEP_LOG="$WORKDIR/keep-environment.log"
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_BENCH_ENV_PROBE="$KEEP_LOG" \
CBM_BENCH_KEEP_RUNTIME=1 \
    "$ROOT/scripts/benchmark-index.sh" "$ENV_PROBE" keep "$REPO" "$WORKDIR/results" \
    > "$WORKDIR/keep.out" 2>&1 || true
HANDOFF="$WORKDIR/results/keep/runtime-root.txt"
[[ -s "$HANDOFF" ]] || fail "benchmark-index was asked to keep its runtime but recorded no handoff"
KEPT_ROOT=$(bash -c '. "$1" && printf "%s" "${CBM_BENCH_RUNTIME_ROOT-}"' _ "$HANDOFF")
KEPT_RUNTIME=$(bash -c '. "$1" && printf "%s" "${CBM_RUNTIME_DIR-}"' _ "$HANDOFF")
KEPT_CACHE=$(bash -c '. "$1" && printf "%s" "${CBM_CACHE_DIR-}"' _ "$HANDOFF")
[[ -n "$KEPT_ROOT" && -d "$KEPT_ROOT" && ! -L "$KEPT_ROOT" ]] ||
    fail "benchmark-index did not keep its runtime root: ${KEPT_ROOT:-<none>}"
[[ -d "$KEPT_ROOT/cache" && -d "$KEPT_ROOT/runtime" ]] ||
    fail "the kept root $KEPT_ROOT lost its cache or runtime directory"
grep -qF -- "${KEPT_CACHE}"$'\t'"${KEPT_RUNTIME}" "$KEEP_LOG" ||
    fail "runtime-root.txt does not name the runtime and cache the product processes used"
rm -rf -- "$KEPT_ROOT"

SEARCH_LOG="$WORKDIR/search-environment.log"
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_BENCH_ENV_PROBE="$SEARCH_LOG" \
    "$ROOT/scripts/benchmark-search-graph.sh" "$ENV_PROBE" "$REPO" \
    > "$WORKDIR/search.out" 2>&1 || true
assert_isolated "benchmark-search-graph" "$SEARCH_LOG"

# A refused index must name its cause. The fixture writes one line to stderr and
# exits non-zero; discarding it leaves "did not report a project" as the only
# thing an operator sees.
grep -q -- '--- index/parse stderr ---' "$WORKDIR/search.out" ||
    fail "benchmark-search-graph hid the index stderr behind its own message"
grep -q 'probe: index refused' "$WORKDIR/search.out" ||
    fail "benchmark-search-graph did not surface the cause of the index failure"

# The request carrying the repository path has to be built as JSON. A path may
# legitimately contain a quote or a backslash — hand-built JSON turns that into
# a payload the server cannot parse, or one that means something else. NTFS
# rejects both characters in a path component, so this case is POSIX-only.
case "$(uname -s)" in
MINGW* | MSYS* | CYGWIN*) ;;
*)
    QUIRKY_REPO="$WORKDIR/re\"po\\dir"
    mkdir -p "$QUIRKY_REPO"
    echo 'def bench(): return 1' > "$QUIRKY_REPO/bench.py"
    QUIRKY_RESOLVED=$(cd "$QUIRKY_REPO" && pwd -P)
    REQUEST_LOG="$WORKDIR/requests.log"
    CBM_CACHE_DIR="$CALLER_CACHE" \
    CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
    CBM_BENCH_ENV_PROBE="$WORKDIR/quirky-environment.log" \
    CBM_BENCH_REQUEST_LOG="$REQUEST_LOG" \
        "$ROOT/scripts/benchmark-search-graph.sh" "$ENV_PROBE" "$QUIRKY_REPO" \
        > "$WORKDIR/quirky.out" 2>&1 || true
    python3 - "$REQUEST_LOG" "$QUIRKY_RESOLVED" <<'PY' || fail "benchmark-search-graph built an index request that is not valid JSON for a quoted path"
import json
import sys

request_log, expected = sys.argv[1], sys.argv[2]
try:
    lines = [line for line in open(request_log).read().splitlines() if line.strip()]
except OSError:
    sys.exit("the search benchmark sent no index request")
if not lines:
    sys.exit("the search benchmark sent no index request")
payload = json.loads(lines[0])
if payload.get("repo_path") != expected:
    sys.exit(f"repo_path is {payload.get('repo_path')!r}, expected {expected!r}")
PY
    ;;
esac

echo "PASS: benchmark harnesses isolate their daemon runtime and cache from the caller"
