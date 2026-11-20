#!/usr/bin/env bash
set -euo pipefail

# Runtime-isolation contract for the memlab harness (#1696, follow-up to #1691).
#
# scripts/memlab.sh starts the product over stdio to attribute retained memory
# and then removes its work directory. It used to give the run a private
# CBM_CACHE_DIR only, but only CBM_RUNTIME_DIR moves the daemon rendezvous
# (docs/CONFIGURATION.md), so the profiled process joined the operator's
# account daemon — refused with a cache-root conflict when one was live, or
# left as the account daemon with its cache deleted underneath it otherwise.
#
# memlab launches the product through its Python driver, and the driver hands
# the product the environment it inherited (memlab-drive.py passes no env= to
# Popen). Recording the environment at the driver boundary therefore observes
# exactly what the product receives, on every host — including Windows, whose
# native Python cannot exec a shell fixture — so a python3 shim stands in for
# the driver and no product process is started at all.

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

if grep -q 'env=' "$ROOT/scripts/memlab-drive.py"; then
    fail "memlab-drive.py no longer passes the harness environment through unchanged"
fi

SHIM_DIR="$WORKDIR/bin"
mkdir -p "$SHIM_DIR"
cat > "$SHIM_DIR/python3" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\n' "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_MEMLAB_ENV_PROBE"
echo "served=0 failed=0"
exit 1
EOF
DUMMY_BINARY="$WORKDIR/dummy-binary"
printf '#!/usr/bin/env bash\nexit 0\n' > "$DUMMY_BINARY"
chmod +x "$SHIM_DIR/python3" "$DUMMY_BINARY"

CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_CACHE" "$CALLER_RUNTIME" "$WORKDIR/cwd"

# memlab writes its profile and log into $PWD, hence the cwd change. It is
# tracked without an executable bit, so run it through bash.
(
    cd "$WORKDIR/cwd"
    PATH="$SHIM_DIR:$PATH" \
    CBM_CACHE_DIR="$CALLER_CACHE" \
    CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
    CBM_MEMLAB_ENV_PROBE="$ENV_LOG" \
        bash "$ROOT/scripts/memlab.sh" "$DUMMY_BINARY" 1 probe > "$WORKDIR/memlab.out" 2>&1 || true
)

[[ -s "$ENV_LOG" ]] || fail "memlab did not reach its driver"

CALLER_CACHE_NORMALIZED=$(normalize_path "$CALLER_CACHE")
CALLER_RUNTIME_NORMALIZED=$(normalize_path "$CALLER_RUNTIME")
private_root=""
while IFS=$'\t' read -r child_cache_raw child_runtime_raw; do
    child_cache=$(normalize_path "$child_cache_raw")
    child_runtime=$(normalize_path "$child_runtime_raw")
    if [[ -z "$child_runtime" || "$child_runtime" == "$CALLER_RUNTIME_NORMALIZED" ]]; then
        fail "memlab exposed the caller CBM_RUNTIME_DIR to the product"
    fi
    if [[ -z "$child_cache" || "$child_cache" == "$CALLER_CACHE_NORMALIZED" ]]; then
        fail "memlab exposed the caller CBM_CACHE_DIR to the product"
    fi
    if [[ "${child_runtime%/*}" != "${child_cache%/*}" ||
          "${child_runtime##*/}" != "runtime" || "${child_cache##*/}" != "cache" ]]; then
        fail "memlab runtime/cache were not isolated beneath one private root"
    fi
    private_root="${child_runtime%/*}"
done < "$ENV_LOG"

[[ ! -e "$private_root" ]] || fail "memlab left its private root behind: $private_root"

echo "PASS: memlab harness isolates its daemon runtime and cache from the caller"
