#!/usr/bin/env bash
# smoke-wrappers.sh — install the npm, PyPI and Go wrappers from this checkout
# against a release fixture and run the binary each one installed.
#
# The wrappers download the release archive themselves. This runs each one the
# way a user's package manager does, against CBM_DOWNLOAD_URL (the test
# override install.sh honours too) serving the real release archives, so a
# wrapper that cannot install what we ship fails here instead of on users
# (#2657: a 256 MiB archive-member cap against 331 MB binaries).
#
# Usage: scripts/smoke-wrappers.sh DOWNLOAD_URL [npm] [pypi] [go]
#   DOWNLOAD_URL  base URL serving the release archives and checksums.txt:
#                 https, or plain http to a loopback authority
#   (no wrapper names = all three)
# Environment:
#   SMOKE_REQUIRE_WRAPPERS=1  a missing toolchain fails instead of skipping (CI)
# Exit: 0 = every wrapper installed and ran its binary (or was skipped) ·
#       1 = a wrapper failed · 2 = usage error.
set -euo pipefail

case "${BASH_SOURCE[0]}" in */*) cd "${BASH_SOURCE[0]%/*}/.." ;; *) cd .. ;; esac
ROOT="$PWD"
URL="${1:-}"
[ -n "$URL" ] || { sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }
shift
WRAPPERS=("$@")
[ ${#WRAPPERS[@]} -gt 0 ] || WRAPPERS=(npm pypi go)
REQUIRE="${SMOKE_REQUIRE_WRAPPERS:-0}"
EXE=""
case "${OSTYPE:-}" in msys* | cygwin* | win32*) EXE=".exe" ;; esac

# mktemp -d sets 0700 at creation. `mkdir -m 700` changes the mode in a second
# step, which MSYS2 cannot do under the smoke profile's owner-only DACL.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-smoke-wrappers.XXXXXX")"
# Go writes its module cache read-only; restore write permission so it can be removed.
trap 'chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"' EXIT
FAILED=0

missing() { # wrapper, what
    if [ "$REQUIRE" = "1" ]; then
        echo "FAIL $1: SMOKE_REQUIRE_WRAPPERS=1 but $2 is not available"
        FAILED=1
    else
        echo "SKIP $1: $2 not available on this host"
    fi
}

# A fresh HOME, cache and profile per wrapper: nothing a wrapper writes leaks
# into the next one, and a binary cached by an earlier run cannot stand in.
isolated() { # wrapper dir, command...
    local dir="$1"
    shift
    mkdir -p "$dir/home" "$dir/cache" "$dir/localappdata"
    HOME="$dir/home" USERPROFILE="$dir/home" XDG_CACHE_HOME="$dir/cache" \
        LOCALAPPDATA="$dir/localappdata" CBM_CACHE_DIR="$dir/cbm-cache" \
        CBM_DOWNLOAD_URL="$URL" "$@"
}

ran() { # wrapper, output file, rc
    # The binary's own answer ("codebase-memory-mcp 0.12.0"), not a wrapper
    # message ("codebase-memory-mcp: downloading ..."), proves it ran.
    if [ "$3" -eq 0 ] && grep -E -q '^codebase-memory-mcp [^:]+$' "$2"; then
        echo "OK $1: installed and ran the release binary: $(grep -E '^codebase-memory-mcp [^:]+$' "$2" | tail -1)"
    else
        echo "FAIL $1: the installed wrapper did not run the release binary (rc=$3):"
        sed 's/^/    /' "$2" | tail -20
        FAILED=1
    fi
}

smoke_npm() {
    if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
        missing npm "node/npm"
        return
    fi
    local dir="$WORK/npm" tgz="" line modules rc=0
    mkdir -p "$dir/prefix"
    (cd "$ROOT/pkg/npm" && npm pack --silent --pack-destination "$dir") >"$dir/pack.out"
    while IFS= read -r line; do [ -n "$line" ] && tgz="$line"; done <"$dir/pack.out"
    # A global install from the packed tarball runs postinstall (install.js)
    # exactly as `npm i -g codebase-memory-mcp` does.
    isolated "$dir" npm install --global --prefix "$dir/prefix" --cache "$dir/cache/npm" \
        --foreground-scripts --no-audit --no-fund "$dir/$tgz" >"$dir/install.log" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ]; then
        ran npm "$dir/install.log" "$rc"
        return
    fi
    local modules
    npm root --global --prefix "$dir/prefix" >"$dir/root.out"
    IFS= read -r modules <"$dir/root.out"
    isolated "$dir" node "$modules/codebase-memory-mcp/bin.js" --version >"$dir/run.log" 2>&1 ||
        rc=$?
    ran npm "$dir/run.log" "$rc"
}

smoke_pypi() {
    local py=""
    if command -v python3 >/dev/null 2>&1; then
        py=python3
    elif command -v python >/dev/null 2>&1; then
        py=python
    fi
    if [ -z "$py" ]; then
        missing pypi "python3"
        return
    fi
    local dir="$WORK/pypi" rc=0
    mkdir -p "$dir"
    "$py" -m venv "$dir/venv"
    local vpy="$dir/venv/bin/python"
    [ -x "$vpy" ] || vpy="$dir/venv/Scripts/python.exe"
    "$vpy" -m pip install --quiet --disable-pip-version-check "$ROOT/pkg/pypi" \
        >"$dir/install.log" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ]; then
        ran pypi "$dir/install.log" "$rc"
        return
    fi
    # The console script's entry point; the first run downloads the binary.
    isolated "$dir" "$vpy" -c \
        'import sys, codebase_memory_mcp; sys.argv = ["codebase-memory-mcp", "--version"]; codebase_memory_mcp.main()' \
        >"$dir/run.log" 2>&1 || rc=$?
    ran pypi "$dir/run.log" "$rc"
}

smoke_go() {
    if ! command -v go >/dev/null 2>&1; then
        missing go "go"
        return
    fi
    local dir="$WORK/go" rc=0
    mkdir -p "$dir"
    # All Go state stays under $WORK: a toolchain that GOTOOLCHAIN=auto downloads
    # lands read-only in the module cache, which must not end up in the caller's
    # HOME (the smoke harness could not delete its profile afterwards).
    (cd "$ROOT/pkg/go" && GOTOOLCHAIN="${GOTOOLCHAIN:-auto}" \
        GOPATH="$dir/gopath" GOMODCACHE="$dir/gomodcache" GOCACHE="$dir/gocache" \
        go build -o "$dir/codebase-memory-mcp$EXE" ./cmd/codebase-memory-mcp) \
        >"$dir/install.log" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ]; then
        ran go "$dir/install.log" "$rc"
        return
    fi
    # The first run downloads the binary into the wrapper's cache.
    isolated "$dir" "$dir/codebase-memory-mcp$EXE" --version >"$dir/run.log" 2>&1 || rc=$?
    ran go "$dir/run.log" "$rc"
}

for wrapper in "${WRAPPERS[@]}"; do
    case "$wrapper" in
        npm) smoke_npm ;;
        pypi) smoke_pypi ;;
        go) smoke_go ;;
        *) echo "unknown wrapper: $wrapper" >&2; exit 2 ;;
    esac
done
exit "$FAILED"
