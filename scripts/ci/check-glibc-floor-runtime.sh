#!/usr/bin/env bash
# check-glibc-floor-runtime.sh -- the standard (dynamic) Linux binary must START
# and INDEX on the oldest userland it claims.
#
# The standard Linux release binaries build in a pinned ubuntu:22.04 container
# (_build.yml build-linux) and check-binary-composition.sh A6 asserts their
# glibc / libstdc++ floor on the built bytes. This RUNS those bytes in a plain
# container of that same pinned image with nothing installed (only the libc,
# libstdc++, libgcc and zlib a stock ubuntu:22.04 ships) and requires:
#   1. --version exits 0: the loader resolved every symbol version;
#   2. cli index_repository on a tiny C + Python fixture reports "indexed";
#   3. cli list_projects lists that project.
# A binary built on a newer userland fails step 1, and the loader's
# "version `GLIBC_2.xx' not found" lines are in the output.
#
# Usage: scripts/ci/check-glibc-floor-runtime.sh --image IMAGE [--platform P] BINARY
#   --image IMAGE  the floor image, pinned by digest (ubuntu:22.04@sha256:...).
#                  The release smoke passes the digest build-linux builds in;
#                  the venue-parity contract asserts it is the same one.
#   --platform P   docker --platform (e.g. linux/amd64) for a binary that is
#                  not the host's architecture; default: the host's.
#   BINARY         the linux binary to run (mounted read-only).
# Exit: 0 = starts and indexes on the floor; 1 = it does not; 2 = usage error.
# Needs: docker.
set -euo pipefail

usage() { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; }

IMAGE=""
PLATFORM=""
BIN=""
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        usage
        exit 0
        ;;
    --image)
        IMAGE="${2:-}"
        [ $# -gt 1 ] && shift
        ;;
    --image=*) IMAGE="${1#--image=}" ;;
    --platform)
        PLATFORM="${2:-}"
        [ $# -gt 1 ] && shift
        ;;
    --platform=*) PLATFORM="${1#--platform=}" ;;
    -*)
        echo "check-glibc-floor-runtime.sh: unknown option '$1'. Please consult --help." >&2
        exit 2
        ;;
    *)
        if [ -n "$BIN" ]; then
            echo "check-glibc-floor-runtime.sh: more than one binary. Please consult --help." >&2
            exit 2
        fi
        BIN="$1"
        ;;
    esac
    shift
done
case "$IMAGE" in
*@sha256:*) ;;
*)
    echo "check-glibc-floor-runtime.sh: --image must be pinned by digest (name@sha256:...). Please consult --help." >&2
    exit 2
    ;;
esac
if [ -z "$BIN" ] || [ ! -f "$BIN" ]; then
    echo "check-glibc-floor-runtime.sh: no such binary '$BIN'. Please consult --help." >&2
    exit 2
fi
BIN_ABS="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"

# Runs INSIDE the floor container under its own /bin/sh. The fixture is
# written there too, so the only host path the container sees is the binary.
# shellcheck disable=SC2016 # expanded by the container's shell, not here
PROGRAM='
set -eu
BIN=/opt/cbm/codebase-memory-mcp
. /etc/os-release
echo "floor userland: $PRETTY_NAME, $(uname -m), $(ldd --version | head -n 1)"
echo "--- libraries the loader resolves"
ldd "$BIN" || true
echo "--- $BIN --version"
"$BIN" --version
mkdir -p /tmp/fixture/src /tmp/home
printf "%s\n" "static int helper(int x) { return x + 1; }" \
    "int run(int x) { return helper(x) * 2; }" \
    "int main(void) { return run(1) == 4 ? 0 : 1; }" >/tmp/fixture/src/main.c
printf "%s\n" "def greet(name):" "    return \"hi \" + name" "" \
    "def main():" "    print(greet(\"x\"))" >/tmp/fixture/src/app.py
export HOME=/tmp/home
echo "--- cli index_repository --repo-path /tmp/fixture"
INDEXED=$("$BIN" cli index_repository --repo-path /tmp/fixture)
printf "%s\n" "$INDEXED"
case "$INDEXED" in
*"\"status\":\"indexed\""*) ;;
*)
    echo "FAIL: index_repository did not report status indexed"
    exit 1
    ;;
esac
echo "--- cli list_projects"
PROJECTS=$("$BIN" cli list_projects)
printf "%s\n" "$PROJECTS"
case "$PROJECTS" in
*tmp-fixture*) ;;
*)
    echo "FAIL: list_projects does not list the indexed fixture (tmp-fixture)"
    exit 1
    ;;
esac
'

set -- run --rm
[ -z "$PLATFORM" ] || set -- "$@" --platform "$PLATFORM"
set -- "$@" -v "$BIN_ABS:/opt/cbm/codebase-memory-mcp:ro" "$IMAGE" sh -c "$PROGRAM"

echo "==> $(basename "$BIN") on the glibc floor: plain $IMAGE, nothing installed"
if docker "$@"; then
    echo "PASS: starts, indexes and lists on the floor userland ($IMAGE)"
else
    echo "FAIL: $(basename "$BIN") does not run on the floor userland ($IMAGE)" >&2
    exit 1
fi
