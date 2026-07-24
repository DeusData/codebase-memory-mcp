#!/usr/bin/env bash
# Regression guard: build cleanup paths must stay within one direct child of
# the repository's build/ directory.  A malformed BUILD_DIR must be rejected
# before any rm invocation can observe it.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# shellcheck source=../scripts/path-safety.sh
source "$ROOT/scripts/path-safety.sh"

for path in build/c build/linux-arm64 build/linux-amd64 build/win-cross; do
    cbm_require_safe_build_dir "$path"
done

for path in \
    "" \
    build \
    build/ \
    . \
    .. \
    ../outside \
    /tmp/outside \
    build/../outside \
    build/./c \
    build//c \
    build/nested/c \
    'build\..\outside'; do
    if cbm_require_safe_build_dir "$path" >/dev/null 2>&1; then
        echo "FAIL: unsafe BUILD_DIR was accepted: '$path'" >&2
        exit 1
    fi
done

# Create a real directory link: a native symlink, or an NTFS junction on
# Windows (MSYS reports junctions as symlinks).  MSYS2's default `ln -s`
# silently DEEP-COPIES the target, which would turn these traversal fixtures
# into vacuous copies that the validator rightly accepts.  Returns nonzero
# when this environment cannot create a real link at all — the traversal
# scenario then cannot exist, and the caller skips instead of asserting
# against a copy.
make_real_dir_link() {
    local target="$1" link="$2"
    if MSYS=winsymlinks:nativestrict ln -s -- "$target" "$link" 2>/dev/null &&
        [ -L "$link" ]; then
        return 0
    fi
    rm -rf -- "$link"
    case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
        cmd //c "mklink /J \"$(cygpath -w "$link")\" \"$(cygpath -w "$target")\"" \
            >/dev/null 2>&1 || true
        ;;
    esac
    [ -L "$link" ]
}

# Lexical containment is not enough: an attacker-controlled or accidental
# build/ symlink would make ROOT/build/c traverse outside the repository.
symlink_fixture="$(mktemp -d "${TMPDIR:-/tmp}/cbm-build-dir-symlink.XXXXXX")"
trap 'rm -rf -- "$symlink_fixture"' EXIT
mkdir -p "$symlink_fixture/repo" "$symlink_fixture/outside"
if make_real_dir_link "$symlink_fixture/outside" "$symlink_fixture/repo/build"; then
    if (
        cd "$symlink_fixture/repo"
        cbm_require_safe_build_dir build/c "$symlink_fixture/repo"
    ); then
        echo "FAIL: BUILD_DIR accepted a symlinked build/ ancestor" >&2
        exit 1
    fi
else
    echo "SKIP: no real symlink/junction support here; ancestor-traversal fixture not exercisable"
fi

mkdir -p "$symlink_fixture/safe-repo/build" "$symlink_fixture/final-target"
printf 'keep\n' >"$symlink_fixture/final-target/sentinel"
if make_real_dir_link "$symlink_fixture/final-target" "$symlink_fixture/safe-repo/build/c"; then
    if ! cbm_remove_build_dir "$symlink_fixture/safe-repo" build/c; then
        echo "FAIL: safe removal rejected a final-component symlink" >&2
        exit 1
    fi
    if [ -e "$symlink_fixture/safe-repo/build/c" ] ||
        [ ! -f "$symlink_fixture/final-target/sentinel" ]; then
        echo "FAIL: final-component symlink removal followed the link" >&2
        exit 1
    fi
else
    echo "SKIP: no real symlink/junction support here; final-component fixture not exercisable"
fi
mkdir -p "$symlink_fixture/safe-repo/build/ordinary"
printf 'remove\n' >"$symlink_fixture/safe-repo/build/ordinary/artifact"
cbm_remove_build_dir "$symlink_fixture/safe-repo" build/ordinary
if [ -e "$symlink_fixture/safe-repo/build/ordinary" ]; then
    echo "FAIL: ordinary build child was not removed" >&2
    exit 1
fi

# Direct Makefile cleanup is a public developer entrypoint too; it must use
# the same guard instead of interpolating an untrusted value into a shell.
if make -s -f "$ROOT/Makefile.cbm" clean-c BUILD_DIR=build/nested/c >/dev/null 2>&1; then
    echo "FAIL: make clean-c accepted an unsafe BUILD_DIR" >&2
    exit 1
fi

echo "Build directory safety contract passed"
