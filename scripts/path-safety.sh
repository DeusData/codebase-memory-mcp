#!/usr/bin/env bash
# Shared destructive-path validation for build/test/clean entrypoints.

cbm_require_safe_build_dir() {
    local build_dir="${1-}"
    local root="${2:-$PWD}"
    local leaf
    local physical_root
    local physical_build

    # Keep cleanup targets to one ordinary repository-owned directory directly
    # beneath build/.  Besides rejecting absolute and parent-relative paths,
    # the single-component rule prevents an intermediate symlink from routing
    # rm into a directory outside the repository.
    if [[ ! "$build_dir" =~ ^build/[A-Za-z0-9._-]+$ ]]; then
        echo "ERROR: BUILD_DIR must be one direct child of build/ (got '$build_dir')" >&2
        return 1
    fi
    leaf="${build_dir#build/}"
    if [[ "$leaf" == "." || "$leaf" == ".." ]]; then
        echo "ERROR: BUILD_DIR must name an ordinary build/ child (got '$build_dir')" >&2
        return 1
    fi
    if ! physical_root="$(cd "$root" 2>/dev/null && pwd -P)"; then
        echo "ERROR: repository root is missing or inaccessible" >&2
        return 1
    fi
    if [ -L "$physical_root/build" ] ||
        { [ -e "$physical_root/build" ] && [ ! -d "$physical_root/build" ]; }; then
        echo "ERROR: repository build/ ancestor must be absent or a real directory" >&2
        return 1
    fi
    if [ -d "$physical_root/build" ]; then
        if ! physical_build="$(cd "$physical_root/build" 2>/dev/null && pwd -P)" ||
            [ "$physical_build" != "$physical_root/build" ]; then
            echo "ERROR: repository build/ ancestor resolves outside the repository" >&2
            return 1
        fi
    fi
}

cbm_remove_build_dir() {
    local root="${1-}"
    local build_dir="${2-}"
    local physical_root
    local leaf

    cbm_require_safe_build_dir "$build_dir" "$root" || return 1
    physical_root="$(cd "$root" 2>/dev/null && pwd -P)" || return 1
    leaf="${build_dir#build/}"
    rm -rf -- "$physical_root/build/$leaf"
}
