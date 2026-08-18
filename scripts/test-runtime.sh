#!/usr/bin/env bash

# Sourced by test harnesses. Host paths stay separate from paths exported to a
# native Windows product binary.
CBM_TEST_RUNTIME_ROOT=""
CBM_TEST_RUNTIME_DIR_HOST=""
CBM_TEST_CACHE_DIR_HOST=""
_CBM_TEST_RUNTIME_CREATED_ROOT=""
_CBM_TEST_RUNTIME_PRODUCT_RUNTIME=""
_CBM_TEST_RUNTIME_PRODUCT_CACHE=""

_cbm_test_runtime_error() {
    echo "test-runtime: $*" >&2
}

_cbm_test_runtime_discard_partial() {
    local root="$1" parent="$2" name="${1##*/}"
    [[ -n "$root" && "$root" != "$name" && "${root%/*}" == "$parent" && ! -L "$root" ]] || return 0
    case "$name" in cbmrt.?*|cbmrt-?*) rm -rf -- "$root" >/dev/null 2>&1 || true ;; esac
}

cbm_test_runtime_init() {
    [[ -z "$_CBM_TEST_RUNTIME_CREATED_ROOT" ]] ||
        { _cbm_test_runtime_error "runtime already initialized"; return 1; }
    local platform windows=0 parent root="" runtime_host cache_host
    local product_runtime product_cache helper_dir helper_native root_native
    platform="$(uname -s)" ||
        { _cbm_test_runtime_error "cannot identify the host platform"; return 1; }
    case "$platform" in
        MINGW*|MSYS*|CYGWIN*) windows=1 ;;
        Darwin) parent="/private/tmp" ;;
        *) parent="/tmp" ;;
    esac
    if (( windows )); then
        command -v cygpath >/dev/null 2>&1 ||
            { _cbm_test_runtime_error "cygpath is required on Windows"; return 1; }
        if [[ -n "${CBM_CI_TEMP_ROOT:-}" ]]; then
            parent="$(cygpath -u "$CBM_CI_TEMP_ROOT")" ||
                { _cbm_test_runtime_error "cannot convert CBM_CI_TEMP_ROOT"; return 1; }
            [[ -d "$parent" && ! -L "$parent" ]] ||
                { _cbm_test_runtime_error "CBM_CI_TEMP_ROOT is not a real directory"; return 1; }
            root="$(mktemp -d "${parent%/}/cbmrt.XXXXXX")" ||
                { _cbm_test_runtime_error "cannot create a private Windows CI root"; return 1; }
        else
            helper_dir="$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || return 1
            helper_native="$(cygpath -w "$helper_dir/ci/new-protected-temp-root.ps1")" ||
                { _cbm_test_runtime_error "cannot convert protected-root helper"; return 1; }
            root_native="$(MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoProfile \
                -ExecutionPolicy Bypass -File "$helper_native" -Prefix 'cbmrt-')" ||
                { _cbm_test_runtime_error "protected Windows root creation failed"; return 1; }
            root_native="${root_native//$'\r'/}"
            [[ -n "$root_native" && "$root_native" != *$'\n'* ]] ||
                { _cbm_test_runtime_error "protected-root helper returned an invalid path"; return 1; }
            root="$(cygpath -u "$root_native")" ||
                { _cbm_test_runtime_error "cannot convert protected Windows root"; return 1; }
            parent="${root%/*}"
        fi
    else
        root="$(mktemp -d "${parent%/}/cbmrt.XXXXXX")" ||
            { _cbm_test_runtime_error "cannot create private root under $parent"; return 1; }
        chmod 700 "$root" || {
            _cbm_test_runtime_discard_partial "$root" "$parent"
            _cbm_test_runtime_error "cannot protect runtime root $root"
            return 1
        }
    fi
    runtime_host="$root/runtime"
    cache_host="$root/cache"
    mkdir "$runtime_host" "$cache_host" || {
        _cbm_test_runtime_discard_partial "$root" "$parent"
        _cbm_test_runtime_error "cannot create runtime/cache directories"
        return 1
    }
    if (( ! windows )); then
        chmod 700 "$runtime_host" "$cache_host" || {
            _cbm_test_runtime_discard_partial "$root" "$parent"
            _cbm_test_runtime_error "cannot protect runtime/cache directories"
            return 1
        }
        product_runtime="$runtime_host"
        product_cache="$cache_host"
    else
        product_runtime="$(cygpath -m "$runtime_host")" || {
            _cbm_test_runtime_discard_partial "$root" "$parent"
            _cbm_test_runtime_error "cannot convert the runtime directory"
            return 1
        }
        product_cache="$(cygpath -m "$cache_host")" || {
            _cbm_test_runtime_discard_partial "$root" "$parent"
            _cbm_test_runtime_error "cannot convert the cache directory"
            return 1
        }
    fi
    CBM_TEST_RUNTIME_ROOT="$root"
    CBM_TEST_RUNTIME_DIR_HOST="$runtime_host"
    CBM_TEST_CACHE_DIR_HOST="$cache_host"
    _CBM_TEST_RUNTIME_CREATED_ROOT="$root"
    _CBM_TEST_RUNTIME_PRODUCT_RUNTIME="$product_runtime"
    _CBM_TEST_RUNTIME_PRODUCT_CACHE="$product_cache"
    export CBM_RUNTIME_DIR="$product_runtime" CBM_CACHE_DIR="$product_cache"
}

_cbm_test_runtime_daemon() {
    CBM_RUNTIME_DIR="$_CBM_TEST_RUNTIME_PRODUCT_RUNTIME" \
        CBM_CACHE_DIR="$_CBM_TEST_RUNTIME_PRODUCT_CACHE" "$1" daemon "$2"
}

cbm_test_runtime_cleanup() {
    local binary="${1:-}" root="$_CBM_TEST_RUNTIME_CREATED_ROOT"
    local name="${_CBM_TEST_RUNTIME_CREATED_ROOT##*/}" runtime_entry="" active=0
    [[ -n "$root" ]] || return 0
    case "$name" in
        cbmrt.?*|cbmrt-?*) ;;
        *) _cbm_test_runtime_error "refusing cleanup for unexpected basename: $name"; return 0 ;;
    esac
    if [[ "$root" != "$CBM_TEST_RUNTIME_ROOT" || -L "$root" ]]; then
        _cbm_test_runtime_error "refusing cleanup for an unexpected runtime root: $root"
        return 0
    fi
    if [[ ! -e "$root" ]]; then
        _CBM_TEST_RUNTIME_CREATED_ROOT=""
        return 0
    fi
    if [[ ! -d "$root" || "$CBM_TEST_RUNTIME_DIR_HOST" != "$root/runtime" ||
          "$CBM_TEST_CACHE_DIR_HOST" != "$root/cache" ||
          -L "$CBM_TEST_RUNTIME_DIR_HOST" || -L "$CBM_TEST_CACHE_DIR_HOST" ]]; then
        _cbm_test_runtime_error "refusing cleanup after runtime path replacement: $root"
        return 0
    fi
    if [[ -d "$CBM_TEST_RUNTIME_DIR_HOST" ]] &&
       ! runtime_entry="$(find "$CBM_TEST_RUNTIME_DIR_HOST" -mindepth 1 -print -quit 2>/dev/null)"; then
        _cbm_test_runtime_error "cannot inspect runtime state; leaving $root"
        return 0
    fi
    if [[ -n "$runtime_entry" ]]; then
        [[ -x "$binary" ]] || {
            _cbm_test_runtime_error "cannot check the private daemon; leaving $root"
            return 0
        }
        if _cbm_test_runtime_daemon "$binary" status >/dev/null 2>&1; then
            active=1
            _cbm_test_runtime_daemon "$binary" stop >/dev/null 2>&1 || true
            for _attempt in {1..50}; do
                if ! _cbm_test_runtime_daemon "$binary" status >/dev/null 2>&1; then
                    active=0
                    break
                fi
                sleep 0.1
            done
        fi
    fi
    if (( active )); then
        _cbm_test_runtime_error "private daemon is still active; leaving $root"
        return 0
    fi
    if rm -rf -- "$root"; then
        _CBM_TEST_RUNTIME_CREATED_ROOT=""
    else
        _cbm_test_runtime_error "cleanup failed; leaving $root"
    fi
    return 0
}
