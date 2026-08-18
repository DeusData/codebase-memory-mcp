#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
fail() {
    echo "FAIL: $*" >&2
    exit 1
}
normalize_path() {
    local path="${1%$'\r'}"
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$path" 2>/dev/null && return 0
    fi
    printf '%s\n' "${path//\\//}"
}
INSTALL_FIXTURE="$WORKDIR/install-fixture"
cat > "$INSTALL_FIXTURE" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2" == "daemon status" ]] && exit 1
if [[ "$1 $2" == "install -y" ]]; then
    printf '%s\t%s\t%s\n' "$HOME" "$CBM_CACHE_DIR" "$CBM_RUNTIME_DIR" > "$CBM_TEST_INSTALL_ENV_PROBE"
fi
exit 0
EOF
chmod +x "$INSTALL_FIXTURE"
CONTROL="$INSTALL_FIXTURE"
CALLER_ROOT="$WORKDIR/caller-a"
mkdir -p "$CALLER_ROOT/runtime" "$CALLER_ROOT/cache"
touch "$CALLER_ROOT/sentinel"
(
    export CBM_RUNTIME_DIR="$CALLER_ROOT/runtime"
    export CBM_CACHE_DIR="$CALLER_ROOT/cache"
    source "$ROOT/scripts/test-runtime.sh"
    cbm_test_runtime_init
    [[ "$CBM_RUNTIME_DIR" != "$CALLER_ROOT/runtime" ]] || fail "inherited runtime was reused"
    [[ "$CBM_CACHE_DIR" != "$CALLER_ROOT/cache" ]] || fail "inherited cache was reused"
    private_root="$CBM_TEST_RUNTIME_ROOT"
    [[ -d "$private_root/runtime" && -d "$private_root/cache" ]] || fail "private directories missing"
    cbm_test_runtime_cleanup "$CONTROL"
    [[ ! -e "$private_root" ]] || fail "normal cleanup left its private root"
)
[[ -f "$CALLER_ROOT/sentinel" ]] || fail "cleanup touched the caller root"
for slot in 1 2; do
    (
        source "$ROOT/scripts/test-runtime.sh"
        cbm_test_runtime_init
        printf '%s\n' "$CBM_TEST_RUNTIME_ROOT" > "$WORKDIR/root-$slot"
        cbm_test_runtime_cleanup "$CONTROL"
    ) &
done
wait
[[ "$(<"$WORKDIR/root-1")" != "$(<"$WORKDIR/root-2")" ]] || fail "parallel roots collided"
FAIL_FIXTURE="$WORKDIR/failure-fixture"
cat > "$FAIL_FIXTURE" <<'EOF'
#!/usr/bin/env bash
touch "$CBM_TEST_FAILURE_MARKER"
EOF
chmod +x "$FAIL_FIXTURE"
mkdir "$WORKDIR/windows-ci-root"
set +e
(
    set -e
    source "$ROOT/scripts/test-runtime.sh"
    case "$(uname -s)" in
        MINGW*|MSYS*|CYGWIN*) export CBM_CI_TEMP_ROOT="$(cygpath -m "$WORKDIR/windows-ci-root")" ;;
    esac
    mktemp() { return 1; }
    cbm_test_runtime_init
    CBM_TEST_FAILURE_MARKER="$WORKDIR/failure.marker" "$FAIL_FIXTURE"
) > "$WORKDIR/failure.out" 2>&1
failure_rc=$?
set -e
[[ $failure_rc -ne 0 ]] || fail "simulated creation failure did not stop the harness"
[[ ! -e "$WORKDIR/failure.marker" ]] || fail "fixture ran after runtime creation failed"
mkdir "$WORKDIR/caller-home" "$WORKDIR/caller-cache" "$WORKDIR/caller-runtime"
HOME="$WORKDIR/caller-home" \
CBM_CACHE_DIR="$WORKDIR/caller-cache" \
CBM_RUNTIME_DIR="$WORKDIR/caller-runtime" \
CBM_TEST_INSTALL_ENV_PROBE="$WORKDIR/install.env" \
    "$ROOT/scripts/security-install.sh" "$INSTALL_FIXTURE" > "$WORKDIR/install.out" 2>&1
IFS=$'\t' read -r install_home install_cache install_runtime < "$WORKDIR/install.env"
install_home="$(normalize_path "$install_home")"
install_cache="$(normalize_path "$install_cache")"
install_runtime="$(normalize_path "$install_runtime")"
[[ "$install_home" != "$(normalize_path "$WORKDIR/caller-home")" ]] || fail "install reused caller HOME"
[[ "$install_cache" != "$(normalize_path "$WORKDIR/caller-cache")" ]] || fail "install reused caller cache"
[[ "$install_runtime" != "$(normalize_path "$WORKDIR/caller-runtime")" ]] || fail "install reused caller runtime"
install_root="${install_home%/*}"
[[ "$install_home" == "$install_root/home" && "$install_cache" == "$install_root/cache" &&
   "$install_runtime" == "$install_root/runtime" ]] || fail "install env did not share one private root"
ENTRY_POINTS=(
    scripts/security-install.sh scripts/security-fuzz.sh scripts/security-fuzz-random.sh
    scripts/security-network.sh tests/test_parent_watchdog.sh tests/test_worker_watchdog.sh
    tests/test_worker_error_response.sh tests/test_hook_conflict_notice.sh
)
for entry in "${ENTRY_POINTS[@]}"; do
    grep -q 'test-runtime.sh' "$ROOT/$entry" || fail "$entry does not source the helper"
    grep -q 'cbm_test_runtime_init' "$ROOT/$entry" || fail "$entry does not initialize isolation"
    grep -q 'cbm_test_runtime_cleanup' "$ROOT/$entry" || fail "$entry does not clean isolation"
done
echo "PASS: test harness runtimes are private, unique, fail-closed, and wired"
