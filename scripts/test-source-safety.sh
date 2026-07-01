#!/usr/bin/env bash
# test-source-safety.sh — self-tests for scripts/check-source-safety.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP_PARENT="${TMPDIR:-/tmp}"
TMP_ROOT="$(mktemp -d "$TMP_PARENT/cbm-source-safety-test-XXXXXX")"

cleanup() {
    rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

make_tree() {
    local root="$1"
    mkdir -p "$root/src/mcp" "$root/src/pipeline" "$root/src/graph_buffer" \
        "$root/src/semantic" "$root/internal/cbm" "$root/cmd"
}

run_guard() {
    local root="$1"
    local log="$2"
    CBM_SOURCE_SAFETY_ROOT="$root" bash "$REPO_ROOT/scripts/check-source-safety.sh" \
        >"$log" 2>&1
}

expect_pass() {
    local name="$1"
    local root="$2"
    local log="$TMP_ROOT/$name.log"
    if ! run_guard "$root" "$log"; then
        echo "[source-safety-test] expected pass failed: $name"
        cat "$log"
        exit 1
    fi
}

expect_fail_contains() {
    local name="$1"
    local root="$2"
    local needle="$3"
    local log="$TMP_ROOT/$name.log"
    if run_guard "$root" "$log"; then
        echo "[source-safety-test] expected failure passed unexpectedly: $name"
        cat "$log"
        exit 1
    fi
    if ! grep -q "$needle" "$log"; then
        echo "[source-safety-test] expected '$needle' in $name output"
        cat "$log"
        exit 1
    fi
}

clean_root="$TMP_ROOT/clean"
make_tree "$clean_root"
printf 'void ok(void) {}\n' >"$clean_root/src/mcp/ok.c"
expect_pass "clean" "$clean_root"

stdout_root="$TMP_ROOT/stdout"
make_tree "$stdout_root"
printf '#include <stdio.h>\nvoid bad(void) { printf("bad\\n"); }\n' \
    >"$stdout_root/src/mcp/bad.c"
expect_fail_contains "stdout" "$stdout_root" "stdout write"

fprintf_stdout_root="$TMP_ROOT/fprintf_stdout"
make_tree "$fprintf_stdout_root"
printf '#include <stdio.h>\nvoid bad(void) { fprintf(stdout, "bad\\n"); }\n' \
    >"$fprintf_stdout_root/src/pipeline/bad.c"
expect_fail_contains "fprintf_stdout" "$fprintf_stdout_root" "stdout write"

string_root="$TMP_ROOT/string"
make_tree "$string_root"
printf '#include <string.h>\nvoid bad(char *d, const char *s) { strcpy(d, s); }\n' \
    >"$string_root/src/pipeline/bad.c"
expect_fail_contains "unsafe_string" "$string_root" "unsafe string API"

rawfs_root="$TMP_ROOT/rawfs"
make_tree "$rawfs_root"
git -C "$rawfs_root" init -q
git -C "$rawfs_root" config user.email source-safety@example.invalid
git -C "$rawfs_root" config user.name source-safety
printf 'void ok(void) {}\n' >"$rawfs_root/src/pipeline/ok.c"
git -C "$rawfs_root" add src/pipeline/ok.c
git -C "$rawfs_root" commit -qm init
printf 'void bad(void) { unlink("x"); }\n' >>"$rawfs_root/src/pipeline/ok.c"
expect_fail_contains "raw_fs_diff" "$rawfs_root" "new raw env/fs API"

rawdup_root="$TMP_ROOT/rawdup"
make_tree "$rawdup_root"
git -C "$rawdup_root" init -q
git -C "$rawdup_root" config user.email source-safety@example.invalid
git -C "$rawdup_root" config user.name source-safety
printf 'void ok(void) {}\n' >"$rawdup_root/src/pipeline/ok.c"
git -C "$rawdup_root" add src/pipeline/ok.c
git -C "$rawdup_root" commit -qm init
printf 'char *bad(const char *s) { return strdup(s); }\n' >>"$rawdup_root/src/pipeline/ok.c"
expect_fail_contains "raw_strdup_diff" "$rawdup_root" "new raw strdup"

echo "[source-safety-test] OK"
