#!/usr/bin/env bash
# Exercise the real VM wrapper with inert OS adapters and canonical-entry spies.
# This checks dispatch/ownership only; it is not a native Windows or suite gate.
set -uo pipefail
task_source_root="${1:?source repository required}"
task_case_root="${2:?fresh test output directory required}"
if [ -e "$task_case_root" ]; then
    echo 'Test output directory must not preexist' >&2
    exit 2
fi
mkdir -p "$task_case_root/bin" "$task_case_root/case/scripts/ci"
task_case_root="$(cd "$task_case_root" && pwd)"
export CBM_VM_ISOLATION_EVENTS="$task_case_root/events"
export CBM_VM_ISOLATION_TEMP="$task_case_root/protected-root"
cat > "$task_case_root/bin/powershell.exe" <<'SH'
#!/usr/bin/env bash
printf 'powershell' >> "$CBM_VM_ISOLATION_EVENTS"
printf ' <%s>' "$@" >> "$CBM_VM_ISOLATION_EVENTS"
printf '\n' >> "$CBM_VM_ISOLATION_EVENTS"
printf '%s\n' "$CBM_VM_ISOLATION_TEMP"
SH
cat > "$task_case_root/bin/cygpath" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$2"
SH
cat > "$task_case_root/bin/icacls" <<'SH'
#!/usr/bin/env bash
printf 'icacls' >> "$CBM_VM_ISOLATION_EVENTS"
printf ' <%s>' "$@" >> "$CBM_VM_ISOLATION_EVENTS"
printf '\n' >> "$CBM_VM_ISOLATION_EVENTS"
SH
cat > "$task_case_root/case/scripts/test.sh" <<'SH'
#!/usr/bin/env bash
printf 'canonical-test' >> "$CBM_VM_ISOLATION_EVENTS"
printf ' <%s>' "$@" >> "$CBM_VM_ISOLATION_EVENTS"
printf '\n' >> "$CBM_VM_ISOLATION_EVENTS"
printf '1 passed\n'
if [ "${1:-}" != '--suites' ]; then printf '=== All tests passed ===\n'; fi
SH
chmod +x "$task_case_root/bin/"* "$task_case_root/case/scripts/test.sh"
task_wrapper="$(cd "$task_source_root" && pwd)/test-infrastructure/vm/vm-run-tests.sh"
task_failures=0
check() {
    if "$@"; then return; fi
    task_failures=$((task_failures + 1))
    printf 'FAIL: %s\n' "$*" >&2
}
for task_run in alpha beta; do
    : > "$CBM_VM_ISOLATION_EVENTS"
    (
        cd "$task_case_root/case" || exit 2
        PATH="$task_case_root/bin:$PATH" CBM_CI_RUN_ID="$task_run" CBM_CI_KEEP=1 \
            CBM_VM_TEST_LOG="$task_case_root/$task_run.log" \
            bash "$task_wrapper" --par
    ) > "$task_case_root/$task_run.output" 2>&1
    check test "$?" -eq 0
    if grep -F -- '-PruneStale' "$CBM_VM_ISOLATION_EVENTS" >/dev/null; then
        printf 'FAIL: isolated %s requested shared pruning\n' "$task_run" >&2
        task_failures=$((task_failures + 1))
    fi
    check grep -F -- "<-ProtectDir> <build/vm-$task_run>" "$CBM_VM_ISOLATION_EVENTS"
    check grep -F -- "icacls <build/vm-$task_run>" "$CBM_VM_ISOLATION_EVENTS"
    check grep -F -- "canonical-test <CC=clang> <CXX=clang++> <BUILD_DIR=build/vm-$task_run>" "$CBM_VM_ISOLATION_EVENTS"
    check grep -F -- '=== All tests passed ===' "$task_case_root/$task_run.log"
    cp "$CBM_VM_ISOLATION_EVENTS" "$task_case_root/$task_run.events"
done
# Invalid external identifiers must fail before an OS adapter is invoked.
: > "$CBM_VM_ISOLATION_EVENTS"
(
    cd "$task_case_root/case" || exit 2
    PATH="$task_case_root/bin:$PATH" CBM_CI_RUN_ID='../foreign' CBM_CI_KEEP=1 \
        CBM_VM_TEST_LOG="$task_case_root/invalid.log" bash "$task_wrapper" --par
) > "$task_case_root/invalid.output" 2>&1
task_invalid_exit=$?
check test "$task_invalid_exit" -eq 2
check test ! -s "$CBM_VM_ISOLATION_EVENTS"
if [ "$task_failures" -ne 0 ]; then
    printf 'VM isolation dispatch: %s failed checks\n' "$task_failures" >&2
    exit 1
fi
printf 'VM isolation dispatch passed: two isolated full entries and invalid identifier rejection\n'
