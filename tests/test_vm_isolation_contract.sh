#!/usr/bin/env bash
# Keep isolated Windows runs out of shared cleanup and on their own build paths.
set -euo pipefail
task_repo="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
task_contract_dir="$(mktemp -d "${TMPDIR:-/tmp}/vm-cbm-isolation.XXXXXX")"
task_contract_done=0
task_contract_cleanup() {
    if [ "$task_contract_done" -eq 1 ]; then
        rm -rf -- "$task_contract_dir"
    else
        printf 'VM isolation diagnostics retained: %s\n' "$task_contract_dir" >&2
    fi
}
trap task_contract_cleanup EXIT
env -u CBM_VM_RUNNER -u CBM_VM_SOAK_BINARY bash \
    "$task_repo/tests/test_vm_isolated_dispatch.sh" "$task_repo" "$task_contract_dir/dispatch"
case "$(uname -s)" in
MINGW* | MSYS* | CYGWIN*)
    # The log is printed before the verdict: the retained directory does not
    # outlive the next win.sh call (its preflight sweeps cbm-* roots), so the
    # leg's own log is the only post-mortem a failure leaves.
    task_native_rc=0
    MSYS2_ARG_CONV_EXCL='*' powershell.exe -NoLogo -NoProfile -NonInteractive \
        -ExecutionPolicy Bypass \
        -File "$(cygpath -w "$task_repo/tests/test_windows_preflight_contract.ps1")" \
        -ScriptPath "$(cygpath -w "$task_repo/scripts/ci/clean-test-residue.ps1")" \
        -OutputDirectory "$(cygpath -w "$task_contract_dir/native")" \
        > "$task_contract_dir/native.log" 2>&1 || task_native_rc=$?
    cat "$task_contract_dir/native.log"
    if [ "$task_native_rc" -ne 0 ] ||
        ! grep -Fq 'WINDOWS_CHECK_ONLY_COMPLETE failures=0 sentinels=2' "$task_contract_dir/native.log"; then
        echo "native preflight contract failed (exit $task_native_rc); per-case output:" >&2
        for task_case_file in "$task_contract_dir"/native/*.out "$task_contract_dir"/native/*.err; do
            [ -f "$task_case_file" ] || continue
            echo "--- ${task_case_file##*/}" >&2
            # PowerShell 5.1 redirects in UTF-16LE: drop the NULs and the
            # byte-order mark, which would make the leg log read as binary.
            LC_ALL=C tr -d '\000\376\377' <"$task_case_file" >&2
        done
        exit 1
    fi
    ;;
*)
    # Native PowerShell/DACL behavior is covered by this same contract on the
    # real Windows leg. Unix runs exercise the shared Bash dispatch above.
    echo 'Native preflight cases require Windows; Bash dispatch verified here.'
    ;;
esac
task_contract_done=1
echo 'VM isolation contract passed'
