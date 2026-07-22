#!/usr/bin/env bash
# vm-run-tests.sh — run C test suites on the Windows VM under a CI-shaped
# protected temp root. Runs ON the VM (MSYS2 shell), invoked by win.sh
# test / ubsan-test / trap-ubsan-test.
#
# Why the temp root: the daemon/coordination suites fail closed on the
# MSYS-shared /tmp (C:\msys64\tmp), whose ancestry grants mutation rights to
# Authenticated Users — running them there produces security refusals, not
# test signal. CI gives the harness a per-user root under the profile with an
# owner-stamped, protected current-SID DACL (.github/workflows/_test.yml
# "Create protected per-user temp root"); this script mirrors that exactly so
# the VM leg validates what CI will see.
#
# Why the guard: this leg once piped through `tail -40`, and a suite that
# never validly ran looked green — 40 Windows failures reached CI unseen.
# Output now streams in full, and a run whose log lacks the runner's
# completion summary is a hard failure regardless of exit code.
set -uo pipefail

RUNNER="${CBM_VM_RUNNER:-build/c/test-runner}"
LOG="${CBM_VM_TEST_LOG:-/tmp/win-test.log}"

[ $# -ge 1 ] || { echo "usage: vm-run-tests.sh <suite...> | --par" >&2; exit 2; }
[ -x "$RUNNER" ] || { echo "ERROR: runner '$RUNNER' missing — build first" >&2; exit 2; }

# Stale roots from earlier runs are removed up front; the current root is kept
# after the run for post-mortem inspection.
root_windows="$(powershell -NoProfile -Command '
  $ErrorActionPreference = "Stop"
  Get-ChildItem -LiteralPath $env:USERPROFILE -Directory -Filter "cbm-vm-tmp-*" -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
  $root = Join-Path $env:USERPROFILE ("cbm-vm-tmp-" + [guid]::NewGuid().ToString("N"))
  New-Item -ItemType Directory -Path $root | Out-Null
  $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
  $acl = [System.Security.AccessControl.DirectorySecurity]::new()
  $acl.SetOwner($sid)
  $acl.SetAccessRuleProtection($true, $false)
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $sid,
    [System.Security.AccessControl.FileSystemRights]::FullControl,
    ([System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
      [System.Security.AccessControl.InheritanceFlags]::ObjectInherit),
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow)
  $acl.AddAccessRule($rule) | Out-Null
  Set-Acl -LiteralPath $root -AclObject $acl
  Write-Output $root
' | tr -d '\r')"
[ -n "$root_windows" ] || { echo "ERROR: protected temp root creation failed" >&2; exit 2; }

TEMP="$(cygpath -m "$root_windows")"
TMP="$TEMP"
TMPDIR="$(cygpath -u "$root_windows")"
export TEMP TMP TMPDIR

# The runner's directory must look like a real user checkout: repos under a
# profile carry no Authenticated-Users ACE, but C:\cbm (like CI's workspace
# drive) inherits Modify for Authenticated Users from the drive root, which
# the activation transaction's source-directory policy correctly refuses —
# install-flow tests would then fail on the environment, not the code.
# Two steps, both idempotent: protect the DIRECTORY (inheritance flags are
# directory-only — a /T re-root leaves files with empty, deny-all DACLs),
# then /reset the children so they re-inherit the clean set from it.
runner_dir_w="$(cygpath -w "$(dirname "$RUNNER")")"
me="$(whoami | tr -d '\r')"
MSYS2_ARG_CONV_EXCL='*' icacls "$runner_dir_w" /inheritance:r \
    /grant:r "${me}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' \
    /Q >/dev/null 2>&1 || true
MSYS2_ARG_CONV_EXCL='*' icacls "${runner_dir_w}\\*" /reset /T /C /Q >/dev/null 2>&1 || true

echo "=== vm-run-tests: runner=$RUNNER temp=$TEMP suites: $* ==="

# --par runs the repo's full parallel harness (wave + tail scheduling,
# per-shard union guard, manifest) under this same protected environment;
# explicit suite names keep the direct single-runner invocation.
if [ "$1" = "--par" ]; then
    bash scripts/run-tests-parallel.sh "$RUNNER" 2>&1 | tee "$LOG"
    rc="${PIPESTATUS[0]}"
else
    "$RUNNER" "$@" 2>&1 | tee "$LOG"
    rc="${PIPESTATUS[0]}"
fi

if ! grep -Eq '[0-9]+ passed' "$LOG"; then
    echo "GUARD: test runner produced no completion summary — the suites did" \
         "not validly run; treating as failure (runner rc=$rc)" >&2
    exit 90
fi
exit "$rc"
