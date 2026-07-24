<#
.SYNOPSIS
    Run the native-Windows product-surface test suite for codebase-memory-mcp.

.DESCRIPTION
    Builds the payload and permanent launcher if they are not already present,
    stages them under their release names, then runs the deterministic Windows
    integration tests under tests/windows/ through the launcher (real stdio /
    CLI / HTTP UI, real SQLite DB).

    Two categories of test:

      GUARDS      - regression guards for Windows bugs already fixed on main.
                    They must stay GREEN (exit 0); a RED (exit 1) means the fix
                    regressed and fails this runner.
                      * test_non_ascii_path.py    guards #636/#357 (fixed by #700)
                      * test_non_ascii_cache_dump.py guards #996 (writer cbm_fopen)
                      * test_hook_augment.py      guards #618      (fixed by #619)
                      * test_ui_drive_listing.py  guards #548      (roots field)
                      * test_cli_non_ascii_arg.py guards #423/#20  (wide-argv main())
                      * test_daemon_stability.py guards the daemon parameter
                        surface, crash recovery, busy-stop refusal, and churn
                      * test_windows_launcher.py guards the permanent launcher,
                        managed layout, portable refusal, and crash containment

      KNOWN REDS  - genuine, still-open Windows bugs reproduced at the product
                    surface. They are EXPECTED to be RED (exit 1) and are opt-in
                    (never gate CI). If one turns GREEN the underlying bug was
                    fixed and it should be promoted to a guard.
                      * (none currently - test_cli_non_ascii_arg.py was promoted to a
                        guard when the wide-argv fix for #423/#20 landed)

    Indexing runs through the real supervisor -> worker spawn on every guard:
    under the mandatory coordination daemon CBM_INDEX_SUPERVISOR=0 is a
    fail-closed refusal seam, never an in-process fallback, so the old
    determinism override would turn every indexing guard into a refusal.

    On native Windows the MinGW/LLVM toolchain ships no libasan/libubsan, so the
    build disables sanitizers (SANITIZE=). Where the toolchain provides
    AddressSanitizer/UBSan (Linux containers, WSL), prefer scripts/test.sh.

.PARAMETER Binary
    Path to an existing portable payload executable. If omitted, the script
    builds it (target selected by -Target) into build/c/.

.PARAMETER Launcher
    Path to the permanent launcher executable. If omitted, the script uses
    build/c/codebase-memory-mcp-launcher.exe, building target cbm-launcher when
    needed.

.PARAMETER Target
    Makefile.cbm target used when building: 'cbm-with-ui' (default; needed for the
    drive-picker guard's embedded HTTP UI) or 'cbm' (no UI - the drive guard then
    reports a precondition and is skipped).

.PARAMETER GuardsOnly
    Run only the green guards (the CI gate). Skips the opt-in known-red repros.

.PARAMETER Make
    Path to GNU make (default: 'make' on PATH; MSYS2 ships it at
    C:\msys64\usr\bin\make.exe).

.EXAMPLE
    pwsh -File scripts/test-windows.ps1
.EXAMPLE
    pwsh -File scripts/test-windows.ps1 -GuardsOnly -Binary build\c\codebase-memory-mcp.exe
#>
[CmdletBinding()]
param(
    [string]$Binary,
    [string]$Launcher,
    [ValidateSet("cbm-with-ui", "cbm")]
    [string]$Target = "cbm-with-ui",
    [switch]$GuardsOnly,
    [string]$Make = "make"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $python) { $python = (Get-Command py -ErrorAction SilentlyContinue) }
if (-not $python) { throw "Python 3 is required to run the Windows tests." }
$py = $python.Source

# A writable Windows temp dir that GNU make forwards to the native gcc. MSYS2
# strips TMP/TEMP from the environment it hands native children, so pass them as
# make command-line variables (make exports those to recipe processes).
$tmp = $env:TEMP
if (-not $tmp) { $tmp = "$env:USERPROFILE\AppData\Local\Temp" }

function Resolve-Binary {
    param([string]$Explicit)
    if ($Explicit) { return (Resolve-Path $Explicit).Path }
    $built = Join-Path $repoRoot "build\c\codebase-memory-mcp.exe"
    if (Test-Path $built) { return $built }
    Write-Host "Building $Target via Makefile.cbm ..." -ForegroundColor Cyan
    & $Make "-j" "-f" "Makefile.cbm" $Target "SANITIZE=" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp" | Out-Host
    $buildExit = $LASTEXITCODE
    if ($buildExit -ne 0) { throw "build failed (exit $buildExit)" }
    if (-not (Test-Path $built)) { throw "binary not produced at $built" }
    return $built
}

function Resolve-Launcher {
    param([string]$Explicit)
    if ($Explicit) { return (Resolve-Path $Explicit).Path }
    $built = Join-Path $repoRoot "build\c\codebase-memory-mcp-launcher.exe"
    if (Test-Path $built) { return $built }
    Write-Host "Building permanent launcher via Makefile.cbm ..." -ForegroundColor Cyan
    & $Make "-j" "-f" "Makefile.cbm" "cbm-launcher" "SANITIZE=" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp" | Out-Host
    $buildExit = $LASTEXITCODE
    if ($buildExit -ne 0) { throw "launcher build failed (exit $buildExit)" }
    if (-not (Test-Path $built)) { throw "launcher not produced at $built" }
    return $built
}

function Resolve-AbiMismatchLauncher {
    $built = Join-Path $repoRoot "build\c\codebase-memory-mcp-launcher-abi2.exe"
    if (Test-Path $built) { return $built }
    Write-Host "Building launcher ABI mismatch fixture via Makefile.cbm ..." -ForegroundColor Cyan
    & $Make "-j" "-f" "Makefile.cbm" "build/c/codebase-memory-mcp-launcher-abi2.exe" "SANITIZE=" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp" | Out-Host
    $buildExit = $LASTEXITCODE
    if ($buildExit -ne 0) { throw "launcher ABI fixture build failed (exit $buildExit)" }
    if (-not (Test-Path $built)) { throw "launcher ABI fixture not produced at $built" }
    return $built
}

$bin = Resolve-Binary -Explicit $Binary
$launcherBin = Resolve-Launcher -Explicit $Launcher
$abiMismatchLauncher = Resolve-AbiMismatchLauncher
Write-Host "Payload: $bin" -ForegroundColor Green
Write-Host "Launcher: $launcherBin" -ForegroundColor Green
Write-Host "ABI mismatch fixture: $abiMismatchLauncher" -ForegroundColor Green

$previousTemp = $env:TEMP
$previousTmp = $env:TMP
$previousTmpDir = $env:TMPDIR
$guardRoot = $null
try {
    $userProfile = [Environment]::GetFolderPath([Environment+SpecialFolder]::UserProfile)
    if (-not $userProfile) { throw "could not resolve the current user's profile directory" }
    $guardRoot = Join-Path $userProfile ("cbm-windows-guards-root-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $guardRoot | Out-Null

    # GitHub-hosted runner profile children can inherit mutation-capable ACEs
    # even though the profile ancestry itself passes the launcher's bounded
    # trust policy. Replace that inheritance before creating any executable or
    # Python temporary descendant. Use SIDs rather than localized account names.
    $currentSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
    if (-not $currentSid) { throw "could not resolve the current user's SID" }
    $guardAcl = [System.Security.AccessControl.DirectorySecurity]::new()
    $guardAcl.SetOwner($currentSid)
    $guardAcl.SetAccessRuleProtection($true, $false)
    $guardRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $currentSid,
        [System.Security.AccessControl.FileSystemRights]::FullControl,
        ([System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
            [System.Security.AccessControl.InheritanceFlags]::ObjectInherit),
        [System.Security.AccessControl.PropagationFlags]::None,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    $guardAcl.AddAccessRule($guardRule) | Out-Null
    Set-Acl -LiteralPath $guardRoot -AclObject $guardAcl

    $guardBundle = Join-Path $guardRoot ("cbm-windows-guards-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $guardBundle | Out-Null
    $guardBin = Join-Path $guardBundle "codebase-memory-mcp.exe"
    $guardPayload = Join-Path $guardBundle "codebase-memory-mcp.payload.exe"
    Copy-Item -LiteralPath $launcherBin -Destination $guardBin
    Copy-Item -LiteralPath $bin -Destination $guardPayload

    # Ownership is never inherited on Windows: descendants created under the
    # hardened root by an admin-group token can default to the Administrators
    # SID, and the launcher's exe policy demands the exact current user as
    # owner. Stamp the current SID explicitly on everything staged here.
    foreach ($staged in @($guardBundle, $guardBin, $guardPayload)) {
        $stagedAcl = Get-Acl -LiteralPath $staged
        $stagedAcl.SetOwner($currentSid)
        Set-Acl -LiteralPath $staged -AclObject $stagedAcl
    }
    Write-Host "Guard bundle: $guardBin" -ForegroundColor Green

    # The launcher deliberately rejects GitHub's shared D:\a ancestry and the
    # hosted runner's inherited LocalAppData\Temp ACL. Keep launcher fixtures
    # and Python-created descendants below the accepted profile ancestry.
    $env:TEMP = $guardRoot
    $env:TMP = $guardRoot
    $env:TMPDIR = $guardRoot
    $env:PYTHONUTF8 = "1"           # encode argv/stdio as UTF-8

# Green regression guards - must stay GREEN (exit 0). RED (exit 1) = the fix for
# the referenced issue regressed. The drive-picker guard needs the embedded HTTP
# UI (build target cbm-with-ui); against a non-UI binary it reports a precondition
# (exit 2) and is skipped rather than failed.
$guards = @(
    "tests\windows\test_non_ascii_path.py",
    "tests\windows\test_non_ascii_cache_dump.py",
    "tests\windows\test_daemon_lifecycle.py",
    "tests\windows\test_daemon_stability.py",
    "tests\windows\test_hook_augment.py",
    "tests\windows\test_ui_drive_listing.py",
    "tests\windows\test_cli_non_ascii_arg.py",
    "tests\windows\test_windows_launcher.py"
)

# Opt-in known-red repros - EXPECTED red (exit 1); never gate CI. Currently empty:
# test_cli_non_ascii_arg.py was promoted to a guard when #423/#20's wide-argv fix landed.
$knownReds = @()

$guardFailures = @()
$guardSkips = @()
$fixedKeepers = @()

Write-Host "`n--- Green guards ---" -ForegroundColor Cyan
foreach ($t in $guards) {
    Write-Host "`n=== $t ===" -ForegroundColor Cyan
    if ($t -eq "tests\windows\test_windows_launcher.py") {
        & $py $t $guardBin $guardPayload $abiMismatchLauncher
    } else {
        & $py $t $guardBin
    }
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "GREEN ($t)" -ForegroundColor Green
    } elseif ($code -eq 1 -or $t -eq "tests\windows\test_windows_launcher.py") {
        Write-Host "RED ($t) - REGRESSION: a fixed Windows bug is broken again" -ForegroundColor Red
        $guardFailures += $t
    } elseif ($code -eq 2) {
        # Exit 2 is the guards' DOCUMENTED precondition-skip contract; every
        # other unexpected code (a crashed python, an access-violation status,
        # a mistyped guard) is a FAILURE - an uncontracted exit once let a
        # crashing guard read as an invisible skip under a green banner.
        Write-Host "PRECONDITION ($t) exit=2 - skipped (see message above)" -ForegroundColor Yellow
        $guardSkips += $t
    } else {
        Write-Host "FAILED ($t) exit=$code - crashed or exited outside the guard contract 0/1/2" -ForegroundColor Red
        $guardFailures += $t
    }
}

if (-not $GuardsOnly) {
    Write-Host "`n--- Known reds (opt-in, expected red) ---" -ForegroundColor Cyan
    foreach ($t in $knownReds) {
        Write-Host "`n=== $t ===" -ForegroundColor Cyan
        & $py $t $guardBin
        $code = $LASTEXITCODE
        if ($code -eq 1) {
            Write-Host "RED ($t) - expected; the underlying Windows bug is still open" -ForegroundColor DarkYellow
        } elseif ($code -eq 0) {
            Write-Host "GREEN ($t) - the bug appears FIXED; promote this to a guard" -ForegroundColor Green
            $fixedKeepers += $t
        } else {
            Write-Host "PRECONDITION ($t) exit=$code - skipped (see message above)" -ForegroundColor Yellow
        }
    }
}
} finally {
    $env:TEMP = $previousTemp
    $env:TMP = $previousTmp
    $env:TMPDIR = $previousTmpDir
    if ($guardRoot) {
        Remove-Item -LiteralPath $guardRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host ""
if ($guardSkips.Count -gt 0) {
    Write-Host ("Guards skipped (precondition): {0} - e.g. the drive-picker guard " -f $guardSkips.Count) -ForegroundColor Yellow
    Write-Host "needs a UI build (-Target cbm-with-ui, the default)." -ForegroundColor Yellow
}
if ($fixedKeepers.Count -gt 0) {
    Write-Host ("Known-red repros that are now GREEN (promote to guards): {0}" -f ($fixedKeepers -join ", ")) -ForegroundColor Green
}
if ($guardSkips.Count -eq $guards.Count -and $guards.Count -gt 0) {
    Write-Host "FAIL: every guard skipped - nothing was actually verified" -ForegroundColor Red
    exit 1
}
if ($guardFailures.Count -gt 0) {
    Write-Host ("REGRESSION: {0} green guard(s) went red: {1}" -f $guardFailures.Count, ($guardFailures -join ", ")) -ForegroundColor Red
    Write-Host "A previously-fixed Windows bug is broken again (see the guard's docstring and its referenced issue)." -ForegroundColor Red
    exit 1
}
Write-Host "All Windows green guards passed." -ForegroundColor Green
exit 0
