<#
.SYNOPSIS
    Run the native-Windows product-surface test suite for codebase-memory-mcp.

.DESCRIPTION
    Builds the product binary (build/c/codebase-memory-mcp.exe) if it is not
    already present, then runs the deterministic Windows integration tests under
    tests/windows/ against a real codebase-memory-mcp.exe (real stdio / CLI /
    HTTP UI, real SQLite DB).

    Two categories of test:

      GUARDS      - regression guards for Windows bugs already fixed on main.
                    They must stay GREEN (exit 0); a RED (exit 1) means the fix
                    regressed and fails this runner.
                      * test_non_ascii_path.py    guards #636/#357 (fixed by #700)
                      * test_non_ascii_cache_dump.py guards #996 (writer cbm_fopen)
                      * test_hook_augment.py      guards #618      (fixed by #619)
                      * test_ui_drive_listing.py  guards #548      (roots field)
                      * test_cli_non_ascii_arg.py guards #423/#20  (wide-argv main())

      KNOWN REDS  - genuine, still-open Windows bugs reproduced at the product
                    surface. They are EXPECTED to be RED (exit 1) and are opt-in
                    (never gate CI). If one turns GREEN the underlying bug was
                    fixed and it should be promoted to a guard.
                      * (none currently - test_cli_non_ascii_arg.py was promoted to a
                        guard when the wide-argv fix for #423/#20 landed)

    Determinism: the runner sets CBM_INDEX_SUPERVISOR=0 so the path / hook / drive
    guards index in-process (the pass-level readers under test, e.g. #700's cbm_fopen
    routing, run in-process either way). The non-ASCII CLI guard is the exception - it
    drops that override to cross the real supervisor -> worker spawn, where the second
    half of #423/#20 lives (CreateProcessW delivering the wide command line).

    On native Windows the MinGW/LLVM toolchain ships no libasan/libubsan, so the
    build disables sanitizers (SANITIZE=). Where the toolchain provides
    AddressSanitizer/UBSan (Linux containers, WSL), prefer scripts/test.sh.

.PARAMETER Binary
    Path to an existing codebase-memory-mcp.exe. If omitted, the script builds it
    (target selected by -Target) into build/c/.

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
    & $Make "-j" "-f" "Makefile.cbm" $Target "SANITIZE=" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp"
    if ($LASTEXITCODE -ne 0) { throw "build failed (exit $LASTEXITCODE)" }
    if (-not (Test-Path $built)) { throw "binary not produced at $built" }
    return $built
}

$bin = Resolve-Binary -Explicit $Binary
Write-Host "Binary: $bin" -ForegroundColor Green

$env:PYTHONUTF8 = "1"           # encode argv/stdio as UTF-8
$env:CBM_INDEX_SUPERVISOR = "0" # in-process indexing (see .DESCRIPTION)

# Green regression guards - must stay GREEN (exit 0). RED (exit 1) = the fix for
# the referenced issue regressed. The drive-picker guard needs the embedded HTTP
# UI (build target cbm-with-ui); against a non-UI binary it reports a precondition
# (exit 2) and is skipped rather than failed.
$guards = @(
    "tests\windows\test_non_ascii_path.py",
    "tests\windows\test_non_ascii_cache_dump.py",
    "tests\windows\test_hook_augment.py",
    "tests\windows\test_ui_drive_listing.py",
    "tests\windows\test_cli_non_ascii_arg.py"
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
    & $py $t $bin
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "GREEN ($t)" -ForegroundColor Green
    } elseif ($code -eq 1) {
        Write-Host "RED ($t) - REGRESSION: a fixed Windows bug is broken again" -ForegroundColor Red
        $guardFailures += $t
    } else {
        Write-Host "PRECONDITION ($t) exit=$code - skipped (see message above)" -ForegroundColor Yellow
        $guardSkips += $t
    }
}

if (-not $GuardsOnly) {
    Write-Host "`n--- Known reds (opt-in, expected red) ---" -ForegroundColor Cyan
    foreach ($t in $knownReds) {
        Write-Host "`n=== $t ===" -ForegroundColor Cyan
        & $py $t $bin
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

Write-Host ""
if ($guardSkips.Count -gt 0) {
    Write-Host ("Guards skipped (precondition): {0} - e.g. the drive-picker guard " -f $guardSkips.Count) -ForegroundColor Yellow
    Write-Host "needs a UI build (-Target cbm-with-ui, the default)." -ForegroundColor Yellow
}
if ($fixedKeepers.Count -gt 0) {
    Write-Host ("Known-red repros that are now GREEN (promote to guards): {0}" -f ($fixedKeepers -join ", ")) -ForegroundColor Green
}
if ($guardFailures.Count -gt 0) {
    Write-Host ("REGRESSION: {0} green guard(s) went red: {1}" -f $guardFailures.Count, ($guardFailures -join ", ")) -ForegroundColor Red
    Write-Host "A previously-fixed Windows bug is broken again (see the guard's docstring and its referenced issue)." -ForegroundColor Red
    exit 1
}
Write-Host "All Windows green guards passed." -ForegroundColor Green
exit 0
