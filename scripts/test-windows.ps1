<#
.SYNOPSIS
    Run the native-Windows red-test suite for codebase-memory-mcp.

.DESCRIPTION
    Builds the production binary (build/c/codebase-memory-mcp.exe) if it is not
    already present, then runs the deterministic Windows red tests under
    tests/windows/. These tests reproduce platform-specific failures at the
    product surface (real MCP process, real stdio, real SQLite DB).

    The unit/invariant C suite is built and run via Makefile.cbm. On native
    Windows the MinGW/LLVM toolchain ships no libasan/libubsan, so the sanitizer
    flags must be disabled for the local build (SANITIZE=). Where the toolchain
    *does* provide AddressSanitizer/UBSan (Linux containers, WSL), prefer
    scripts/test.sh which keeps the sanitizers on.

.PARAMETER Binary
    Path to an existing codebase-memory-mcp.exe. If omitted, the script looks for
    build/c/codebase-memory-mcp.exe and builds it when missing.

.PARAMETER Make
    Path to GNU make (default: 'make' on PATH; MSYS2 ships it at
    C:\msys64\usr\bin\make.exe).

.EXAMPLE
    pwsh -File scripts/test-windows.ps1
#>
[CmdletBinding()]
param(
    [string]$Binary,
    [string]$Make = "make"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $python) { $python = (Get-Command py -ErrorAction SilentlyContinue) }
if (-not $python) { throw "Python 3 is required to run the Windows red tests." }
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
    Write-Host "Building production binary via Makefile.cbm ..." -ForegroundColor Cyan
    & $Make "-j" "-f" "Makefile.cbm" "cbm" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp"
    if ($LASTEXITCODE -ne 0) { throw "build failed (exit $LASTEXITCODE)" }
    if (-not (Test-Path $built)) { throw "binary not produced at $built" }
    return $built
}

$bin = Resolve-Binary -Explicit $Binary
Write-Host "Binary: $bin" -ForegroundColor Green

$env:PYTHONUTF8 = "1"   # ensure the harness encodes argv/stdio as UTF-8

# test_ui_drive_listing.py reproduces the UI directory-picker bug (#548) and
# therefore needs a UI build (make -f Makefile.cbm cbm-with-ui) plus a machine
# with more than one drive. Against a non-UI binary it reports a precondition
# (exit 2), which is treated as a skip-with-reason, not a failure.
$tests = @(
    "tests\windows\test_non_ascii_path.py",
    "tests\windows\test_cli_non_ascii_arg.py",
    "tests\windows\test_hook_augment.py",
    "tests\windows\test_ui_drive_listing.py"
)

$reds = @()
$precond = @()
foreach ($t in $tests) {
    Write-Host "`n=== $t ===" -ForegroundColor Cyan
    & $py $t $bin
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "GREEN ($t)" -ForegroundColor Green
    } elseif ($code -eq 1) {
        Write-Host "RED ($t) - Windows-specific failure reproduced" -ForegroundColor Red
        $reds += $t
    } else {
        Write-Host "PRECONDITION ($t) exit=$code - skipped (see message above)" -ForegroundColor Yellow
        $precond += $t
    }
}

Write-Host ""
if ($precond.Count -gt 0) {
    Write-Host ("Precondition-skipped: {0} (e.g. test_ui_drive_listing needs a UI " -f $precond.Count) -ForegroundColor Yellow
    Write-Host "build: make -f Makefile.cbm cbm-with-ui, and >1 drive)." -ForegroundColor Yellow
}
if ($reds.Count -gt 0) {
    Write-Host ("RED suite: {0} Windows red tests reproduced platform failures " -f $reds.Count) -ForegroundColor Red
    Write-Host "(expected until fixed). See tests/windows/RED_TEST_ANALYSIS.md." -ForegroundColor Red
    exit 1
}
Write-Host "All runnable Windows red tests are GREEN." -ForegroundColor Green
exit 0
