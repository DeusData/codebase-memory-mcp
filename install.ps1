# install.ps1 — Build codebase-memory-mcp from THIS repository's sources and
# install the resulting binary (Windows). No downloads: everything needed for
# the standard build is vendored in the repo, so the build runs fully offline.
#
# Usage (from the repository root):
#   powershell -ExecutionPolicy ByPass -File install.ps1
#   powershell -ExecutionPolicy ByPass -File install.ps1 --ui
#   powershell -ExecutionPolicy ByPass -File install.ps1 --dir=C:\tools\cbm
#   powershell -ExecutionPolicy ByPass -File install.ps1 --skip-config
#
# Requirements: MinGW-w64 gcc/g++ and GNU make — the MSYS2 toolchain
# (install MSYS2, then: pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-g++ make).
# The --ui variant additionally requires node + npm on PATH.

$ErrorActionPreference = "Stop"

$RepoRoot = $PSScriptRoot
$InstallDir = "$env:LOCALAPPDATA\Programs\codebase-memory-mcp"
$BinName = "codebase-memory-mcp.exe"

# Parse args (--ui / --standard / --dir=<path> / --skip-config)
$Variant = "standard"
$SkipConfig = $false
foreach ($arg in $args) {
    if ($arg -eq "--ui") { $Variant = "ui" }
    if ($arg -eq "--standard") { $Variant = "standard" }
    if ($arg -eq "--skip-config") { $SkipConfig = $true }
    if ($arg -like "--dir=*") { $InstallDir = $arg.Substring(6) }
}

# ── Toolchain check ─────────────────────────────────────────────
# GNU make: PATH first, then the standard MSYS2 location.
$Make = $null
$makeCmd = Get-Command "make" -ErrorAction SilentlyContinue
if ($makeCmd) {
    $Make = $makeCmd.Source
} elseif (Test-Path "C:\msys64\usr\bin\make.exe") {
    $Make = "C:\msys64\usr\bin\make.exe"
}
if (-not $Make) {
    Write-Host "error: GNU make not found (PATH or C:\msys64\usr\bin\make.exe)" -ForegroundColor Red
    Write-Host "  Install the MSYS2 toolchain: pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-g++ make"
    exit 1
}

# gcc: PATH first, then the standard MSYS2 MinGW64 location (prepend to PATH
# so make's compiler invocations resolve).
$gccCmd = Get-Command "gcc" -ErrorAction SilentlyContinue
if (-not $gccCmd) {
    if (Test-Path "C:\msys64\mingw64\bin\gcc.exe") {
        $env:PATH = "C:\msys64\mingw64\bin;C:\msys64\usr\bin;$env:PATH"
    } else {
        Write-Host "error: gcc not found (PATH or C:\msys64\mingw64\bin\gcc.exe)" -ForegroundColor Red
        Write-Host "  Install the MSYS2 toolchain: pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-g++ make"
        exit 1
    }
}

if ($Variant -eq "ui") {
    $nodeCmd = Get-Command "node" -ErrorAction SilentlyContinue
    $npmCmd = Get-Command "npm" -ErrorAction SilentlyContinue
    if (-not $nodeCmd -or -not $npmCmd) {
        Write-Host "error: the --ui variant requires node and npm on PATH" -ForegroundColor Red
        exit 1
    }
}

Write-Host "codebase-memory-mcp installer (build from source, Windows)"
Write-Host "  repo:    $RepoRoot"
Write-Host "  variant: $Variant"
Write-Host "  make:    $Make"
Write-Host "  target:  $InstallDir\$BinName"
Write-Host ""

# ── Build ───────────────────────────────────────────────────────
# Same targets scripts/test-windows.ps1 uses for native Windows builds.
# MinGW/LLVM on Windows ships no libasan/libubsan — disable sanitizers.
$Target = "cbm"
if ($Variant -eq "ui") { $Target = "cbm-with-ui" }

# A writable Windows temp dir that GNU make forwards to the native gcc. MSYS2
# strips TMP/TEMP from the environment it hands native children, so pass them
# as make command-line variables (same invocation as scripts/test-windows.ps1).
$tmp = $env:TEMP
if (-not $tmp) { $tmp = "$env:USERPROFILE\AppData\Local\Temp" }

Write-Host "Building ($Target)..."
Push-Location $RepoRoot
try {
    & $Make "-j" "-f" "Makefile.cbm" $Target "SANITIZE=" "TMP=$tmp" "TEMP=$tmp" "TMPDIR=$tmp"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "error: build failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

$BuiltBin = Join-Path $RepoRoot "build\c\codebase-memory-mcp.exe"
if (-not (Test-Path $BuiltBin)) {
    # Some make setups produce the binary without .exe
    $alt = Join-Path $RepoRoot "build\c\codebase-memory-mcp"
    if (Test-Path $alt) {
        $BuiltBin = $alt
    } else {
        Write-Host "error: build finished but binary not found at $BuiltBin" -ForegroundColor Red
        exit 1
    }
}

# ── Install ─────────────────────────────────────────────────────
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
$Dest = Join-Path $InstallDir $BinName

# Handle replace-if-running (rename-aside)
if (Test-Path $Dest) {
    $OldDest = "$Dest.old"
    Remove-Item $OldDest -Force -ErrorAction SilentlyContinue
    try {
        Rename-Item $Dest $OldDest -ErrorAction Stop
    } catch {
        Write-Host "warning: could not rename existing binary (may be in use)"
    }
}

Copy-Item $BuiltBin $Dest -Force

# ── Verify ──────────────────────────────────────────────────────
try {
    $ver = & $Dest --version 2>&1
    Write-Host "Installed: $ver"
} catch {
    Write-Host "error: installed binary failed to run" -ForegroundColor Red
    exit 1
}

# ── Configure agents ────────────────────────────────────────────
if ($SkipConfig) {
    Write-Host ""
    Write-Host "Skipping agent configuration (--skip-config)"
} else {
    Write-Host ""
    Write-Host "Configuring coding agents..."
    & $Dest install -y 2>&1 | Write-Host
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "error: agent configuration failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        Write-Host "The binary was installed, but no coding agents were configured."
        Write-Host "Run manually to configure: `"$Dest`" install"
        exit 1
    }
}

# ── Add to PATH (user scope, no admin needed) ───────────────────
$UserPath = [Environment]::GetEnvironmentVariable("PATH", "User")
if ($UserPath -notlike "*$InstallDir*") {
    [Environment]::SetEnvironmentVariable("PATH", "$UserPath;$InstallDir", "User")
    $env:PATH = "$env:PATH;$InstallDir"
    Write-Host "Added $InstallDir to user PATH"
}

Write-Host ""
Write-Host "Done! Restart your terminal and coding agent to start using codebase-memory-mcp."
