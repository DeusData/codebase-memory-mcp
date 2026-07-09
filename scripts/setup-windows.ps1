# codebase-memory-mcp setup script (Windows)
# Default: build natively from this repository's sources (delegates to
#          install.ps1 — requires the MSYS2 MinGW toolchain: gcc + make)
# -FromSource: build inside WSL from this repository's sources (requires gcc
#              and make in WSL; the MCP entry then runs through wsl.exe)
#
# No downloads — both modes build from the local checkout.

param(
    [switch]$FromSource,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

$BinaryName = "codebase-memory-mcp"
$RepoRoot = Split-Path -Parent $PSScriptRoot

# --- Helpers ---

function Write-Ok($msg)   { Write-Host "  $msg" -ForegroundColor Green }
function Write-Fail($msg)  { Write-Host "  $msg" -ForegroundColor Red }
function Write-Warn($msg)  { Write-Host "  $msg" -ForegroundColor Yellow }

function Read-SettingsJson($Path) {
    # PS5.1-compatible: ConvertFrom-Json returns PSCustomObject, not Hashtable.
    # We convert to ordered hashtable manually.
    if (-not (Test-Path $Path)) {
        return @{}
    }
    $raw = Get-Content $Path -Raw
    if (-not $raw -or $raw.Trim() -eq "") {
        return @{}
    }
    $obj = $raw | ConvertFrom-Json
    $ht = [ordered]@{}
    foreach ($prop in $obj.PSObject.Properties) {
        if ($prop.Value -is [System.Management.Automation.PSCustomObject]) {
            $inner = [ordered]@{}
            foreach ($p in $prop.Value.PSObject.Properties) {
                $inner[$p.Name] = $p.Value
            }
            $ht[$prop.Name] = $inner
        } else {
            $ht[$prop.Name] = $prop.Value
        }
    }
    return $ht
}

function Write-SettingsJson($Path, $Settings) {
    # Back up existing file before writing
    if (Test-Path $Path) {
        Copy-Item $Path "$Path.bak" -Force
    }
    $Settings | ConvertTo-Json -Depth 10 | Set-Content $Path -Encoding UTF8
}

function Configure-ClaudeCode($McpConfig) {
    Write-Host ""
    $answer = Read-Host "Configure Claude Code to use codebase-memory-mcp? [y/N]"

    if ($answer -match '^[Yy]$') {
        $settingsPath = Join-Path $env:USERPROFILE ".claude\settings.json"
        $settingsDir = Split-Path $settingsPath -Parent

        if (-not (Test-Path $settingsDir)) {
            New-Item -ItemType Directory -Path $settingsDir -Force | Out-Null
        }

        $settings = Read-SettingsJson $settingsPath

        if (-not $settings.Contains("mcpServers")) {
            $settings["mcpServers"] = [ordered]@{}
        }

        $settings["mcpServers"]["codebase-memory-mcp"] = $McpConfig
        Write-SettingsJson $settingsPath $settings
        Write-Ok "Updated $settingsPath"
    } else {
        Write-Host ""
        Write-Host "  Add this to your .mcp.json or %USERPROFILE%\.claude\settings.json:" -ForegroundColor White
        Write-Host ""
        $snippet = @{ mcpServers = @{ "codebase-memory-mcp" = $McpConfig } }
        $snippet | ConvertTo-Json -Depth 10 | Write-Host
    }
}

function Test-WSL {
    try {
        $null = wsl.exe --status 2>&1
        if ($LASTEXITCODE -ne 0) { return $false }
        return $true
    } catch {
        return $false
    }
}

function Get-WSLDistro {
    $output = wsl.exe -l -v 2>&1 | Out-String
    $lines = $output -split "`n" | Where-Object { $_ -match '\S' } | Select-Object -Skip 1
    foreach ($line in $lines) {
        $clean = $line -replace '\x00', '' -replace '^\s+', ''
        if ($clean -match '^\*?\s*(\S+)\s+') {
            $name = $Matches[1]
            if ($name -ne "NAME" -and $name -ne "") {
                return $name
            }
        }
    }
    return $null
}

function Invoke-WSL {
    param([string]$Command)
    $result = wsl.exe -- bash -c $Command 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "WSL command failed: $Command`n$result"
    }
    return $result
}

# --- Main ---

if ($Help) {
    Write-Host ""
    Write-Host "Usage: .\setup-windows.ps1 [-FromSource] [-Help]"
    Write-Host ""
    Write-Host "  Default:      Build natively from this repo (MSYS2 gcc + make; runs install.ps1)"
    Write-Host "  -FromSource:  Build inside WSL from this repo (requires gcc and make in WSL)"
    Write-Host ""
    exit 0
}

Write-Host ""
Write-Host "codebase-memory-mcp installer (Windows)" -ForegroundColor White
Write-Host ""

if ($FromSource) {
    # --- Build from source via WSL ---
    Write-Host "Checking WSL2 (required for building from source)..." -ForegroundColor White

    if (-not (Test-WSL)) {
        Write-Fail "WSL2 is not available. Required for the -FromSource (WSL) build."
        Write-Host ""
        Write-Host "  Install WSL2:" -ForegroundColor Yellow
        Write-Host "    wsl --install -d Ubuntu"
        Write-Host "  Then restart your computer and run this script again."
        Write-Host ""
        Write-Host "  Alternatively, build natively with MSYS2 (no WSL needed):" -ForegroundColor Yellow
        Write-Host "    .\setup-windows.ps1"
        exit 1
    }
    Write-Ok "WSL2 is available"

    $distro = Get-WSLDistro
    if (-not $distro) {
        Write-Fail "No WSL Linux distribution found."
        Write-Host ""
        Write-Host "  Install Ubuntu:" -ForegroundColor Yellow
        Write-Host "    wsl --install -d Ubuntu"
        exit 1
    }
    Write-Ok "WSL distro: $distro"

    $wslUser = (Invoke-WSL "whoami").Trim()
    Write-Ok "WSL user: $wslUser"

    Write-Host ""
    Write-Host "Checking prerequisites inside WSL..." -ForegroundColor White

    # Check for gcc + make
    try {
        Invoke-WSL "command -v gcc && command -v make" | Out-Null
        Write-Ok "gcc + make found"
    } catch {
        Write-Warn "gcc/make not found. Installing build-essential..."
        Invoke-WSL "sudo apt-get update && sudo apt-get install -y build-essential"
    }

    # Copy THIS repository's sources into the WSL filesystem (building on
    # /mnt/c is an order of magnitude slower than on ext4). tar over the
    # pipe avoids permission-bit loss; .git/build/node_modules are skipped.
    Write-Host ""
    $sourceDir = "/home/$wslUser/.local/share/codebase-memory-mcp"
    $mntRepo = (Invoke-WSL "wslpath -a '$($RepoRoot -replace '\\', '/')'").Trim()
    Write-Host "Copying sources into WSL ($sourceDir)..." -ForegroundColor White
    Invoke-WSL "rm -rf $sourceDir && mkdir -p $sourceDir && tar -C '$mntRepo' --exclude=.git --exclude=build --exclude=graph-ui/node_modules -cf - . | tar -C $sourceDir -xf -"
    Write-Ok "Source at $sourceDir"

    # Build
    Write-Host ""
    Write-Host "Building binary (this may take a minute)..." -ForegroundColor White
    $wslBinaryPath = "/home/$wslUser/.local/bin/$BinaryName"
    Invoke-WSL "mkdir -p /home/$wslUser/.local/bin && cd $sourceDir && scripts/build.sh && cp build/c/codebase-memory-mcp $wslBinaryPath"
    Write-Ok "Built to $wslBinaryPath (inside WSL)"

    # Verify
    try {
        Invoke-WSL "test -x $wslBinaryPath" | Out-Null
        Write-Ok "Binary is executable"
    } catch {
        Write-Fail "Binary at $wslBinaryPath is not executable"
        exit 1
    }

    # Configure — WSL binary needs wsl.exe wrapper
    $mcpConfig = [ordered]@{
        type    = "stdio"
        command = "wsl.exe"
        args    = @("-d", $distro, "--", $wslBinaryPath)
    }

    Configure-ClaudeCode $mcpConfig

    Write-Host ""
    Write-Ok "Done! Restart Claude Code and verify with /mcp"
    Write-Host ""
    Write-Host "  To uninstall:" -ForegroundColor White
    Write-Host "    wsl.exe -- rm $wslBinaryPath"
    Write-Host "    wsl.exe -- rm -rf $sourceDir"
    Write-Host "    wsl.exe -- rm -rf ~/.cache/codebase-memory-mcp/"

} else {
    # --- Native build from this repository (delegates to install.ps1) ---
    $installer = Join-Path $RepoRoot "install.ps1"
    if (-not (Test-Path $installer)) {
        Write-Fail "install.ps1 not found at $installer"
        exit 1
    }
    Write-Host "Building natively from $RepoRoot (via install.ps1)..." -ForegroundColor White
    & powershell.exe -ExecutionPolicy ByPass -File $installer @args
    exit $LASTEXITCODE
}
