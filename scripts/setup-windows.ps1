# codebase-memory-mcp setup script (Windows)
# Default: download pre-built native Windows binary
# -FromSource: build from source inside WSL (requires Go + gcc in WSL)

param(
    [switch]$FromSource,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

$Repo = "DeusData/codebase-memory-mcp"
$BinaryName = "codebase-memory-mcp"
$InstallDir = Join-Path $env:LOCALAPPDATA "codebase-memory-mcp"

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
    Write-Host "  Default:      Download pre-built Windows binary"
    Write-Host "  -FromSource:  Build from source inside WSL (requires Go 1.23+ and gcc in WSL)"
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
        Write-Fail "WSL2 is not available. Required for building from source (CGO needs gcc)."
        Write-Host ""
        Write-Host "  Install WSL2:" -ForegroundColor Yellow
        Write-Host "    wsl --install -d Ubuntu"
        Write-Host "  Then restart your computer and run this script again."
        Write-Host ""
        Write-Host "  Alternatively, download a pre-built binary (no WSL needed):" -ForegroundColor Yellow
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

    # Check for gcc
    try {
        Invoke-WSL "command -v gcc" | Out-Null
        Write-Ok "gcc found"
    } catch {
        Write-Warn "gcc not found. Installing build-essential..."
        Invoke-WSL "sudo apt-get update && sudo apt-get install -y build-essential"
    }

    # Check for Go
    try {
        $goVersion = Invoke-WSL "go version"
        Write-Ok "Go: $goVersion"
    } catch {
        Write-Fail "Go not found in WSL."
        Write-Host ""
        Write-Host "  Install Go 1.23+ inside WSL:" -ForegroundColor Yellow
        Write-Host "    See https://go.dev/dl/ for Linux amd64 tarball"
        exit 1
    }

    # Check for git
    try {
        Invoke-WSL "command -v git" | Out-Null
        Write-Ok "git found"
    } catch {
        Write-Fail "git not found in WSL. Install with: sudo apt install git"
        exit 1
    }

    # Clone or update
    Write-Host ""
    $sourceDir = "/home/$wslUser/.local/share/codebase-memory-mcp"
    try {
        Invoke-WSL "test -d $sourceDir/.git" | Out-Null
        Write-Host "Updating source..." -ForegroundColor White
        Invoke-WSL "git -C $sourceDir pull --ff-only"
    } catch {
        Write-Host "Cloning repository..." -ForegroundColor White
        Invoke-WSL "mkdir -p /home/$wslUser/.local/share && git clone https://github.com/$Repo.git $sourceDir"
    }
    Write-Ok "Source at $sourceDir"

    # Build
    Write-Host ""
    Write-Host "Building binary (this may take a minute)..." -ForegroundColor White
    $wslBinaryPath = "/home/$wslUser/.local/bin/$BinaryName"
    Invoke-WSL "mkdir -p /home/$wslUser/.local/bin && cd $sourceDir && CGO_ENABLED=1 go build -buildvcs=false -o $wslBinaryPath ./cmd/codebase-memory-mcp/"
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
    # --- Download pre-built native Windows binary ---
    Write-Host "Fetching latest release..." -ForegroundColor White

    $releaseUrl = "https://api.github.com/repos/$Repo/releases/latest"
    $release = Invoke-RestMethod -Uri $releaseUrl -Headers @{ "User-Agent" = "codebase-memory-mcp-setup" }
    $tag = $release.tag_name

    if (-not $tag) {
        Write-Fail "Could not determine latest release."
        Write-Host "  Check: https://github.com/$Repo/releases"
        exit 1
    }
    Write-Ok "Latest release: $tag"

    $asset = "codebase-memory-mcp-windows-amd64.zip"
    $downloadUrl = "https://github.com/$Repo/releases/download/$tag/$asset"

    Write-Host "Downloading $asset..." -ForegroundColor White

    # Create install directory
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    $tmpZip = Join-Path $env:TEMP $asset
    Invoke-WebRequest -Uri $downloadUrl -OutFile $tmpZip -UseBasicParsing

    # Extract
    Expand-Archive -Path $tmpZip -DestinationPath $InstallDir -Force
    Remove-Item $tmpZip -Force

    $binaryPath = Join-Path $InstallDir "$BinaryName.exe"

    if (-not (Test-Path $binaryPath)) {
        Write-Fail "Binary not found at $binaryPath after extraction"
        exit 1
    }
    Write-Ok "Installed to $binaryPath"

    # Verify binary runs
    try {
        $verOut = & $binaryPath --version 2>&1
        Write-Ok "Version: $verOut"
    } catch {
        Write-Warn "Could not verify binary version (may still work)"
    }

    # SmartScreen note
    Write-Host ""
    Write-Warn "Windows SmartScreen may show a warning when the binary runs for the first time."
    Write-Host "    This is normal for unsigned open-source binaries." -ForegroundColor Yellow
    Write-Host "    Click 'More info' then 'Run anyway' to proceed." -ForegroundColor Yellow
    Write-Host "    Verify checksums at: https://github.com/$Repo/releases" -ForegroundColor Yellow

    # Check if install dir is on PATH
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$InstallDir*") {
        Write-Host ""
        Write-Warn "$InstallDir is not on your PATH."
        $addPath = Read-Host "  Add it to your user PATH? [y/N]"
        if ($addPath -match '^[Yy]$') {
            [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
            Write-Ok "Added to user PATH (restart your terminal to take effect)"
        }
    }

    # Configure Claude Code
    $mcpConfig = [ordered]@{
        type    = "stdio"
        command = $binaryPath
    }

    Configure-ClaudeCode $mcpConfig

    Write-Host ""
    Write-Ok "Done! Restart Claude Code and verify with /mcp"
    Write-Host ""
    Write-Host "  To uninstall:" -ForegroundColor White
    Write-Host "    Remove-Item -Recurse -Force '$InstallDir'"
    Write-Host "    Remove-Item -Recurse -Force `"$env:LOCALAPPDATA\codebase-memory-mcp`"  # graph database"
}
