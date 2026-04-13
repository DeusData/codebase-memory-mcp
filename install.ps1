# install.ps1 — One-line installer for codebase-memory-mcp (Windows).
#
# Usage:
#   powershell -ExecutionPolicy ByPass -c "irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.ps1 | iex"
#
# Environment:
#   CBM_DOWNLOAD_URL  Override base URL for downloads (for testing)

$ErrorActionPreference = "Stop"

$Repo = "DeusData/codebase-memory-mcp"
$InstallDir = "$env:LOCALAPPDATA\Programs\codebase-memory-mcp"
$BinName = "codebase-memory-mcp.exe"
$BaseUrl = if ($env:CBM_DOWNLOAD_URL) { $env:CBM_DOWNLOAD_URL } else { "https://github.com/$Repo/releases/latest/download" }

# Detect variant from args (--ui or --standard)
$Variant = "standard"
$SkipConfig = $false
foreach ($arg in $args) {
    if ($arg -eq "--ui") { $Variant = "ui" }
    if ($arg -eq "--standard") { $Variant = "standard" }
    if ($arg -eq "--skip-config") { $SkipConfig = $true }
    if ($arg -like "--dir=*") { $InstallDir = $arg.Substring(6) }
}

Write-Host "codebase-memory-mcp installer (Windows)"
Write-Host "  variant: $Variant"
Write-Host "  target:  $InstallDir\$BinName"
Write-Host ""

# Build download URL
if ($Variant -eq "ui") {
    $Archive = "codebase-memory-mcp-ui-windows-amd64.zip"
} else {
    $Archive = "codebase-memory-mcp-windows-amd64.zip"
}
$Url = "$BaseUrl/$Archive"

# Download
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "cbm-install-$(Get-Random)"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

Write-Host "Downloading $Archive..."
try {
    Invoke-WebRequest -Uri $Url -OutFile "$TmpDir\$Archive" -UseBasicParsing
} catch {
    Write-Host "error: download failed: $_" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}


# Checksum verification
$ChecksumUrl = "$BaseUrl/checksums.txt"
try {
    Invoke-WebRequest -Uri $ChecksumUrl -OutFile "$TmpDir\checksums.txt" -UseBasicParsing
    $checksumLine = Get-Content "$TmpDir\checksums.txt" | Where-Object { $_ -like "*$Archive*" }
    if ($checksumLine) {
        $expected = ($checksumLine -split '\s+')[0]
        $actual = (Get-FileHash -Path "$TmpDir\$Archive" -Algorithm SHA256).Hash.ToLower()
        if ($expected -ne $actual) {
            Write-Host "error: CHECKSUM MISMATCH!" -ForegroundColor Red
            Write-Host "  expected: $expected"
            Write-Host "  actual:   $actual"
            Remove-Item -Recurse -Force $TmpDir
            exit 1
        }
        Write-Host "Checksum verified."
    }
} catch {
    Write-Host "warning: could not verify checksum (non-fatal)"
}

# Extract
Write-Host "Extracting..."
Expand-Archive -Path "$TmpDir\$Archive" -DestinationPath $TmpDir -Force

$DlBin = Join-Path $TmpDir $BinName
if (-not (Test-Path $DlBin)) {
    # UI variant may have different name in zip
    $UiBin = Join-Path $TmpDir "codebase-memory-mcp-ui.exe"
    if (Test-Path $UiBin) {
        Rename-Item $UiBin $BinName
        $DlBin = Join-Path $TmpDir $BinName
    } else {
        Write-Host "error: binary not found after extraction" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
}

# Remove MOTW from extracted binary
Unblock-File -Path $DlBin -ErrorAction SilentlyContinue

# Install
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

Copy-Item $DlBin $Dest -Force
Unblock-File -Path $Dest -ErrorAction SilentlyContinue

# Verify
try {
    $ver = & $Dest --version 2>&1
    Write-Host "Installed: $ver"
} catch {
    Write-Host "error: installed binary failed to run" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir
    exit 1
}

# Configure agents
if ($SkipConfig) {
    Write-Host ""
    Write-Host "Skipping agent configuration (--skip-config)"
} else {
    Write-Host ""
    Write-Host "Configuring coding agents..."
    try {
        & $Dest install -y 2>&1 | Write-Host
    } catch {
        Write-Host "Agent configuration failed (non-fatal)."
        Write-Host "Run manually: codebase-memory-mcp install"
    }
}

# Add to PATH (user scope, no admin needed)
$UserPath = [Environment]::GetEnvironmentVariable("PATH", "User")
if ($UserPath -notlike "*$InstallDir*") {
    [Environment]::SetEnvironmentVariable("PATH", "$UserPath;$InstallDir", "User")
    $env:PATH = "$env:PATH;$InstallDir"
    Write-Host "Added $InstallDir to user PATH"
}

# Cleanup
Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Done! Restart your terminal and coding agent to start using codebase-memory-mcp."
