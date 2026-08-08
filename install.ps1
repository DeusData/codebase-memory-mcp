# install.ps1 - One-line installer for codebase-memory-mcp (Windows).
#
# Usage: see README.md for install instructions.
#
# Environment:
#   CBM_DOWNLOAD_URL  Override base URL for downloads (for testing)

$ErrorActionPreference = "Stop"

# Enforce TLS 1.2+ (older PowerShell defaults to TLS 1.0 which GitHub rejects)
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13
Add-Type -AssemblyName System.Net.Http

$Repo = "DeusData/codebase-memory-mcp"
$InstallDir = "$env:LOCALAPPDATA\Programs\codebase-memory-mcp"
$BinName = "codebase-memory-mcp.exe"
$WindowsArchiveNames = @(
    $BinName,
    "cbm-integrations.json",
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md"
)
$UiPackPattern = '^cbm-ui-[0-9a-f]{64}\.pack$'
$BaseUrl = if ($env:CBM_DOWNLOAD_URL) { $env:CBM_DOWNLOAD_URL } else { "https://github.com/$Repo/releases/latest/download" }

try { $BaseUri = [Uri]$BaseUrl } catch { $BaseUri = $null }
$AllowLoopbackHttp = (
    $BaseUri -and $BaseUri.IsAbsoluteUri -and
    $BaseUri.Scheme -eq "http" -and $BaseUri.IsLoopback -and
    [string]::IsNullOrEmpty($BaseUri.UserInfo)
)
if (-not $BaseUri -or -not $BaseUri.IsAbsoluteUri -or
    ($BaseUri.Scheme -ne "https" -and -not $AllowLoopbackHttp) -or
    -not [string]::IsNullOrEmpty($BaseUri.UserInfo)) {
    Write-Host "error: refusing non-HTTPS download URL: $BaseUrl" -ForegroundColor Red
    exit 1
}

function Invoke-CbmDownload {
    param([Parameter(Mandatory=$true)][string]$Url,
          [Parameter(Mandatory=$true)][string]$OutFile)

    $current = [Uri]$Url
    $handler = New-Object System.Net.Http.HttpClientHandler
    $handler.AllowAutoRedirect = $false
    $client = New-Object -TypeName System.Net.Http.HttpClient -ArgumentList $handler
    $client.Timeout = [TimeSpan]::FromMinutes(10)
    try {
        for ($redirects = 0; $redirects -le 5; $redirects++) {
            $allowed = $current.IsAbsoluteUri -and
                [string]::IsNullOrEmpty($current.UserInfo) -and
                ($current.Scheme -eq "https" -or
                 ($AllowLoopbackHttp -and $current.Scheme -eq "http" -and
                  $current.IsLoopback))
            if (-not $allowed) {
                throw "download redirect escaped the allowed transport: $current"
            }
            $response = $client.GetAsync(
                $current, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            ).GetAwaiter().GetResult()
            try {
                $status = [int]$response.StatusCode
                if ($status -in @(301, 302, 303, 307, 308)) {
                    if ($redirects -eq 5 -or -not $response.Headers.Location) {
                        throw "invalid or excessive download redirect from $current"
                    }
                    $current = [Uri]::new($current, $response.Headers.Location)
                    continue
                }
                if (-not $response.IsSuccessStatusCode) {
                    throw "HTTP $status for $current"
                }
                $input = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
                try {
                    $output = [System.IO.File]::Open(
                        $OutFile, [System.IO.FileMode]::Create,
                        [System.IO.FileAccess]::Write,
                        [System.IO.FileShare]::None)
                    try { $input.CopyTo($output) } finally { $output.Dispose() }
                } finally { $input.Dispose() }
                return
            } finally { $response.Dispose() }
        }
        throw "too many download redirects"
    } finally {
        $client.Dispose()
        $handler.Dispose()
    }
}

function New-CbmExclusiveSiblingTemp {
    param([Parameter(Mandatory=$true)][string]$Destination)

    $directory = [System.IO.Path]::GetDirectoryName($Destination)
    $leaf = [System.IO.Path]::GetFileName($Destination)
    for ($attempt = 0; $attempt -lt 32; $attempt++) {
        $random = [System.IO.Path]::GetRandomFileName()
        $candidate = Join-Path $directory ".$leaf.tmp-$random"
        try {
            $reservation = [System.IO.File]::Open(
                $candidate,
                [System.IO.FileMode]::CreateNew,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::None)
            $reservation.Dispose()
            return $candidate
        } catch [System.IO.IOException] {
            # A collision belongs to someone else. Never remove it; choose a
            # fresh unpredictable sibling and reserve that path exclusively.
        }
    }
    throw "could not reserve an exclusive temporary sibling for $Destination"
}

function New-CbmExclusiveTempDirectory {
    param([Parameter(Mandatory=$true)][string]$ParentDirectory)

    for ($attempt = 0; $attempt -lt 32; $attempt++) {
        $candidate = Join-Path $ParentDirectory (
            "cbm-install-" + [guid]::NewGuid().ToString("N")
        )
        try {
            # Without -Force, an existing path is never adopted. Only return a
            # directory successfully created by this installer invocation.
            New-Item -ItemType Directory -Path $candidate -ErrorAction Stop | Out-Null
            return $candidate
        } catch [System.IO.IOException] {
            # A collision belongs to someone else. Never remove it; choose a
            # fresh unpredictable name and try again within the fixed bound.
        }
    }
    throw "could not reserve an exclusive installer temporary directory"
}

# Detect variant from args (--ui or --standard)
$Variant = "standard"
$SkipConfig = $false
foreach ($arg in $args) {
    if ($arg -eq "--ui") { $Variant = "ui" }
    if ($arg -eq "--standard") { $Variant = "standard" }
    if ($arg -eq "--skip-config") { $SkipConfig = $true }
    if ($arg -like "--dir=*") { $InstallDir = $arg.Substring(6) }
}

# Detect the OS architecture. RuntimeInformation.OSArchitecture reports the real
# OS arch (Arm64) even from an x64 process running under emulation on ARM64 --
# unlike $env:PROCESSOR_ARCHITECTURE, which reports the emulated "AMD64", and
# PROCESSOR_ARCHITEW6432, which is unset for 64-bit emulated processes. Fall back
# to the env vars only if the .NET API is somehow unavailable.
if ($env:CBM_ARCH) {
    # Explicit override wins - used by CI/tests, and an escape hatch under x64
    # emulation on ARM64 where no in-process detection is reliable.
    $Arch = $env:CBM_ARCH
} else {
    try {
        $osArch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
        $Arch = if ($osArch -eq 'Arm64') { "arm64" } else { "amd64" }
    } catch {
        if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64" -or $env:PROCESSOR_ARCHITEW6432 -eq "ARM64") {
            $Arch = "arm64"
        } else {
            $Arch = "amd64"
        }
    }
}

Write-Host "codebase-memory-mcp installer (Windows)"
Write-Host "  variant: $Variant"
Write-Host "  arch:    $Arch"
Write-Host "  target:  $InstallDir\$BinName"
Write-Host ""

# Build download URL
if ($Variant -eq "ui") {
    $Archive = "codebase-memory-mcp-ui-windows-$Arch.zip"
} else {
    $Archive = "codebase-memory-mcp-windows-$Arch.zip"
}
$Url = "$BaseUrl/$Archive"

# Download
$TmpDir = New-CbmExclusiveTempDirectory -ParentDirectory ([System.IO.Path]::GetTempPath())

Write-Host "Downloading $Archive..."
try {
    Invoke-CbmDownload -Url $Url -OutFile "$TmpDir\$Archive"
} catch {
    Write-Host "error: download failed: $_" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}


# Checksum verification is mandatory. Do not request coordinated shutdown for
# a candidate that was not positively matched to the published release digest.
$ChecksumUrl = "$BaseUrl/checksums.txt"
try {
    Invoke-CbmDownload -Url $ChecksumUrl -OutFile "$TmpDir\checksums.txt"
    $checksumPath = "$TmpDir\checksums.txt"
    if ((Get-Item -LiteralPath $checksumPath).Length -gt 1048576) {
        throw "checksums.txt exceeds the 1 MiB safety limit"
    }
    $checksumLines = @(Get-Content -LiteralPath $checksumPath | Where-Object {
        $parts = $_ -split '\s+'
        $parts.Count -ge 2 -and $parts[1].TrimStart('*') -eq $Archive
    })
    if ($checksumLines.Count -eq 0) {
        throw "no digest for $Archive in checksums.txt"
    }
    $expected = $null
    foreach ($checksumLine in $checksumLines) {
        $digest = (($checksumLine -split '\s+')[0]).ToLower()
        if ($digest -notmatch '^[0-9a-f]{64}$') {
            throw "invalid SHA-256 digest for $Archive"
        }
        if ($null -ne $expected -and $expected -ne $digest) {
            throw "conflicting SHA-256 digests for $Archive"
        }
        $expected = $digest
    }
    $actual = (Get-FileHash -Path "$TmpDir\$Archive" -Algorithm SHA256).Hash.ToLower()
    if ($expected -ne $actual) {
        throw "CHECKSUM MISMATCH (expected $expected, actual $actual)"
    }
    Write-Host "Checksum verified."
} catch {
    Write-Host "error: checksum verification failed: $_" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}

# Validate the zip namespace before extraction. Windows paths are
# case-insensitive, so two entries that differ only in case are ambiguous and
# must never be allowed to overwrite each other. The official five entries are
# required at the archive root; UI adds exactly one hash-shaped pack.
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead("$TmpDir\$Archive")
    try {
        $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
        $archiveCounts = @{}
        foreach ($archiveName in $WindowsArchiveNames) { $archiveCounts[$archiveName] = 0 }
        $uiPackName = $null
        $uiPackCount = 0
        foreach ($entry in $zip.Entries) {
            $entryName = $entry.FullName.Replace('\', '/')
            $isDirectory = $entryName.EndsWith('/')
            $pathForSegments = if ($isDirectory) { $entryName.TrimEnd('/') } else { $entryName }
            $segments = @($pathForSegments.Split('/'))
            if ([string]::IsNullOrEmpty($pathForSegments) -or
                $entryName.StartsWith('/') -or
                $entryName.Contains(':') -or
                $segments -contains '' -or
                $segments -contains '.' -or
                $segments -contains '..' -or
                @($segments | Where-Object { $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -gt 0) {
                throw "unsafe zip entry path: $($entry.FullName)"
            }
            if (-not $seen.Add($pathForSegments)) {
                throw "duplicate or case-conflicting zip entry: $($entry.FullName)"
            }
            if ($isDirectory) {
                throw "archive contains an unexpected root entry: $($entry.FullName)"
            }
            if ($WindowsArchiveNames -ccontains $entryName) {
                $archiveCounts[$entryName] = $archiveCounts[$entryName] + 1
            } elseif ($Variant -eq "ui" -and $entryName -cmatch $UiPackPattern) {
                $uiPackName = $entryName
                $uiPackCount++
            } else {
                throw "archive contains an unexpected root entry: $($entry.FullName)"
            }
        }
        foreach ($archiveName in $WindowsArchiveNames) {
            if ($archiveCounts[$archiveName] -ne 1) {
                throw "archive must contain exactly one $archiveName"
            }
        }
        $expectedUiPackCount = if ($Variant -eq "ui") { 1 } else { 0 }
        $expectedArchiveCount = $WindowsArchiveNames.Count + $expectedUiPackCount
        if ($uiPackCount -ne $expectedUiPackCount -or
            $seen.Count -ne $expectedArchiveCount) {
            throw "archive does not match the exact $Variant Windows release allowlist"
        }
    } finally {
        $zip.Dispose()
    }
} catch {
    Write-Host "error: unsafe or incomplete release archive: $_" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}

# Extract the validated bundle. Windows ships ONE binary, exactly like Linux
# and macOS; this script is what replaces it, because a running executable
# cannot replace itself on Windows. Re-running this script IS the update.
Write-Host "Extracting..."
Expand-Archive -Path "$TmpDir\$Archive" -DestinationPath $TmpDir -Force

foreach ($archiveName in $WindowsArchiveNames) {
    $extractedMember = Join-Path $TmpDir $archiveName
    if (-not (Test-Path -LiteralPath $extractedMember -PathType Leaf)) {
        Write-Host "error: release member is not a regular file: $archiveName" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
    $extractedItem = Get-Item -LiteralPath $extractedMember
    if ($extractedItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        Write-Host "error: refusing reparse-point release member: $archiveName" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
}

$DownloadedBinary = Join-Path $TmpDir $BinName
if (-not (Test-Path -LiteralPath $DownloadedBinary -PathType Leaf)) {
    Write-Host "error: $BinName not found after extraction" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir
    exit 1
}
$binaryItem = Get-Item -LiteralPath $DownloadedBinary
if ($binaryItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
    Write-Host "error: refusing reparse-point executable in release archive" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir
    exit 1
}

$DownloadedUiPack = $null
if ($Variant -eq "ui") {
    $DownloadedUiPack = Join-Path $TmpDir $uiPackName
    if (-not (Test-Path -LiteralPath $DownloadedUiPack -PathType Leaf)) {
        Write-Host "error: UI asset pack not found after extraction" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
    $packItem = Get-Item -LiteralPath $DownloadedUiPack
    if ($packItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        Write-Host "error: refusing reparse-point UI asset pack" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
    $expectedPackDigest = $uiPackName.Substring(7, 64)
    $actualPackDigest = (Get-FileHash -LiteralPath $DownloadedUiPack -Algorithm SHA256).Hash.ToLower()
    if ($expectedPackDigest -cne $actualPackDigest) {
        Write-Host "error: UI asset pack digest does not match its filename" -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir
        exit 1
    }
}

# Prove the downloaded binary runs before touching an existing installation.
try {
    $candidateVersion = & $DownloadedBinary --version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "candidate exited with $LASTEXITCODE" }
    Write-Host "Verified candidate: $candidateVersion"
} catch {
    Write-Host "error: downloaded binary failed to run: $_" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}

# The candidate publishes the runtime set under one native activation guard,
# with sidecars before the executable and retained per-file backups for
# cooperative rollback. This is recoverable/fail-closed ordering, not a claim
# that several filesystem entries change in one crash-atomic transaction.
$Dest = Join-Path $InstallDir $BinName

$InstallArgs = @("install", "-y", "--force", "--dir=$InstallDir")
if ($SkipConfig) { $InstallArgs += "--skip-config" }
& $DownloadedBinary @InstallArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "error: installation failed (exit code $LASTEXITCODE)" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
    exit 1
}

# Place the installer beside the binary so `update` points at a local file
# rather than a URL, and so the next update runs THIS release's installer.
#
# Sourced from the archive we just checksum-verified, and published by rename
# rather than written over the live path. PowerShell parses a script fully
# before executing it, so self-overwrite is less hazardous here than it is for
# bash -- but rename costs nothing and keeps both platforms on one rule.
# Best effort: a failure here still leaves a working install.
$DownloadedInstaller = Join-Path $TmpDir "install.ps1"
if (Test-Path -LiteralPath $DownloadedInstaller -PathType Leaf) {
    $InstallerDest = Join-Path $InstallDir "install.ps1"
    $InstallerTmp = $null
    try {
        $InstallerTmp = New-CbmExclusiveSiblingTemp -Destination $InstallerDest
        Copy-Item -LiteralPath $DownloadedInstaller -Destination $InstallerTmp -Force -ErrorAction Stop
        Move-Item -LiteralPath $InstallerTmp -Destination $InstallerDest -Force -ErrorAction Stop
        $InstallerTmp = $null
        Write-Host "Installed updater -> $InstallerDest"
    } catch {
        if ($InstallerTmp) {
            Remove-Item -LiteralPath $InstallerTmp -Force -ErrorAction SilentlyContinue
        }
        Write-Host "note: could not place install.ps1 in $InstallDir (update will explain where to find it)"
    }
}

# Verify
try {
    $ver = & $Dest --version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "installed binary exited with $LASTEXITCODE" }
    Write-Host "Installed: $ver"
} catch {
    Write-Host "error: installed binary failed to run" -ForegroundColor Red
    Remove-Item -Recurse -Force $TmpDir
    exit 1
}

# Agent configuration was included in the candidate-owned activation window.
if ($SkipConfig) {
    Write-Host ""
    Write-Host "Skipping agent configuration (--skip-config)"
}

# The verified candidate persisted the current-user PATH while holding the
# coordinated activation lease. Do not perform a second registry mutation here
# after running sessions have been allowed to restart.

# Cleanup
Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Done! Restart your terminal and coding agent to start using codebase-memory-mcp."
