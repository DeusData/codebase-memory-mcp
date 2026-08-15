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
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md"
)
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

$SkipConfig = $false
foreach ($arg in $args) {
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
Write-Host "  arch:    $Arch"
Write-Host "  target:  $InstallDir\$BinName"
Write-Host ""

# Build download URL
$Archive = "codebase-memory-mcp-windows-$Arch.zip"
$Url = "$BaseUrl/$Archive"

# Download
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "cbm-install-$(Get-Random)"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

# Give the staging directory a protected owner-only DACL.
#
# Without this it inherits whatever %TEMP% carries, and the binary we are about
# to run from here validates its own directory and refuses inherited
# cross-account mutation grants. That is not a hypothetical: sandboxed clients
# leave ACEs on %TEMP% (a CodexSandboxUsers group, AppContainer SIDs, and
# orphaned SIDs from uninstalled software have all been reported), and installs
# failed with
#   activation transaction I/O failed: acl-grants-cross-account-mutation to S-1-5-21-...
# naming an ACE the installer itself inherited. See issues 1529, 1614 and 1571.
#
# cbm's own C staging already creates its directory this way; install.ps1 was
# the one path that did not, which is why redirecting TMP/TEMP worked around it.
#
# Applied after creation rather than atomically on purpose: the overload that
# takes a DirectorySecurity exists on Windows PowerShell 5.1 but not on
# PowerShell 7, and Set-Acl works on both. The directory name is unpredictable
# and nothing is written into it until the download below, so the window is not
# usefully attackable.
#
# Best-effort: a filesystem that cannot carry a DACL must not fail the install.
# If this does not take, the binary's own validation still refuses to proceed,
# which is the honest outcome rather than a silent downgrade.
try {
    $stagingAcl = New-Object System.Security.AccessControl.DirectorySecurity
    $stagingAcl.SetAccessRuleProtection($true, $false)
    $stagingOwner = ([System.Security.Principal.WindowsIdentity]::GetCurrent()).User
    $stagingAcl.SetOwner($stagingOwner)
    $stagingAcl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule(
        $stagingOwner, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')))
    Set-Acl -Path $TmpDir -AclObject $stagingAcl -ErrorAction Stop
} catch {
    Write-Host "note: could not harden the staging directory ACL: $($_.Exception.Message)"
}

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
# required at the archive root with their exact release names.
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead("$TmpDir\$Archive")
    try {
        $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
        $archiveCounts = @{}
        foreach ($archiveName in $WindowsArchiveNames) { $archiveCounts[$archiveName] = 0 }
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
            if (-not ($WindowsArchiveNames -ccontains $entryName) -or $isDirectory) {
                throw "archive contains an unexpected root entry: $($entry.FullName)"
            }
            $archiveCounts[$entryName] = $archiveCounts[$entryName] + 1
        }
        foreach ($archiveName in $WindowsArchiveNames) {
            if ($archiveCounts[$archiveName] -ne 1) {
                throw "archive must contain exactly one $archiveName"
            }
        }
        if ($seen.Count -ne $WindowsArchiveNames.Count) {
            throw "archive does not match the exact Windows release allowlist"
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

$Dest = Join-Path $InstallDir $BinName

# Retire the running installation before replacing it. Windows keeps an image
# lock on a running .exe: the file cannot be overwritten, but it CAN be renamed
# out of the way, which is what makes an in-place update possible from here.
if (Test-Path -LiteralPath $Dest -PathType Leaf) {
    try { & $Dest daemon stop 2>&1 | Out-Null } catch { }
    $retired = "$Dest.retired-$(Get-Date -Format yyyyMMddHHmmss)"
    $renamed = $false
    foreach ($attempt in 1..10) {
        try { Move-Item -LiteralPath $Dest -Destination $retired -Force -ErrorAction Stop; $renamed = $true; break }
        catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $renamed) {
        Write-Host "error: could not retire the existing $BinName - close all running" -ForegroundColor Red
        Write-Host "       codebase-memory-mcp sessions and coding agents, then re-run." -ForegroundColor Red
        Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
        exit 1
    }
    # A retired image stays locked until its last process exits; delete it when
    # we can, and leave it for the next run when we cannot. Never fail here.
    Remove-Item -LiteralPath $retired -Force -ErrorAction SilentlyContinue
}
Get-ChildItem -LiteralPath $InstallDir -Filter "$BinName.retired-*" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }

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
    $InstallerTmp = "$InstallerDest.new"
    try {
        Copy-Item -LiteralPath $DownloadedInstaller -Destination $InstallerTmp -Force -ErrorAction Stop
        Move-Item -LiteralPath $InstallerTmp -Destination $InstallerDest -Force -ErrorAction Stop
        Write-Host "Installed updater -> $InstallerDest"
    } catch {
        Remove-Item -LiteralPath $InstallerTmp -Force -ErrorAction SilentlyContinue
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
