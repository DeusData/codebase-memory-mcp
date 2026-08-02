param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("prepare", "verify", "cleanup")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{32}$")]
    [string]$RunId,

    [Parameter(Mandatory = $true)]
    [string]$SnapshotPath,

    [Parameter(Mandatory = $true)]
    [string]$SmokeRoot
)

$ErrorActionPreference = "Stop"
$TestKeyPath = "Software\CodebaseMemoryMCP\Smoke\$RunId"
$SentinelName = "CbmSmokeRunId"
$RegistryView = [Microsoft.Win32.RegistryView]::Registry64

function Open-CurrentUser {
    return [Microsoft.Win32.RegistryKey]::OpenBaseKey(
        [Microsoft.Win32.RegistryHive]::CurrentUser,
        $RegistryView
    )
}

function Read-PathValue {
    param([string]$KeyPath)

    $base = Open-CurrentUser
    try {
        $key = $base.OpenSubKey($KeyPath, $false)
        if ($null -eq $key) {
            return [pscustomobject]@{ Exists = $false; Kind = $null; Value = $null }
        }
        try {
            $pathName = @(
                $key.GetValueNames() | Where-Object {
                    [string]::Equals(
                        $_,
                        "Path",
                        [System.StringComparison]::OrdinalIgnoreCase
                    )
                }
            ) | Select-Object -First 1
            if ($null -eq $pathName) {
                return [pscustomobject]@{ Exists = $false; Kind = $null; Value = $null }
            }
            $kind = $key.GetValueKind($pathName)
            if (
                $kind -ne [Microsoft.Win32.RegistryValueKind]::String -and
                $kind -ne [Microsoft.Win32.RegistryValueKind]::ExpandString
            ) {
                throw "$KeyPath\Path has unsupported registry kind $kind"
            }
            $value = $key.GetValue(
                $pathName,
                $null,
                [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames
            )
            if ($null -eq $value) {
                $value = ""
            }
            return [pscustomobject]@{
                Exists = $true
                Kind = $kind.ToString()
                Value = [string]$value
            }
        }
        finally {
            $key.Dispose()
        }
    }
    finally {
        $base.Dispose()
    }
}

function Get-StateHash {
    param($State)

    $serialized = (
        [string][int][bool]$State.Exists + "`0" +
        [string]$State.Kind + "`0" +
        [string]$State.Value
    )
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($serialized))
        return -join ($digest | ForEach-Object { $_.ToString("x2") })
    }
    finally {
        $sha.Dispose()
    }
}

function Assert-StateEqual {
    param($Expected, $Actual)

    if (
        [bool]$Expected.Exists -ne [bool]$Actual.Exists -or
        -not [string]::Equals(
            [string]$Expected.Kind,
            [string]$Actual.Kind,
            [System.StringComparison]::Ordinal
        ) -or
        -not [string]::Equals(
            [string]$Expected.Value,
            [string]$Actual.Value,
            [System.StringComparison]::Ordinal
        )
    ) {
        throw (
            "HKCU\Environment\Path changed while the isolated smoke ran; " +
            "before=$(Get-StateHash $Expected), after=$(Get-StateHash $Actual)"
        )
    }
}

function Assert-SmokePathValue {
    param($State)

    if (-not $State.Exists) {
        throw "isolated smoke registry Path was not written"
    }
    $canonicalRoot = [System.IO.Path]::GetFullPath($SmokeRoot).TrimEnd(
        [char[]]@('\', '/')
    )
    $rootPrefix = $canonicalRoot + [System.IO.Path]::DirectorySeparatorChar
    $segments = @(([string]$State.Value).Split(";"))
    if ($segments.Count -eq 0) {
        throw "isolated smoke registry Path is empty"
    }
    foreach ($segment in $segments) {
        $trimmed = $segment.Trim()
        if ($trimmed.Length -eq 0) {
            throw "isolated smoke registry Path contains an empty segment"
        }
        $canonicalSegment = [System.IO.Path]::GetFullPath($trimmed).TrimEnd(
            [char[]]@('\', '/')
        )
        if (
            -not [string]::Equals(
                $canonicalSegment,
                $canonicalRoot,
                [System.StringComparison]::OrdinalIgnoreCase
            ) -and
            -not $canonicalSegment.StartsWith(
                $rootPrefix,
                [System.StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "isolated smoke registry Path escaped the smoke root"
        }
    }
}

function Assert-TestKeySentinel {
    $base = Open-CurrentUser
    try {
        $key = $base.OpenSubKey($TestKeyPath, $false)
        if ($null -eq $key) {
            throw "isolated smoke registry key is missing"
        }
        try {
            $sentinel = $key.GetValue(
                $SentinelName,
                $null,
                [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames
            )
            if (
                $key.GetValueKind($SentinelName) -ne
                    [Microsoft.Win32.RegistryValueKind]::String -or
                -not [string]::Equals(
                    [string]$sentinel,
                    $RunId,
                    [System.StringComparison]::Ordinal
                )
            ) {
                throw "isolated smoke registry sentinel is invalid"
            }
        }
        finally {
            $key.Dispose()
        }
    }
    finally {
        $base.Dispose()
    }
}

if ($Mode -eq "prepare") {
    $base = Open-CurrentUser
    try {
        $existing = $base.OpenSubKey($TestKeyPath, $false)
        if ($null -ne $existing) {
            $existing.Dispose()
            throw "isolated smoke registry key already exists"
        }
        $key = $base.CreateSubKey($TestKeyPath, $true)
        if ($null -eq $key) {
            throw "could not create isolated smoke registry key"
        }
        try {
            $key.SetValue(
                $SentinelName,
                $RunId,
                [Microsoft.Win32.RegistryValueKind]::String
            )
        }
        finally {
            $key.Dispose()
        }
    }
    finally {
        $base.Dispose()
    }

    $state = Read-PathValue "Environment"
    $record = [pscustomobject]@{
        Version = 2
        RunId = $RunId
        Exists = $state.Exists
        Kind = $state.Kind
        Value = $state.Value
    }
    $parent = Split-Path -Parent $SnapshotPath
    [System.IO.Directory]::CreateDirectory($parent) | Out-Null
    $temporary = "$SnapshotPath.new"
    $json = $record | ConvertTo-Json -Compress
    [System.IO.File]::WriteAllText(
        $temporary,
        $json,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $temporary -Destination $SnapshotPath -Force
    Write-Host "Prepared isolated Windows PATH smoke (live PATH hash: $(Get-StateHash $state))"
    exit 0
}

if ($Mode -eq "verify") {
    if (-not [System.IO.File]::Exists($SnapshotPath)) {
        throw "Windows user PATH snapshot is missing"
    }
    $json = [System.IO.File]::ReadAllText(
        $SnapshotPath,
        [System.Text.Encoding]::UTF8
    )
    $record = $json | ConvertFrom-Json
    if (
        $record.Version -ne 2 -or
        $null -eq $record.Exists -or
        -not [string]::Equals(
            [string]$record.RunId,
            $RunId,
            [System.StringComparison]::Ordinal
        )
    ) {
        throw "Windows user PATH snapshot is invalid"
    }

    Assert-TestKeySentinel
    $current = Read-PathValue "Environment"
    Assert-StateEqual $record $current
    Assert-SmokePathValue (Read-PathValue $TestKeyPath)
    Write-Host "Verified isolated Windows PATH smoke; live user PATH was not mutated"
    exit 0
}

$base = Open-CurrentUser
try {
    $key = $base.OpenSubKey($TestKeyPath, $false)
    if ($null -ne $key) {
        try {
            $sentinel = $key.GetValue(
                $SentinelName,
                $null,
                [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames
            )
            if (
                $key.GetValueKind($SentinelName) -ne
                    [Microsoft.Win32.RegistryValueKind]::String -or
                -not [string]::Equals(
                    [string]$sentinel,
                    $RunId,
                    [System.StringComparison]::Ordinal
                )
            ) {
                throw "refusing to delete an isolated registry key with the wrong sentinel"
            }
        }
        finally {
            $key.Dispose()
        }
        $base.DeleteSubKeyTree($TestKeyPath, $false)
    }
}
finally {
    $base.Dispose()
}
Write-Host "Removed isolated Windows PATH smoke registry key"
