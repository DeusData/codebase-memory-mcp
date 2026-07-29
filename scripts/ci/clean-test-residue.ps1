<#
.SYNOPSIS
Sweep cbm test residue from a Windows test host and assert a runner-like free
disk, before any test/smoke/soak run.

.DESCRIPTION
A GitHub-hosted runner is ephemeral: every job starts on a fresh image with a
known-free disk. The local Windows VM is long-lived, so each run leaves temp
roots, staged bundles and ~100 MB binary copies behind. Left alone that drift
is not merely untidy - it changes the environment under test, and a disk that
has quietly filled produces ERROR_DISK_FULL deep inside the install path, which
reads as a product failure rather than an environment one. That misdiagnosis
has already cost a full session.

So every run starts from the same shape the runner would give it: residue gone,
free space asserted. Refusing to start is correct - an unusable environment is a
run blocker to escalate, never something to quietly run anyway on a bad disk.

Sweeps only cbm-prefixed directories under the profile and TEMP. The checkout
(C:\cbm) is not under either, and is never touched.

.PARAMETER MinFreeGB
Free space required before a run. Defaults to 14, the SSD size a GitHub-hosted
Windows runner advertises, so the local venue cannot run under conditions the
CI venue would never see.

.PARAMETER SkipGate
Sweep and report, but do not fail when free space is short. For inspection only.
#>
[CmdletBinding()]
param(
    [double]$MinFreeGB = 14,
    [switch]$SkipGate
)

$ErrorActionPreference = 'Stop'

function Get-FreeGB {
    [math]::Round((Get-PSDrive C).Free / 1GB, 2)
}

# The guard suites deliberately build adversarial trees - names with spaces and
# non-representable characters, deep .cbm-retired/generations nesting - whose
# full paths exceed MAX_PATH. Remove-Item cannot address those and fails with
# DirectoryNotFoundException, so `rd` under the \\?\ prefix (which bypasses the
# 260-char limit) is the fallback. Every removal is then VERIFIED: an earlier
# revision counted attempts, not successes, and cheerfully reported 86 swept
# directories while 11 GB of them were still sitting on the disk.
function Remove-ResidueDir([string]$Path) {
    Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue
    if (-not (Test-Path -LiteralPath $Path)) { return $true }
    & cmd.exe /c "rd /s /q `"\\?\$Path`"" 2>&1 | Out-Null
    return -not (Test-Path -LiteralPath $Path)
}

$before = Get-FreeGB
$removed = 0
$failed = @()
$roots = @($env:USERPROFILE, $env:TEMP) | Where-Object { $_ } | Select-Object -Unique

foreach ($root in $roots) {
    if (-not (Test-Path -LiteralPath $root)) { continue }
    Get-ChildItem -LiteralPath $root -Directory -Filter 'cbm-*' -ErrorAction SilentlyContinue |
        ForEach-Object {
            if (Remove-ResidueDir $_.FullName) {
                $removed++
            } else {
                $failed += $_.FullName
            }
        }
}

$after = Get-FreeGB
Write-Output "residue-dirs-removed=$removed residue-dirs-failed=$($failed.Count) free-gb-before=$before free-gb-after=$after"
foreach ($f in $failed) { Write-Warning "could not sweep: $f" }

if (-not $SkipGate -and $after -lt $MinFreeGB) {
    $msg = "BLOCKED: $after GB free, need $MinFreeGB GB. " +
        "A GitHub runner never starts this low, so a run here would not be " +
        "comparable - and a disk that fills mid-run surfaces as a bogus " +
        "product failure rather than an environment one. Free space, re-run."
    Write-Error $msg
    exit 1
}
