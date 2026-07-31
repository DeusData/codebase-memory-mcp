# ensure-defender.ps1 — Windows Defender real-time protection must be ACTIVE
# on every Windows venue: the local test VM and every GitHub Windows runner
# alike. The AV interaction surface (file locks on fresh binaries, scan-on-
# first-execute latency, quarantine behavior) is part of what the Windows legs
# test; a venue with Defender off is testing a different operating system.
#
# GitHub's runner images ship with Defender disabled, so this script ENABLES
# it, then VERIFIES it is actually running — and fails closed if it cannot be
# made active. No silent skip: a leg that cannot get Defender on must go red,
# not quietly measure the wrong environment. (Same doctrine as the smoke
# no-skip rules: a gate that can be skipped is not a gate.)
#
# Canonical entry: local VM preflight (win.sh) and the Windows CI jobs all run
# THIS file — venue parity by construction.

$ErrorActionPreference = 'Stop'

function Try-Step {
    param([string]$What, [scriptblock]$Action)
    try {
        & $Action
        Write-Host "ensure-defender: $What - ok"
    } catch {
        # Individual enable steps may legitimately no-op (already enabled,
        # service already running); only the final verification gates.
        Write-Host "ensure-defender: $What - $($_.Exception.Message)"
    }
}

Try-Step "WinDefend service startup type" {
    Set-Service -Name WinDefend -StartupType Automatic
}
Try-Step "WinDefend service start" {
    Start-Service -Name WinDefend
}
Try-Step "enable real-time monitoring" {
    Set-MpPreference -DisableRealtimeMonitoring $false
}

$status = $null
try {
    $status = Get-MpComputerStatus
} catch {
    Write-Host "ensure-defender: FAIL - Defender engine unavailable: $($_.Exception.Message)"
    exit 1
}

if (-not $status.AntivirusEnabled) {
    Write-Host "ensure-defender: FAIL - antivirus engine is not enabled on this venue"
    exit 1
}
if (-not $status.RealTimeProtectionEnabled) {
    Write-Host "ensure-defender: FAIL - real-time protection is OFF and could not be enabled"
    exit 1
}

Write-Host ("=== ensure-defender: ACTIVE (engine {0}, signatures {1}, RTP on) ===" -f `
    $status.AMEngineVersion, $status.AntivirusSignatureVersion)
exit 0
