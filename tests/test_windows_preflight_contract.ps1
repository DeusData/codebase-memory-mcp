# Native Windows checks for a non-destructive capacity preflight.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [Parameter(Mandatory = $true)][string]$OutputDirectory
)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Test output must be fresh' }
New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
$taskFirst = Join-Path $OutputDirectory 'cbm-foreign-sentinel'
$taskSecond = Join-Path $OutputDirectory 'cbm-vm-tmp-foreign-sentinel'
New-Item -ItemType Directory -Path $taskFirst, $taskSecond | Out-Null
[IO.File]::WriteAllText((Join-Path $taskFirst 'keep.txt'), 'first owned sentinel')
[IO.File]::WriteAllText((Join-Path $taskSecond 'keep.txt'), 'second owned sentinel')
$taskBefore = @{}
foreach ($taskPath in @($taskFirst, $taskSecond)) {
    $taskBefore[$taskPath] = (Get-FileHash -LiteralPath (Join-Path $taskPath 'keep.txt')).Hash
}
$taskSavedProfile = $env:USERPROFILE
$taskSavedTemp = $env:TEMP
$taskFailures = 0
try {
    # Only this disposable test process and its child receive these roots.
    $env:USERPROFILE = $OutputDirectory
    $env:TEMP = $OutputDirectory
    $taskPowerShell = Join-Path $PSHOME 'powershell.exe'
    $taskCases = @(
        @{ Name = 'capacity_accept'; Extra = @('-CheckOnly', '-MinFreeGB', '0'); Expected = 0 },
        @{ Name = 'capacity_reject'; Extra = @('-CheckOnly', '-MinFreeGB', '1048576'); Expected = 1 },
        @{ Name = 'skip_gate_conflict'; Extra = @('-CheckOnly', '-SkipGate'); Expected = 1 }
    )
    foreach ($taskCase in $taskCases) {
        $taskOut = Join-Path $OutputDirectory ($taskCase.Name + '.out')
        $taskErr = Join-Path $OutputDirectory ($taskCase.Name + '.err')
        $taskArgs = @('-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath) + $taskCase.Extra
        $ErrorActionPreference = 'Continue'
        & $taskPowerShell @taskArgs 1>$taskOut 2>$taskErr
        $taskExit = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'
        $taskPassed = if ($taskCase.Expected -eq 0) { $taskExit -eq 0 } else { $taskExit -ne 0 }
        if ($taskCase.Name -eq 'capacity_reject') {
            $taskPassed = $taskPassed -and ((Get-Content -Raw -LiteralPath $taskErr) -match 'BLOCKED:')
        }
        if (-not $taskPassed) { $taskFailures++ }
        Write-Output ($taskCase.Name + ' exit=' + $taskExit + ' expected=' + $taskCase.Expected + ' passed=' + $taskPassed)
        foreach ($taskPath in @($taskFirst, $taskSecond)) {
            $taskFile = Join-Path $taskPath 'keep.txt'
            if (-not (Test-Path -LiteralPath $taskFile) -or
                (Get-FileHash -LiteralPath $taskFile).Hash -ne $taskBefore[$taskPath]) {
                throw 'The preflight changed a sentinel belonging to another run'
            }
        }
    }
} finally {
    $env:USERPROFILE = $taskSavedProfile
    $env:TEMP = $taskSavedTemp
}
Write-Output ('WINDOWS_CHECK_ONLY_COMPLETE failures=' + $taskFailures + ' sentinels=2')
if ($taskFailures) { exit 1 }
