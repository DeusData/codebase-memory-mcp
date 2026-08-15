<#
.SYNOPSIS
Create a protected per-user temp root and print its path.

.DESCRIPTION
The daemon and install-flow suites fail closed on the MSYS-shared /tmp and on a
runner's inherited LocalAppData\Temp ACLs: both grant mutation rights to
Authenticated Users, which the launcher/daemon trust policy correctly refuses.
Running them there produces security refusals, not test signal. Every venue
therefore gives the harness a per-user root beneath the profile carrying an
owner-stamped, protected current-SID DACL.

This is the single implementation. It was duplicated between
.github/workflows/_test.yml and test-infrastructure/vm/vm-run-tests.sh - which
let the two venues drift - and _soak.yml carried no copy at all, so CI soaked
under the default runner TEMP while the local soak ran hardened.

.PARAMETER Prefix
Directory-name prefix for the root. Also selects what -PruneStale sweeps, so
each venue keeps its own namespace.

.PARAMETER ProtectDir
Extra directories to create and stamp with the same protected DACL. The
workspace drive root grants Authenticated Users Modify by inheritance - a shape
real user checkouts under the profile do not have - so build outputs living
there need the same treatment or the activation transaction's source-directory
policy refuses them.

.PARAMETER PruneStale
Remove earlier roots sharing this prefix before creating the new one. Intended
for the long-lived VM, where roots accumulate; a fresh runner has none.
#>
[CmdletBinding()]
param(
    [string]$Prefix = 'cbm-tmp-',
    [string[]]$ProtectDir = @(),
    [switch]$PruneStale
)

$ErrorActionPreference = 'Stop'

if ($PruneStale) {
    Get-ChildItem -LiteralPath $env:USERPROFILE -Directory -Filter "$Prefix*" `
        -ErrorAction SilentlyContinue |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $sid,
    [System.Security.AccessControl.FileSystemRights]::FullControl,
    ([System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit),
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow)

function Set-ProtectedAcl([string]$Path) {
    $acl = [System.Security.AccessControl.DirectorySecurity]::new()
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule($rule) | Out-Null
    Set-Acl -LiteralPath $Path -AclObject $acl
}

$root = Join-Path $env:USERPROFILE ($Prefix + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $root | Out-Null
Set-ProtectedAcl $root

foreach ($dir in $ProtectDir) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    Set-ProtectedAcl $dir
}

Write-Output $root
