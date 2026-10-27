<#
.SYNOPSIS
  Build a repository's code graph into the local Neo4j of docker/graph/compose.yaml.

.EXAMPLE
  .\cbm-graph.ps1 -RepoPath "D:\batch applications\alias-directory-sync-service"

.EXAMPLE
  .\cbm-graph.ps1 -RepoPath "D:\repos\my-service" -Name my-service -Fresh

.EXAMPLE
  .\cbm-graph.ps1 -RepoPath "D:\opt\i2c-fleet\repos" -Name i2c-fleet -NoCopy

.NOTES
  -NoCopy indexes the repository where it is instead of syncing a copy into
  the Docker volume first. Only the repository's own .cbmignore applies then
  (-IgnoreFile cannot be merged in without a copy).


  The Neo4j password comes from docker/graph/.env (copy .env.example). Docker
  Compose reads it for both the Neo4j container and the indexer, so nothing has
  to be set in the terminal. Neo4j is started (and the image built) on demand.
#>
param(
    [Parameter(Mandatory = $true)][string]$RepoPath,
    [string]$Name,
    [switch]$Fresh,
    [switch]$NoCopy,
    [string]$IgnoreFile = "D:\cbm-fleet\.cbmignore"
)
$ErrorActionPreference = "Stop"

$compose = Join-Path $PSScriptRoot "compose.yaml"
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot ".env") -PathType Leaf)) {
    throw "Missing $PSScriptRoot\.env: copy .env.example to .env and set NEO4J_PASSWORD."
}
if (-not (Test-Path -LiteralPath $RepoPath -PathType Container)) {
    throw "Repository folder not found: $RepoPath"
}
$RepoPath = (Resolve-Path -LiteralPath $RepoPath).Path
if (-not $Name) {
    $Name = (Split-Path -Leaf $RepoPath).ToLowerInvariant() -replace '[^a-z0-9._-]+', '-'
    $Name = $Name.Trim('-')
}
if ($Name -notmatch '^[A-Za-z0-9._-]+$') { throw "Invalid -Name '$Name'" }

$dockerArgs = @(
    "compose", "-f", $compose, "run", "--rm",
    "-e", "CBM_NAME=$Name",
    "-e", "CBM_FRESH=$(if ($Fresh) { '1' } else { '0' })",
    "-e", "CBM_NO_COPY=$(if ($NoCopy) { '1' } else { '0' })"
)
if ($NoCopy) {
    # Mounted under its project name so each project keeps distinct file paths.
    $dockerArgs += @("-v", "${RepoPath}:/repos/${Name}:ro")
    if ($PSBoundParameters.ContainsKey('IgnoreFile') -and $IgnoreFile) {
        Write-Warning "-IgnoreFile is not used with -NoCopy; only $RepoPath\.cbmignore applies."
    }
    $IgnoreFile = ""
} else {
    $dockerArgs += @("-v", "${RepoPath}:/input/repo:ro")
}
if ($IgnoreFile) {
    if (-not (Test-Path -LiteralPath $IgnoreFile -PathType Leaf)) {
        throw "Ignore file not found: $IgnoreFile (pass -IgnoreFile '' to skip it)"
    }
    $dockerArgs += @("-v", "$((Resolve-Path -LiteralPath $IgnoreFile).Path):/input/cbmignore:ro")
}
$dockerArgs += "cbm"

$mode = if ($NoCopy) { " (in place, no copy)" } else { "" }
Write-Host "Graphing '$RepoPath' as project '$Name'$mode -> local Neo4j (http://localhost:7474)" -ForegroundColor Cyan
& docker @dockerArgs
exit $LASTEXITCODE
