<#
.SYNOPSIS
  Build a repository's code graph into Neo4j using the cbm-graph Docker image.

.EXAMPLE
  .\cbm-graph.ps1 -RepoPath "D:\batch applications\alias-directory-sync-service"

.EXAMPLE
  .\cbm-graph.ps1 -RepoPath "D:\repos\my-service" -Name my-service -Fresh

.NOTES
  The Neo4j password is read from $env:NEO4J_PASSWORD (set it once with
  setx NEO4J_PASSWORD "..." and open a new terminal). It is handed to the
  container through the environment, never on a command line.
#>
param(
    [Parameter(Mandatory = $true)][string]$RepoPath,
    [string]$Name,
    [switch]$Fresh,
    [string]$IgnoreFile = "D:\cbm-fleet\.cbmignore",
    [string]$Neo4jUri = "http://host.docker.internal:7474",
    [string]$Neo4jUser = "neo4j",
    [string]$Neo4jDatabase = "neo4j",
    [string]$Image = "cbm-graph:local",
    [string]$CacheVolume = "cbm-graph-data"
)
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $RepoPath -PathType Container)) {
    throw "Repository folder not found: $RepoPath"
}
$RepoPath = (Resolve-Path -LiteralPath $RepoPath).Path
if (-not $env:NEO4J_PASSWORD) {
    throw 'Set the Neo4j password first: $env:NEO4J_PASSWORD = "..."  (or setx NEO4J_PASSWORD "..." for every new terminal)'
}
if (-not $Name) {
    $Name = (Split-Path -Leaf $RepoPath).ToLowerInvariant() -replace '[^a-z0-9._-]+', '-'
    $Name = $Name.Trim('-')
}
if ($Name -notmatch '^[A-Za-z0-9._-]+$') { throw "Invalid -Name '$Name'" }

docker image inspect $Image *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker image '$Image' not found. Build it from the fork root: docker build -f docker/graph/Dockerfile -t $Image ."
}

$dockerArgs = @(
    "run", "--rm",
    "--add-host=host.docker.internal:host-gateway",
    "-e", "CBM_NAME=$Name",
    "-e", "CBM_FRESH=$(if ($Fresh) { '1' } else { '0' })",
    "-e", "NEO4J_URI=$Neo4jUri",
    "-e", "NEO4J_USER=$Neo4jUser",
    "-e", "NEO4J_DATABASE=$Neo4jDatabase",
    "-e", "NEO4J_PASSWORD",
    "-v", "${RepoPath}:/input/repo:ro",
    "-v", "${CacheVolume}:/data"
)
if ($IgnoreFile) {
    if (-not (Test-Path -LiteralPath $IgnoreFile -PathType Leaf)) {
        throw "Ignore file not found: $IgnoreFile (pass -IgnoreFile '' to skip it)"
    }
    $dockerArgs += @("-v", "$((Resolve-Path -LiteralPath $IgnoreFile).Path):/input/cbmignore:ro")
}
$dockerArgs += $Image

Write-Host "Graphing '$RepoPath' as project '$Name' -> $Neo4jUri" -ForegroundColor Cyan
& docker @dockerArgs
exit $LASTEXITCODE
