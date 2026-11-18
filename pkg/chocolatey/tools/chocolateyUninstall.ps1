$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp-neo4j'
$installDir  = Join-Path $env:ChocolateyBinRoot $packageName

Uninstall-BinFile -Name 'codebase-memory-mcp-neo4j'

if (Test-Path $installDir) {
  Remove-Item $installDir -Recurse -Force
}
