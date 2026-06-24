$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp'
$installDir  = Join-Path $env:ChocolateyBinRoot $packageName

Uninstall-BinFile -Name 'codebase-memory-mcp'

if (Test-Path $installDir) {
  Remove-Item $installDir -Recurse -Force
}
