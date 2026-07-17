$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp'
$version     = '0.9.0'
$url64       = "https://github.com/DeusData/codebase-memory-mcp/releases/download/v${version}/codebase-memory-mcp-windows-amd64.zip"
$checksum64  = '92f96896f952e539f0d6cb34d7892a25064b677ccbf808b8f8310ad897e86f2c'
$installDir  = Join-Path $env:ChocolateyBinRoot $packageName

Install-ChocolateyZipPackage `
  -PackageName   $packageName `
  -Url64bit      $url64 `
  -Checksum64    $checksum64 `
  -ChecksumType64 'sha256' `
  -UnzipLocation $installDir

# Shim the binary so it is on PATH
$binPath = Join-Path $installDir 'codebase-memory-mcp.exe'
Install-BinFile -Name 'codebase-memory-mcp' -Path $binPath

# Configure coding agents (non-fatal)
try {
  & $binPath install -y 2>&1 | Out-Null
} catch {
  Write-Warning "Agent configuration failed (non-fatal). Run manually: codebase-memory-mcp install"
}
