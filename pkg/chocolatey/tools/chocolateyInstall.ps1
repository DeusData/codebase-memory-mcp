$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp'
$version     = '0.6.1'
$url64       = "https://github.com/DeusData/codebase-memory-mcp/releases/download/v${version}/codebase-memory-mcp-windows-amd64.zip"
$checksum64  = 'e331f84f2ec760d179025b16b440b0f14282e83f2432318969dc69dde8ffffd0'
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
