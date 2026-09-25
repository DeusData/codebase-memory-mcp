$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp'
$version     = '0.11.0'
$url64       = "https://github.com/DeusData/codebase-memory-mcp/releases/download/v${version}/codebase-memory-mcp-windows-amd64.zip"
$checksum64  = '6eb6beaf261b19e419766e78baf93cbc3cf1c6338cff8fb7c0234859f96d1685'
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
