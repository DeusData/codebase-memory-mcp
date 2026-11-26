$ErrorActionPreference = 'Stop'

$packageName = 'codebase-memory-mcp'
$version     = '0.12.1'
$url64       = "https://github.com/DeusData/codebase-memory-mcp/releases/download/v${version}/codebase-memory-mcp-windows-amd64.zip"
$checksum64  = '4e4e0f16b45233af5c757a1fcbe3c3b16e9056375b1fbe2ce020b6de75dedcc0'
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
