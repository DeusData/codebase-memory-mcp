# Add Aider integration to RPC API
$aiderIntegration = Get-Content -Path graph-ui/src/api/rpc.ts | Select-String -Pattern '(?<=AiderIntegration {).*?(?=})'
$aiderIntegration = $aiderIntegration.Matches.Value
$aiderIntegration = $aiderIntegration -replace '}$'
$aiderIntegration = "    $aiderIntegration}"
$aiderIntegration = "}"
$aiderIntegration = "AiderIntegration { $aiderIntegration"

Set-Content -Path graph-ui/src/api/rpc.ts -Value ($aiderIntegration + (Get-Content -Path graph-ui/src/api/rpc.ts))

# Install Aider integration
npm install @aider/ai