#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
foreach ($relativePath in @(
    'docker-compose.multi-gateway.yml',
    'deploy/nginx/multi-gateway.conf',
    'tests/compose.multi-gateway-validation.yml',
    'tests/multi_gateway.rs'
)) {
    $path = Join-Path $repoRoot $relativePath
    if (-not [IO.File]::Exists($path)) { throw "Multi-gateway validation entry point is missing: $relativePath" }
}

Write-Host 'multi-gateway validation entry points: 4 passed; 0 failed'
