#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
foreach ($relativePath in @(
    'docker-compose.postgres.yml',
    'tests/compose.postgres-production-validation.yml'
)) {
    $path = Join-Path $repoRoot $relativePath
    if (-not [IO.File]::Exists($path)) { throw "PostgreSQL validation entry point is missing: $relativePath" }
}

Write-Host 'postgres production validation entry points: 2 passed; 0 failed'
