#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$entryPoints = @(
    'tests/postgres-production-baseline.Tests.ps1',
    'tests/postgres-lifecycle.Tests.ps1',
    'tests/path-redaction.Tests.ps1',
    'tests/multi-gateway.Tests.ps1',
    'tests/lifecycle-transition.Tests.ps1',
    'tests/cluster.Tests.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/native-runner.Tests.ps1',
    'tests/cleanup-inventory.Tests.ps1',
    'tests/run-postgres-lifecycle-validation.ps1',
    'tests/run-lifecycle-transition-validation.ps1',
    'scripts/client-smoke.ps1',
    'scripts/object-versioning-smoke.ps1',
    'scripts/lifecycle-expiration-smoke.ps1',
    'scripts/bucket-cors-smoke.ps1'
)

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
foreach ($relativePath in $entryPoints) {
    $path = Join-Path $repoRoot $relativePath
    if (-not [IO.File]::Exists($path)) { throw "Validation entry point is missing: $relativePath" }
    $tokens = $null
    $errors = $null
    [Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors.Count -ne 0) { throw "Validation entry point does not parse: $relativePath" }
}

Write-Host "validation entry points: $($entryPoints.Count) passed; 0 failed"
