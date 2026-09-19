#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$pwsh = (@(Get-Command pwsh -CommandType Application -ErrorAction Stop))[0].Source
$cases = @(
    @{ Path = 'scripts/client-smoke.ps1'; Receipt = '[RESULT] client=Rclone status=SKIPPED' },
    @{ Path = 'scripts/object-versioning-smoke.ps1'; Receipt = '[RESULT] object-versioning-client=NOT RUN reason=execution-not-requested' },
    @{ Path = 'scripts/lifecycle-expiration-smoke.ps1'; Receipt = '[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested' },
    @{ Path = 'scripts/bucket-cors-smoke.ps1'; Receipt = 'Bucket CORS validation: NOT RUN' }
)

foreach ($case in $cases) {
    $path = Join-Path $repoRoot $case.Path
    $output = @(& $pwsh -NoLogo -NoProfile -File $path 2>&1) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw "No-run validation failed: $($case.Path)" }
    if (-not $output.Contains($case.Receipt, [StringComparison]::Ordinal)) {
        throw "No-run receipt changed: $($case.Path)"
    }
}

$corsRunner = Join-Path $repoRoot 'scripts/bucket-cors-smoke.ps1'
$conflict = @(& $pwsh -NoLogo -NoProfile -File $corsRunner -Run -PostgresOnly 2>&1) -join "`n"
if ($LASTEXITCODE -eq 0 -or -not $conflict.Contains('mutually exclusive', [StringComparison]::Ordinal)) {
    throw 'Bucket CORS runner did not reject conflicting modes before execution'
}

Write-Host 'client smoke runner behavior: 5 passed; 0 failed'
