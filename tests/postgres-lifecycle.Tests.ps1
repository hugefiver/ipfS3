#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$runnerPath = Join-Path $PSScriptRoot 'run-postgres-lifecycle-validation.ps1'
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($runnerPath, [ref]$tokens, [ref]$errors)
if ($errors.Count -ne 0) { throw 'PostgreSQL lifecycle runner has syntax errors' }

$redactors = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Protect-Diagnostic'
}, $true))
if ($redactors.Count -ne 1) { throw 'PostgreSQL lifecycle diagnostic redactor is unavailable' }
. ([scriptblock]::Create($redactors[0].Extent.Text))

$PostgresUrl = 'postgresql://fixture:private-a@127.0.0.1:5432/fixture'
$MultiGatewayDatabaseUrl = 'postgres://fixture:private-b@127.0.0.1:5432/gateway'
$safe = Protect-Diagnostic "$PostgresUrl $MultiGatewayDatabaseUrl POSTGRESQL://other:private-c@host/db"
if ($safe -cne '[REDACTED_DATABASE_URL] [REDACTED_DATABASE_URL] [REDACTED_DATABASE_URL]') {
    throw 'PostgreSQL lifecycle diagnostics exposed a database URL'
}
if ((Protect-Diagnostic 'test result: ok. 8 passed; 0 failed') -cne 'test result: ok. 8 passed; 0 failed') {
    throw 'PostgreSQL lifecycle redaction changed safe diagnostics'
}

$pwsh = (@(Get-Command pwsh -CommandType Application -ErrorAction Stop))[0].Source
$output = @(& $pwsh -NoLogo -NoProfile -File $runnerPath 2>&1) -join "`n"
if ($LASTEXITCODE -eq 0 -or -not $output.Contains('PostgresUrl', [StringComparison]::Ordinal)) {
    throw 'PostgreSQL lifecycle runner did not reject a missing required database URL'
}

Write-Host 'postgres lifecycle behavior: 3 passed; 0 failed'
