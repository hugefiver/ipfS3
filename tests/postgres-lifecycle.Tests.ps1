$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$RunnerPath = Join-Path $PSScriptRoot "run-postgres-lifecycle-validation.ps1"

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Read-NormalizedText {
    param([string]$Path)
    Assert-True ([IO.File]::Exists($Path)) "Required file is missing: $Path"
    return [IO.File]::ReadAllText($Path).Replace("`r`n", "`n").Replace("`r", "`n")
}

$Runner = Read-NormalizedText $RunnerPath
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($RunnerPath, [ref]$tokens, [ref]$parseErrors)
Assert-True ($parseErrors.Count -eq 0) "Lifecycle runner must parse without errors"
foreach ($parameter in @('PostgresUrl', 'MultiGatewayDatabaseUrl', 'SkipTeardown')) {
    Assert-True ($parameter -cin @($ast.ParamBlock.Parameters.Name.VariablePath.UserPath)) "Missing runner parameter: $parameter"
}
Assert-True ($Runner -match '(?s)\[Parameter\(Mandatory\)\]\s*\[ValidateNotNullOrEmpty\(\)\]\s*\[string\]\$PostgresUrl') "PostgresUrl must be required and nonempty"

$pullPattern = '(?i)\b(?:docker(?:\.exe)?\s+(?:compose\s+)?|docker-compose(?:\.exe)?\s+|podman(?:\.exe)?\s+(?:compose\s+)?|crane(?:\.exe)?\s+|nerdctl(?:\.exe)?\s+)pull\b'
Assert-True (-not [regex]::IsMatch($Runner, $pullPattern)) "Runner must never request an image pull"
# Forbidding every container command also closes implicit image acquisition paths.
Assert-True ($Runner -notmatch '(?i)\b(?:docker|docker-compose|podman|crane|nerdctl)(?:\.exe)?\b') "Runner must not invoke container tooling at all"
$cargoCalls = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.CommandAst] -and
        ($node.CommandElements[0].Extent.Text -ceq '$CargoPath' -or $node.GetCommandName() -match '(?i)(?:^|[/\\])cargo(?:\.exe)?$')
}, $true))
Assert-True ($cargoCalls.Count -eq 1) "All cargo work must pass through one reviewed invocation"
Assert-True ($cargoCalls[0].Extent.Text -match '^& \$CargoPath test --locked --offline --manifest-path \$ManifestPath @arguments 2>&1$') "Every cargo invocation must be a locked offline test with captured stderr"
foreach ($fragment in @(
    'Get-Command cargo -CommandType Application',
    '[ValidateSet("postgres_lifecycle", "multi_gateway")]',
    '"--no-run", "--message-format=json"',
    '"--nocapture", "--test-threads=1"',
    '$code = $LASTEXITCODE',
    'if ($code -ne 0)',
    'throw "$Name failed with exit code $code"',
    '[IO.File]::Exists($artifacts[0])',
    "'compiler-artifact'",
    'tests/results/postgres-lifecycle-validation',
    '.stdout.log', '.stderr.log', '.summary.json',
    'ConvertTo-Json -Depth 6',
    '[guid]::NewGuid().ToString("N")',
    'IPFS_S3_TEST_POSTGRES_URL',
    'IPFS_S3_MULTI_GATEWAY_DATABASE_URL',
    'IPFS_S3_MULTI_GATEWAY_A_ENDPOINT',
    'IPFS_S3_MULTI_GATEWAY_B_ENDPOINT',
    'IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT',
    'IPFS_S3_MULTI_GATEWAY_KUBO_URL',
    '$env:IPFS_S3_TEST_POSTGRES_URL = $PostgresUrl',
    '$env:IPFS_S3_MULTI_GATEWAY_DATABASE_URL = $MultiGatewayDatabaseUrl',
    '[Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], "Process")',
    '[TimeSpan]::FromSeconds(5)',
    '$stream.ReadTimeout = 5000',
    '[byte[]]@(0, 0, 0, 8, 4, 210, 22, 47)',
    'Prepare a reachable PostgreSQL 17 instance yourself',
    'does not disable Rust fixture cleanup',
    'exit $exitCode'
)) {
    Assert-True ($Runner.Contains($fragment, [StringComparison]::Ordinal)) "Missing runner safety contract: $fragment"
}
Assert-True ($Runner -notmatch '(?i)DROP\s+(?:SCHEMA|DATABASE)|Remove-Item|Invoke-Expression|\biex\b') "Runner must not delete external resources or evaluate command text"
Assert-True ($Runner -match '(?s)finally\s*\{.*?SetEnvironmentVariable.*?ConvertTo-Json') "Environment restoration and summary must run even on failure"

# Only the pure redactor is evaluated; static validation must never enter the live runner.
$redactor = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Protect-Diagnostic'
}, $true))
Assert-True ($redactor.Count -eq 1) "Expected one diagnostic redactor"
. ([scriptblock]::Create($redactor[0].Extent.Text))
$PostgresUrl = 'postgresql://fixture:private-a@127.0.0.1:5432/fixture'
$MultiGatewayDatabaseUrl = 'postgres://fixture:private-b@127.0.0.1:5432/gateway'
$safe = Protect-Diagnostic "$PostgresUrl $MultiGatewayDatabaseUrl POSTGRESQL://other:private-c@host/db"
Assert-True ($safe -ceq '[REDACTED_DATABASE_URL] [REDACTED_DATABASE_URL] [REDACTED_DATABASE_URL]') "Diagnostics must redact both configured URLs and alternate PostgreSQL URL forms"
Assert-True ((Protect-Diagnostic 'test result: ok. 8 passed; 0 failed') -ceq 'test result: ok. 8 passed; 0 failed') "Redaction must preserve safe test diagnostics"

$sequence = @(
    'Assert-PostgresReachable $PostgresUrl',
    'Assert-MultiGatewayEnvironment',
    'Invoke-CargoStep "compile-postgres_lifecycle" "postgres_lifecycle" -Compile',
    'Invoke-CargoStep "compile-multi_gateway" "multi_gateway" -Compile',
    'Invoke-CargoStep "test-postgres_lifecycle" "postgres_lifecycle"',
    'Invoke-CargoStep "test-multi_gateway" "multi_gateway"'
)
$cursor = 0
foreach ($fragment in $sequence) {
    $index = $Runner.IndexOf($fragment, $cursor, [StringComparison]::Ordinal)
    Assert-True ($index -ge 0) "Missing or out-of-order fail-fast step: $fragment"
    $cursor = $index + $fragment.Length
}

$postgres = Read-NormalizedText (Join-Path $PSScriptRoot 'postgres_lifecycle.rs')
$multi = Read-NormalizedText (Join-Path $PSScriptRoot 'multi_gateway.rs')
foreach ($name in @(
    'postgres_lifecycle_abort_migration_preserves_version_identity_and_checks_shapes',
    'postgres_lifecycle_abort_down_refuses_abort_state_and_restores_phase_a',
    'postgres_lifecycle_abort_multiworker_claim_crash_retry_is_fenced',
    'postgres_lifecycle_abort_configuration_and_bucket_lock_races_are_atomic'
)) {
    Assert-True ($postgres -match "(?m)^async fn $name\(\) \{") "Missing PostgreSQL regression: $name"
}
foreach ($name in @(
    'multi_gateway_cross_replica_contract',
    'multi_gateway_lifecycle_configuration_visible_across_replicas',
    'multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome',
    'multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome'
)) {
    Assert-True ($multi -match "(?m)^async fn $name\(\) \{") "Missing multi-gateway regression: $name"
}
Assert-True ($postgres.Contains('impl Drop for OwnedPgSchemaCleanup')) "Rust must retain ownership-aware failure cleanup"
Assert-True ($postgres.Contains('schema.strip_prefix("lifecycle_")')) "Rust schema cleanup must remain ownership-scoped"

$notRun = Read-NormalizedText (Join-Path $PSScriptRoot 'results/postgres-lifecycle-validation/NOT-RUN.md')
foreach ($fragment in @('2026-09-09', 'NOT RUN', '运行器已就绪、尚未实跑', 'Task 9', '-PostgresUrl', '-SkipTeardown')) {
    Assert-True ($notRun.Contains($fragment)) "Initial evidence is missing: $fragment"
}
Write-Host 'PostgreSQL lifecycle static safety contracts: PASS'
