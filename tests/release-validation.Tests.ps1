$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
if (-not [IO.File]::Exists($WorkflowPath)) {
    throw "Release validation workflow is missing: $WorkflowPath"
}

$Workflow = [IO.File]::ReadAllText($WorkflowPath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")

function Assert-True {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { throw $Message }
}

function Assert-Contains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ($Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not $Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-Matches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ([regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-NotMatches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not [regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-ExactLine {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Line,
        [Parameter(Mandatory)][string]$Message
    )

    $count = @($Text -split "`n" | Where-Object { $_ -ceq $Line }).Count
    Assert-True ($count -eq 1) "$Message Expected exactly one line, found $count."
}

function Get-YamlBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][int]$Indent
    )

    $lines = @($Text -split "`n")
    $header = (" " * $Indent) + $Key + ":"
    $matchingIndexes = @(
        for ($index = 0; $index -lt $lines.Count; $index++) {
            if ($lines[$index].TrimEnd() -ceq $header) { $index }
        }
    )
    if ($matchingIndexes.Count -ne 1) {
        throw "Expected one YAML key '$header', found $($matchingIndexes.Count)"
    }

    $start = $matchingIndexes[0]
    $end = $lines.Count
    for ($index = $start + 1; $index -lt $lines.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace($lines[$index])) { continue }
        $leadingSpaces = [regex]::Match($lines[$index], '^( *)').Groups[1].Length
        if ($leadingSpaces -le $Indent) {
            $end = $index
            break
        }
    }
    if ($end -le $start + 1) { return "" }
    return ($lines[($start + 1)..($end - 1)] -join "`n")
}

function Assert-InOrder {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string[]]$Fragments,
        [Parameter(Mandatory)][string]$Message
    )

    $cursor = 0
    foreach ($fragment in $Fragments) {
        $index = $Text.IndexOf($fragment, $cursor, [StringComparison]::Ordinal)
        if ($index -lt 0) { throw "$Message Missing or out of order: $fragment" }
        $cursor = $index + $fragment.Length
    }
}

function Assert-RustSetup {
    param(
        [Parameter(Mandatory)][string]$JobBlock,
        [Parameter(Mandatory)][string]$JobName
    )

    foreach ($fragment in @(
        "      - uses: actions/checkout@v7",
        "        uses: dtolnay/rust-toolchain@v1",
        '          toolchain: "1.92"',
        "        uses: Swatinem/rust-cache@v2"
    )) {
        Assert-Contains $JobBlock $fragment "$JobName is missing required Rust setup fragment: $fragment"
    }
}

Assert-NotContains $Workflow "`t" "Workflow must use spaces, not tabs"

# Read `on:` as an exact textual key so PowerShell never delegates YAML 1.1 key coercion.
$onBlock = Get-YamlBlock -Text $Workflow -Key "on" -Indent 0
Assert-Matches $onBlock '(?m)^  pull_request:\s*$' "pull_request trigger is missing"
Assert-Matches $onBlock '(?m)^  workflow_dispatch:\s*$' "workflow_dispatch trigger is missing"
$pushBlock = Get-YamlBlock -Text $onBlock -Key "push" -Indent 2
Assert-Matches $pushBlock '(?m)^    branches: \[master\]\s*$' "push trigger must target only master"

$permissionsBlock = Get-YamlBlock -Text $Workflow -Key "permissions" -Indent 0
$permissionLines = @($permissionsBlock -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($permissionLines.Count -eq 1) "Workflow permissions must contain only contents: read"
Assert-True ($permissionLines[0].Trim() -ceq "contents: read") "Workflow permissions must be read-only contents: read"
Assert-NotMatches $Workflow '(?m)^\s*[A-Za-z0-9_-]+:\s*write\s*$' "Workflow must not grant write permission"

$concurrencyBlock = Get-YamlBlock -Text $Workflow -Key "concurrency" -Indent 0
$concurrencyLines = @($concurrencyBlock -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($concurrencyLines.Count -eq 2) "Concurrency must contain only group and cancel-in-progress"
Assert-Contains $concurrencyBlock '  group: ${{ github.workflow }}-${{ github.ref }}' "Concurrency group must combine workflow and ref"
Assert-Contains $concurrencyBlock "  cancel-in-progress: true" "Concurrency must cancel an older run for the same ref"

$jobsBlock = Get-YamlBlock -Text $Workflow -Key "jobs" -Indent 0
$jobNames = @([regex]::Matches($jobsBlock, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($jobNames.Count -eq 5) "Expected exactly five jobs, found $($jobNames.Count): $($jobNames -join ', ')"
foreach ($expectedJob in @(
    "postgres-import",
    "postgres-production-deployment",
    "multi-gateway-deployment",
    "e2e",
    "client-smoke-infrastructure"
)) {
    Assert-True ($jobNames -ccontains $expectedJob) "Required job is missing: $expectedJob"
}
Assert-NotMatches $jobsBlock '(?m)^    needs:' "Release-validation jobs must be independent"
Assert-NotMatches $jobsBlock '(?m)^    continue-on-error:' "Release-validation jobs must be blocking"

$postgresJob = Get-YamlBlock -Text $jobsBlock -Key "postgres-import" -Indent 2
$productionJob = Get-YamlBlock -Text $jobsBlock -Key "postgres-production-deployment" -Indent 2
$multiGatewayJob = Get-YamlBlock -Text $jobsBlock -Key "multi-gateway-deployment" -Indent 2
$e2eJob = Get-YamlBlock -Text $jobsBlock -Key "e2e" -Indent 2
$clientJob = Get-YamlBlock -Text $jobsBlock -Key "client-smoke-infrastructure" -Indent 2

Assert-Contains $postgresJob "    runs-on: ubuntu-latest" "PostgreSQL job must use ubuntu-latest"
Assert-Contains $postgresJob "    timeout-minutes: 30" "PostgreSQL job timeout must be 30 minutes"
Assert-RustSetup -JobBlock $postgresJob -JobName "PostgreSQL job"
$postgresEnv = Get-YamlBlock -Text $postgresJob -Key "env" -Indent 4
$postgresEnvLines = @($postgresEnv -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($postgresEnvLines.Count -eq 1) "PostgreSQL job env must contain exactly the test URL"
Assert-True ($postgresEnvLines[0].Trim() -ceq "IPFS_S3_TEST_POSTGRES_URL: postgres://ipfs3:ipfs3@127.0.0.1:5432/ipfs3_import_test") "PostgreSQL URL is missing or not job-scoped"
$postgresServices = Get-YamlBlock -Text $postgresJob -Key "services" -Indent 4
$postgresService = Get-YamlBlock -Text $postgresServices -Key "postgres" -Indent 6
foreach ($fragment in @(
    "        image: postgres:17",
    "          POSTGRES_DB: ipfs3_import_test",
    "          POSTGRES_USER: ipfs3",
    "          POSTGRES_PASSWORD: ipfs3",
    "          - 5432:5432",
    '          --health-cmd "pg_isready -U ipfs3 -d ipfs3_import_test"',
    "          --health-interval 1s",
    "          --health-timeout 5s",
    "          --health-retries 30"
)) {
    Assert-Contains $postgresService $fragment "PostgreSQL service contract is missing: $fragment"
}
$postgresRunLines = @($postgresJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($postgresRunLines.Count -eq 1) "PostgreSQL job must contain exactly one run command"
Assert-ExactLine $postgresJob "        run: cargo test --test postgres_import -- --nocapture --test-threads=1" "PostgreSQL job must run the exact serial target."

Assert-Contains $productionJob "    runs-on: ubuntu-latest" "Production deployment job must use ubuntu-latest"
Assert-Contains $productionJob "    timeout-minutes: 60" "Production deployment timeout must be 60 minutes"
Assert-RustSetup -JobBlock $productionJob -JobName "Production deployment job"
$productionEnv = Get-YamlBlock -Text $productionJob -Key "env" -Indent 4
Assert-ExactLine $productionEnv '      COMPOSE_DISABLE_ENV_FILE: "1"' "Production deployment job must disable implicit project environment files."
Assert-NotMatches $productionEnv '(?im)^\s*COMPOSE_DISABLE_ENV_FILE:\s*(?:"?0"?|"?false"?)\s*$' "Production deployment job must not enable implicit project environment files"
Assert-NotContains $productionJob "--env-file" "Production deployment job must not use an explicit Compose environment file"
Assert-Contains $productionJob '      COMPOSE_PROJECT_NAME: ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}' "Production project name must include run ID and attempt"
Assert-Contains $productionJob "      IPFS_S3_ACCESS_KEY_ID: test" "Production E2E access key must match tests/e2e.rs"
Assert-Contains $productionJob "      IPFS_S3_SECRET_ACCESS_KEY: test" "Production E2E secret key must match tests/e2e.rs"
Assert-Contains $productionJob "      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000" "Production E2E endpoint is incorrect"
Assert-Contains $productionJob "      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55001" "Production Kubo endpoint is incorrect"
Assert-NotMatches $productionJob '(?m)^    continue-on-error:' "Production deployment job must be blocking"
Assert-InOrder -Text $productionJob -Message "Production deployment checks are missing or out of order." -Fragments @(
    "      - name: Verify Docker Compose",
    "      - name: Verify production environment contract",
    "      - name: Claim unique Compose project and fixed ports",
    "      - name: Build and start production topology",
    "      - name: Verify liveness and readiness",
    "      - name: Verify latest migration, JSON columns, and application role",
    "      - name: Run serial PostgreSQL-backed end-to-end tests",
    "      - name: Stop PostgreSQL and verify readiness failure",
    "      - name: Production Compose diagnostics",
    "      - name: Production Compose cleanup and residual assertion"
)
foreach ($fragment in @(
    "docker-compose.postgres.yml",
    "tests/compose.postgres-production-validation.yml",
    "Docker Compose 2.23.1 or newer",
    "config --quiet",
    "up --detach --build --wait --wait-timeout 300 postgres kubo gateway",
    'm20260813_000001_postgres_json_columns',
    'multipart_uploads.metadata:jsonb',
    'multipart_uploads.tags_json:jsonb',
    'objects.metadata:jsonb',
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "stop postgres",
    'http://127.0.0.1:59000/health',
    'http://127.0.0.1:59000/ready',
    "logs --no-color postgres kubo gateway",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $productionJob $fragment "Production deployment contract is missing: $fragment"
}
Assert-NotContains $productionJob 'm20260730_000001_standard_mutation_fence' "Production deployment job must not validate the previous latest migration"
Assert-NotContains $productionJob "compose --wait" "No Compose wait is allowed after PostgreSQL is stopped"
Assert-True (([regex]::Matches($productionJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Production diagnostics and cleanup must both use always()"
Assert-NotMatches $productionJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Production job must not use SetEnvironmentVariable(..., `$null, ...) to remove or restore an environment variable"
foreach ($restoreFragment in @(
    'if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required job variable is absent before validation: $requiredName" }',
    'Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop',
    'if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal did not produce absence: $name" }',
    'if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Required variable removal retained a process value: $name" }',
    'if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required variable restoration lost presence: $requiredName" }',
    '$restoredRequiredValue = [Environment]::GetEnvironmentVariable($requiredName, "Process")',
    'if ($restoredRequiredValue -cne $savedRequiredValues[$requiredName]) { throw "Required variable restoration changed value: $requiredName" }'
)) {
    Assert-Contains $productionJob $restoreFragment "Production environment restoration contract is missing: $restoreFragment"
}
Assert-Matches $productionJob '(?s)try \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?\} finally \{.*?SetEnvironmentVariable\(\$requiredName, \$savedRequiredValues\[\$requiredName\], "Process"\).*?Test-Path -LiteralPath "Env:\$requiredName".*?\$restoredRequiredValue -cne \$savedRequiredValues\[\$requiredName\]' "Each missing-secret probe must remove deterministically and restore exact presence/value in finally"

Assert-Contains $multiGatewayJob "    runs-on: ubuntu-latest" "Multi-gateway job must use ubuntu-latest"
Assert-Contains $multiGatewayJob "    timeout-minutes: 60" "Multi-gateway job timeout must be 60 minutes"
Assert-RustSetup -JobBlock $multiGatewayJob -JobName "Multi-gateway job"
$multiGatewayEnv = Get-YamlBlock -Text $multiGatewayJob -Key "env" -Indent 4
foreach ($line in @(
    '      COMPOSE_DISABLE_ENV_FILE: "1"',
    '      COMPOSE_PROJECT_NAME: ipfs3-mg-${{ github.run_id }}-${{ github.run_attempt }}',
    "      IPFS_S3_MULTI_GATEWAY_A_ENDPOINT: http://127.0.0.1:59001",
    "      IPFS_S3_MULTI_GATEWAY_B_ENDPOINT: http://127.0.0.1:59002",
    "      IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT: http://127.0.0.1:59000",
    "      IPFS_S3_MULTI_GATEWAY_KUBO_URL: http://127.0.0.1:55002",
    "      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000",
    "      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55002"
)) {
    Assert-ExactLine $multiGatewayEnv $line "Multi-gateway environment line is missing or changed."
}
Assert-NotContains $multiGatewayJob "--env-file" "Multi-gateway validation must not use an explicit environment file"
Assert-NotMatches $multiGatewayJob '(?m)^    (?:needs|continue-on-error):' "Multi-gateway job must be independent and blocking"
Assert-InOrder -Text $multiGatewayJob -Message "Multi-gateway deployment steps are missing or out of order." -Fragments @(
    "      - name: Verify Docker Compose for multi-gateway deployment",
    "      - name: Verify multi-gateway environment contract",
    "      - name: Claim unique multi-gateway project and fixed ports",
    "      - name: Build and start the multi-gateway topology",
    "      - name: Verify both gateways, load balancer, and migrations",
    "      - name: Run direct cross-replica acceptance",
    "      - name: Run existing E2E through the load balancer",
    "      - name: Capture pre-failover diagnostics",
    "      - name: Stop gateway A and verify surviving route",
    "      - name: Run new CRUD through the surviving load-balanced path",
    "      - name: Multi-gateway Compose diagnostics",
    "      - name: Multi-gateway Compose cleanup and residual assertion"
)
foreach ($fragment in @(
    "docker-compose.multi-gateway.yml",
    "tests/compose.multi-gateway-validation.yml",
    "up --detach --build --wait --wait-timeout 300 postgres kubo gateway-a gateway-b load-balancer",
    "cargo test --test multi_gateway multi_gateway_cross_replica_contract -- --exact --nocapture --test-threads=1",
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "logs --no-color postgres kubo gateway-a gateway-b load-balancer",
    "stop gateway-a",
    "cargo test --test multi_gateway load_balancer_surviving_replica_crud -- --exact --nocapture --test-threads=1",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $multiGatewayJob $fragment "Multi-gateway deployment contract is missing: $fragment"
}
Assert-True (([regex]::Matches($multiGatewayJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Multi-gateway diagnostics and cleanup must both use always()"
Assert-NotMatches $multiGatewayJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Multi-gateway job must not use null SetEnvironmentVariable removal"

Assert-Contains $e2eJob "    runs-on: ubuntu-latest" "E2E job must use ubuntu-latest"
Assert-Contains $e2eJob "    timeout-minutes: 60" "E2E job timeout must be 60 minutes"
Assert-RustSetup -JobBlock $e2eJob -JobName "E2E job"
$e2eRunLines = @($e2eJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($e2eRunLines.Count -eq 5) "E2E job must contain exactly five run commands"
foreach ($line in @(
    "        run: docker compose version",
    "        run: docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway",
    "        run: cargo test --test e2e -- --nocapture --test-threads=1",
    "        run: docker compose -f docker-compose.yml logs --no-color kubo gateway",
    "        run: docker compose -f docker-compose.yml down --volumes --remove-orphans"
)) {
    Assert-ExactLine $e2eJob $line "E2E job command is missing or changed."
}
Assert-InOrder -Text $e2eJob -Message "E2E commands must preserve setup/test/diagnostics/cleanup order." -Fragments @(
    "        run: docker compose version",
    "        run: docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway",
    "        run: cargo test --test e2e -- --nocapture --test-threads=1",
    "      - name: Compose diagnostics",
    "        run: docker compose -f docker-compose.yml logs --no-color kubo gateway",
    "      - name: Compose cleanup",
    "        run: docker compose -f docker-compose.yml down --volumes --remove-orphans"
)
Assert-True (([regex]::Matches($e2eJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Diagnostics and cleanup must both use always()"
Assert-True (([regex]::Matches($e2eJob, '(?m)^        continue-on-error: true\s*$')).Count -eq 2) "Diagnostics and cleanup must both continue on error"
Assert-True (([regex]::Matches($jobsBlock, '(?m)^        continue-on-error: true\s*$')).Count -eq 2) "Only E2E diagnostics and cleanup may continue on error"

Assert-Contains $clientJob "    runs-on: ubuntu-latest" "Client-smoke infrastructure job must use ubuntu-latest"
Assert-Contains $clientJob "    timeout-minutes: 15" "Client-smoke infrastructure timeout must be 15 minutes"
Assert-Contains $clientJob "      - uses: actions/checkout@v7" "Client-smoke infrastructure job must check out the repository"
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($clientRunLines.Count -eq 4) "Client-smoke infrastructure job must contain exactly four run commands"
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1" "Release-validation contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1" "PostgreSQL production baseline contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1" "Multi-gateway deployment contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1" "Client-smoke infrastructure command is missing or changed."
Assert-InOrder -Text $clientJob -Message "Static release contracts must run before the existing client-smoke infrastructure test." -Fragments @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
Assert-NotMatches $clientJob '(?m)^        continue-on-error:' "Client-smoke infrastructure contract steps must be blocking"

Assert-NotContains $Workflow "cloudflared" "Release validation must not start or reference cloudflared"
Assert-NotContains $Workflow "scripts/client-smoke.ps1" "Release validation must not invoke the real client-smoke runner"
Assert-NotMatches $Workflow '(?im)^\s*run:[^\r\n]*(?:\s|/)(?:aws|mc|rclone)(?:\.exe)?(?:\s|$)' "Release validation must not execute a real AWS, mc, or rclone client"
Assert-NotMatches $clientJob '(?m)(?:^|\s)-Run(?:\s|$)' "Client-smoke infrastructure job must not request real client execution"

Write-Host "release-validation workflow contract tests: PASSED"
