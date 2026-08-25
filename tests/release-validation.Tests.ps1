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

function Get-PwshRunBlocks {
    param([Parameter(Mandatory)][string]$JobBlock)

    $jobLines = @($JobBlock -split "`n")
    $blocks = [Collections.Generic.List[string]]::new()
    for ($lineIndex = 0; $lineIndex -lt $jobLines.Count; $lineIndex++) {
        if ($jobLines[$lineIndex].Trim() -cne "shell: pwsh") { continue }
        $runIndex = $lineIndex + 1
        while ($runIndex -lt $jobLines.Count -and [string]::IsNullOrWhiteSpace($jobLines[$runIndex])) { $runIndex++ }
        Assert-True ($runIndex -lt $jobLines.Count -and $jobLines[$runIndex].Trim() -ceq "run: |") "Each workflow PowerShell step must use a literal run block"

        $sourceLines = [Collections.Generic.List[string]]::new()
        for ($bodyIndex = $runIndex + 1; $bodyIndex -lt $jobLines.Count; $bodyIndex++) {
            $line = $jobLines[$bodyIndex]
            if (-not [string]::IsNullOrWhiteSpace($line) -and ([regex]::Match($line, '^( *)').Groups[1].Length -le 8)) { break }
            if ([string]::IsNullOrWhiteSpace($line)) {
                $sourceLines.Add("")
                continue
            }
            Assert-True ($line.StartsWith("          ", [StringComparison]::Ordinal)) "Workflow PowerShell run line lost YAML indentation"
            $sourceLines.Add($line.Substring(10))
        }
        $blocks.Add($sourceLines -join "`n")
    }
    return @($blocks.ToArray())
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
Assert-True ($jobNames.Count -eq 6) "Expected exactly six jobs, found $($jobNames.Count): $($jobNames -join ', ')"
foreach ($expectedJob in @(
    "postgres-import",
    "postgres-production-deployment",
    "multi-gateway-deployment",
    "cluster-pinset-replication",
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
$clusterJob = Get-YamlBlock -Text $jobsBlock -Key "cluster-pinset-replication" -Indent 2
$e2eJob = Get-YamlBlock -Text $jobsBlock -Key "e2e" -Indent 2
$clientJob = Get-YamlBlock -Text $jobsBlock -Key "client-smoke-infrastructure" -Indent 2

$allWorkflowPwshBlocks = [Collections.Generic.List[string]]::new()
foreach ($jobName in $jobNames) {
    $jobBlock = Get-YamlBlock -Text $jobsBlock -Key $jobName -Indent 2
    foreach ($source in (Get-PwshRunBlocks $jobBlock)) {
        $tokens = $null
        $parseErrors = $null
        [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$parseErrors) | Out-Null
        if ($parseErrors.Count -ne 0) { throw "Workflow PowerShell AST parse failed in ${jobName}: $($parseErrors.Message -join '; ')" }
        Assert-NotMatches $source '(?m)(?:^|\s)(?:export\s+|source\s+)|&&|/dev/null' "Workflow PowerShell contains Bash syntax in $jobName"
        $allWorkflowPwshBlocks.Add($source)
    }
}
Assert-True ($allWorkflowPwshBlocks.Count -gt 0) "Release workflow must contain PowerShell run blocks to parse"

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

Assert-Contains $clusterJob "    runs-on: ubuntu-latest" "Cluster pinset replication job must use ubuntu-latest"
Assert-Contains $clusterJob "    timeout-minutes: 60" "Cluster pinset replication job timeout must be 60 minutes"
Assert-RustSetup -JobBlock $clusterJob -JobName "Cluster pinset replication job"
Assert-NotMatches $clusterJob '(?m)^    (?:needs|continue-on-error):' "Cluster pinset replication job must be independent and blocking"
Assert-NotMatches $clusterJob '(?m)^        continue-on-error:' "Cluster pinset replication product gates must be blocking"
$clusterEnv = Get-YamlBlock -Text $clusterJob -Key "env" -Indent 4
$clusterEnvLines = @($clusterEnv -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($clusterEnvLines.Count -eq 19) "Cluster pinset replication job must contain exactly nineteen job environment values"
foreach ($line in @(
    '      COMPOSE_DISABLE_ENV_FILE: "1"',
    '      COMPOSE_PROJECT_NAME: ipfs3-cl-${{ github.run_id }}-${{ github.run_attempt }}',
    '      POSTGRES_PASSWORD: cl-${{ github.run_id }}-${{ github.run_attempt }}',
    "      IPFS_S3_ACCESS_KEY_ID: test",
    "      IPFS_S3_SECRET_ACCESS_KEY: test",
    "      IPFS_S3_MASTER_KEY: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "      IPFS_S3_CLUSTER_SECRET: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    "      IPFS_S3_GATEWAY_BIND: 127.0.0.1",
    "      IPFS_S3_GATEWAY_PORT: 59100",
    "      IPFS_S3_CLUSTER_GATEWAY_ENDPOINT: http://127.0.0.1:59100",
    "      IPFS_S3_CLUSTER_A_REST_URL: http://127.0.0.1:59101",
    "      IPFS_S3_CLUSTER_B_REST_URL: http://127.0.0.1:59102",
    "      IPFS_S3_CLUSTER_A_PROXY_URL: http://127.0.0.1:59103",
    "      IPFS_S3_CLUSTER_KUBO_A_URL: http://127.0.0.1:55100",
    "      IPFS_S3_CLUSTER_KUBO_B_URL: http://127.0.0.1:55101",
    "      IPFS_S3_CLUSTER_KUBO_C_URL: http://127.0.0.1:55102",
    '      IPFS_S3_SWARM_KEY_FILE: ${{ runner.temp }}/ipfs3-swarm-${{ github.run_id }}-${{ github.run_attempt }}.key',
    '      IPFS_S3_SWARM_KEY_WRONG_FILE: ${{ runner.temp }}/ipfs3-swarm-wrong-${{ github.run_id }}-${{ github.run_attempt }}.key',
    '      IPFS_S3_CLUSTER_STATE_PATH: ${{ runner.temp }}/ipfs3-cluster-${{ github.run_id }}-${{ github.run_attempt }}.json'
)) {
    Assert-ExactLine $clusterEnv $line "Cluster pinset replication environment line is missing or changed."
}
Assert-NotContains $clusterJob "--env-file" "Cluster validation must not use an explicit environment file"
Assert-NotMatches $clusterJob '(?i)(?<![A-Za-z0-9_])\.env(?![A-Za-z0-9_])' "Cluster validation must not read, modify, or emit a project environment file"
Assert-InOrder -Text $clusterJob -Message "Cluster pinset replication steps are missing or out of order." -Fragments @(
    "      - name: Verify Docker Compose for Cluster deployment",
    "      - name: Verify Cluster environment contract",
    "      - name: Claim unique Cluster project, ports, and state receipt",
    "      - name: Build and start Cluster topology",
    "      - name: Prove Cluster release-version representation contract",
    "      - name: Prove private swarm causality before topology",
    "      - name: Prove exact two-peer topology without writes",
    "      - name: Prove direct Kubo wire compatibility against Cluster A proxy",
    "      - name: Prove replication and retained deletion",
    "      - name: Capture diagnostics before peer B stop",
    "      - name: Stop Cluster and Kubo peer B",
    "      - name: Prove stopped peer loses two-pin evidence",
    "      - name: Capture stopped-peer diagnostics before restart",
    "      - name: Restart both Cluster peers and Kubo swarm with existing volumes",
    "      - name: Prove same-volume private swarm and peer B recovery",
    "      - name: Final sanitized Cluster diagnostics",
    "      - name: Cluster cleanup and residual assertion"
)
foreach ($fragment in @(
    "docker-compose.cluster.yml",
    "tests/compose.cluster-validation.yml",
    "Docker Compose 2.23.1 or newer",
    "config --quiet",
    'if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Gateway bind must be exactly 127.0.0.1" }',
    "[IO.FileMode]::CreateNew",
    "CLUSTER_PINSET_OWNED=true",
    "CLUSTER_STATE_RECEIPT_OWNED=true",
    "CLUSTER_PINSET_ATTEMPTED=true",
    "55435, 55100, 55101, 55102, 59100, 59101, 59102, 59103",
    "--profile private-swarm-validation @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway",
    "cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact",
    "Cluster release-version unit contract failed",
    "cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1",
    "CLUSTER_TOPOLOGY_GREEN=true",
    "Topology GREEN receipt is required before compatibility",
    "PROXY_COMPATIBILITY_BLOCKER",
    "CLUSTER_PROXY_COMPATIBILITY_GREEN=true",
    "Add-pin-cat proxy GREEN receipt is required before replication",
    "cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1",
    "stop cluster-b kubo-b",
    "cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1",
    "stop cluster-a kubo-a",
    "start kubo-a kubo-b",
    "restart --timeout 30 swarm-bootstrap",
    "start cluster-a cluster-b",
    "cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1",
    "logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $clusterJob $fragment "Cluster pinset replication contract is missing: $fragment"
}
Assert-NotContains $clusterJob 'IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")' "Cluster validation must not retain the weaker wildcard-only bind check"
Assert-InOrder -Text $clusterJob -Message "Cluster validation must validate fixed loopback before any Compose config or startup." -Fragments @(
    'if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Gateway bind must be exactly 127.0.0.1" }',
    "docker compose @compose config --quiet",
    "--profile private-swarm-validation @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway"
)
Assert-True (([regex]::Matches($clusterJob, '(?m)^          cargo test --test cluster [^\r\n]+$')).Count -eq 12) "Cluster pinset replication job must contain exactly twelve explicit Cluster cargo commands"
Assert-InOrder -Text $clusterJob -Message "Cluster cargo commands must preserve pure-unit then live causal order." -Fragments @(
    "cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact",
    "cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact",
    "cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact",
    "cargo test --test cluster cluster_support::kubo_swarm_peers_null_is_empty_without_ndjson -- --exact",
    "cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster private_swarm_wrong_key_rejected -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1",
    "cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1"
)
Assert-InOrder -Text $clusterJob -Message "Cluster topology receipt must be written only after a successful topology gate and checked before proxy compatibility." -Fragments @(
    "cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1",
    'if ($LASTEXITCODE -ne 0) { throw "TOPOLOGY_CONVERGENCE_BLOCKER: exact v1.1.6 two-peer topology did not converge" }',
    '"CLUSTER_TOPOLOGY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'if ($env:CLUSTER_TOPOLOGY_GREEN -ne "true") { throw "Topology GREEN receipt is required before compatibility" }',
    "cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1"
)
Assert-InOrder -Text $clusterJob -Message "Cluster proxy receipt must be written only after compatibility success and checked before replication receipt ownership." -Fragments @(
    "cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1",
    'if ($LASTEXITCODE -ne 0) { throw "PROXY_COMPATIBILITY_BLOCKER: stop and revise the approved design; do not add app fallback code" }',
    '"CLUSTER_PROXY_COMPATIBILITY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'if ($env:CLUSTER_PROXY_COMPATIBILITY_GREEN -ne "true") { throw "Add-pin-cat proxy GREEN receipt is required before replication" }',
    'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before replication" }',
    "cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1"
)
Assert-NotMatches $clusterJob '(?i)docker\s+(?:compose\s+)?pull\b|--pull(?:=|\s)' "Cluster validation must not explicitly pull images"
Assert-NotMatches $clusterJob '(?i)docker\s+(?:system|container|network|volume)\s+prune|docker\s+rm\s+-f' "Cluster cleanup must not prune broad Docker resources"
Assert-NotMatches $clusterJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Cluster validation must not use null environment removal"
Assert-Contains $clusterJob 'dst=/run/secrets/swarm_key,readonly' "Cluster supervisor proof must mount only the exact swarm-key secret path"
foreach ($fragment in @(
    '"--mount", "type=bind,src=$fullKeyPath,dst=/run/secrets/swarm_key,readonly"',
    '"--env", "IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key"',
    'docker run --rm --entrypoint /bin/sh ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0 -ec $sourceFixture 2>&1',
    '. /private-swarm-entrypoint.sh',
    'redact_swarm_fingerprint',
    'select_supervisor_exit',
    'ordinary=0123456789abcdef0123456789abcdef',
    "cid=Qm`$(printf '%044d' 0 | tr 0 c)",
    'Swarm key fingerprint: [redacted]',
    'check_selector 37 37 1 1',
    'check_selector 1 0 1 0',
    'check_selector 1 0 0 1',
    'check_selector 0 0 0 0',
    'CLUSTER_PRIVATE_SWARM_FILTER_GREEN=true',
    'logs --no-color kubo-a kubo-b kubo-c 2>&1',
    'Swarm key fingerprint: [redacted]',
    '$rawKuboRedactedFingerprintCount -lt 3',
    '$rawKuboLogs = $null',
    '$rawKuboText = $null',
    '$rawKuboRedactedFingerprintCount = $null',
    'docker compose --profile private-swarm-validation --project-name $project -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml down --volumes --remove-orphans'
)) {
    Assert-Contains $clusterJob $fragment "Cluster private-swarm completion contract is missing: $fragment"
}
foreach ($forbiddenMountFragment in @('src=$keyParent', 'dst=/run/ipfs3-swarm', 'src=$env:RUNNER_TEMP', 'IPFS_SWARM_KEY_FILE=/run/ipfs3-swarm/')) {
    Assert-NotContains $clusterJob $forbiddenMountFragment "Cluster private-swarm proof must not expose a key directory or non-secret key path: $forbiddenMountFragment"
}
Assert-InOrder -Text $clusterJob -Message "Source-only filter receipt must precede disposable supervisor proofs." -Fragments @(
    'docker run --rm --entrypoint /bin/sh ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0 -ec $sourceFixture 2>&1',
    'CLUSTER_PRIVATE_SWARM_FILTER_GREEN=true',
    'Invoke-PrivateSwarmSupervisorProof -Mode normal -ExpectedExit 37',
    'Invoke-PrivateSwarmSupervisorProof -Mode handled -ExpectedExit 43',
    'Invoke-PrivateSwarmSupervisorProof -Mode unhandled -ExpectedExit 143',
    'CLUSTER_PRIVATE_SWARM_SUPERVISOR_GREEN=true'
)

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
Assert-True ($clientRunLines.Count -eq 5) "Client-smoke infrastructure job must contain exactly five run commands"
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1" "Release-validation contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1" "PostgreSQL production baseline contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1" "Multi-gateway deployment contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/cluster.Tests.ps1" "Cluster pinset replication contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1" "Client-smoke infrastructure command is missing or changed."
Assert-InOrder -Text $clientJob -Message "Static release contracts must run before the existing client-smoke infrastructure test." -Fragments @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/cluster.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
Assert-NotMatches $clientJob '(?m)^        continue-on-error:' "Client-smoke infrastructure contract steps must be blocking"

Assert-NotContains $Workflow "cloudflared" "Release validation must not start or reference cloudflared"
Assert-NotContains $Workflow "scripts/client-smoke.ps1" "Release validation must not invoke the real client-smoke runner"
Assert-NotMatches $Workflow '(?im)^\s*run:[^\r\n]*(?:\s|/)(?:aws|mc|rclone)(?:\.exe)?(?:\s|$)' "Release validation must not execute a real AWS, mc, or rclone client"
Assert-NotMatches $clientJob '(?m)(?:^|\s)-Run(?:\s|$)' "Client-smoke infrastructure job must not request real client execution"

Write-Host "release-validation workflow contract tests: PASSED"
