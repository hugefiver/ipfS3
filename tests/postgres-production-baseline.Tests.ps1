$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ProductionPath = Join-Path $RepoRoot "docker-compose.postgres.yml"
$ValidationPath = Join-Path $RepoRoot "tests/compose.postgres-production-validation.yml"
$DefaultPath = Join-Path $RepoRoot "docker-compose.yml"
$DockerfilePath = Join-Path $RepoRoot "Dockerfile"

function Read-NormalizedText {
    param([Parameter(Mandatory)][string]$Path)
    if (-not [IO.File]::Exists($Path)) { throw "Required file is missing: $Path" }
    return [IO.File]::ReadAllText($Path).Replace("`r`n", "`n").Replace("`r", "`n")
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
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

function Get-YamlBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][int]$Indent
    )
    $lines = @($Text -split "`n")
    $header = (" " * $Indent) + $Key + ":"
    $indexes = @(
        for ($index = 0; $index -lt $lines.Count; $index++) {
            if ($lines[$index].TrimEnd() -ceq $header) { $index }
        }
    )
    if ($indexes.Count -ne 1) { throw "Expected one YAML key '$header', found $($indexes.Count)" }
    $start = $indexes[0]
    $end = $lines.Count
    for ($index = $start + 1; $index -lt $lines.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace($lines[$index])) { continue }
        $leading = [regex]::Match($lines[$index], '^( *)').Groups[1].Length
        if ($leading -le $Indent) { $end = $index; break }
    }
    if ($end -le $start + 1) { return "" }
    return $lines[($start + 1)..($end - 1)] -join "`n"
}

$Production = Read-NormalizedText $ProductionPath
$Validation = Read-NormalizedText $ValidationPath
$Default = Read-NormalizedText $DefaultPath
$Dockerfile = Read-NormalizedText $DockerfilePath
$expectedDefaultDigest = "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c"
$expectedDockerfileDigest = "34aa8b4b5b880ec474d14dabe625fce5fecb45c4e906eb3489ad335b7fb6837c"
$expectedDevConfigDigest = "506596ff7da7c684f5ab9b860a49784f676372f31fd9dbf08fef401438714183"
$actualDefaultDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath $DefaultPath).Hash.ToLowerInvariant()
$actualDockerfileDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath $DockerfilePath).Hash.ToLowerInvariant()
$actualDevConfigDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $RepoRoot "config.docker.toml")).Hash.ToLowerInvariant()
Assert-True ($actualDefaultDigest -ceq $expectedDefaultDigest) "Default SQLite Compose changed"
Assert-True ($actualDockerfileDigest -ceq $expectedDockerfileDigest) "Runtime Dockerfile changed or gained a probe package"
Assert-True ($actualDevConfigDigest -ceq $expectedDevConfigDigest) "Default development config changed"

Assert-NotContains $Production "`t" "Production Compose must use spaces"
Assert-Contains $Production "name: ipfs3-postgres" "Production Compose must use its dedicated project name"
$services = Get-YamlBlock $Production "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($serviceNames.Count -eq 3) "Production Compose must define exactly three services"
foreach ($name in @("postgres", "kubo", "gateway")) {
    Assert-True ($serviceNames -ccontains $name) "Production Compose is missing service: $name"
}

$postgres = Get-YamlBlock $services "postgres" 2
$kubo = Get-YamlBlock $services "kubo" 2
$gateway = Get-YamlBlock $services "gateway" 2
Assert-Contains $postgres "    image: postgres:17" "PostgreSQL must use postgres:17"
Assert-Contains $postgres '      POSTGRES_DB: postgres' "PostgreSQL bootstrap database must remain postgres"
Assert-Contains $postgres '      POSTGRES_USER: postgres' "PostgreSQL bootstrap superuser must remain postgres"
Assert-Contains $postgres '      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"' "PostgreSQL password must use required interpolation"
Assert-Contains $postgres '      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]' "PostgreSQL application-account healthcheck is missing"
Assert-Contains $postgres '        target: /docker-entrypoint-initdb.d/10-ipfs3.sql' "PostgreSQL must mount the inline application-role initializer"
Assert-NotMatches $postgres '(?m)^    ports:\s*$' "Production PostgreSQL must not publish a host port"
Assert-Contains $kubo '      test: ["CMD", "ipfs", "id"]' "Kubo must retain its ipfs id healthcheck"
Assert-NotMatches $kubo '(?m)^    ports:\s*$' "Production Kubo must not publish a host port"
Assert-Contains $gateway '      - "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"' "Gateway publication must require an explicit host bind and port"
Assert-Contains $gateway '      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"' "Gateway PostgreSQL URL is incorrect"
Assert-Contains $gateway '      IPFS_S3_KUBO_RPC_URL: http://kubo:5001' "Gateway Kubo URL is incorrect"
Assert-Contains $gateway '      IPFS_S3_BIND: 0.0.0.0:9000' "Internal gateway bind is incorrect"
Assert-Contains $gateway '      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"' "Access key must use required interpolation"
Assert-Contains $gateway '      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"' "Secret key must use required interpolation"
Assert-Contains $gateway '      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"' "Master key must use required interpolation"
Assert-Contains $gateway '      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]' "Gateway must use the in-image binary probe"
Assert-Contains $gateway "        condition: service_healthy" "Gateway dependencies must be health-gated"

foreach ($required in @(
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT"
)) {
    Assert-NotContains $Production ('${' + $required + ':-') "Required variable has a default: $required"
    Assert-NotContains $Production ('${' + $required + '-default') "Required variable has an alternate default: $required"
}
foreach ($forbidden in @(
    "container_name:",
    "cloudflared:",
    "gateway_data",
    "config.docker.toml",
    "IPFS_S3_CONFIG",
    "PINATA_JWT",
    "FILEBASE_PINNING_TOKEN",
    '0.0.0.0:${IPFS_S3_GATEWAY_PORT}'
)) {
    Assert-NotContains $Production $forbidden "Forbidden production Compose fragment: $forbidden"
}
Assert-NotMatches $Production '(?i)\b(?:curl|wget)\b' "Production healthcheck must not use curl or wget"

$configs = Get-YamlBlock $Production "configs" 0
Assert-Contains $configs "  postgres_init:" "PostgreSQL initializer config is missing"
Assert-Contains $configs "    content: |" "PostgreSQL initializer must be inline Compose content"
Assert-Contains $configs "      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE" "Initializer must create a non-superuser application role"
Assert-Contains $configs '      CREATE DATABASE ipfs3 OWNER ipfs3;' "Initializer must create the application-owned database"
Assert-Contains $configs '      \set app_password ''${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}''' "Initializer password must use required interpolation"
Assert-Contains $configs "      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec" "Initializer must quote the interpolated password through psql"
$volumes = Get-YamlBlock $Production "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($volumeNames.Count -eq 2) "Production Compose must declare exactly two named volumes"
Assert-True ($volumeNames -ccontains "postgres_data") "postgres_data volume is missing"
Assert-True ($volumeNames -ccontains "ipfs_data") "ipfs_data volume is missing"

$validationServices = Get-YamlBlock $Validation "services" 0
$validationNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($validationNames.Count -eq 3) "Validation override must mention exactly three services"
foreach ($name in @("postgres", "kubo", "gateway")) {
    Assert-True ($validationNames -ccontains $name) "Validation override is missing service: $name"
}
Assert-Contains (Get-YamlBlock $validationServices "postgres" 2) '      - "127.0.0.1:55433:5432"' "Validation PostgreSQL mapping is incorrect"
Assert-Contains (Get-YamlBlock $validationServices "kubo" 2) '      - "127.0.0.1:55001:5001"' "Validation Kubo mapping is incorrect"
Assert-Contains (Get-YamlBlock $validationServices "gateway" 2) '      - "127.0.0.1:59000:9000"' "Validation gateway mapping is incorrect"
Assert-NotMatches $Validation '(?m)^volumes:' "Validation override must not declare persistent volumes"
Assert-NotContains $Validation "0.0.0.0" "Validation override must publish only to loopback"

Assert-NotContains $Default "  postgres:" "Default Compose must not select PostgreSQL"
Assert-NotMatches $Dockerfile '(?im)\b(?:curl|wget)\b' "Runtime image must not install curl or wget"

$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
$Workflow = Read-NormalizedText $WorkflowPath
$workflowJobs = Get-YamlBlock $Workflow "jobs" 0
$workflowJobNames = @([regex]::Matches($workflowJobs, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($workflowJobNames.Count -eq 6) "Release-validation must contain exactly six jobs"
foreach ($requiredJob in @("postgres-import", "postgres-production-deployment", "multi-gateway-deployment", "cluster-pinset-replication", "e2e", "client-smoke-infrastructure")) {
    Assert-True ($workflowJobNames -ccontains $requiredJob) "Release-validation job is missing: $requiredJob"
}
$productionJob = Get-YamlBlock $workflowJobs "postgres-production-deployment" 2
$clientJob = Get-YamlBlock $workflowJobs "client-smoke-infrastructure" 2
$productionEnv = Get-YamlBlock $productionJob "env" 4
Assert-Matches $productionEnv '(?m)^      COMPOSE_DISABLE_ENV_FILE: "1"$' "Production deployment job must disable implicit project environment files"
Assert-NotMatches $productionEnv '(?im)^\s*COMPOSE_DISABLE_ENV_FILE:\s*(?:"?0"?|"?false"?)\s*$' "Production deployment job must not enable implicit project environment files"
Assert-NotContains $productionJob "--env-file" "Production deployment job must not use an explicit Compose environment file"
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
$expectedClientRunLines = @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/cluster.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
Assert-True ($clientRunLines.Count -eq 5) "Client-smoke infrastructure job must contain exactly five blocking run commands"
for ($index = 0; $index -lt $expectedClientRunLines.Count; $index++) {
    Assert-True ($clientRunLines[$index].TrimEnd() -ceq $expectedClientRunLines[$index]) "Client-smoke infrastructure command $($index + 1) is missing, changed, or out of order"
}
Assert-NotMatches $clientJob '(?m)^        continue-on-error:' "Client-smoke infrastructure contract steps must be blocking"
foreach ($fragment in @(
    'COMPOSE_PROJECT_NAME: ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}',
    "POSTGRES_PRODUCTION_OWNED=true",
    "POSTGRES_PRODUCTION_ATTEMPTED=true",
    "docker-compose.postgres.yml",
    "tests/compose.postgres-production-validation.yml",
    'IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000',
    'IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55001',
    'm20260813_000001_postgres_json_columns',
    'multipart_uploads.metadata:jsonb',
    'multipart_uploads.tags_json:jsonb',
    'objects.metadata:jsonb',
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $productionJob $fragment "Workflow production job is missing: $fragment"
}
Assert-NotContains $productionJob 'm20260730_000001_standard_mutation_fence' "Workflow production job must not validate the previous latest migration"
foreach ($contractFragment in @(
    "POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$'",
    "IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$'",
    'IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")',
    'IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort'
)) {
    Assert-Contains $productionJob $contractFragment "Workflow environment validation is missing: $contractFragment"
}
$composeVersionMatchPattern = '(?m)^          \$composeVersionMatch = \[regex\]::Match\(\$composeVersionText, ''\^v\?\(\?<core>\\d\+\\\.\\d\+\\\.\\d\+\)\(\?:\[-\+\]\[0-9A-Za-z\.\-\]\+\)\?\$''\)\s*$'
$composeVersionMatches = [regex]::Matches($productionJob, $composeVersionMatchPattern)
Assert-True ($composeVersionMatches.Count -eq 1) "Workflow Docker Compose prerequisite must extract exactly one strict three-part numeric core from the complete version text"
Assert-Contains $productionJob '$composeVersionText = (docker compose version --short).Trim()' "Workflow Docker Compose prerequisite must preserve the complete trimmed version text for strict parsing"
Assert-Contains $productionJob '[Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion)' "Workflow Docker Compose prerequisite must parse only the strict numeric core"
Assert-NotContains $productionJob '[Version]::TryParse($composeVersionText, [ref]$composeVersion)' "Workflow Docker Compose prerequisite must not parse the vendor-suffixed version text directly"
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
    Assert-Contains $productionJob $restoreFragment "Workflow environment restoration is missing: $restoreFragment"
}
Assert-Matches $productionJob '(?s)try \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?\} finally \{.*?SetEnvironmentVariable\(\$requiredName, \$savedRequiredValues\[\$requiredName\], "Process"\).*?Test-Path -LiteralPath "Env:\$requiredName".*?\$restoredRequiredValue -cne \$savedRequiredValues\[\$requiredName\]' "Each missing-secret probe must remove deterministically and restore exact presence/value in finally"
Assert-NotContains $productionJob "0.0.0.0:55433" "Validation PostgreSQL cannot bind wildcard"
Assert-NotContains $productionJob "0.0.0.0:55001" "Validation Kubo cannot bind wildcard"
Assert-NotContains $productionJob "0.0.0.0:59000" "Validation gateway cannot bind wildcard"
Assert-NotMatches $productionJob '(?m)^    needs:' "Production job must be independent"
Assert-NotMatches $productionJob '(?m)^    continue-on-error:' "Production job must be blocking"

Write-Host "postgres production baseline contract tests: PASSED"
