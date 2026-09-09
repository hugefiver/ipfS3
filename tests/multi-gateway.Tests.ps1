$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposePath = Join-Path $RepoRoot "docker-compose.multi-gateway.yml"
$NginxPath = Join-Path $RepoRoot "deploy/nginx/multi-gateway.conf"
$OverridePath = Join-Path $RepoRoot "tests/compose.multi-gateway-validation.yml"
$DefaultComposePath = Join-Path $RepoRoot "docker-compose.yml"
$SinglePostgresComposePath = Join-Path $RepoRoot "docker-compose.postgres.yml"
$StorePath = Join-Path $RepoRoot "src/store/mod.rs"
$StatePath = Join-Path $RepoRoot "src/state.rs"
$MainPath = Join-Path $RepoRoot "src/main.rs"
$RustLivePath = Join-Path $RepoRoot "tests/multi_gateway.rs"
$SigV4Path = Join-Path $RepoRoot "tests/support/sigv4.rs"
$WorkerPath = Join-Path $RepoRoot "src/lifecycle/worker.rs"
$PostgresLifecyclePath = Join-Path $RepoRoot "tests/postgres_lifecycle.rs"
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
$E2ePath = Join-Path $RepoRoot "tests/e2e.rs"

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

    Assert-True $Text.Contains($Fragment, [StringComparison]::Ordinal) $Message
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
        if ([regex]::Match($lines[$index], '^( *)').Groups[1].Length -le $Indent) {
            $end = $index
            break
        }
    }
    if ($end -le $start + 1) { return "" }
    return $lines[($start + 1)..($end - 1)] -join "`n"
}

function Get-BracedBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$HeaderPattern,
        [Parameter(Mandatory)][string]$Label
    )

    $matches = [regex]::Matches($Text, $HeaderPattern, [Text.RegularExpressions.RegexOptions]::Multiline)
    if ($matches.Count -ne 1) { throw "Expected exactly one $Label header, found $($matches.Count)" }

    $start = $matches[0].Index
    $open = $Text.IndexOf('{', $start)
    if ($open -lt 0 -or $open -ge ($start + $matches[0].Length)) {
        throw "$Label header has no opening brace"
    }

    $depth = 0
    $quote = [char]0
    $escaped = $false
    $lineComment = $false
    $blockComment = $false
    for ($index = $open; $index -lt $Text.Length; $index++) {
        $character = $Text[$index]
        $next = if ($index + 1 -lt $Text.Length) { $Text[$index + 1] } else { [char]0 }
        if ($lineComment) {
            if ($character -eq "`n") { $lineComment = $false }
            continue
        }
        if ($blockComment) {
            if ($character -eq '*' -and $next -eq '/') {
                $blockComment = $false
                $index++
            }
            continue
        }
        if ($quote -ne [char]0) {
            if ($escaped) {
                $escaped = $false
                continue
            }
            if ($character -eq '\') {
                $escaped = $true
                continue
            }
            if ($character -eq $quote) { $quote = [char]0 }
            continue
        }
        if ($character -eq '#') {
            $lineComment = $true
            continue
        }
        if ($character -eq '/' -and $next -eq '/') {
            $lineComment = $true
            $index++
            continue
        }
        if ($character -eq '/' -and $next -eq '*') {
            $blockComment = $true
            $index++
            continue
        }
        if ($character -eq '"' -or $character -eq "'") {
            $quote = $character
            continue
        }
        if ($character -eq '{') {
            $depth++
            continue
        }
        if ($character -eq '}') {
            $depth--
            if ($depth -eq 0) {
                return [pscustomobject]@{
                    Text = $Text.Substring($start, $index - $start + 1)
                    Body = $Text.Substring($open + 1, $index - $open - 1)
                }
            }
            if ($depth -lt 0) { throw "$Label has an unmatched closing brace" }
        }
    }
    throw "$Label has no matching closing brace"
}

function Assert-DirectiveCount {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Directive,
        [Parameter(Mandatory)][int]$Count,
        [Parameter(Mandatory)][string]$Label
    )

    $pattern = '(?m)^\s*' + [regex]::Escape($Directive) + '\s*$'
    $actual = [regex]::Matches($Text, $pattern).Count
    Assert-True ($actual -eq $Count) "$Label expected $Count exact '$Directive' directives, found $actual"
}

$defaultSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $DefaultComposePath).Hash.ToLowerInvariant()
$singlePgSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $SinglePostgresComposePath).Hash.ToLowerInvariant()
Assert-True ($defaultSha -ceq "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c") "Default SQLite Compose changed"
Assert-True ($singlePgSha -ceq "6a39a43a48beda2cbd79a0ae8ca403a3ead2a67766ed34c214de78fa1a6a1782") "Single-PostgreSQL Compose changed"

$Compose = Read-NormalizedText $ComposePath
$Nginx = Read-NormalizedText $NginxPath
$Override = Read-NormalizedText $OverridePath

$bracedBlockFixture = @'
scope {
    # a comment with a closing brace }
    // a comment with an opening brace {
    /* a comment with both { and } braces */
    value "a quoted brace { and an escaped quote \" followed by }";
    value 'a single-quoted brace }';
    nested { value; }
}
'@
$bracedBlockFixture = $bracedBlockFixture.Replace("`r`n", "`n").Replace("`r", "`n")
$bracedBlock = Get-BracedBlock $bracedBlockFixture '(?m)^scope\s*\{' 'braced-block parser fixture'
Assert-Contains $bracedBlock.Body 'nested { value; }' "Get-BracedBlock must retain nested blocks"
Assert-True ($bracedBlock.Text.TrimEnd("`n") -ceq $bracedBlockFixture.TrimEnd("`n")) "Get-BracedBlock must ignore braces in comments and quoted strings"

$Store = Read-NormalizedText $StorePath
$State = Read-NormalizedText $StatePath
$Main = Read-NormalizedText $MainPath
$publicMigrations = Get-BracedBlock $Store '(?m)^pub async fn run_migrations\(db: &sea_orm::DatabaseConnection\) -> Result<\(\), sea_orm::DbErr> \{' 'public run_migrations function'
$postgresMigrations = Get-BracedBlock $Store '(?m)^async fn run_postgres_migrations\(db: &sea_orm::DatabaseConnection\) -> Result<\(\), sea_orm::DbErr> \{' 'private run_postgres_migrations helper'

Assert-Contains $Store 'pub const POSTGRES_MIGRATION_LOCK_KEY_1: i32 = 1_229_997_651;' "PostgreSQL migration lock key 1 changed"
Assert-Contains $Store 'pub const POSTGRES_MIGRATION_LOCK_KEY_2: i32 = 1_395_879_239;' "PostgreSQL migration lock key 2 changed"
Assert-InOrder $publicMigrations.Body @(
    'if db.get_database_backend() != sea_orm::DatabaseBackend::Postgres',
    'return migrator::Migrator::up(db, None).await;',
    'run_postgres_migrations(db).await'
) "Public migration dispatcher must preserve the direct non-PostgreSQL path and delegate PostgreSQL"
Assert-True (([regex]::Matches($publicMigrations.Body, [regex]::Escape('run_postgres_migrations(db).await'))).Count -eq 1) "Public dispatcher must invoke the PostgreSQL helper exactly once"
Assert-True (([regex]::Matches($publicMigrations.Body, [regex]::Escape('migrator::Migrator::up(db, None).await'))).Count -eq 1) "Public dispatcher must retain exactly one direct non-PostgreSQL Migrator::up call"
Assert-NotContains $publicMigrations.Body '.begin()' "Public migration dispatcher must not begin a transaction for non-PostgreSQL backends"
Assert-NotMatches $publicMigrations.Body '(?i)pg_(?:try_)?advisory' "Public migration dispatcher must not contain advisory-lock SQL"

Assert-InOrder $postgresMigrations.Body @(
    '.begin()',
    '"SET LOCAL lock_timeout = ''60s''"',
    'migration_lock = "waiting"',
    '"SELECT pg_advisory_xact_lock(1229997651, 1395879239)"',
    'migration_lock = "acquired"',
    'migrator::Migrator::up(&txn, None)',
    '.commit()'
) "Private PostgreSQL migration helper sequence changed"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('.begin()'))).Count -eq 1) "PostgreSQL helper must begin exactly one outer transaction"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape("SET LOCAL lock_timeout = '60s'"))).Count -eq 1) "PostgreSQL helper must set exactly one 60-second local lock timeout"
Assert-True (([regex]::Matches($postgresMigrations.Body, '(?i)lock_timeout')).Count -eq 1) "PostgreSQL helper must not add a second or conflicting lock timeout"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('SELECT pg_advisory_xact_lock(1229997651, 1395879239)'))).Count -eq 1) "PostgreSQL helper must take the stable transaction advisory lock exactly once"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('migrator::Migrator::up(&txn, None)'))).Count -eq 1) "PostgreSQL helper must run migrations exactly once through the outer transaction"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('.commit()'))).Count -eq 1) "PostgreSQL helper must commit exactly once"
Assert-NotMatches $postgresMigrations.Body '(?i)\bpg_(?:try_)?advisory_(?!xact_)lock\s*\(' "Production migration helper must not take a session advisory lock"
Assert-NotMatches $postgresMigrations.Body '(?i)lock_timeout\s*=\s*''(?:0|0ms|0s)''' "Production migration helper must not configure unbounded lock waiting"
Assert-Matches $Store 'tracing::error!\(migration_lock\s*=\s*"failure",\s*category\)' "Migration failure logging must contain only the safe category"
Assert-NotMatches $Store 'tracing::(?:error|warn|info)!\([^\r\n]*(?:error|database_url|dsn|password)' "Migration logs must not render private errors or connection data"
Assert-InOrder $State @(
    'let db = crate::store::connect_database(&cfg.storage.database_url).await?;',
    'crate::store::run_migrations(&db).await?;',
    'let store = Store::new(db);'
) "AppState must fail before construction when migrations fail"
Assert-InOrder $Main @(
    'let state = AppState::new(&cfg).await?;',
    'let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;'
) "Gateway must run migrations before binding its listener"

Assert-NotContains $Compose "`t" "Production Compose must use spaces"
Assert-Matches $Compose '(?m)^name: ipfs3-multi-gateway$' "Production Compose must use the dedicated multi-gateway project name"
Assert-NotMatches $Compose '(?m)^\s*<<:\s*' "Production Compose must not use YAML merge keys"
Assert-NotMatches $Compose '(?m)^\s*[A-Za-z0-9_-]+:\s*&' "Production Compose must not use YAML anchors"

$services = Get-YamlBlock $Compose "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedServices = @("postgres", "kubo", "gateway-a", "gateway-b", "load-balancer")
Assert-True ($serviceNames.Count -eq $expectedServices.Count) "Production topology must define exactly five services"
Assert-True ((($serviceNames | Sort-Object) -join "`n") -ceq (($expectedServices | Sort-Object) -join "`n")) "Production service set changed"

$postgres = Get-YamlBlock $services "postgres" 2
$kubo = Get-YamlBlock $services "kubo" 2
$gatewayA = Get-YamlBlock $services "gateway-a" 2
$gatewayB = Get-YamlBlock $services "gateway-b" 2
$loadBalancer = Get-YamlBlock $services "load-balancer" 2

$volumes = Get-YamlBlock $Compose "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedVolumes = @("postgres_data", "ipfs_data")
Assert-True ($volumeNames.Count -eq $expectedVolumes.Count) "Production topology must define exactly two named volumes"
Assert-True ((($volumeNames | Sort-Object) -join "`n") -ceq (($expectedVolumes | Sort-Object) -join "`n")) "Production volume set changed"

$configs = Get-YamlBlock $Compose "configs" 0
$configNames = @([regex]::Matches($configs, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($configNames.Count -eq 1 -and $configNames[0] -ceq "postgres_init") "Production topology must define only inline postgres_init"
$postgresInit = Get-YamlBlock $configs "postgres_init" 2

Assert-Contains $postgres "    image: postgres:17" "PostgreSQL must use postgres:17"
Assert-Contains $postgres "      POSTGRES_DB: postgres" "PostgreSQL bootstrap database must remain postgres"
Assert-Contains $postgres "      POSTGRES_USER: postgres" "PostgreSQL bootstrap superuser must remain postgres"
Assert-Contains $postgres '      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"' "PostgreSQL password must use required interpolation"
Assert-Contains $postgres "      - postgres_data:/var/lib/postgresql/data" "PostgreSQL must retain its only persistent volume"
Assert-Contains $postgres "      - source: postgres_init" "PostgreSQL must use the inline initializer"
Assert-Contains $postgres "        target: /docker-entrypoint-initdb.d/10-ipfs3.sql" "PostgreSQL initializer target is incorrect"
Assert-Contains $postgres '      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]' "PostgreSQL application-account healthcheck is missing"
Assert-NotMatches $postgres '(?m)^    ports:\s*$' "Production PostgreSQL must not publish a host port"
Assert-Contains $postgresInit "    content: |" "PostgreSQL initializer must be inline Compose content"
Assert-Contains $postgresInit "      \set app_password '`$`{POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'" "PostgreSQL initializer password must use required interpolation"
Assert-Contains $postgresInit "      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;" "Initializer must create a non-superuser application role"
Assert-Contains $postgresInit "      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec" "Initializer must quote the interpolated password through psql"
Assert-Contains $postgresInit "      CREATE DATABASE ipfs3 OWNER ipfs3;" "Initializer must create the application-owned database"

Assert-Contains $kubo "    build: ./ipfs" "Kubo build context changed"
Assert-Contains $kubo "    image: ghcr.io/hugefiver/ipfs3-kubo:latest" "Kubo image changed"
Assert-Contains $kubo "      - ipfs_data:/data/ipfs" "Kubo must retain its only persistent volume"
Assert-Contains $kubo "      IPFS_PATH: /data/ipfs" "Kubo IPFS path changed"
Assert-Contains $kubo '      test: ["CMD", "ipfs", "id"]' "Kubo must retain its ipfs id healthcheck"
Assert-NotMatches $kubo '(?m)^    ports:\s*$' "Production Kubo must not publish a host port"

Assert-True ($gatewayA -ceq $gatewayB) "gateway-a and gateway-b configuration blocks must be byte-identical"
foreach ($gateway in @($gatewayA, $gatewayB)) {
    Assert-Contains $gateway "    build: ." "Gateway build context is incorrect"
    Assert-Contains $gateway "    image: ghcr.io/hugefiver/ipfs3:latest" "Gateway image is incorrect"
    Assert-Contains $gateway "      IPFS_S3_BIND: 0.0.0.0:9000" "Gateway internal bind is incorrect"
    Assert-Contains $gateway "      IPFS_S3_KUBO_RPC_URL: http://kubo:5001" "Gateway Kubo URL is incorrect"
    Assert-Contains $gateway '      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"' "Gateway PostgreSQL URL is incorrect"
    Assert-Contains $gateway '      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"' "Gateway access key must use required interpolation"
    Assert-Contains $gateway '      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"' "Gateway secret key must use required interpolation"
    Assert-Contains $gateway '      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"' "Gateway master key must use required interpolation"
    Assert-Contains $gateway "      RUST_LOG: info" "Gateway log level is incorrect"
    Assert-Contains $gateway '      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]' "Gateway must use the binary readiness probe"
    Assert-Contains $gateway "      postgres:`n        condition: service_healthy" "Gateway PostgreSQL dependency must be health-gated"
    Assert-Contains $gateway "      kubo:`n        condition: service_healthy" "Gateway Kubo dependency must be health-gated"
    Assert-NotMatches $gateway '(?m)^    ports:\s*$' "Production gateways must not publish ports"
    Assert-NotMatches $gateway '(?m)^    volumes:\s*$' "Production gateways must not mount volumes"
}

$servicePortNames = @(
    foreach ($serviceName in $serviceNames) {
        if ((Get-YamlBlock $services $serviceName 2) -match '(?m)^    ports:\s*$') { $serviceName }
    }
)
Assert-True ($servicePortNames.Count -eq 1 -and $servicePortNames[0] -ceq "load-balancer") "Only the load balancer may publish a production host port"
$loadBalancerPorts = Get-YamlBlock $loadBalancer "ports" 4
Assert-True ($loadBalancerPorts.Trim() -ceq '- "${IPFS_S3_LOAD_BALANCER_BIND:?IPFS_S3_LOAD_BALANCER_BIND is required}:${IPFS_S3_LOAD_BALANCER_PORT:?IPFS_S3_LOAD_BALANCER_PORT is required}:9000"') "Load-balancer publication must use the exact required bind and port interpolation"
Assert-Contains $loadBalancer "    image: nginx:1.28.0-alpine" "Nginx exact image tag changed"
Assert-Contains $loadBalancer "      - ./deploy/nginx/multi-gateway.conf:/etc/nginx/nginx.conf:ro" "Load balancer must mount the canonical configuration read-only"
Assert-Contains $loadBalancer "      gateway-a:`n        condition: service_healthy" "Load balancer gateway-a dependency must be health-gated"
Assert-Contains $loadBalancer "      gateway-b:`n        condition: service_healthy" "Load balancer gateway-b dependency must be health-gated"
Assert-Contains $loadBalancer '      test: ["CMD-SHELL", "wget -q -O - http://127.0.0.1:9000/ready | grep -qx READY"]' "Load balancer must use its wget readiness healthcheck"

foreach ($required in @(
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_LOAD_BALANCER_BIND",
    "IPFS_S3_LOAD_BALANCER_PORT"
)) {
    Assert-Contains $Compose ('${' + $required + ':?') "Required no-default interpolation is missing: $required"
    Assert-NotContains $Compose ('${' + $required + ':-') "Required input has a default: $required"
    Assert-NotContains $Compose ('${' + $required + '-default') "Required input has an alternate default: $required"
}
foreach ($forbidden in @(
    "container_name:",
    "cloudflared",
    "gateway_data",
    "config.docker.toml",
    "IPFS_S3_CONFIG",
    "PINATA_JWT",
    "FILEBASE_PINNING_TOKEN",
    "CLOUDFLARE_TUNNEL_TOKEN",
    "pinata",
    "filebase",
    "remote"
)) {
    Assert-NotContains $Compose $forbidden "Forbidden multi-gateway production fragment: $forbidden"
}

$validationServices = Get-YamlBlock $Override "services" 0
$validationNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($validationNames.Count -eq $expectedServices.Count) "Validation override must mention exactly five services"
Assert-True ((($validationNames | Sort-Object) -join "`n") -ceq (($expectedServices | Sort-Object) -join "`n")) "Validation override service set changed"
$expectedMappings = @(
    "127.0.0.1:55434:5432",
    "127.0.0.1:55002:5001",
    "127.0.0.1:59001:9000",
    "127.0.0.1:59002:9000",
    "127.0.0.1:59000:9000"
)
$actualMappings = @([regex]::Matches($Override, '(?m)^\s*-\s+"([^"]+)"\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($actualMappings.Count -eq $expectedMappings.Count) "Validation override must define exactly five port mappings"
Assert-True ((($actualMappings | Sort-Object) -join "`n") -ceq (($expectedMappings | Sort-Object) -join "`n")) "Validation override port mappings changed"
$expectedMappingByService = @{
    postgres = "127.0.0.1:55434:5432"
    kubo = "127.0.0.1:55002:5001"
    "gateway-a" = "127.0.0.1:59001:9000"
    "gateway-b" = "127.0.0.1:59002:9000"
    "load-balancer" = "127.0.0.1:59000:9000"
}
foreach ($serviceName in $expectedServices) {
    $mapping = $expectedMappingByService[$serviceName]
    $expectedOverrideBlock = '\A    ports:\n      - "' + [regex]::Escape($mapping) + '"\s*\z'
    Assert-Matches (Get-YamlBlock $validationServices $serviceName 2) $expectedOverrideBlock "Validation override service must contain only its exact loopback mapping: $serviceName"
}
foreach ($mapping in $expectedMappings) {
    Assert-True (([regex]::Matches($Override, [regex]::Escape($mapping))).Count -eq 1) "Validation mapping must occur exactly once: $mapping"
}
Assert-NotContains $Override "0.0.0.0" "Validation ports must be loopback-only"
Assert-NotMatches $Override '(?m)^\s*volumes:\s*$' "Validation override must not declare volumes"
Assert-NotContains $Compose "tests/compose.multi-gateway-validation.yml" "Production Compose must not reference its disposable validation override"

$ExpectedNginx = @'
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    log_format gateway '$request_method $uri status=$status request_time=$request_time upstream_status=$upstream_status';
    access_log /var/log/nginx/access.log gateway;
    error_log /var/log/nginx/error.log warn;

    upstream ipfs3_gateways {
        server gateway-a:9000 max_fails=1 fail_timeout=10s;
        server gateway-b:9000 max_fails=1 fail_timeout=10s;
        keepalive 32;
    }

    server {
        listen 9000;
        server_name _;
        client_max_body_size 0;

        location = /health {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location = /ready {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location / {
            proxy_pass http://ipfs3_gateways;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_request_buffering off;
            proxy_buffering off;
            proxy_connect_timeout 2s;
            proxy_send_timeout 120s;
            proxy_read_timeout 120s;
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }
    }
}
'@
$ExpectedNginx = $ExpectedNginx.Replace("`r`n", "`n").Replace("`r", "`n")
Assert-True ($Nginx.TrimEnd("`n") -ceq $ExpectedNginx.TrimEnd("`n")) "Nginx file differs from the reviewed canonical configuration; comments and extra directives are forbidden"

$httpBlock = Get-BracedBlock $Nginx '(?m)^http\s*\{' "http block"
$upstreamBlock = Get-BracedBlock $httpBlock.Body '(?m)^\s*upstream\s+ipfs3_gateways\s*\{' "ipfs3_gateways upstream block"
$serverBlock = Get-BracedBlock $httpBlock.Body '(?m)^\s*server\s*\{' "Nginx server block"
$healthBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+=\s+/health\s*\{' "exact /health location"
$readyBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+=\s+/ready\s*\{' "exact /ready location"
$rootBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+/\s*\{' "root S3 location"

Assert-True (([regex]::Matches($httpBlock.Body, '(?m)^\s*upstream\s+[^\s{]+\s*\{')).Count -eq 1) "Nginx http block must contain exactly one upstream"
Assert-True (([regex]::Matches($httpBlock.Body, '(?m)^\s*server\s*\{')).Count -eq 1) "Nginx http block must contain exactly one server block"
Assert-True (([regex]::Matches($serverBlock.Body, '(?m)^\s*location\s+(?:=\s+)?/[^\s{]*\s*\{')).Count -eq 3) "Nginx server must contain only /, exact /health, and exact /ready locations"
Assert-DirectiveCount $serverBlock.Body "listen 9000;" 1 "server listener"
Assert-DirectiveCount $serverBlock.Body "server_name _;" 1 "server name"
Assert-DirectiveCount $serverBlock.Body "client_max_body_size 0;" 1 "server unlimited body size"

Assert-DirectiveCount $upstreamBlock.Body "server gateway-a:9000 max_fails=1 fail_timeout=10s;" 1 "upstream gateway-a"
Assert-DirectiveCount $upstreamBlock.Body "server gateway-b:9000 max_fails=1 fail_timeout=10s;" 1 "upstream gateway-b"
Assert-DirectiveCount $upstreamBlock.Body "keepalive 32;" 1 "upstream keepalive"

foreach ($probe in @($healthBlock, $readyBlock)) {
    Assert-DirectiveCount $probe.Body "proxy_pass http://ipfs3_gateways/ready;" 1 "readiness proxy target"
    Assert-DirectiveCount $probe.Body "proxy_http_version 1.1;" 1 "readiness HTTP version"
    Assert-DirectiveCount $probe.Body "proxy_set_header Host `$http_host;" 1 "readiness signed Host"
    Assert-DirectiveCount $probe.Body 'proxy_set_header Connection "";' 1 "readiness connection header"
    Assert-DirectiveCount $probe.Body "proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;" 1 "readiness failover classes"
    Assert-DirectiveCount $probe.Body "proxy_next_upstream_tries 2;" 1 "readiness bounded attempts"
    Assert-True (([regex]::Matches($probe.Body, '(?mi)^\s*proxy_set_header\s+Host\s+')).Count -eq 1) "Readiness location has a conflicting Host directive"
}

Assert-DirectiveCount $rootBlock.Body "proxy_pass http://ipfs3_gateways;" 1 "root upstream target"
Assert-DirectiveCount $rootBlock.Body "proxy_http_version 1.1;" 1 "root HTTP version"
Assert-DirectiveCount $rootBlock.Body "proxy_set_header Host `$http_host;" 1 "root signed Host"
Assert-DirectiveCount $rootBlock.Body 'proxy_set_header Connection "";' 1 "root connection header"
Assert-DirectiveCount $rootBlock.Body "proxy_set_header X-Real-IP `$remote_addr;" 1 "root client address forwarding"
Assert-DirectiveCount $rootBlock.Body "proxy_set_header X-Forwarded-For `$proxy_add_x_forwarded_for;" 1 "root forwarding chain"
Assert-DirectiveCount $rootBlock.Body "proxy_set_header X-Forwarded-Proto `$scheme;" 1 "root forwarding protocol"
Assert-DirectiveCount $rootBlock.Body "proxy_request_buffering off;" 1 "root request streaming"
Assert-DirectiveCount $rootBlock.Body "proxy_buffering off;" 1 "root response streaming"
Assert-DirectiveCount $rootBlock.Body "proxy_connect_timeout 2s;" 1 "root connect timeout"
Assert-DirectiveCount $rootBlock.Body "proxy_send_timeout 120s;" 1 "root send timeout"
Assert-DirectiveCount $rootBlock.Body "proxy_read_timeout 120s;" 1 "root read timeout"
Assert-DirectiveCount $rootBlock.Body "proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;" 1 "root failover classes"
Assert-DirectiveCount $rootBlock.Body "proxy_next_upstream_tries 2;" 1 "root bounded attempts"
Assert-True (([regex]::Matches($rootBlock.Body, '(?mi)^\s*proxy_set_header\s+Host\s+')).Count -eq 1) "Root location has a conflicting Host directive"
Assert-NotMatches $serverBlock.Body '(?mi)^\s*proxy_(?:request_)?buffering\s+on;' "Nginx proxy buffering must never be enabled"
Assert-NotMatches $serverBlock.Body '(?mi)^\s*proxy_next_upstream_tries\s+(?:0|[3-9][0-9]*);' "Nginx upstream retries must remain bounded at two"
Assert-True (([regex]::Matches($serverBlock.Body, '(?mi)^\s*proxy_pass\s+')).Count -eq 3) "Only the three reviewed locations may proxy requests"

$logLine = @($httpBlock.Body -split "`n" | Where-Object { $_ -match '^\s*log_format gateway ' })
Assert-True ($logLine.Count -eq 1) "Expected one safe Nginx log format in the http block"
Assert-True ($logLine[0].Trim() -ceq "log_format gateway '`$request_method `$uri status=`$status request_time=`$request_time upstream_status=`$upstream_status';") "Nginx log format changed"
Assert-NotMatches $logLine[0] '(?i)authorization|request_body|http_' "Nginx access log must not contain headers or body variables"

$RustLive = Read-NormalizedText $RustLivePath
# NO KEY UPDATE permits FK KEY SHARE during action insert but blocks production FOR UPDATE.
Assert-Contains $RustLive '"SELECT name FROM buckets WHERE name = $1 FOR NO KEY UPDATE"' "MPU race requires the owned FK-compatible bucket row lock before aging"
foreach ($testName in @(
    "multi_gateway_cross_replica_contract",
    "load_balancer_surviving_replica_crud"
)) {
    Assert-True (([regex]::Matches($RustLive, "(?m)^async fn $testName\(\) \{")).Count -eq 1) "Expected one live test named $testName"
}
foreach ($environmentContract in @(
    @{ Name = "IPFS_S3_MULTI_GATEWAY_A_ENDPOINT"; Count = 2 },
    @{ Name = "IPFS_S3_MULTI_GATEWAY_B_ENDPOINT"; Count = 2 },
    @{ Name = "IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT"; Count = 2 },
    @{ Name = "IPFS_S3_MULTI_GATEWAY_KUBO_URL"; Count = 1 },
    @{ Name = "IPFS_S3_MULTI_GATEWAY_DATABASE_URL"; Count = 4 }
)) {
    $environmentName = $environmentContract.Name
    $expectedCount = $environmentContract.Count
    Assert-True (([regex]::Matches($RustLive, [regex]::Escape($environmentName))).Count -eq $expectedCount) "Live target environment contract changed: $environmentName"
}
foreach ($requiredLiveFragment in @(
    "mod support;",
    "send_sigv4",
    "tokio::join!",
    "initiate_multipart_upload",
    "put_multipart_chunk",
    "complete_multipart_upload",
    '"x-ipfs3-import-job-id"',
    '"<State>completed</State>"',
    '"<CID>{cid}</CID>"',
    'bucket.put_object(key, body)',
    'bucket.get_object(key)',
    'bucket.head_object(key)',
    'bucket.list(String::new(), None)',
    'bucket.delete_object(key)',
    'bucket.delete()'
)) {
    Assert-Contains $RustLive $requiredLiveFragment "Live multi-gateway scenario is incomplete: $requiredLiveFragment"
}
Assert-NotMatches $RustLive '(?i)std::process::Command|Command::new|\baws\b|\brclone\b|\bmc\b' "Live target must not execute native client tools"

# Task 11: deterministic PostgreSQL reclaim proof and endpoint-only lifecycle scenarios.
$Worker = Read-NormalizedText $WorkerPath
$PostgresLifecycle = Read-NormalizedText $PostgresLifecyclePath
foreach ($fragment in @(
    "#[doc(hidden)]`n#[derive(Clone)]`npub struct LifecycleWorkerTestControl",
    "pub worker_id: String,",
    "pub after_claim: Option<Arc<LifecycleAfterClaimGate>>",
    "#[doc(hidden)]`npub struct LifecycleAfterClaimGate",
    "pub fn new(expected_worker_id: impl Into<String>) -> Arc<Self>",
    "pub async fn wait_claim(&self) -> ClaimedLifecycleAction",
    "pub fn release(&self)",
    "pub fn start_worker_for_test(",
    "#[doc(hidden)]`n    pub fn abort_for_test(self) -> JoinHandle<()>",
    'let worker_id = format!("lifecycle-worker-{}", uuid::Uuid::new_v4());'
)) {
    Assert-Contains $Worker $fragment "Lifecycle worker deterministic test seam is incomplete: $fragment"
}
Assert-Matches $Worker '(?s)pub async fn wait_claim\(&self\) -> ClaimedLifecycleAction\s*\{\s*loop\s*\{' "Lifecycle after-claim gate must wait in a predicate loop"
Assert-Matches $Worker '(?s)pub fn abort_for_test\(self\) -> JoinHandle<\(\)>\s*\{\s*self\.join\.abort\(\);\s*self\.join\s*\}' "Worker test abort must abort and return only its join handle"
Assert-NotContains $Main "start_worker_for_test" "Production startup must not call the lifecycle test seam"

foreach ($testName in @(
    "postgres_lifecycle_migration_and_database_clock_are_engine_owned",
    "postgres_lifecycle_abort_migration_preserves_version_identity_and_checks_shapes",
    "postgres_lifecycle_abort_down_refuses_abort_state_and_restores_phase_a",
    "postgres_lifecycle_abort_multiworker_claim_crash_retry_is_fenced",
    "postgres_lifecycle_abort_configuration_and_bucket_lock_races_are_atomic",
    "postgres_lifecycle_down_refuses_durable_configuration",
    "postgres_lifecycle_claim_uses_database_clock_locks_and_epoch_fences",
    "postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch"
)) {
    Assert-True (([regex]::Matches($PostgresLifecycle, "(?m)^async fn $testName\(\) \{")).Count -eq 1) "Expected one PostgreSQL lifecycle test named $testName"
}
foreach ($fragment in @(
    "lifecycle_{}",
    "uuid::Uuid::new_v4().simple()",
    "OwnedPgSchemaCleanup",
    "impl Drop for OwnedPgSchemaCleanup",
    "DROP SCHEMA {schema} CASCADE",
    "start_worker_for_test",
    "LifecycleWorkerTestControl",
    "LifecycleAfterClaimGate",
    "worker-a",
    "worker-b",
    "action_lease_secs: 1",
    "Duration::from_secs(15)",
    "assert_eq!(claim_b.claim_epoch, claim_a.claim_epoch + 1);",
    "database_now(store_b.db()).await.unwrap() > lease_until",
    "tokio::sync::Barrier",
    "tokio::sync::Notify"
)) {
    Assert-Contains $PostgresLifecycle $fragment "PostgreSQL lifecycle reclaim proof is incomplete: $fragment"
}
Assert-Matches $PostgresLifecycle '(?s)abort_for_test\(\).*?is_cancelled\(\).*?claimed_by.*?worker-a' "Abort proof must observe a cancelled join and unchanged worker-A claim"
Assert-Matches $PostgresLifecycle '(?s)state.*?succeeded.*?stale_terminal_write.*?mark_succeeded\(.*?!stale_terminal_write.*?stale terminal CAS must affect zero rows' "Stale epoch proof must leave a terminal row and assert a zero-row CAS"

foreach ($testName in @(
    "multi_gateway_cross_replica_contract",
    "load_balancer_surviving_replica_crud",
    "multi_gateway_lifecycle_configuration_visible_across_replicas",
    "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
    "multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome"
)) {
    Assert-True (([regex]::Matches($RustLive, "(?m)^async fn $testName\(\) \{")).Count -eq 1) "Expected one endpoint-only lifecycle test named $testName"
}
Assert-True (([regex]::Matches($RustLive, '(?m)^#\[tokio::test\]\s*$')).Count -eq 5) "Multi-gateway target must retain its existing tests and add exactly one lifecycle abort multipart race test"
foreach ($fragment in @(
    "lifecycle_configuration_xml",
    "signed_put_lifecycle_configuration",
    "signed_get_lifecycle_configuration",
    "signed_delete_lifecycle_configuration",
    "signed_list_object_versions",
    "tokio::join!",
    "LifecycleConfiguration",
    "timeout(HTTP_TIMEOUT",
    "successor never deleted"
)) {
    Assert-Contains $RustLive $fragment "Endpoint-only lifecycle scenario is incomplete: $fragment"
}
foreach ($fragment in @(
    "lifecycle_race_configuration_xml",
    "<Filter><Tag><Key>lifecycle-race</Key><Value>expire</Value></Tag></Filter>",
    "signed_put_object_with_tagging",
    'HeaderValue::from_static("lifecycle-race=expire")',
    "signed_put_object_with_tagging(&endpoint_a, &bucket_name, key, predecessor_body)",
    "signed_put_object(&endpoint_b, &bucket_name, key, successor_body.clone())"
)) {
    Assert-Contains $RustLive $fragment "Lifecycle publication race must tag only the predecessor: $fragment"
}
$ageUploadHelper = Get-BracedBlock $RustLive '(?m)^async fn age_exact_multipart_upload_for_lifecycle\(upload_id: &str\) -> u64 \{' 'multipart lifecycle age helper'
$actionRowsHelper = Get-BracedBlock $RustLive '(?m)^async fn lifecycle_action_rows_for_bucket\(bucket: &str\) -> Vec<LifecycleMultipartActionRow> \{' 'multipart lifecycle action rows helper'
$bucketLockHelper = Get-BracedBlock $RustLive '(?m)^async fn lock_lifecycle_bucket\(\s*bucket: &str,\s*\) -> \(sea_orm::DatabaseConnection, sea_orm::DatabaseTransaction\) \{' 'owned bucket row lock helper'
$RustWithoutLifecycleDatabaseHelpers = $RustLive.Replace($ageUploadHelper.Text, "").Replace($actionRowsHelper.Text, "").Replace($bucketLockHelper.Text, "")

Assert-Contains $bucketLockHelper.Body 'std::env::var("IPFS_S3_MULTI_GATEWAY_DATABASE_URL")' "Bucket lock helper must own its connection"
Assert-InOrder $bucketLockHelper.Body @(
    'Database::connect(',
    'database.begin().await?',
    'let rows = transaction',
    '.query_all(Statement::from_sql_and_values(',
    '"SELECT name FROM buckets WHERE name = $1 FOR NO KEY UPDATE"',
    '[bucket.into()]',
    'Ok(transaction)',
    'Ok(Ok(transaction)) => (database, transaction)'
) "Bucket lock must return the owned transaction only after locking the exact bucket"
Assert-Contains $bucketLockHelper.Body 'rows.len() != 1' "Lock helper must require exactly one bucket"
Assert-Matches $bucketLockHelper.Body 'database\s*\.close\(\)\s*\.await' "Acquisition failure must close the owned connection"
Assert-True (([regex]::Matches($bucketLockHelper.Body, [regex]::Escape('Database::connect('))).Count -eq 1) "Lock helper must open one connection"
Assert-True (([regex]::Matches($bucketLockHelper.Body, [regex]::Escape('Statement::from_sql_and_values('))).Count -eq 1) "Lock helper must issue one parameterized statement"
Assert-True (([regex]::Matches($bucketLockHelper.Body, '\$1')).Count -eq 1) "Lock helper must bind one PostgreSQL parameter"
Assert-True (([regex]::Matches($bucketLockHelper.Body, '\.query_all\(')).Count -eq 1) "Lock helper must perform one SELECT"
Assert-NotMatches $bucketLockHelper.Body '(?i)execute_unprepared|Statement::from_string|connect_database|\.execute\(|\.query_one\(|\.query_all_raw\(|\.execute_raw\(|format!\s*\(|push_str|\b(?:CREATE|ALTER|DROP|TRUNCATE|INSERT|DELETE)\b' "Lock helper must not add unreviewed SQL"
$lockSql = [regex]::Matches($bucketLockHelper.Body, '"((?:UPDATE|SELECT) [^"\r\n]*)"')
Assert-True ($lockSql.Count -eq 1 -and $lockSql[0].Groups[1].Value -ceq 'SELECT name FROM buckets WHERE name = $1 FOR NO KEY UPDATE') "Bucket lock SQL must be exactly the single reviewed FK-compatible parameterized statement"

Assert-Contains $ageUploadHelper.Body 'std::env::var("IPFS_S3_MULTI_GATEWAY_DATABASE_URL")' "Multipart age helper must own its configured database connection"
Assert-Contains $actionRowsHelper.Body 'std::env::var("IPFS_S3_MULTI_GATEWAY_DATABASE_URL")' "Lifecycle action rows helper must own its configured database connection"
foreach ($helperContract in @(
    @{ Block = $ageUploadHelper; Label = "multipart age helper" },
    @{ Block = $actionRowsHelper; Label = "lifecycle action rows helper" }
)) {
    $helper = $helperContract.Block
    $label = $helperContract.Label
    Assert-True (([regex]::Matches($helper.Body, [regex]::Escape('Database::connect('))).Count -eq 1) "$label must open exactly one database connection"
    Assert-True (([regex]::Matches($helper.Body, [regex]::Escape('Statement::from_sql_and_values('))).Count -eq 1) "$label must issue exactly one parameterized statement"
    Assert-True (([regex]::Matches($helper.Body, '(?s)\.close\(\)\s*\.await')).Count -eq 1) "$label must close its owned database connection exactly once"
    Assert-NotMatches $helper.Body '(?i)execute_unprepared|Statement::from_string|connect_database|\b(?:CREATE|ALTER|DROP|TRUNCATE|INSERT|DELETE)\b' "$label may use only its reviewed parameterized statement"
}
Assert-Contains $ageUploadHelper.Body 'UPDATE multipart_uploads SET created_at = clock_timestamp() - INTERVAL ''3 days'' WHERE upload_id = $1' "Multipart age helper SQL changed"
Assert-Contains $actionRowsHelper.Body 'SELECT action_kind, target_type, target_upload_id, state, failure_class FROM lifecycle_actions WHERE bucket = $1' "Lifecycle action rows helper SQL changed"
Assert-Contains $ageUploadHelper.Body '[upload_id.into()]' "Multipart age helper must bind the upload ID"
Assert-Contains $actionRowsHelper.Body '[bucket.into()]' "Lifecycle action rows helper must bind the bucket"
Assert-True (([regex]::Matches($ageUploadHelper.Body, '\$1')).Count -eq 1) "Multipart age helper must bind exactly one PostgreSQL parameter"
Assert-True (([regex]::Matches($actionRowsHelper.Body, '\$1')).Count -eq 1) "Lifecycle action rows helper must bind exactly one PostgreSQL parameter"
Assert-True (([regex]::Matches($ageUploadHelper.Body, [regex]::Escape('.execute('))).Count -eq 1) "Multipart age helper must execute exactly one UPDATE"
Assert-NotContains $ageUploadHelper.Body '.query_all(' "Multipart age helper must not issue a read query"
Assert-True (([regex]::Matches($actionRowsHelper.Body, [regex]::Escape('.query_all('))).Count -eq 1) "Lifecycle action rows helper must execute exactly one SELECT"
Assert-NotContains $actionRowsHelper.Body '.execute(' "Lifecycle action rows helper must not issue a write statement"
foreach ($sqlContract in @(
    @{ Block = $ageUploadHelper; Sql = "UPDATE multipart_uploads SET created_at = clock_timestamp() - INTERVAL '3 days' WHERE upload_id = `$1" },
    @{ Block = $actionRowsHelper; Sql = 'SELECT action_kind, target_type, target_upload_id, state, failure_class FROM lifecycle_actions WHERE bucket = $1' }
)) {
    $sqlLiterals = [regex]::Matches($sqlContract.Block.Body, '"((?:UPDATE|SELECT) [^"\r\n]*)"')
    Assert-True ($sqlLiterals.Count -eq 1 -and $sqlLiterals[0].Groups[1].Value -ceq $sqlContract.Sql) "Helper SQL must be exactly the reviewed single statement, without suffixes or additional statements"
    Assert-NotMatches $sqlContract.Block.Body '\.query_one\(|\.query_all_raw\(|\.execute_raw\(' "Reviewed helper must not add another database operation"
}
Assert-NotMatches $ageUploadHelper.Body '(?i)format!\s*\(|push_str|\+\s*&?upload_id' "Multipart age helper SQL must not interpolate the upload ID"
Assert-NotMatches $actionRowsHelper.Body '(?i)format!\s*\(|push_str|\+\s*&?bucket' "Lifecycle action rows helper SQL must not interpolate the bucket"
Assert-True (([regex]::Matches($RustLive, [regex]::Escape('Statement::from_sql_and_values('))).Count -eq 3) "Multi-gateway target must contain exactly the three reviewed SQL statements"
Assert-True (([regex]::Matches($RustLive, '(?i)\bUPDATE\s+multipart_uploads\b')).Count -eq 1) "Multi-gateway target must contain only the reviewed multipart UPDATE"
Assert-True (([regex]::Matches($RustLive, '(?i)\bSELECT\s+action_kind\b')).Count -eq 1) "Multi-gateway target must contain only the reviewed lifecycle action SELECT"
Assert-NotMatches $RustWithoutLifecycleDatabaseHelpers '(?i)Database::connect|DatabaseConnection|Statement::|\.query_(?:all|one)(?:_raw)?\(|\.execute(?:_raw)?\(|execute_unprepared|connect_database|\b(?:SELECT|INSERT|UPDATE|CREATE|ALTER|DROP|TRUNCATE)\s+(?:FROM|INTO|TABLE|SCHEMA|multipart_uploads|lifecycle_actions|buckets)' "Database access is forbidden outside the three reviewed lifecycle helpers"
Assert-NotMatches $RustWithoutLifecycleDatabaseHelpers '\.begin\(|\.transaction\(|\.commit\(|get_postgres_connection_pool|sea_orm::(?:Entity|ActiveModel)|ipfs_s3_gateway::store' "Only the reviewed helper may acquire a database transaction"
Assert-True (([regex]::Matches($bucketLockHelper.Body, '\.begin\(')).Count -eq 1) "Bucket lock helper must begin exactly one owned transaction"
Assert-NotMatches $bucketLockHelper.Body '\.commit\(|\.rollback\(|\.clone\(|sleep' "Acquisition helper must retain the lock and transfer sole ownership without a timing gate"
Assert-True (([regex]::Matches($RustLive, '\.rollback\(')).Count -eq 2) "Only the normal unlock and failure cleanup may roll back the owned transaction"
Assert-NotMatches $RustLive '(?i)pin/rm|pin_rm\(' "Multi-gateway target must never invoke pin/rm"
Assert-NotMatches $RustLive '(?i)\bdocker\b|(?:Command::new|process::(?:kill|abort)|abort_for_test|start_worker_for_test|LifecycleWorkerTestControl|LifecycleAfterClaimGate|LifecycleWorkerHandle)' "Endpoint-only lifecycle tests must not use Docker, process-stop, or worker controls"

foreach ($fragment in @(
    'struct LifecycleMultipartActionRow',
    '"abort_incomplete_multipart_upload"',
    '"multipart_upload"',
    '"pending"',
    '"succeeded"',
    '"cancelled"',
    '"failed_safe"',
    'skipping multi-gateway lifecycle abort multipart race: required environment variable {missing} is unset',
    'const LIFECYCLE_ABORT_MULTIPART_CLAIMED_TIMEOUT: Duration = Duration::from_secs(30);',
    'tokio::time::timeout(LIFECYCLE_ABORT_MULTIPART_CLAIMED_TIMEOUT, async {',
    'tokio::time::timeout(LIFECYCLE_ABORT_MULTIPART_TERMINAL_TIMEOUT, async {',
    'backoff.saturating_mul(2).min(Duration::from_millis(400))',
    'last observed safe row states',
    'NoSuchUpload'
)) {
    Assert-Contains $RustLive $fragment "Lifecycle abort multipart race contract is incomplete: $fragment"
}

foreach ($fragment in @(
    'const LIFECYCLE_ABORT_MULTIPART_RACE_TEST_NAME: &str =',
    '"multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome";',
    'enum LifecycleAbortMultipartRaceStage',
    'BucketCreated',
    'MultipartUploadCreated',
    'PartUploaded',
    'UploadAged',
    'LifecycleConfigured',
    'BucketLockAcquired',
    'ClaimedActionObserved',
    'RaceStarted',
    'CompleteRequestDispatched',
    'ClaimedActionRechecked',
    'BucketLockReleased',
    'CompleteResponse',
    'TerminalWaitEntered',
    'TerminalStateEvaluation',
    'LifecycleConfigDeleted',
    'ObjectCleanup',
    'BucketDelete',
    'fn record_lifecycle_abort_multipart_race_stage(stage: LifecycleAbortMultipartRaceStage)',
    '[LIFECYCLE-ABORT-MULTIPART-RACE-STAGE] test={LIFECYCLE_ABORT_MULTIPART_RACE_TEST_NAME} stage={}'
)) {
    Assert-Contains $RustLive $fragment "Lifecycle abort multipart race safe stage source is incomplete: $fragment"
}
$abortRaceTestStart = $RustLive.IndexOf('async fn multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome()', [StringComparison]::Ordinal)
$abortRaceTest = Get-BracedBlock $RustLive '(?m)^async fn multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome\(\) \{' 'signed multipart abort race'
Assert-InOrder $abortRaceTest.Body @(
    'create_bucket_at(&endpoint_a',
    'signed_create_multipart_upload(&endpoint_a',
    'signed_upload_multipart_part(',
    'signed_put_lifecycle_configuration(',
    'lock_lifecycle_bucket(&bucket_name)',
    'age_exact_multipart_upload_for_lifecycle(&upload_id)',
    'wait_for_claimed_lifecycle_multipart_action(',
    'assert_eq!(claimed.state, "claimed")',
    'tokio::sync::oneshot::channel()',
    'tokio::join!(',
    'signed_complete_multipart_upload(',
    'dispatched_rx',
    'lifecycle_action_rows_for_bucket(&bucket_name)',
    'matching[0].state, "claimed"',
    'bucket_lock.take()',
    '.rollback()',
    'LifecycleAbortMultipartRaceStage::BucketLockReleased',
    '.catch_unwind()',
    'if let Some(transaction) = bucket_lock.take()',
    'database.close()',
    'std::panic::resume_unwind',
    'let complete_status = complete.status()',
    'wait_for_terminal_lifecycle_multipart_action(',
    'assert_eq!(terminal.state, "succeeded")',
    'signed_abort_multipart_upload(&endpoint_b',
    'signed_delete_lifecycle_configuration(&endpoint_b',
    'signed_delete_object(&endpoint_a',
    'signed_delete_bucket(&endpoint_a'
) "Signed MPU race must gate durable work before Complete and inspect the outcome before cleanup"
Assert-NotMatches $abortRaceTest.Body '\bsleep(?:_until)?\s*\(' "The signed MPU race must not decide its winner by sleeping"
$claimedHelper = Get-BracedBlock $RustLive '(?m)^async fn wait_for_claimed_lifecycle_multipart_action\(\s*bucket: &str,\s*upload_id: &str,\s*\) -> LifecycleMultipartActionRow \{' 'pre-race claimed waiter'
Assert-NotContains $claimedHelper.Body '"succeeded"' "Succeeded must never be a race precondition"
Assert-NotContains $RustLive 'wait_for_pending_lifecycle_multipart_action' "Remove the old pending-or-terminal race precondition"
Assert-Contains $claimedHelper.Body 'rows.len() == matching.len() && matching.len() <= 1' "Pre-race observation must reject unrelated or duplicate actions"
Assert-Contains $claimedHelper.Body '"pending" => {}' "Pre-race waiter must allow pending to advance"
Assert-Contains $claimedHelper.Body '"claimed" => return (*row).clone()' "Pre-race waiter may return only claimed"
Assert-Contains $claimedHelper.Body 'state => panic!' "Terminal or unknown states while locked must fail"
$completeHelper = Get-BracedBlock $RustLive '(?m)^async fn signed_complete_multipart_upload\([\s\S]*?\) -> reqwest::Response \{' 'dispatch-signalled Complete helper'
Assert-InOrder $completeHelper.Body @(
    'let request = send_sigv4(',
    'tokio::pin!(request)',
    'std::future::poll_fn(|cx|',
    'let poll = request.as_mut().poll(cx)',
    'if let Some(dispatched) = dispatched.take()',
    'assert!(poll.is_pending()',
    '.send(())',
    'poll'
) "Complete must actually poll its network request before signalling dispatch, never just construct a future"
Assert-NotMatches $completeHelper.Body '(?i)sleep|spawn|Barrier|Notify' "Complete dispatch must not depend on timing or detached tasks"
# The dispatch wrapper polls this helper, so its first suspension must be the
# actual network send, not a newly introduced yield or preparation await.
$SigV4 = Read-NormalizedText $SigV4Path
$sigv4Send = Get-BracedBlock $SigV4 '(?m)^pub async fn send_sigv4\([\s\S]*?\) -> reqwest::Response \{' 'SigV4 network send helper'
Assert-True (([regex]::Matches($sigv4Send.Body, '\.await\b')).Count -eq 1) "Dispatch proof requires SigV4's sole await to be the network operation"
Assert-Matches $sigv4Send.Body 'reqwest::Client::new\(\)\s*\.request\(method, url\)\s*\.headers\(headers\)\s*\.body\(body\)\s*\.send\(\)\s*\.await' "First poll of send_sigv4 must reach reqwest send"
Assert-NotContains $abortRaceTest.Body 'tokio::spawn' "Race cleanup must not leave detached tasks"
Assert-InOrder $abortRaceTest.Body @(
    'let mut bucket_lock = Some(transaction)',
    'let race_result = AssertUnwindSafe(async {',
    '.catch_unwind()',
    'if let Some(transaction) = bucket_lock.take()',
    'transaction.rollback().await',
    'let closed = database.close().await',
    'rollback.is_ok()',
    'closed.is_ok()',
    'std::panic::resume_unwind'
) "A failed race must release the owned transaction and close its connection before propagating panic"
$preRace = $abortRaceTest.Body.Substring(0, $abortRaceTest.Body.IndexOf('LifecycleAbortMultipartRaceStage::BucketLockReleased', [StringComparison]::Ordinal))
Assert-NotContains $preRace '"succeeded"' "No terminal success is allowed before unlock"
Assert-InOrder $preRace @(
    'dispatched_rx',
    'LifecycleAbortMultipartRaceStage::CompleteRequestDispatched',
    'assert_eq!(rows.len(), 1',
    'matching.len(),',
    'matching[0].state, "claimed"',
    'LifecycleAbortMultipartRaceStage::ClaimedActionRechecked',
    'bucket_lock.take()',
    '.rollback()'
) "Dispatch and a second unique claimed observation must precede unlock"
Assert-Matches $preRace 'assert_eq!\(\s*matching\.len\(\),\s*1,' "Recheck must assert exactly one matching action"
Assert-Matches $preRace 'assert_eq!\(\s*matching\[0\]\.state,\s*"claimed",' "Recheck must assert claimed, not merely mention it"
$abortRacePendingStage = $RustLive.IndexOf('LifecycleAbortMultipartRaceStage::ClaimedActionObserved', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceStartedStage = $RustLive.IndexOf('record_lifecycle_abort_multipart_race_stage(LifecycleAbortMultipartRaceStage::RaceStarted);', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceCompleteStage = $RustLive.IndexOf('LifecycleAbortMultipartRaceStage::CompleteResponse', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceTerminalStage = $RustLive.IndexOf('LifecycleAbortMultipartRaceStage::TerminalStateEvaluation', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceLifecycleCleanupStage = $RustLive.IndexOf('LifecycleAbortMultipartRaceStage::LifecycleConfigDeleted', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceObjectCleanupStage = $RustLive.IndexOf('record_lifecycle_abort_multipart_race_stage(LifecycleAbortMultipartRaceStage::ObjectCleanup);', $abortRaceTestStart, [StringComparison]::Ordinal)
$abortRaceBucketStage = $RustLive.IndexOf('record_lifecycle_abort_multipart_race_stage(LifecycleAbortMultipartRaceStage::BucketDelete);', $abortRaceTestStart, [StringComparison]::Ordinal)
Assert-True (
    $abortRaceTestStart -ge 0 -and
    $abortRacePendingStage -gt $abortRaceTestStart -and
    $abortRaceStartedStage -gt $abortRacePendingStage -and
    $abortRaceCompleteStage -gt $abortRaceStartedStage -and
    $abortRaceTerminalStage -gt $abortRaceCompleteStage -and
    $abortRaceLifecycleCleanupStage -gt $abortRaceTerminalStage -and
    $abortRaceObjectCleanupStage -gt $abortRaceLifecycleCleanupStage -and
    $abortRaceBucketStage -gt $abortRaceObjectCleanupStage
) "Lifecycle abort multipart race stages must preserve claimed/race/terminal/cleanup order"

Assert-True (([regex]::Matches($Main, '(?m)\bstart_worker\(')).Count -eq 1) "Production main must start exactly one generic lifecycle worker"
Assert-NotMatches $Main '(?i)abort_incomplete_multipart|abort.*start_worker|start_worker.*abort' "Production main must not add abort-specific lifecycle worker or configuration wiring"
Assert-NotMatches $Main '(?i)\b\w*abort\w*(?:worker|config)\w*\b|\b\w*(?:worker|config)\w*abort\w*\b' "Production main must not introduce abort-specific worker/configuration identifiers"

$raceTimeoutDeclaration = 'const LIFECYCLE_RACE_TIMEOUT: Duration = Duration::from_secs(60);'
Assert-True (([regex]::Matches($RustLive, [regex]::Escape($raceTimeoutDeclaration))).Count -eq 1) "Lifecycle race timeout declaration is missing or duplicated"
Assert-True (([regex]::Matches($RustLive, [regex]::Escape('tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {'))).Count -eq 2) "Lifecycle race timeout must have exactly two outer uses"
Assert-True (([regex]::Matches($RustLive, '\bLIFECYCLE_RACE_TIMEOUT\b')).Count -eq 3) "Lifecycle race timeout leaked beyond declaration plus two uses"
Assert-Contains $RustLive "signed successor must become visible within 60 seconds" "Successor convergence timeout message must state 60 seconds"
Assert-Contains $RustLive "lifecycle race must reach a stable S3-visible state within 60 seconds" "Terminal convergence timeout message must state 60 seconds"

foreach ($fragment in @(
    'const LIFECYCLE_RACE_TEST_NAME: &str =',
    '"multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome";',
    'enum LifecycleRaceStage',
    'BucketCreated',
    'VersioningEnabled',
    'LifecycleConfigured',
    'PredecessorCreated',
    'RaceStarted',
    'SuccessorRequestDispatched',
    'SuccessorRequestComplete',
    'ObserverLoopEntered',
    'ObserverGetResponse',
    'ObserverListResponse',
    'ObserverListStatusOk',
    'ObserverSuccessorVisible',
    'SuccessorResponse',
    'SuccessorObserved',
    'LifecycleConfigDeleted',
    'TerminalWaitEntered',
    'TerminalStateEvaluation',
    'SuccessorRead',
    'VersionCleanup',
    'BucketDelete',
    '"terminal-state-evaluation"',
    '"bucket-created"',
    '"versioning-enabled"',
    '"lifecycle-configured"',
    '"predecessor-created"',
    '"race-started"',
    '"successor-request-dispatched"',
    '"successor-request-complete"',
    '"observer-loop-entered"',
    '"observer-get-response"',
    '"observer-list-response"',
    '"observer-list-status-ok"',
    '"observer-successor-visible"',
    '"successor-response"',
    '"successor-observed"',
    '"lifecycle-config-deleted"',
    '"terminal-wait-entered"',
    '"successor-read"',
    '"version-cleanup"',
    '"bucket-delete"',
    'fn record_lifecycle_race_stage(stage: LifecycleRaceStage)',
    '[LIFECYCLE-RACE-STAGE] test={LIFECYCLE_RACE_TEST_NAME} stage={}',
    'stage.as_str()'
)) {
    Assert-Contains $RustLive $fragment "Lifecycle race safe stage source is incomplete: $fragment"
}
Assert-True (([regex]::Matches($RustLive, '(?m)^fn record_lifecycle_race_stage\(stage: LifecycleRaceStage\) \{$')).Count -eq 1) "Lifecycle race safe stage emitter must exist exactly once"
$raceTestStart = $RustLive.IndexOf('async fn multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome()', [StringComparison]::Ordinal)
$requestDispatchedStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestDispatched);', $raceTestStart, [StringComparison]::Ordinal)
$requestCompleteStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestComplete);', $raceTestStart, [StringComparison]::Ordinal)
$observerStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::ObserverLoopEntered);', $raceTestStart, [StringComparison]::Ordinal)
$terminalStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::TerminalStateEvaluation);', $raceTestStart, [StringComparison]::Ordinal)
$successorStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRead);', $raceTestStart, [StringComparison]::Ordinal)
$cleanupStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::VersionCleanup);', $raceTestStart, [StringComparison]::Ordinal)
$bucketStage = $RustLive.IndexOf('record_lifecycle_race_stage(LifecycleRaceStage::BucketDelete);', $raceTestStart, [StringComparison]::Ordinal)
Assert-True (
    $raceTestStart -ge 0 -and
    $requestDispatchedStage -gt $raceTestStart -and
    $requestCompleteStage -gt $requestDispatchedStage -and
    $observerStage -gt $requestCompleteStage -and
    $terminalStage -gt $raceTestStart -and
    $successorStage -gt $terminalStage -and
    $cleanupStage -gt $successorStage -and
    $bucketStage -gt $cleanupStage
) "Lifecycle race safe stages must put the successor before observation and retain terminal/successor/cleanup/bucket order"

$Workflow = Read-NormalizedText $WorkflowPath
$workflowJobs = Get-YamlBlock $Workflow "jobs" 0
$workflowJobNames = @([regex]::Matches($workflowJobs, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($workflowJobNames.Count -eq 6) "Release validation must define exactly six jobs"
foreach ($requiredJob in @("postgres-import", "postgres-production-deployment", "multi-gateway-deployment", "cluster-pinset-replication", "e2e", "client-smoke-infrastructure")) {
    Assert-True ($workflowJobNames -ccontains $requiredJob) "Release validation is missing $requiredJob"
}
$multiJob = Get-YamlBlock $workflowJobs "multi-gateway-deployment" 2
$clientJob = Get-YamlBlock $workflowJobs "client-smoke-infrastructure" 2
$expectedClientRunLines = @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/cluster.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($clientRunLines.Count -eq 5) "Client-smoke infrastructure job must contain exactly five blocking run commands"
for ($index = 0; $index -lt $expectedClientRunLines.Count; $index++) {
    Assert-True ($clientRunLines[$index].TrimEnd() -ceq $expectedClientRunLines[$index]) "Client-smoke infrastructure command $($index + 1) is missing, changed, or out of order"
}
Assert-Contains $multiJob "    runs-on: ubuntu-latest" "Multi-gateway job must use ubuntu-latest"
Assert-Contains $multiJob "    timeout-minutes: 60" "Multi-gateway job timeout must be 60 minutes"
Assert-NotMatches $multiJob '(?m)^    (?:needs|continue-on-error):' "Multi-gateway job must remain independent and blocking"
$multiEnv = Get-YamlBlock $multiJob "env" 4
foreach ($exactEnvironmentLine in @(
    '      COMPOSE_DISABLE_ENV_FILE: "1"',
    '      COMPOSE_PROJECT_NAME: ipfs3-mg-${{ github.run_id }}-${{ github.run_attempt }}',
    "      IPFS_S3_MULTI_GATEWAY_A_ENDPOINT: http://127.0.0.1:59001",
    "      IPFS_S3_MULTI_GATEWAY_B_ENDPOINT: http://127.0.0.1:59002",
    "      IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT: http://127.0.0.1:59000",
    "      IPFS_S3_MULTI_GATEWAY_KUBO_URL: http://127.0.0.1:55002",
    "      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000",
    "      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55002"
)) {
    Assert-True ((@($multiEnv -split "`n" | Where-Object { $_ -ceq $exactEnvironmentLine })).Count -eq 1) "Multi-gateway environment line changed: $exactEnvironmentLine"
}
Assert-NotContains $multiJob "--env-file" "Multi-gateway validation must not use an explicit environment file"
Assert-NotMatches $multiJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Multi-gateway validation must not use null SetEnvironmentVariable removal"
Assert-InOrder $multiJob @(
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
) "Multi-gateway workflow order changed"
foreach ($requiredWorkflowFragment in @(
    "MULTI_GATEWAY_OWNED=true",
    "MULTI_GATEWAY_ATTEMPTED=true",
    "config --quiet",
    "55434, 55002, 59000, 59001, 59002",
    "up --detach --build --wait --wait-timeout 300 postgres kubo gateway-a gateway-b load-balancer",
    "cargo test --test multi_gateway multi_gateway_cross_replica_contract -- --exact --nocapture --test-threads=1",
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "logs --no-color postgres kubo gateway-a gateway-b load-balancer",
    "stop gateway-a",
    'AddSeconds(30)',
    "cargo test --test multi_gateway load_balancer_surviving_replica_crud -- --exact --nocapture --test-threads=1",
    "down --volumes --remove-orphans",
    'label=com.docker.compose.project=$project',
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE'
)) {
    Assert-Contains $multiJob $requiredWorkflowFragment "Multi-gateway workflow contract is missing: $requiredWorkflowFragment"
}
Assert-Matches $multiJob '(?s)try \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?\} finally \{.*?SetEnvironmentVariable\(\$requiredName, \$savedRequiredValues\[\$requiredName\], "Process"\).*?\$restoredRequiredValue -cne \$savedRequiredValues\[\$requiredName\]' "Secret probes must restore exact values in finally"
Assert-True (([regex]::Matches($multiJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Multi-gateway diagnostics and cleanup must both use always()"
Assert-InOrder $multiJob @(
    "      - name: Multi-gateway Compose diagnostics",
    "      - name: Multi-gateway Compose cleanup and residual assertion",
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'if ($downExit -ne 0 -or $containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0)'
) "Diagnostics, cleanup, and all residual exits must remain fail-closed and ordered"
Assert-NotMatches $multiJob '(?i)docker\s+(?:system|container|network|volume)\s+prune|docker\s+rm\s+-f' "Multi-gateway cleanup must not prune broad Docker resources"

$jobLines = @($multiJob -split "`n")
$pwshBlocks = @()
for ($lineIndex = 0; $lineIndex -lt $jobLines.Count; $lineIndex++) {
    if ($jobLines[$lineIndex].Trim() -cne "shell: pwsh") { continue }
    $runIndex = $lineIndex + 1
    while ($runIndex -lt $jobLines.Count -and [string]::IsNullOrWhiteSpace($jobLines[$runIndex])) { $runIndex++ }
    Assert-True ($runIndex -lt $jobLines.Count -and $jobLines[$runIndex].Trim() -ceq "run: |") "Each multi-gateway pwsh step must use a literal run block"
    $sourceLines = [Collections.Generic.List[string]]::new()
    for ($bodyIndex = $runIndex + 1; $bodyIndex -lt $jobLines.Count; $bodyIndex++) {
        $line = $jobLines[$bodyIndex]
        if (-not [string]::IsNullOrWhiteSpace($line) -and ([regex]::Match($line, '^( *)').Groups[1].Length -le 8)) { break }
        if ([string]::IsNullOrWhiteSpace($line)) { $sourceLines.Add(""); continue }
        Assert-True ($line.StartsWith("          ", [StringComparison]::Ordinal)) "PowerShell run line lost YAML indentation"
        $sourceLines.Add($line.Substring(10))
    }
    $pwshBlocks += ,($sourceLines -join "`n")
}
Assert-True ($pwshBlocks.Count -eq 9) "Expected exactly nine multi-gateway PowerShell run blocks"
foreach ($source in $pwshBlocks) {
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$parseErrors) | Out-Null
    if ($parseErrors.Count -ne 0) { throw ($parseErrors.Message -join "; ") }
    Assert-NotMatches $source '(?m)(?:^|\s)(?:export\s+|source\s+)|&&|/dev/null' "Workflow PowerShell contains Bash syntax"
}

$E2e = Read-NormalizedText $E2ePath
Assert-True (([regex]::Matches($E2e, '(?m)^#\[tokio::test\]\s*$')).Count -eq 12) "Existing E2E target must contain exactly twelve Tokio tests"
Assert-True (([regex]::Matches($multiJob, [regex]::Escape('cargo test --test e2e -- --nocapture --test-threads=1'))).Count -eq 1) "Multi-gateway job must run the complete serial E2E target exactly once"

Write-Host "multi-gateway static contract tests: PASSED"
