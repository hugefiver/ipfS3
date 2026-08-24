$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposePath = Join-Path $RepoRoot "docker-compose.cluster.yml"
$KuboDockerfilePath = Join-Path $RepoRoot "ipfs/cluster.Dockerfile"
$OverridePath = Join-Path $RepoRoot "tests/compose.cluster-validation.yml"
$RustPath = Join-Path $RepoRoot "tests/cluster.rs"
$RustSupportPath = Join-Path $RepoRoot "tests/support/cluster.rs"
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"

function Require-File {
    param([Parameter(Mandatory)][string]$Path)

    if (-not [IO.File]::Exists($Path)) { throw "Required file is missing: $Path" }
}

function Read-NormalizedText {
    param([Parameter(Mandatory)][string]$Path)

    Require-File $Path
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

function Assert-ExactSet {
    param(
        [Parameter(Mandatory)][string[]]$Actual,
        [Parameter(Mandatory)][string[]]$Expected,
        [Parameter(Mandatory)][string]$Message
    )

    Assert-True ($Actual.Count -eq $Expected.Count) "$Message Count changed"
    Assert-True ((@($Actual | Sort-Object) -join "`n") -ceq (@($Expected | Sort-Object) -join "`n")) $Message
}

function Protect-ClusterDiagnosticLine {
    param([AllowEmptyString()][string]$Line)

    $safe = $Line
    $safe = [regex]::Replace(
        $safe,
        '(?i)((?:"?)(?:PeerID|peer_id|peer-id)(?:"?)\s*[:=]\s*)(?:"[^"]*"|''[^'']*''|[^\s,;]+)',
        '$1[REDACTED_PEER_ID]'
    )
    $safe = [regex]::Replace(
        $safe,
        '(?i)(/p2p/)[1-9A-HJ-NP-Za-km-z]+',
        '$1[REDACTED_PEER_ID]'
    )
    $safe = [regex]::Replace(
        $safe,
        '(?<![1-9A-HJ-NP-Za-km-z])12D3Koo[1-9A-HJ-NP-Za-km-z]{20,}(?![1-9A-HJ-NP-Za-km-z])',
        '[REDACTED_PEER_ID]'
    )
    if ($safe -match '(?i)\b(?:peer(?:[_-]?id)?|peerid|identity|peerstore)\b') {
        $safe = [regex]::Replace(
            $safe,
            '(?<![1-9A-HJ-NP-Za-km-z])Qm[1-9A-HJ-NP-Za-km-z]{44}(?![1-9A-HJ-NP-Za-km-z])',
            '[REDACTED_PEER_ID]'
        )
    }
    return $safe
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

function Get-PwshRunBlocks {
    param([Parameter(Mandatory)][string]$JobBlock)

    $jobLines = @($JobBlock -split "`n")
    $blocks = [Collections.Generic.List[string]]::new()
    for ($lineIndex = 0; $lineIndex -lt $jobLines.Count; $lineIndex++) {
        if ($jobLines[$lineIndex].Trim() -cne "shell: pwsh") { continue }
        $runIndex = $lineIndex + 1
        while ($runIndex -lt $jobLines.Count -and [string]::IsNullOrWhiteSpace($jobLines[$runIndex])) { $runIndex++ }
        Assert-True ($runIndex -lt $jobLines.Count -and $jobLines[$runIndex].Trim() -ceq "run: |") "Each Cluster PowerShell step must use a literal run block"

        $sourceLines = [Collections.Generic.List[string]]::new()
        for ($bodyIndex = $runIndex + 1; $bodyIndex -lt $jobLines.Count; $bodyIndex++) {
            $line = $jobLines[$bodyIndex]
            if (-not [string]::IsNullOrWhiteSpace($line) -and ([regex]::Match($line, '^( *)').Groups[1].Length -le 8)) { break }
            if ([string]::IsNullOrWhiteSpace($line)) {
                $sourceLines.Add("")
                continue
            }
            Assert-True ($line.StartsWith("          ", [StringComparison]::Ordinal)) "Cluster PowerShell run line lost YAML indentation"
            $sourceLines.Add($line.Substring(10))
        }
        $blocks.Add($sourceLines -join "`n")
    }
    return @($blocks.ToArray())
}

$protectedHashes = [ordered]@{
    "docker-compose.yml" = "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c"
    "docker-compose.postgres.yml" = "6a39a43a48beda2cbd79a0ae8ca403a3ead2a67766ed34c214de78fa1a6a1782"
    "docker-compose.multi-gateway.yml" = "0397160b0abf7e5597b1fbfd4395ad0cc40a1112603eeeb90ea0d75422bf51bd"
    "Dockerfile" = "34aa8b4b5b880ec474d14dabe625fce5fecb45c4e906eb3489ad335b7fb6837c"
    "config.docker.toml" = "506596ff7da7c684f5ab9b860a49784f676372f31fd9dbf08fef401438714183"
    "Cargo.toml" = "86f5c654c8b54e57d4b324da0faf75df8f0353c4fa2df208da924dddc49f8764"
    "Cargo.lock" = "b579f0d8669ee6c05c9c09cc71f692c2c4dd667772f59fcbbf0f5ac57452d5c1"
    "ipfs/Dockerfile" = "ba4e543d3004ea5b2db18866f189afd5b2724f35b4e2ed64a6a37573748304f0"
    "ipfs/entrypoint.sh" = "6fb8407d5dcdc206aa4bf50d0e451c36eb28ad133896702271a5cb0a8d17a6ad"
    "src/kubo/add.rs" = "d4698d5e01df5dcf5a32e5737323890960d043110d858f05c8e14938b5b1b751"
    "src/kubo/pin.rs" = "6047757369227cf6fa4c4bbc7debe1cbabe5a9aa1a0bba1498b2c834c7a22216"
    "src/kubo/cat.rs" = "e970271249f4e10efb61f0720a11ee0cbca18b1aad7d5a0f2629bb432c0c90bf"
    "src/state.rs" = "bfbe43c3d9acb9a0b4b3cb901e6cd6c0af8c7e4c728d3149f19c0bb77f938541"
    "src/s3/ops/object.rs" = "3cef9c6b169a717720b721399ea35d28e405943529a2fe7b680c16a01624296e"
    "src/s3/ops/multipart.rs" = "3e08ab1dca7ded499193bee88e9c0014fe34a744d907259ac60573e57bcda4cf"
}

foreach ($entry in $protectedHashes.GetEnumerator()) {
    $path = Join-Path $RepoRoot $entry.Key
    Require-File $path
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
    Assert-True ($actual -ceq $entry.Value) "Protected baseline changed: $($entry.Key)"
}

if (-not [IO.File]::Exists($ComposePath)) {
    throw "RED: required production Compose file is missing: docker-compose.cluster.yml"
}

$Compose = Read-NormalizedText $ComposePath
$KuboDockerfile = Read-NormalizedText $KuboDockerfilePath
$Override = Read-NormalizedText $OverridePath

$ExpectedKuboDockerfile = @'
FROM ipfs/kubo:v0.43.0

COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
'@
$ExpectedKuboDockerfile = $ExpectedKuboDockerfile.Replace("`r`n", "`n").TrimEnd("`n")
Assert-True ($KuboDockerfile.TrimEnd("`n") -ceq $ExpectedKuboDockerfile) "Cluster Kubo Dockerfile changed"

Assert-NotContains $Compose "`t" "Cluster Compose must use spaces"
Assert-Matches $Compose '(?m)^name: ipfs3-cluster$' "Cluster Compose project name changed"
Assert-NotMatches $Compose '(?m)^\s*<<:\s*' "Cluster Compose must not use YAML merge keys"
Assert-NotMatches $Compose '(?m)^\s*[A-Za-z0-9_-]+:\s*&' "Cluster Compose must not use YAML anchors"

$services = Get-YamlBlock $Compose "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedServices = @("postgres", "kubo-a", "kubo-b", "cluster-a", "cluster-b", "gateway")
Assert-ExactSet $serviceNames $expectedServices "Cluster service set changed"

$volumes = Get-YamlBlock $Compose "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedVolumes = @("postgres_data", "kubo_a_data", "kubo_b_data", "cluster_a_data", "cluster_b_data")
Assert-ExactSet $volumeNames $expectedVolumes "Cluster volume set changed"

$postgres = Get-YamlBlock $services "postgres" 2
$kuboA = Get-YamlBlock $services "kubo-a" 2
$kuboB = Get-YamlBlock $services "kubo-b" 2
$clusterA = Get-YamlBlock $services "cluster-a" 2
$clusterB = Get-YamlBlock $services "cluster-b" 2
$gateway = Get-YamlBlock $services "gateway" 2

foreach ($fragment in @(
    "    image: postgres:17",
    "      POSTGRES_DB: postgres",
    "      POSTGRES_USER: postgres",
    '      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"',
    "      - postgres_data:/var/lib/postgresql/data",
    "      - source: postgres_init",
    "        target: /docker-entrypoint-initdb.d/10-ipfs3.sql",
    '      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]',
    "      interval: 5s",
    "      timeout: 3s",
    "      retries: 20",
    "      start_period: 10s",
    "    restart: unless-stopped"
)) {
    Assert-Contains $postgres $fragment "PostgreSQL contract changed: $fragment"
}
Assert-NotMatches $postgres '(?m)^    ports:\s*$' "Production PostgreSQL must not publish ports"

$configs = Get-YamlBlock $Compose "configs" 0
$postgresInit = Get-YamlBlock $configs "postgres_init" 2
foreach ($fragment in @(
    "    content: |",
    "      \set app_password '`${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'",
    "      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;",
    "      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec",
    "      CREATE DATABASE ipfs3 OWNER ipfs3;"
)) {
    Assert-Contains $postgresInit $fragment "PostgreSQL initializer changed: $fragment"
}

foreach ($kubo in @($kuboA, $kuboB)) {
    foreach ($fragment in @(
        "    build:",
        "      context: ./ipfs",
        "      dockerfile: cluster.Dockerfile",
        "    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0",
        "      IPFS_PATH: /data/ipfs",
        '      test: ["CMD", "ipfs", "id"]',
        "      interval: 5s",
        "      timeout: 3s",
        "      retries: 20",
        "      start_period: 15s",
        "    restart: unless-stopped"
    )) {
        Assert-Contains $kubo $fragment "Kubo contract changed: $fragment"
    }
    Assert-NotMatches $kubo '(?m)^    ports:\s*$' "Production Kubo must not publish ports"
}
Assert-Contains $kuboA "      - kubo_a_data:/data/ipfs" "Kubo A volume changed"
Assert-Contains $kuboB "      - kubo_b_data:/data/ipfs" "Kubo B volume changed"

$clusterImage = "    image: ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd"
foreach ($cluster in @($clusterA, $clusterB)) {
    foreach ($fragment in @(
        $clusterImage,
        "      IPFS_CLUSTER_CONSENSUS: crdt",
        '      CLUSTER_SECRET: "${IPFS_S3_CLUSTER_SECRET:?IPFS_S3_CLUSTER_SECRET is required}"',
        '      CLUSTER_CRDT_TRUSTEDPEERS: "*"',
        '      CLUSTER_REPLICATIONFACTORMIN: "2"',
        '      CLUSTER_REPLICATIONFACTORMAX: "2"',
        "      CLUSTER_RESTAPI_HTTPLISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9094",
        "      CLUSTER_IPFSPROXY_LISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9095",
        '      test: ["CMD", "ipfs-cluster-ctl", "id"]',
        "      interval: 5s",
        "      timeout: 5s",
        "      retries: 30",
        "      start_period: 15s",
        "    restart: unless-stopped"
    )) {
        Assert-Contains $cluster $fragment "Cluster service contract changed: $fragment"
    }
    Assert-NotMatches $cluster '(?m)^    (?:ports|entrypoint|command):\s*$' "Cluster must retain its full default ipfs-cluster-service entrypoint and no production ports"
}
Assert-Contains $clusterA "      CLUSTER_PEERNAME: cluster-a" "Cluster A peer name changed"
Assert-Contains $clusterB "      CLUSTER_PEERNAME: cluster-b" "Cluster B peer name changed"
Assert-Contains $clusterA "      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001" "Cluster A Kubo connector changed"
Assert-Contains $clusterB "      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001" "Cluster B Kubo connector changed"
Assert-Contains $clusterA "      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001" "RED: Cluster A proxy node target is missing or incorrect"
Assert-Contains $clusterB "      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001" "RED: Cluster B proxy node target is missing or incorrect"
foreach ($peer in @(
    [pscustomobject]@{ Name = "cluster-a"; Block = $clusterA; Target = "/dns4/kubo-a/tcp/5001" },
    [pscustomobject]@{ Name = "cluster-b"; Block = $clusterB; Target = "/dns4/kubo-b/tcp/5001" }
)) {
    $connectorMatches = @([regex]::Matches($peer.Block, '(?m)^      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: ([^\r\n]+)\s*$'))
    $proxyMatches = @([regex]::Matches($peer.Block, '(?m)^      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: ([^\r\n]+)\s*$'))
    Assert-True ($connectorMatches.Count -eq 1) "$($peer.Name) must declare exactly one paired Kubo connector target"
    Assert-True ($proxyMatches.Count -eq 1) "$($peer.Name) must declare exactly one proxy node target"
    $connectorTarget = $connectorMatches[0].Groups[1].Value.Trim()
    $proxyTarget = $proxyMatches[0].Groups[1].Value.Trim()
    Assert-True ($connectorTarget -ceq $peer.Target) "$($peer.Name) connector target is cross-wired or incorrect"
    Assert-True ($proxyTarget -ceq $peer.Target) "$($peer.Name) proxy node target is cross-wired or incorrect"
    Assert-True ($connectorTarget -ceq $proxyTarget) "$($peer.Name) connector and proxy must use the same paired Kubo target"
    Assert-NotMatches $peer.Block '(?m)^      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /ip4/127\.0\.0\.1/tcp/5001\s*$' "$($peer.Name) proxy must not use Kubo's loopback default"
}
Assert-NotMatches $Compose '(?m)^      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /ip4/127\.0\.0\.1/tcp/5001\s*$' "Cluster proxy must not use Kubo's loopback default"
Assert-Contains $clusterA "      - cluster_a_data:/data/ipfs-cluster" "Cluster A identity volume changed"
Assert-Contains $clusterB "      - cluster_b_data:/data/ipfs-cluster" "Cluster B identity volume changed"
Assert-Contains $clusterA "      kubo-a:`n        condition: service_healthy" "Cluster A Kubo dependency must be health-gated"
Assert-Contains $clusterB "      kubo-b:`n        condition: service_healthy" "Cluster B Kubo dependency must be health-gated"

foreach ($fragment in @(
    "    build: .",
    "    image: ghcr.io/hugefiver/ipfs3:latest",
    "      IPFS_S3_BIND: 0.0.0.0:9000",
    '      IPFS_S3_PUBLISHED_BIND: "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}"',
    "      IPFS_S3_KUBO_RPC_URL: http://cluster-a:9095",
    '      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"',
    '      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"',
    '      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"',
    '      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"',
    "      RUST_LOG: info",
    "      postgres:`n        condition: service_healthy",
    "      cluster-a:`n        condition: service_healthy",
    '      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]',
    "      interval: 5s",
    "      timeout: 3s",
    "      retries: 20",
    "      start_period: 10s",
    "    restart: unless-stopped"
)) {
    Assert-Contains $gateway $fragment "Gateway contract changed: $fragment"
}
Assert-NotMatches $gateway '(?m)^    volumes:\s*$' "Production gateway must not mount persistent data"
$gatewayEntrypoint = Get-YamlBlock $gateway "entrypoint" 4
$expectedGatewayEntrypoint = @'
      - /bin/sh
      - -ec
      - |
        case "$${IPFS_S3_PUBLISHED_BIND}" in
          "127.0.0.1")
            ;;
          *)
            exit 64
            ;;
        esac
        exec /app/ipfs-s3-gateway
'@.TrimEnd("`n")
Assert-True ($gatewayEntrypoint.TrimEnd("`n") -ceq $expectedGatewayEntrypoint) "Gateway must allow only fixed loopback before starting the unchanged application command"
Assert-True (([regex]::Matches($gateway, '(?m)^      IPFS_S3_PUBLISHED_BIND: ')).Count -eq 1) "Gateway must define exactly one published-bind runtime variable"
Assert-True (([regex]::Matches($gateway, '(?m)^    entrypoint:\s*$')).Count -eq 1) "Gateway must define exactly one runtime guard entrypoint"
Assert-InOrder $gateway @(
    'case "$${IPFS_S3_PUBLISHED_BIND}" in',
    '"127.0.0.1")',
    '*)',
    'exit 64',
    'esac',
    'exec /app/ipfs-s3-gateway'
) "Gateway must reject every non-loopback publication before its unchanged application command"
Assert-NotMatches $gatewayEntrypoint '(?im)\b(?:echo|printf|printenv|env)\b|\$\$\{?IPFS_S3_PUBLISHED_BIND\}?[^\r\n]*(?:\||>|<)' "Gateway runtime guard must not emit the published bind value"

$publishedServices = @($serviceNames | Where-Object { (Get-YamlBlock $services $_ 2) -match '(?m)^    ports:\s*$' })
Assert-ExactSet $publishedServices @("gateway") "Only gateway may publish a production port"
$gatewayPorts = Get-YamlBlock $gateway "ports" 4
Assert-True ($gatewayPorts.Trim() -ceq '- "127.0.0.1:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"') "Gateway production publication must be fixed host loopback"
Assert-NotContains $gatewayPorts '${IPFS_S3_GATEWAY_BIND' "Gateway host publication must not be variable-driven"
Assert-NotMatches $gatewayPorts '(?:0\.0\.0\.0|::|\[::\])' "Gateway host publication must not allow wildcard addresses"

foreach ($required in @(
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_CLUSTER_SECRET",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT"
)) {
    Assert-Contains $Compose ('${' + $required + ':?') "Required interpolation missing: $required"
    Assert-NotContains $Compose ('${' + $required + ':-') "Required interpolation gained a default: $required"
    Assert-NotContains $Compose ('${' + $required + '-default') "Required interpolation gained an alternate default: $required"
}
Assert-NotMatches $Compose '(?i)\$\{(?:POSTGRES_PASSWORD|IPFS_S3_ACCESS_KEY_ID|IPFS_S3_SECRET_ACCESS_KEY|IPFS_S3_MASTER_KEY|IPFS_S3_CLUSTER_SECRET|IPFS_S3_GATEWAY_BIND|IPFS_S3_GATEWAY_PORT)(?::-[^}]*)?\}' "Production secrets must not have development fallbacks"
foreach ($forbidden in @(
    "container_name:",
    "cloudflared",
    "gateway_data",
    "config.docker.toml",
    "IPFS_S3_CONFIG",
    "PINATA_JWT",
    "FILEBASE_PINNING_TOKEN",
    "CLOUDFLARE_TUNNEL_TOKEN",
    "swarm.key",
    "CLUSTER_ID",
    "CLUSTER_PRIVATEKEY",
    "identity.json",
    "peerstore",
    "service.json",
    "private swarm",
    "remote"
)) {
    Assert-NotContains $Compose $forbidden "Forbidden Cluster production fragment: $forbidden"
}

$validationServices = Get-YamlBlock $Override "services" 0
$validationServiceNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-ExactSet $validationServiceNames $expectedServices "Validation override service set changed"
$expectedMappings = @(
    "127.0.0.1:55435:5432",
    "127.0.0.1:55100:5001",
    "127.0.0.1:55101:5001",
    "127.0.0.1:59101:9094",
    "127.0.0.1:59103:9095",
    "127.0.0.1:59102:9094",
    "127.0.0.1:59100:9000"
)
$actualMappings = @([regex]::Matches($Override, '(?m)^\s*-\s+"([^"]+)"\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-ExactSet $actualMappings $expectedMappings "Validation mappings changed"
foreach ($mapping in $actualMappings) {
    Assert-True ($mapping.StartsWith("127.0.0.1:", [StringComparison]::Ordinal)) "Validation mapping is not loopback-only: $mapping"
}
$expectedMappingsByService = [ordered]@{
    "postgres" = @("127.0.0.1:55435:5432")
    "kubo-a" = @("127.0.0.1:55100:5001")
    "kubo-b" = @("127.0.0.1:55101:5001")
    "cluster-a" = @("127.0.0.1:59101:9094", "127.0.0.1:59103:9095")
    "cluster-b" = @("127.0.0.1:59102:9094")
    "gateway" = @("127.0.0.1:59100:9000")
}
foreach ($serviceName in $expectedMappingsByService.Keys) {
    $service = Get-YamlBlock $validationServices $serviceName 2
    $ports = Get-YamlBlock $service "ports" 4
    $serviceMappings = @([regex]::Matches($ports, '(?m)^\s*-\s+"([^"]+)"\s*$') | ForEach-Object { $_.Groups[1].Value })
    Assert-ExactSet $serviceMappings $expectedMappingsByService[$serviceName] "Validation mappings changed for $serviceName"
}
Assert-NotContains $Override "0.0.0.0" "Validation ports must be loopback-only"
Assert-NotMatches $Override '(?m)^volumes:\s*$' "Validation override must not declare volumes"
Assert-NotContains (Get-YamlBlock $validationServices "cluster-b" 2) ":9095" "Validation must not expose Cluster B proxy"

$AddSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/add.rs")
$PinSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/pin.rs")
$CatSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/cat.rs")
$ObjectSource = Read-NormalizedText (Join-Path $RepoRoot "src/s3/ops/object.rs")
foreach ($fragment in @(
    "pub async fn stream_add_with_progress<S, E>(",
    "api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false&progress=true",
    "let body = ReqwestBody::wrap_stream(ReaderStream::new(reader));",
    "let part = multipart::Part::stream(body)",
    '.file_name("object")',
    '.mime_str("application/octet-stream")',
    'let form = multipart::Form::new().part("file", part);',
    "let mut lines = NdjsonBuffer::new();",
    "let mut final_record_was_root = false;",
    "pub async fn stream_add<S, E>(kubo: &KuboClient, stream: S, cid_version: u8) -> AppResult<String>"
)) {
    Assert-Contains $AddSource $fragment "Kubo add production interface changed: $fragment"
}
Assert-Contains $PinSource "pub async fn pin_add(kubo: &KuboClient, cid: &str) -> AppResult<()>" "Kubo pin interface changed"
Assert-Contains $PinSource "api/v0/pin/add?arg={cid}" "Kubo pin/add query changed"
Assert-Contains $CatSource "pub async fn stream_cat(" "Kubo cat interface changed"
Assert-Contains $CatSource 'format!("{}/api/v0/cat?arg={cid}", kubo.base_url())' "Kubo cat query changed"
Assert-InOrder $ObjectSource @(
    "let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1).await?;",
    "crate::kubo::pin::pin_add(&state.kubo, &cid).await?;",
    "Ok(StoredObject {"
) "Plain object path must remain add then pin then publication input"

$Rust = Read-NormalizedText $RustPath
$RustSupport = Read-NormalizedText $RustSupportPath

Assert-Matches $Rust '(?m)^#\[allow\(dead_code\)\]\s*$\r?\nmod support;$' "Cluster target must retain its private shared support module"
Assert-Matches $Rust '(?m)^#\[path = "support/cluster\.rs"\]\s*$\r?\nmod cluster_support;$' "Cluster target must load the split Cluster support module"
Assert-True (([regex]::Matches($Rust, '(?m)^#\[tokio::test\]\s*$')).Count -eq 5) "Cluster target must define exactly five Tokio tests"

foreach ($testName in @(
    "cluster_topology_converges",
    "cluster_proxy_compatibility",
    "cluster_replication_and_retention",
    "cluster_peer_b_outage_contract",
    "cluster_peer_b_restart_recovery"
)) {
    Assert-True (([regex]::Matches($Rust, "(?m)^async fn $testName\(\) \{")).Count -eq 1) "Expected one live test named $testName"
}
Assert-InOrder $Rust @(
    "async fn cluster_topology_converges()",
    "async fn cluster_proxy_compatibility()",
    "async fn cluster_replication_and_retention()",
    "async fn cluster_peer_b_outage_contract()",
    "async fn cluster_peer_b_restart_recovery()"
) "Cluster live test order changed"
foreach ($environmentName in @(
    "IPFS_S3_CLUSTER_GATEWAY_ENDPOINT",
    "IPFS_S3_CLUSTER_A_REST_URL",
    "IPFS_S3_CLUSTER_B_REST_URL",
    "IPFS_S3_CLUSTER_A_PROXY_URL",
    "IPFS_S3_CLUSTER_KUBO_A_URL",
    "IPFS_S3_CLUSTER_KUBO_B_URL",
    "IPFS_S3_CLUSTER_STATE_PATH"
)) {
    Assert-Contains $Rust $environmentName "Cluster live environment contract is missing: $environmentName"
}

foreach ($requiredSupportFragment in @(
    "pub struct ClusterClient",
    "pub async fn health_probe",
    "pub async fn peers_probe",
    "pub async fn allocation_probe",
    "pub async fn pin_status_probe",
    "pub struct ClusterPeer",
    "pub struct ClusterIpfsIdentity",
    "pub struct PinAllocation",
    "pub struct GlobalPinInfo",
    "pub struct PeerPinInfo",
    "pub enum PinStatusObservation",
    "pub enum ProbeError",
    "Transient(&'static str)",
    "Terminal(&'static str)",
    "pub type ProbeResult",
    "pub struct ReplicationEvidence",
    "pub async fn wait_for_shared_two_peer_view",
    "pub fn peer_set_digest",
    "pub async fn wait_for_two_pinned",
    "pub async fn wait_until_not_fully_pinned",
    "pub async fn kubo_cat",
    "pub struct RecoveryState",
    "pub fn write_claimed",
    "pub fn read",
    "peer_set_sha256",
    "ipfs3-cluster-recovery-v1",
    "s3-replication-retention-v1",
    "replication_factor_min",
    "replication_factor_max",
    "peer_map",
    'status == "pinned"',
    "StatusCode::NO_CONTENT",
    "text.lines()",
    "peers_malformed_ndjson",
    "fn is_release_1_1_6_version",
    "peer_version_not_release_1_1_6",
    "metadata.is_file() && metadata.len() == 0",
    ".open(path)",
    "file.write_all(&bytes)",
    "file.sync_all()"
)) {
    Assert-Contains $RustSupport $requiredSupportFragment "Cluster support contract is missing: $requiredSupportFragment"
}
Assert-Matches $RustSupport 'Client::builder\(\)[\s\S]*?connect_timeout\(Duration::from_secs\(5\)\)[\s\S]*?timeout\(CALL_TIMEOUT\)' "Cluster client must bound loopback connect and call time"
Assert-Matches $RustSupport 'parsed\.scheme\(\) == "http"[\s\S]*?parsed\.host_str\(\) == Some\("127\.0\.0\.1"\)' "Cluster client must accept only loopback HTTP"
Assert-Matches $RustSupport 'response\.status\(\) != StatusCode::NO_CONTENT' "Cluster health must require exact 204"
Assert-Matches $RustSupport 'response\.status\(\) == StatusCode::NOT_FOUND[\s\S]*?return Ok\(None\)' "Allocation 404 must be pending"
Assert-Matches $RustSupport 'response\.status\(\) == StatusCode::NOT_FOUND[\s\S]*?PinStatusObservation::Pending' "Pin status 404 must be pending"
Assert-Matches $RustSupport 'Err\(ProbeError::Transient\(_\)\)' "Cluster polling must branch explicitly on transient probes"
Assert-Matches $RustSupport 'Err\(error @ ProbeError::Terminal\(_\)\)' "Cluster polling must return terminal probes"
Assert-Matches $RustSupport 'allocation\.replication_factor_min != 2[\s\S]*?allocation\.replication_factor_max != 2' "Pin evidence must require exact 2/2 factors"
Assert-Matches $RustSupport 'status\.peer_map\.keys\(\)\.any\(\|id\| !expected\.contains\(id\)\)' "Pin evidence must reject unknown tracker peers"
Assert-Matches $RustSupport 'info\.status == \"pinned\"' "Pin evidence must require pinned tracker states"
Assert-Matches $RustSupport 'version\.strip_prefix\("1\.1\.6\+"\)' "Cluster version validator must recognize release build metadata"
Assert-Matches $RustSupport 'byte\.is_ascii_alphanumeric\(\) \|\| byte == b''-''' "Cluster version metadata must use the SemVer build identifier alphabet"
Assert-NotContains $RustSupport 'peer.version != "1.1.6"' "Cluster version must not require byte-exact bare output"
Assert-NotMatches $RustSupport '(?i)peer_ids' "Recovery state must never serialize peer identities"
Assert-NotMatches $RustSupport 'OpenOptions::new\(\)[\s\S]*?\.create\(' "Recovery state must not create its receipt"
Assert-True (([regex]::Matches($RustSupport, '(?m)^#\[test\]\s*$')).Count -eq 1) "Cluster support must define exactly one deterministic plain unit test"
$versionTest = Get-BracedBlock $RustSupport '(?m)^fn release_version_validator_accepts_exact_release_and_build_metadata\(\) \{' 'release version validator test'
foreach ($acceptedVersion in @(
    "1.1.6",
    "1.1.6+git2182e",
    "1.1.6+git2182e.20260824",
    "1.1.6+build-7"
)) {
    Assert-Contains $versionTest.Body ('"' + $acceptedVersion + '"') "Release version validator accepted fixture is missing: $acceptedVersion"
}
foreach ($rejectedVersion in @(
    "1.1.6-*",
    "1.1.7",
    "1.1.6+",
    "1.1.6+.build",
    "1.1.6+build.",
    "1.1.6+build..meta",
    "1.1.6+build meta",
    "1.1.6+build/meta",
    "1.1.6+build_meta",
    "1.1.6+*"
)) {
    Assert-Contains $versionTest.Body ('"' + $rejectedVersion + '"') "Release version validator rejected fixture is missing: $rejectedVersion"
}
Assert-Matches $versionTest.Body '(?m)^\s*assert!\(is_release_1_1_6_version\(version\)\);\s*$' "Release version validator must assert accepted fixtures"
Assert-Matches $versionTest.Body '(?m)^\s*assert!\(!is_release_1_1_6_version\(version\)\);\s*$' "Release version validator must assert rejected fixtures"

foreach ($requiredLiveImport in @(
    "use ipfs_s3_gateway::kubo::{",
    "KuboClient",
    "add::stream_add",
    "pin::pin_add",
    "cat::stream_cat",
    "use support::sigv4::send_sigv4"
)) {
    Assert-Contains $Rust $requiredLiveImport "Cluster live production import is missing: $requiredLiveImport"
}
Assert-Contains $Rust "Method::HEAD" "Replication scenario must issue a signed HEAD"
Assert-Contains $Rust "send_sigv4(" "Replication scenario must use SigV4 HEAD"
Assert-Contains $Rust "== 404" "Signed HEAD must require exact 404"
Assert-Contains $Rust "IPFS_S3_CLUSTER_KUBO_A_URL" "Cluster A Kubo validation endpoint is missing"
Assert-Contains $Rust "IPFS_S3_CLUSTER_KUBO_B_URL" "Cluster B Kubo validation endpoint is missing"
Assert-Contains $Rust 'println!("peers=2 version=1.1.6");' "Topology output must normalize the Cluster release version"
Assert-Matches $Rust 'Err\(error\) => panic!\("\{category\}: \{error\}"\)' "Cluster probe failures may print only their static category display"

$proxyTest = Get-BracedBlock $Rust '(?m)^async fn cluster_proxy_compatibility\(\) \{' 'cluster_proxy_compatibility test'
foreach ($requiredProxyFragment in @(
    "IPFS_S3_CLUSTER_A_PROXY_URL",
    "http://127.0.0.1:59103",
    "KuboClient::new_with_timeouts(",
    "PROXY_CONTROL_TIMEOUT",
    "PROXY_DOWNLOAD_IDLE_TIMEOUT",
    "futures_util::stream::iter",
    "stream_add(&client, source, 1)",
    "pin_add(&client, &cid)",
    "stream_cat(&client, &cid, None)",
    "tokio::time::timeout(PROXY_SEQUENCE_TIMEOUT",
    "production_stream_add_failed",
    "production_pin_add_failed",
    "production_stream_cat_failed",
    "production_stream_cat_chunk_failed",
    "proxy_compatibility_sequence_timeout",
    "actual.len().saturating_add(chunk.len())"
)) {
    Assert-Contains $proxyTest.Body $requiredProxyFragment "Direct proxy compatibility contract is missing: $requiredProxyFragment"
}
Assert-True (([regex]::Matches($proxyTest.Body, [regex]::Escape("IPFS_S3_CLUSTER_A_PROXY_URL"))).Count -eq 1) "Direct proxy test must use only its one proxy endpoint"
foreach ($forbiddenProxyFragment in @(
    "IPFS_S3_CLUSTER_GATEWAY_ENDPOINT",
    "create_bucket",
    "s3_call",
    "Bucket",
    "put_object",
    "get_object",
    "delete_object",
    "state_path",
    "RecoveryState",
    "POSTGRES",
    "write_claimed",
    "tracing_subscriber"
)) {
    Assert-NotContains $proxyTest.Body $forbiddenProxyFragment "Direct proxy test must not use: $forbiddenProxyFragment"
}

foreach ($forbiddenLiveFragment in @(
    "reqwest::Client",
    "reqwest::multipart",
    "multipart::",
    "Part::bytes",
    "Part::stream",
    ".file_name(",
    "ReqwestBody",
    "ProxyAddEvent",
    "ProxyPinResponse",
    "serde_json::from_slice",
    "url::Url",
    '"/api/v0/add',
    '"/api/v0/pin/add',
    '"/api/v0/cat',
    "bounded_proxy_client",
    "proxy_add_pin_false",
    "proxy_pin_add",
    "proxy_cat",
    "observe_add_record",
    "read_bounded_proxy_body",
    "pin_rm(",
    "/pin/rm",
    "unwrap_or_default()"
)) {
    Assert-NotContains $Rust $forbiddenLiveFragment "Cluster live target must not contain: $forbiddenLiveFragment"
}
Assert-NotMatches $Rust '(?i)std::process::Command|Command::new|\bdocker\b|\baws\b|\bmc\b|\brclone\b' "Cluster live target must not execute native tools"
Assert-NotMatches $Rust '(?m)^\s*if let Ok\b' "Cluster polling must not catch and suppress errors"
Assert-NotMatches $Rust '(?i)(?:panic!|assert!|eprintln!)\([^\r\n]*(?:\{(?:cid|peer|body|identity)|response\.text|response\.bytes)' "Cluster test output must not interpolate identities, CIDs, or bodies"
Assert-NotMatches $RustSupport '(?i)(?:panic!|assert!|eprintln!)\([^\r\n]*(?:\{(?:cid|peer|body|identity)|response\.text|response\.bytes)' "Cluster support output must not interpolate identities, CIDs, or bodies"
Assert-NotMatches $RustSupport '(?i)(?:println!|eprintln!|format!)\([^\r\n]*version' "Cluster support must not emit or interpolate version metadata"

$replicationTest = Get-BracedBlock $Rust '(?m)^async fn cluster_replication_and_retention\(\) \{' 'cluster_replication_and_retention test'
Assert-Contains $replicationTest.Body "write_claimed" "Only replication must write the owned receipt"
foreach ($otherTestName in @("cluster_topology_converges", "cluster_proxy_compatibility", "cluster_peer_b_outage_contract", "cluster_peer_b_restart_recovery")) {
    $otherTest = Get-BracedBlock $Rust ("(?m)^async fn " + $otherTestName + "\(\) \{") $otherTestName
    Assert-NotContains $otherTest.Body "write_claimed" "Only replication may write the owned receipt"
}

$Workflow = Read-NormalizedText $WorkflowPath
$workflowJobs = Get-YamlBlock $Workflow "jobs" 0
$workflowJobNames = @([regex]::Matches($workflowJobs, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedWorkflowJobs = @(
    "postgres-import",
    "postgres-production-deployment",
    "multi-gateway-deployment",
    "cluster-pinset-replication",
    "e2e",
    "client-smoke-infrastructure"
)
Assert-ExactSet $workflowJobNames $expectedWorkflowJobs "Release validation job set changed"
Assert-NotMatches $workflowJobs '(?m)^    needs:' "Release validation jobs must remain independent"
Assert-NotMatches $workflowJobs '(?m)^    continue-on-error:' "Release validation jobs must remain blocking"

$clusterJob = Get-YamlBlock $workflowJobs "cluster-pinset-replication" 2
$clientJob = Get-YamlBlock $workflowJobs "client-smoke-infrastructure" 2
Assert-Contains $clusterJob "    runs-on: ubuntu-latest" "Cluster release job must use ubuntu-latest"
Assert-Contains $clusterJob "    timeout-minutes: 60" "Cluster release job timeout must be 60 minutes"
Assert-NotMatches $clusterJob '(?m)^    (?:needs|continue-on-error):' "Cluster release job must be independent and blocking"
Assert-NotMatches $clusterJob '(?m)^        continue-on-error:' "Cluster release product gates must be blocking"
foreach ($fragment in @(
    "      - uses: actions/checkout@v7",
    "        uses: dtolnay/rust-toolchain@v1",
    '          toolchain: "1.92"',
    "        uses: Swatinem/rust-cache@v2"
)) {
    Assert-Contains $clusterJob $fragment "Cluster release job is missing Rust setup: $fragment"
}

$clusterEnv = Get-YamlBlock $clusterJob "env" 4
$clusterEnvLines = @($clusterEnv -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($clusterEnvLines.Count -eq 16) "Cluster release job must contain exactly sixteen environment lines"
foreach ($exactEnvironmentLine in @(
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
    '      IPFS_S3_CLUSTER_STATE_PATH: ${{ runner.temp }}/ipfs3-cluster-${{ github.run_id }}-${{ github.run_attempt }}.json'
)) {
    Assert-True ((@($clusterEnv -split "`n" | Where-Object { $_ -ceq $exactEnvironmentLine })).Count -eq 1) "Cluster release environment line changed: $exactEnvironmentLine"
}
Assert-NotContains $clusterJob "--env-file" "Cluster release job must not use an explicit environment file"
Assert-NotMatches $clusterJob '(?i)(?<![A-Za-z0-9_])\.env(?![A-Za-z0-9_])' "Cluster release job must not read, modify, or emit a project environment file"
Assert-NotMatches $clusterJob '(?i)docker\s+(?:compose\s+)?pull\b|--pull(?:=|\s)' "Cluster release job must not explicitly pull images"
Assert-NotMatches $clusterJob '(?i)docker\s+(?:system|container|network|volume)\s+prune|docker\s+rm\s+-f' "Cluster release cleanup must not prune broad Docker resources"
Assert-NotMatches $clusterJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Cluster release job must not use null environment removal"
Assert-NotMatches $clusterJob '(?im)(?:^|\s)(?:aws|mc|rclone)(?:\.exe)?(?:\s|$)' "Cluster release job must not use native S3 client tools"
Assert-NotMatches $clusterJob '(?im)^\s*(?:git\s+(?:checkout|restore|clean|reset)|cargo\s+(?:add|remove|update|fix))\b' "Cluster release job must not edit application or dependency files"
Assert-NotMatches $clusterJob '(?i)ipfs-cluster-ctl\s+peers\s+ls|peer_ids|unwrap_or_default\(\)' "Cluster release job must not emit direct peer lists or suppress polling failures"

Assert-InOrder $clusterJob @(
    "      - name: Verify Docker Compose for Cluster deployment",
    "      - name: Verify Cluster environment contract",
    "      - name: Claim unique Cluster project, ports, and state receipt",
    "      - name: Build and start Cluster topology",
    "      - name: Prove Cluster release-version representation contract",
    "      - name: Prove exact two-peer topology without writes",
    "      - name: Prove direct Kubo wire compatibility against Cluster A proxy",
    "      - name: Prove replication and retained deletion",
    "      - name: Capture diagnostics before peer B stop",
    "      - name: Stop Cluster and Kubo peer B",
    "      - name: Prove stopped peer loses two-pin evidence",
    "      - name: Capture stopped-peer diagnostics before restart",
    "      - name: Restart Cluster and Kubo peer B with existing volumes",
    "      - name: Prove same-volume peer B recovery",
    "      - name: Final sanitized Cluster diagnostics",
    "      - name: Cluster cleanup and residual assertion"
) "Cluster release workflow order changed"

$pwshBlocks = Get-PwshRunBlocks $clusterJob
Assert-True ($pwshBlocks.Count -eq 16) "Expected exactly sixteen Cluster PowerShell run blocks"
foreach ($source in $pwshBlocks) {
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$parseErrors) | Out-Null
    if ($parseErrors.Count -ne 0) { throw ($parseErrors.Message -join "; ") }
    Assert-NotMatches $source '(?m)(?:^|\s)(?:export\s+|source\s+)|&&|/dev/null' "Cluster workflow PowerShell contains Bash syntax"
}

$composeVersionSource = $pwshBlocks[0]
$configurationSource = $pwshBlocks[1]
$ownershipSource = $pwshBlocks[2]
$startupSource = $pwshBlocks[3]
$releaseVersionSource = $pwshBlocks[4]
$topologySource = $pwshBlocks[5]
$proxySource = $pwshBlocks[6]
$replicationSource = $pwshBlocks[7]
$preStopDiagnosticSource = $pwshBlocks[8]
$stopSource = $pwshBlocks[9]
$outageSource = $pwshBlocks[10]
$preRestartDiagnosticSource = $pwshBlocks[11]
$restartSource = $pwshBlocks[12]
$recoverySource = $pwshBlocks[13]
$finalDiagnosticSource = $pwshBlocks[14]
$cleanupSource = $pwshBlocks[15]
foreach ($blockContract in @(
    [pscustomobject]@{ Index = 0; Fragment = 'docker compose version'; Role = 'compose' },
    [pscustomobject]@{ Index = 1; Fragment = 'docker compose @compose config --quiet'; Role = 'configuration' },
    [pscustomobject]@{ Index = 2; Fragment = 'CLUSTER_PINSET_OWNED=true'; Role = 'ownership' },
    [pscustomobject]@{ Index = 3; Fragment = 'up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b cluster-a cluster-b gateway'; Role = 'startup' },
    [pscustomobject]@{ Index = 4; Fragment = 'cluster_support::release_version_validator_accepts_exact_release_and_build_metadata'; Role = 'release-version unit' },
    [pscustomobject]@{ Index = 5; Fragment = 'cluster_topology_converges'; Role = 'topology' },
    [pscustomobject]@{ Index = 6; Fragment = 'cluster_proxy_compatibility'; Role = 'proxy compatibility' },
    [pscustomobject]@{ Index = 7; Fragment = 'cluster_replication_and_retention'; Role = 'replication' },
    [pscustomobject]@{ Index = 8; Fragment = 'Pre-stop Cluster diagnostics failed'; Role = 'pre-stop diagnostics' },
    [pscustomobject]@{ Index = 9; Fragment = 'stop cluster-b kubo-b'; Role = 'peer stop' },
    [pscustomobject]@{ Index = 10; Fragment = 'cluster_peer_b_outage_contract'; Role = 'outage' },
    [pscustomobject]@{ Index = 11; Fragment = 'Stopped-peer diagnostics failed'; Role = 'pre-restart diagnostics' },
    [pscustomobject]@{ Index = 12; Fragment = 'start kubo-b cluster-b'; Role = 'peer restart' },
    [pscustomobject]@{ Index = 13; Fragment = 'cluster_peer_b_restart_recovery'; Role = 'recovery' },
    [pscustomobject]@{ Index = 14; Fragment = 'Final Cluster diagnostics failed'; Role = 'final diagnostics' },
    [pscustomobject]@{ Index = 15; Fragment = '$errors = [Collections.Generic.List[string]]::new()'; Role = 'cleanup' }
)) {
    Assert-Contains $pwshBlocks[$blockContract.Index] $blockContract.Fragment "Cluster PowerShell block $($blockContract.Index) must remain $($blockContract.Role)"
}
Assert-Contains $composeVersionSource '$composeVersionText = (docker compose version --short).Trim()' "Cluster Compose check must preserve the complete version string"
Assert-Matches $composeVersionSource '\$composeVersionMatch = \[regex\]::Match\(\$composeVersionText, ''\^v\?\(\?<core>\\d\+\\\.\\d\+\\\.\\d\+\)\(\?:\[-\+\]\[0-9A-Za-z\.\-\]\+\)\?\$''\)' "Cluster Compose version parser must accept only a full vendor-suffixed semantic version"
Assert-Contains $composeVersionSource '[Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion)' "Cluster Compose check must parse only the numeric core"
Assert-NotContains $composeVersionSource '[Version]::TryParse($composeVersionText, [ref]$composeVersion)' "Cluster Compose check must not parse vendor suffixes directly"
foreach ($fragment in @(
    '"--project-name", $env:COMPOSE_PROJECT_NAME',
    '"-f", "docker-compose.cluster.yml"',
    '"-f", "tests/compose.cluster-validation.yml"',
    '"POSTGRES_PASSWORD"',
    '"IPFS_S3_ACCESS_KEY_ID"',
    '"IPFS_S3_SECRET_ACCESS_KEY"',
    '"IPFS_S3_MASTER_KEY"',
    '"IPFS_S3_CLUSTER_SECRET"',
    '"IPFS_S3_GATEWAY_BIND"',
    '"IPFS_S3_GATEWAY_PORT"',
    "POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$'",
    '[string]::IsNullOrWhiteSpace($env:IPFS_S3_ACCESS_KEY_ID)',
    '[string]::IsNullOrWhiteSpace($env:IPFS_S3_SECRET_ACCESS_KEY)',
    "IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$'",
    "IPFS_S3_MASTER_KEY -match '^0{64}$'",
    "IPFS_S3_CLUSTER_SECRET -notmatch '^[0-9A-Fa-f]{64}$'",
    'if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Gateway bind must be exactly 127.0.0.1" }',
    'IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort',
    '$gatewayPort -ne 59100',
    'Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop',
    'docker compose @compose config --quiet',
    '$missingExit = $LASTEXITCODE',
    '[Environment]::SetEnvironmentVariable($requiredName, $savedRequiredValues[$requiredName], "Process")',
    'if ([Environment]::GetEnvironmentVariable($requiredName, "Process") -cne $savedRequiredValues[$requiredName])',
    'if ($missingExit -eq 0) { throw "Compose accepted missing required variable: $name" }',
    'if ($LASTEXITCODE -ne 0) { throw "Complete Cluster Compose configuration failed" }'
)) {
    Assert-Contains $configurationSource $fragment "Cluster configuration contract is missing: $fragment"
}
Assert-NotContains $configurationSource 'IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")' "Cluster configuration must not retain the weaker wildcard-only bind check"
Assert-InOrder $configurationSource @(
    'if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Gateway bind must be exactly 127.0.0.1" }',
    'docker compose @compose config --quiet'
) "Cluster configuration must validate exact fixed loopback before Compose config"
Assert-Matches $configurationSource '(?s)foreach \(\$name in \$requiredNames\) \{.*?try \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?docker compose @compose config --quiet.*?\} finally \{.*?SetEnvironmentVariable\(\$requiredName, \$savedRequiredValues\[\$requiredName\], "Process"\).*?\}' "Cluster configuration probes must restore every required variable in finally"
Assert-NotMatches $configurationSource '(?m)^\s*docker compose .*config(?!\s+--quiet\b)' "Cluster configuration must never render Compose configuration"
Assert-NotMatches $configurationSource '(?im)^\s*(?:Write-Host|Write-Output|Write-Warning|\[Console\]::Out\.WriteLine).*?(?:POSTGRES_PASSWORD|IPFS_S3_ACCESS_KEY_ID|IPFS_S3_SECRET_ACCESS_KEY|IPFS_S3_MASTER_KEY|IPFS_S3_CLUSTER_SECRET)' "Cluster configuration must not emit a secret value"

Assert-InOrder $ownershipSource @(
    'label=com.docker.compose.project=$project',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'if ($containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0)',
    '55435, 55100, 55101, 59100, 59101, 59102, 59103',
    'if (Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH)',
    '"CLUSTER_PINSET_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    '[IO.File]::Open(',
    '[IO.FileMode]::CreateNew',
    '[IO.FileAccess]::Write',
    '[IO.FileShare]::None',
    '$stateReceiptOwned = $true',
    '"CLUSTER_STATE_RECEIPT_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Cluster ownership and state-receipt order changed"
Assert-Contains $ownershipSource 'if (-not $stateReceiptOwned) { throw "State receipt claim did not complete" }' "Cluster state receipt claim must fail closed"
Assert-InOrder $clusterJob @(
    '"CLUSTER_PINSET_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b cluster-a cluster-b gateway'
) "Cluster attempted marker must precede exact six-service startup"
Assert-Contains $startupSource 'if ($LASTEXITCODE -ne 0) { throw "Cluster topology did not become healthy" }' "Cluster startup failure must be blocking"
Assert-Matches $startupSource '(?s)\A\s*"CLUSTER_PINSET_ATTEMPTED=true" \| Add-Content -LiteralPath \$env:GITHUB_ENV\s*docker compose --project-name \$env:COMPOSE_PROJECT_NAME -f docker-compose\.cluster\.yml -f tests/compose\.cluster-validation\.yml up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b cluster-a cluster-b gateway' "Cluster attempted marker must occur immediately before exact startup"

$expectedCargoCommands = @(
    'cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact',
    'cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1'
)
Assert-True (([regex]::Matches($clusterJob, '(?m)^          cargo test --test cluster [^\r\n]+$')).Count -eq 6) "Cluster job must contain exactly six explicit Cluster cargo commands"
Assert-InOrder $clusterJob $expectedCargoCommands "Cluster cargo commands changed causal order"
Assert-InOrder $releaseVersionSource @(
    $expectedCargoCommands[0],
    'if ($LASTEXITCODE -ne 0) { throw "Cluster release-version unit contract failed" }'
) "Cluster release-version unit contract must fail immediately"
Assert-Contains $topologySource 'TOPOLOGY_CONVERGENCE_BLOCKER' "Topology failure must retain its own classification"
Assert-Contains $proxySource 'PROXY_COMPATIBILITY_BLOCKER: stop and revise the approved design; do not add app fallback code' "Only proxy compatibility failure may be a design blocker"
Assert-NotContains $topologySource 'PROXY_COMPATIBILITY_BLOCKER' "Topology failure must not be relabeled as proxy incompatibility"
Assert-True (([regex]::Matches($clusterJob, [regex]::Escape('PROXY_COMPATIBILITY_BLOCKER'))).Count -eq 1) "Only direct proxy compatibility may use PROXY_COMPATIBILITY_BLOCKER"
Assert-InOrder $topologySource @(
    $expectedCargoCommands[1],
    'if ($LASTEXITCODE -ne 0) { throw "TOPOLOGY_CONVERGENCE_BLOCKER: exact v1.1.6 two-peer topology did not converge" }',
    '"CLUSTER_TOPOLOGY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Topology GREEN receipt must follow only a successful topology gate"
Assert-InOrder $proxySource @(
    'if ($env:CLUSTER_TOPOLOGY_GREEN -ne "true") { throw "Topology GREEN receipt is required before compatibility" }',
    $expectedCargoCommands[2],
    'if ($LASTEXITCODE -ne 0) { throw "PROXY_COMPATIBILITY_BLOCKER: stop and revise the approved design; do not add app fallback code" }',
    '"CLUSTER_PROXY_COMPATIBILITY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Proxy compatibility must require topology GREEN and write its receipt only after add-pin-cat success"
Assert-InOrder $replicationSource @(
    'if ($env:CLUSTER_PROXY_COMPATIBILITY_GREEN -ne "true") { throw "Add-pin-cat proxy GREEN receipt is required before replication" }',
    'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before replication" }',
    $expectedCargoCommands[3]
) "Replication must require proxy GREEN before its receipt-owned check"
$stateConsumerBlocks = @(
    [pscustomobject]@{ Index = 7; CargoCommand = $expectedCargoCommands[3] },
    [pscustomobject]@{ Index = 10; CargoCommand = $expectedCargoCommands[4] },
    [pscustomobject]@{ Index = 13; CargoCommand = $expectedCargoCommands[5] }
)
foreach ($stateConsumerBlock in $stateConsumerBlocks) {
    Assert-Contains $pwshBlocks[$stateConsumerBlock.Index] 'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true")' "Every state-consuming Cluster test must require the owned receipt"
    Assert-InOrder $pwshBlocks[$stateConsumerBlock.Index] @(
        'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true")',
        $stateConsumerBlock.CargoCommand
    ) "Owned state receipt must be checked before its Cluster live test"
}
Assert-InOrder $clusterJob @(
    'logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway',
    'stop cluster-b kubo-b',
    'logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway',
    'start kubo-b cluster-b',
    'logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway',
    'down --volumes --remove-orphans'
) "Cluster diagnostics, peer stop/restart, and cleanup order changed"

$diagnosticSources = @($preStopDiagnosticSource, $preRestartDiagnosticSource, $finalDiagnosticSource)
Assert-True (([regex]::Matches($clusterJob, [regex]::Escape('logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway'))).Count -eq 3) "Cluster job must have exactly three sanitized diagnostics captures"
Assert-NotMatches $clusterJob '(?m)^\s*docker compose .*logs --no-color .*\|' "Cluster diagnostics must not stream raw logs through a pipeline"
foreach ($diagnosticSource in $diagnosticSources) {
    foreach ($fragment in @(
        'function Protect-ClusterDiagnosticLine',
        'function Write-SanitizedClusterDiagnostics',
        '$rawDiagnosticLines = @(',
        'docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway 2>&1',
        '$diagnosticExit = $LASTEXITCODE',
        '[Console]::Out.WriteLine((Protect-ClusterDiagnosticLine -Line "$rawLine"))',
        'if ($diagnosticExit -ne 0) { throw "$FailureMessage (exit=$diagnosticExit)" }'
    )) {
        Assert-Contains $diagnosticSource $fragment "Cluster diagnostics must sanitize before output: $fragment"
    }
    Assert-Matches $diagnosticSource '(?s)\$rawDiagnosticLines\s*=\s*@\(\s*docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway 2>&1\s*\)\s*\$diagnosticExit = \$LASTEXITCODE.*?\[Console\]::Out\.WriteLine\(\(Protect-ClusterDiagnosticLine -Line "\$rawLine"\)\).*?if \(\$diagnosticExit -ne 0\)' "Cluster diagnostics must capture, record exit, sanitize, then fail"
    Assert-NotMatches $diagnosticSource '(?m)^\s*(?:Write-Host|Write-Output|Write-Warning)\b' "Cluster diagnostics must not emit unredacted output"
}
Assert-Matches $clusterJob '(?m)^        if: \$\{\{ always\(\) && env\.CLUSTER_PINSET_ATTEMPTED == ''true'' \}\}\s*$' "Final Cluster diagnostics must run only after an attempted topology"
Assert-Matches $clusterJob '(?m)^        if: \$\{\{ always\(\) \}\}\s*$' "Cluster cleanup must always run"

$peerId = '12D3Koo' + (('a' * 40) -join '')
$peerCid = 'Qm' + (('a' * 44) -join '')
foreach ($fixture in @(
    "PeerID: $peerId",
    "peer_id=$peerId",
    "peer-id=$peerId",
    "route=/p2p/$peerId",
    "peer=$peerId",
    "identity=$peerCid"
)) {
    $sanitized = Protect-ClusterDiagnosticLine $fixture
    Assert-NotContains $sanitized $peerId "Cluster diagnostic redactor leaked a peer identity"
    Assert-NotContains $sanitized $peerCid "Cluster diagnostic redactor leaked a peer CID-shaped identity"
    Assert-Contains $sanitized "[REDACTED_PEER_ID]" "Cluster diagnostic redactor failed to mark a peer identity"
}
$contentCid = 'Qm' + (('b' * 44) -join '')
$contentFixture = "content cid=$contentCid retained"
Assert-True ((Protect-ClusterDiagnosticLine $contentFixture) -ceq $contentFixture) "Cluster diagnostic redactor must preserve ordinary content CIDs byte-for-byte"

foreach ($fragment in @(
    '$errors = [Collections.Generic.List[string]]::new()',
    'if ($env:CLUSTER_PINSET_OWNED -eq "true" -and $env:CLUSTER_PINSET_ATTEMPTED -eq "true")',
    'down --volumes --remove-orphans',
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'if ($env:CLUSTER_STATE_RECEIPT_OWNED -eq "true")',
    'Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -PathType Leaf',
    'Remove-Item -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -ErrorAction Stop',
    '$errors.Add("owned state receipt cleanup failed: $($_.Exception.Message)")',
    'if ($errors.Count -ne 0) { throw ($errors -join "; ") }'
)) {
    Assert-Contains $cleanupSource $fragment "Cluster cleanup contract is missing: $fragment"
}
Assert-InOrder $cleanupSource @(
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'if ($env:CLUSTER_STATE_RECEIPT_OWNED -eq "true")',
    'if (-not (Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -PathType Leaf))',
    'Remove-Item -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -ErrorAction Stop',
    '$errors.Add("owned state receipt cleanup failed: $($_.Exception.Message)")',
    'if ($errors.Count -ne 0) { throw ($errors -join "; ") }'
) "Cluster cleanup must aggregate failures only after all residual checks and owned receipt cleanup"

$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
$expectedClientRunLines = @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/cluster.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
Assert-True ($clientRunLines.Count -eq 5) "Client-smoke infrastructure must contain exactly five static commands"
for ($index = 0; $index -lt $expectedClientRunLines.Count; $index++) {
    Assert-True ($clientRunLines[$index].TrimEnd() -ceq $expectedClientRunLines[$index]) "Client static command $($index + 1) changed or moved"
}
Assert-NotMatches $clientJob '(?m)^        continue-on-error:' "Client static contract steps must be blocking"

$Readme = Read-NormalizedText (Join-Path $RepoRoot "README.md")
$Roadmap = Read-NormalizedText (Join-Path $RepoRoot "ROADMAP.md")
$clusterReadmeSections = [regex]::Matches(
    $Readme,
    '(?ms)^### IPFS Cluster pinset replication\r?\n(?<body>.*?)(?=^### |\z)'
)
if ($clusterReadmeSections.Count -ne 1) {
    throw "RED: README must contain exactly one '### IPFS Cluster pinset replication' section; found $($clusterReadmeSections.Count)"
}
$clusterReadme = $clusterReadmeSections[0].Groups["body"].Value
$normalizedClusterReadme = [regex]::Replace($clusterReadme, '\s+', ' ').Trim()

foreach ($fragment in @(
    "docker-compose.cluster.yml",
    "separate one-gateway profile",
    "not combined with horizontal scaling",
    "PostgreSQL 17",
    "Kubo A and Kubo B",
    "v0.43.0",
    "separate repositories",
    "Cluster A and Cluster B",
    "full CRDT",
    "v1.1.6",
    "separate identities",
    "gateway through the Cluster A proxy",
    "connector and proxy forwarder",
    "paired Kubo DNS endpoint",
    "min=max 2",
    "same-host Compose mDNS only",
    'fixed host loopback `127.0.0.1`',
    '`IPFS_S3_GATEWAY_BIND=127.0.0.1` is a required acknowledgement',
    "other values are rejected",
    "Direct non-loopback publication is unsupported",
    "separately secured TLS/auth reverse proxy",
    "out of scope and not shipped",
    "Local service health is insufficient",
    "identity-suppressed no-write exact-two-peer gate",
    "count/normalized v1.1.6",
    '`add(pin=false)` -> `pin/add` -> `cat`',
    "PUT followed by an immediate GET",
    "local A path only",
    "exact 2/2 allocations",
    'physical tracker states are `pinned`',
    "Kubo B reads only afterward",
    "gateway has no fallback",
    '`S3 DELETE` removes metadata and produces `HEAD 404`',
    "does not unpin",
    "allocation and Kubo B bytes remain",
    "stop/restart evidence",
    "loss and recovery of the two-pin state",
    "does not demonstrate high availability",
    "2/2 does not guarantee writes while degraded",
    "PostgreSQL, gateway, Cluster A, Kubo A, and the Docker host are single points",
    "does not provide PostgreSQL, Kubo, Cluster, gateway, or host high availability",
    "Production PostgreSQL, Kubo, Cluster REST, Cluster proxy, and swarm endpoints are internal",
    'validation alone exposes the Cluster A proxy at loopback `59103`',
    "TLS and authentication are not implemented",
    "Cluster secret protects Cluster membership",
    "not the private Kubo swarm",
    "Private swarm remains unchecked",
    'Hosted job: `NOT RUN`',
    '$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"',
    '$env:IPFS_S3_GATEWAY_PORT = "9000"',
    'COMPOSE_DISABLE_ENV_FILE = "1"',
    "cryptographically random URL-safe",
    "32-byte master key",
    "32-byte Cluster secret",
    "lowercase hex",
    "Store these generated secrets before the first write and restore them unchanged",
    "Changing the master key breaks encrypted objects",
    "changing the Cluster secret breaks membership",
    "docker compose -f docker-compose.cluster.yml config --quiet",
    "Cluster Compose configuration failed",
    "docker compose -f docker-compose.cluster.yml up --detach --build --wait --wait-timeout 300",
    "Cluster profile did not become healthy",
    "For production shutdown, use only the following command",
    "down --remove-orphans",
    'Never use the `--volumes` flag',
    "five named volumes are durable"
)) {
    Assert-Contains $normalizedClusterReadme $fragment "Cluster README contract is missing: $fragment"
}

foreach ($forbidden in @(
    "127.0.0.1:5001",
    "cross-wire",
    "down --volumes",
    "ipfs-cluster-ctl peers ls",
    "docker compose logs"
)) {
    Assert-NotContains $clusterReadme $forbidden "Cluster README must not contain: $forbidden"
}
Assert-NotMatches $clusterReadme '(?im)^.*(?:peer list|PeerID|peer_id|peer-id).*$' "Cluster README must not show peer-list commands or output"
Assert-NotMatches $clusterReadme '(?i)docker compose(?:\s+[^\r\n]+)?\s+logs\b' "Cluster README must not show raw Compose logs"
Assert-NotMatches $clusterReadme '(?i)0\.0\.0\.0[^\r\n]*59103|59103[^\r\n]*0\.0\.0\.0' "Cluster README must not show nonloopback proxy exposure"
Assert-NotMatches $clusterReadme '(?i)production[^\r\n]*(?:publish|expose)[^\r\n]*proxy|proxy[^\r\n]*(?:publish|expose)[^\r\n]*production' "Cluster README must not claim production proxy publication"
Assert-NotMatches $clusterReadme '(?i)\bdirect non-loopback(?: gateway)? (?:publication|access) (?:is )?supported\b' "Cluster README must not claim direct non-loopback publication support"
Assert-NotMatches $clusterReadme '(?i)hosted[^\r\n]*\bPASS\b|\bPASS\b[^\r\n]*hosted' "Cluster README must not claim hosted PASS"
Assert-NotMatches $clusterReadme '(?i)\bHA\b|multi-host' "Cluster README must not claim HA or multi-host operation"

Assert-Matches $Roadmap '(?m)^- \[x\] IPFS Cluster for pinset replication\s*$' "RED: ROADMAP Cluster checkbox must be checked"
Assert-Matches $Roadmap '(?m)^- \[ \] Private swarm \(swarm\.key\) for node-to-node communication\s*$' "ROADMAP Private swarm checkbox must remain unchecked"

Write-Host "cluster static contract tests: PASSED"
