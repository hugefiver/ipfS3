$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposePath = Join-Path $RepoRoot "docker-compose.cluster.yml"
$KuboDockerfilePath = Join-Path $RepoRoot "ipfs/cluster.Dockerfile"
$PrivateSwarmEntrypointPath = Join-Path $RepoRoot "ipfs/private-swarm-entrypoint.sh"
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
    $safe = [regex]::Replace($safe, '(Swarm key fingerprint: )[0-9a-f]{32}(?=\s*$)', '$1[redacted]')
    $safe = [regex]::Replace($safe, '(?m)^/key/swarm/psk/1\.0\.0/\s*$', '[REDACTED_SWARM_KEY_HEADER]')
    $safe = [regex]::Replace($safe, '(?m)^/base16/\s*$', '[REDACTED_SWARM_KEY_HEADER]')
    $safe = [regex]::Replace($safe, '(?<![0-9A-Fa-f])[0-9A-Fa-f]{64}(?![0-9A-Fa-f])', '[REDACTED_HEX]')
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
$PrivateSwarmEntrypoint = Read-NormalizedText $PrivateSwarmEntrypointPath
$Override = Read-NormalizedText $OverridePath

$ExpectedKuboDockerfile = @'
FROM ipfs/kubo:v0.43.0

COPY private-swarm-entrypoint.sh /private-swarm-entrypoint.sh
RUN chmod 0755 /private-swarm-entrypoint.sh

ENTRYPOINT ["/private-swarm-entrypoint.sh"]
'@
$ExpectedKuboDockerfile = $ExpectedKuboDockerfile.Replace("`r`n", "`n").TrimEnd("`n")
Assert-True ($KuboDockerfile.TrimEnd("`n") -ceq $ExpectedKuboDockerfile) "Cluster Kubo Dockerfile changed"

Assert-NotContains $Compose "`t" "Cluster Compose must use spaces"
Assert-Matches $Compose '(?m)^name: ipfs3-cluster$' "Cluster Compose project name changed"
Assert-NotMatches $Compose '(?m)^\s*<<:\s*' "Cluster Compose must not use YAML merge keys"
Assert-NotMatches $Compose '(?m)^\s*[A-Za-z0-9_-]+:\s*&' "Cluster Compose must not use YAML anchors"

$services = Get-YamlBlock $Compose "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedServices = @("postgres", "kubo-a", "kubo-b", "swarm-bootstrap", "cluster-a", "cluster-b", "gateway")
Assert-ExactSet $serviceNames $expectedServices "Cluster service set changed"

$volumes = Get-YamlBlock $Compose "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedVolumes = @("postgres_data", "kubo_a_data", "kubo_b_data", "cluster_a_data", "cluster_b_data")
Assert-ExactSet $volumeNames $expectedVolumes "Cluster volume set changed"

$postgres = Get-YamlBlock $services "postgres" 2
$kuboA = Get-YamlBlock $services "kubo-a" 2
$kuboB = Get-YamlBlock $services "kubo-b" 2
$swarmBootstrap = Get-YamlBlock $services "swarm-bootstrap" 2
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

$swarmSecrets = Get-YamlBlock $Compose "secrets" 0
$swarmKey = Get-YamlBlock $swarmSecrets "swarm_key" 2
Assert-True ($swarmKey.Trim() -ceq 'file: "${IPFS_S3_SWARM_KEY_FILE:?IPFS_S3_SWARM_KEY_FILE is required}"') "Private swarm key secret contract changed"
Assert-True (([regex]::Matches($swarmSecrets, '(?m)^  swarm_key:\s*$')).Count -eq 1) "Production Compose must define exactly one swarm_key secret"

foreach ($kubo in @($kuboA, $kuboB)) {
    $kuboEnvironment = Get-YamlBlock $kubo "environment" 4
    $kuboSecrets = Get-YamlBlock $kubo "secrets" 4
    Assert-True (([regex]::Matches($kuboEnvironment, '(?m)^      IPFS_SWARM_KEY_FILE: /run/secrets/swarm_key\s*$')).Count -eq 1) "Kubo must expose the approved private swarm key-path variable"
    Assert-NotContains $kuboEnvironment "IPFS_S3_SWARM_KEY_FILE" "Kubo environment must not expose the host secret-file interpolation variable"
    Assert-True (([regex]::Matches($kuboEnvironment, '(?m)^      LIBP2P_FORCE_PNET: "1"\s*$')).Count -eq 1) "Kubo must force private networking"
    Assert-True ($kuboSecrets.Trim() -ceq "- source: swarm_key`n        target: swarm_key") "Kubo long secret target must be the secret name"
}
Assert-True (([regex]::Matches($PrivateSwarmEntrypoint, '(?m)^        ipfs config Swarm\.AddrFilters --json ''\[\]'' >/dev/null 2>&1 &&\s*$')).Count -eq 1) "Private swarm entrypoint must clear AddrFilters exactly once"

foreach ($fragment in @(
    "    image: ipfs/kubo:v0.43.0",
    "      kubo-a:",
    "        condition: service_healthy",
    "      kubo-b:",
    "        condition: service_healthy",
    '    restart: "no"',
    "private swarm bootstrap failed",
    "ipfs --api /dns4/kubo-a/tcp/5001 id -f '<id>'",
    "ipfs --api /dns4/kubo-b/tcp/5001 id -f '<id>'",
    'swarm peering add "$$address"',
    'swarm connect "/dns4/kubo-b/tcp/4001/p2p/$$kubo_b_id"',
    'swarm connect "/dns4/kubo-a/tcp/4001/p2p/$$kubo_a_id"',
    "swarm peering ls",
    "swarm peers 2>/dev/null",
    'while [ "$$attempt" -le 60 ]',
    "sleep 1"
)) {
    Assert-Contains $swarmBootstrap $fragment "Private swarm bootstrap contract is missing: $fragment"
}
Assert-Contains $swarmBootstrap "      - /bin/sh" "Private swarm bootstrap must use POSIX sh"
Assert-Contains $swarmBootstrap "      - -ec" "Private swarm bootstrap must fail closed"
Assert-NotMatches $swarmBootstrap '(?m)^    (?:ports|volumes):\s*$' "Private swarm bootstrap must not publish ports or persist data"
Assert-Contains $swarmBootstrap 'ensure_peering() {' "Bootstrap must isolate idempotent peering setup"
Assert-Contains $swarmBootstrap 'peering=$$(ipfs --api "$$api" swarm peering ls 2>/dev/null) || return 1' "Bootstrap must capture existing peering before deciding whether to add"
Assert-Contains $swarmBootstrap 'peering=$$(printf ''%s\n'' "$$peering" | sed ''/^[[:space:]]*$$/d'') || return 1' "Bootstrap must discard only empty peering lines"
Assert-Contains $swarmBootstrap 'peering_matches() {' "Bootstrap must separately verify exact peering after connection"
Assert-Contains $swarmBootstrap '[ "$$peering" = "$$expected" ]' "Bootstrap must require exactly its two expected peering lines"
Assert-Contains $swarmBootstrap "tab=`$`$(printf '\t')" "Private swarm bootstrap must construct its exact tab indentation without YAML tabs"
Assert-Contains $swarmBootstrap 'expected=$$(printf ''%s\n%s'' "$$expected_id" "$$tab$$expected_transport")' "Peering output must be peer ID then one tab-indented transport address"
Assert-InOrder $swarmBootstrap @(
    "ensure_peering() {",
    'peering=$$(ipfs --api "$$api" swarm peering ls 2>/dev/null) || return 1',
    'if [ -z "$$peering" ]; then',
    'ipfs --api "$$api" swarm peering add "$$address" >/dev/null 2>&1',
    '[ "$$peering" = "$$expected" ]',
    'ensure_peering /dns4/kubo-a/tcp/5001 "/dns4/kubo-b/tcp/4001/p2p/$$kubo_b_id" "$$kubo_b_id" "/dns4/kubo-b/tcp/4001"',
    'ensure_peering /dns4/kubo-b/tcp/5001 "/dns4/kubo-a/tcp/4001/p2p/$$kubo_a_id" "$$kubo_a_id" "/dns4/kubo-a/tcp/4001"',
    'swarm connect "/dns4/kubo-b/tcp/4001/p2p/$$kubo_b_id"'
) "Bootstrap must verify existing peering before conditionally adding only a missing entry"
Assert-Matches $swarmBootstrap '(?s)ensure_peering\(\).*?if \[ -z "\$\$peering" \]; then\s*ipfs --api "\$\$api" swarm peering add "\$\$address" >/dev/null 2>&1 \|\| return 1\s*return 0\s*fi\s*\[ "\$\$peering" = "\$\$expected" \]' "Existing peering must avoid duplicate add commands and require exact two-line output"
Assert-Contains $swarmBootstrap 'sole_peer() {' "Bootstrap must isolate exact sole-peer verification"
Assert-Contains $swarmBootstrap 'peers=$$(ipfs --api "$$api" swarm peers 2>/dev/null) || return 1' "Bootstrap must capture full peer multiaddrs"
Assert-Contains $swarmBootstrap 'peers=$$(printf ''%s\n'' "$$peers" | sed ''/^[[:space:]]*$$/d'') || return 1' "Bootstrap must discard only empty peer lines"
Assert-Contains $swarmBootstrap '[ -n "$$peers" ] || return 1' "Bootstrap must reject an empty peer set"
Assert-Contains $swarmBootstrap 'peer_count=$$(printf ''%s\n'' "$$peers" | wc -l | tr -d ''[:space:]'') || return 1' "Bootstrap must count normalized peer lines"
Assert-Contains $swarmBootstrap '[ "$$peer_count" -eq 1 ] || return 1' "Bootstrap must require exactly one nonempty peer line"
Assert-Contains $swarmBootstrap 'line=$$(printf ''%s\n'' "$$peers" | sed -n ''1p'') || return 1' "Bootstrap must select its one peer multiaddr"
Assert-Contains $swarmBootstrap 'case "$$line" in' "Bootstrap must match a full peer multiaddr"
Assert-Contains $swarmBootstrap '*/p2p/"$$expected_id") return 0 ;;' "Bootstrap must accept only the expected peer ID at a multiaddr suffix"
Assert-Contains $swarmBootstrap 'sole_peer /dns4/kubo-a/tcp/5001 "$$kubo_b_id"' "Bootstrap must verify Kubo A has only Kubo B"
Assert-Contains $swarmBootstrap 'sole_peer /dns4/kubo-b/tcp/5001 "$$kubo_a_id"' "Bootstrap must verify Kubo B has only Kubo A"
Assert-NotContains $swarmBootstrap "swarm peers -q" "Bootstrap must not use the unsupported swarm peers -q form"
Assert-NotMatches $swarmBootstrap '\[ "\$\$peers_[ab]" = "\$\$kubo_[ab]_id" \]' "Bootstrap must not compare a peer transport line as a bare peer ID"
Assert-NotMatches $swarmBootstrap '(?im)^\s*(?:echo|printf)\b(?![^\r\n]*private swarm bootstrap failed)' "Bootstrap must not emit CLI-derived data"

foreach ($cluster in @($clusterA, $clusterB)) {
    Assert-Contains $cluster "      swarm-bootstrap:`n        condition: service_completed_successfully" "Cluster must wait for successful private swarm bootstrap"
}

foreach ($fragment in @(
    "#!/bin/sh",
    "private swarm startup rejected",
    '"${LIBP2P_FORCE_PNET-}" = "1"',
    'case "${IPFS_SWARM_KEY_FILE-}" in',
    '/run/secrets/*)',
    'wc -c < "$swarm_key_path"',
    'wc -l < "$swarm_key_path"',
    '-eq 96',
    '-eq 3',
    'first_line" = "/key/swarm/psk/1.0.0/"',
    'second_line" = "/base16/"',
    '[!0123456789abcdef]',
    '-eq 64',
    'ipfs init --empty-repo --profile=server',
    'ipfs config Addresses.API "/ip4/0.0.0.0/tcp/5001"',
    'ipfs config Addresses.Gateway "/ip4/0.0.0.0/tcp/8080"',
    'ipfs config Addresses.Swarm --json ''["/ip4/0.0.0.0/tcp/4001"]''',
    'ipfs config Swarm.AddrFilters --json ''[]''',
    "ipfs config Bootstrap --json '[]'",
    'ipfs config Routing.Type none',
    'ipfs config AutoConf.Enabled --json false',
    'ipfs config Discovery.MDNS.Enabled --json false',
    'ipfs config Gateway.NoDNSLink --json true',
    'ipfs config Gateway.HTTPHeaders.Cache-Control --json ''["public, max-age=29030400, immutable"]''',
    'ipfs config Datastore.BloomFilterSize --json 0',
    'cp "$IPFS_SWARM_KEY_FILE" "$IPFS_PATH/swarm.key"',
    'chmod 0400 "$IPFS_PATH/swarm.key"',
    'ipfs daemon --migrate=true --enable-gc=false',
    'mkfifo "$fifo_path"',
    'redact_swarm_fingerprint < "$fifo_path" &',
    'trap ''forward_term'' TERM',
    'trap ''forward_int'' INT',
    'trap ''forward_hup'' HUP',
    'wait_interrupted=0',
    'set +e',
    'rm -f "$fifo_path"',
    'basename "$0"'
)) {
    Assert-Contains $PrivateSwarmEntrypoint $fragment "Private swarm entrypoint contract is missing: $fragment"
}
foreach ($legacyTypedConfigCommand in @(
    'ipfs config AutoConf.Enabled false',
    'ipfs config Discovery.MDNS.Enabled false',
    'ipfs config Gateway.HTTPHeaders.Cache-Control "public, max-age=29030400, immutable"',
    'ipfs config Datastore.BloomFilterSize 0'
)) {
    Assert-NotContains $PrivateSwarmEntrypoint $legacyTypedConfigCommand "Private swarm entrypoint must use --json for the Kubo v0.43 typed setting: $legacyTypedConfigCommand"
}
Assert-True (([regex]::Matches($PrivateSwarmEntrypoint, '(?m)^        ipfs config AutoConf\.Enabled --json false >/dev/null 2>&1 &&\s*$')).Count -eq 1) "Private swarm entrypoint must apply AutoConf exactly once"
Assert-Matches $PrivateSwarmEntrypoint '(?s)ipfs config Routing\.Type none >/dev/null 2>&1 &&\s*ipfs config AutoConf\.Enabled --json false >/dev/null 2>&1 &&\s*ipfs config Discovery\.MDNS\.Enabled --json false >/dev/null 2>&1 &&' "AutoConf must be fail-closed in the private swarm config chain between Routing and mDNS"
Assert-True (([regex]::Matches($PrivateSwarmEntrypoint, '(?m)^        ipfs config Swarm\.AddrFilters --json ''\[\]'' >/dev/null 2>&1 &&\s*$')).Count -eq 1) "Private swarm entrypoint must clear AddrFilters exactly once"
$addrFilterCommands = @([regex]::Matches($PrivateSwarmEntrypoint, '(?m)^\s*ipfs config Swarm\.AddrFilters[^\r\n]*$') | ForEach-Object { $_.Value.Trim() })
Assert-ExactSet $addrFilterCommands @("ipfs config Swarm.AddrFilters --json '[]' >/dev/null 2>&1 &&") "AddrFilters must use only the exact empty JSON list command"
Assert-Matches $PrivateSwarmEntrypoint '(?s)ipfs config Addresses\.Swarm --json ''\["/ip4/0\.0\.0\.0/tcp/4001"\]'' >/dev/null 2>&1 &&\s*ipfs config Swarm\.AddrFilters --json ''\[\]'' >/dev/null 2>&1 &&\s*ipfs config Bootstrap --json ''\[\]'' >/dev/null 2>&1 &&' "AddrFilters must be fail-closed in the private swarm config chain"
Assert-NotMatches $PrivateSwarmEntrypoint '(?s)redact_swarm_fingerprint\(\).*?\bsed\b' "Fingerprint filter must not use buffered sed"
Assert-InOrder $PrivateSwarmEntrypoint @(
    "redact_swarm_fingerprint() {",
    'while IFS= read -r log_line || [ -n "$log_line" ]; do',
    'case "$log_line" in',
    'fingerprint=${log_line#Swarm key fingerprint: }',
    '[ "${#fingerprint}" -eq 32 ]',
    'case "$fingerprint" in',
    "Swarm key fingerprint: [redacted]",
    'continue',
    'printf ''%s\n'' "$log_line"',
    'done'
) "Fingerprint filter must process each line immediately and redact only the exact marker"
Assert-Matches $PrivateSwarmEntrypoint '(?m)^\s*(?:\[0-9a-f\]){32}\)\s*$' "Fingerprint filter must validate exactly 32 lowercase hexadecimal characters"
Assert-NotMatches $PrivateSwarmEntrypoint '(?i)(?:sha256|digest|mask|redact).*\*' "Entrypoint must not add a broad fingerprint or digest masker"
Assert-NotContains $PrivateSwarmEntrypoint "kill -0" "Entrypoint must not use kill -0 as a wait retry criterion"
Assert-NotContains $PrivateSwarmEntrypoint "exec ipfs daemon" "Entrypoint must retain PID 1 supervision instead of execing the daemon"
Assert-NotContains $PrivateSwarmEntrypoint "IPFS_S3_SWARM_KEY_FILE" "Entrypoint must not read the host secret-file interpolation variable"
Assert-Matches $PrivateSwarmEntrypoint '(?s)wait_interrupted=0\s*set \+e\s*wait "\$child_pid"\s*child_status=\$\?\s*if \[ "\$wait_interrupted" -eq 1 \] && \[ "\$child_status" -gt 128 \]; then\s*continue\s*fi' "Daemon wait must retry only a signal-interrupted wait with an over-128 status"
Assert-Matches $PrivateSwarmEntrypoint '(?s)select_supervisor_exit\(\).*?if \[ "\$daemon_status" -ne 0 \]; then.*?return "\$daemon_status".*?if \[ "\$filter_status" -ne 0 \] \|\| \[ "\$fifo_cleanup_status" -ne 0 \]; then.*?return 1' "Daemon failure must take precedence and clean daemon exits must reject filter or FIFO failures"
Assert-Matches $PrivateSwarmEntrypoint '(?s)wait_for_pid "\$daemon_pid".*?wait_for_pid "\$filter_pid".*?cleanup_fifo.*?select_supervisor_exit' "Entrypoint must reap both children, clean up the FIFO, then select its final status"
Assert-InOrder $PrivateSwarmEntrypoint @(
    'wait_for_pid "$daemon_pid"',
    'daemon_status=$?',
    'daemon_pid=',
    'wait_for_pid "$filter_pid"',
    'filter_status=$?',
    'filter_pid=',
    'cleanup_fifo',
    'fifo_cleanup_status=$?',
    'select_supervisor_exit "$daemon_status" "$filter_status" "$fifo_cleanup_status"'
) "Supervisor must clear each reaped PID and include FIFO cleanup status in final selection"
Assert-Matches $PrivateSwarmEntrypoint '(?s)cleanup_fifo\(\).*?rm -f "\$fifo_path".*?fifo_cleanup_status=\$\?.*?if \[ "\$fifo_cleanup_status" -eq 0 \]; then\s*fifo_path=.*?fi.*?return "\$fifo_cleanup_status"' "FIFO cleanup must retain a failed path and return its actual status"
$cleanupRuntimeSource = (Get-BracedBlock -Text $PrivateSwarmEntrypoint -HeaderPattern '(?m)^cleanup_runtime\(\) \{' -Label 'runtime cleanup').Body
Assert-InOrder $cleanupRuntimeSource @(
    'trap - 0 TERM INT HUP',
    'kill -TERM "$daemon_pid"',
    'wait_for_pid "$daemon_pid"',
    'daemon_pid=',
    'wait_for_pid "$filter_pid"',
    'filter_pid=',
    'cleanup_fifo'
) "EXIT cleanup must terminate and reap the daemon before reaping the FIFO filter and removing the FIFO"
Assert-Matches $cleanupRuntimeSource '(?s)if \[ -n "\$filter_pid" \]; then\s*wait_for_pid "\$filter_pid"' "EXIT cleanup must reap the FIFO filter only after daemon closure can deliver FIFO EOF"
Assert-NotMatches $cleanupRuntimeSource 'kill\s+-[A-Z]+\s+"\$filter_pid"' "EXIT cleanup must not signal the FIFO filter"
foreach ($signalHandler in @(
    [pscustomobject]@{ Name = 'forward_term'; Signal = 'TERM' },
    [pscustomobject]@{ Name = 'forward_int'; Signal = 'INT' },
    [pscustomobject]@{ Name = 'forward_hup'; Signal = 'HUP' }
)) {
    $handlerSource = (Get-BracedBlock -Text $PrivateSwarmEntrypoint -HeaderPattern ("(?m)^$($signalHandler.Name)\(\) \{") -Label $signalHandler.Name).Body
    Assert-Matches $handlerSource ('(?s)wait_interrupted=1.*?kill -' + $signalHandler.Signal + ' "\$daemon_pid"') "$($signalHandler.Name) must mark an interrupted wait before forwarding to Kubo"
    Assert-NotMatches $handlerSource 'kill\s+-[A-Z]+\s+"\$filter_pid"' "$($signalHandler.Name) must never signal the FIFO filter"
}
Assert-Contains $PrivateSwarmEntrypoint "trap 'cleanup_runtime' 0" "Entrypoint must install full abnormal runtime cleanup"
Assert-InOrder $PrivateSwarmEntrypoint @(
    'export IPFS_PATH',
    'umask 077',
    'rm -f "$IPFS_PATH/swarm.key"',
    'cp "$IPFS_SWARM_KEY_FILE" "$IPFS_PATH/swarm.key"',
    'chmod 0400 "$IPFS_PATH/swarm.key"'
) "Private swarm key installation must remove a persisted target before securely copying the secret"
Assert-Matches $PrivateSwarmEntrypoint '(?s)supervise_daemon\s*runtime_status=\$\?.*?if \[ "\$daemon_started" -eq 0 \] && \[ "\$runtime_status" -ne 0 \]; then\s*private_swarm_failure' "Pre-daemon FIFO setup failure must emit the fixed startup rejection"
Assert-Matches $PrivateSwarmEntrypoint '(?m)^if \[ "\$\(basename "\$0"\)" = "private-swarm-entrypoint\.sh" \]; then\s*$\r?\n^    main "\$@"\s*$\r?\n^fi\s*$' "Entrypoint main must run only when invoked under its entrypoint basename"

$temporaryKeyPath = [IO.Path]::GetTempFileName()
try {
    $validPrivateSwarmKey = "/key/swarm/psk/1.0.0/`n/base16/`n" + ("a" * 64) + "`n"
    [IO.File]::WriteAllText($temporaryKeyPath, $validPrivateSwarmKey, [Text.Encoding]::ASCII)
    $shPath = (Get-Command sh -ErrorAction Stop).Source
    & $shPath -c '. "$1"; validate_swarm_key "$2"' sh $PrivateSwarmEntrypointPath $temporaryKeyPath
    Assert-True ($LASTEXITCODE -eq 0) "Source-only private swarm key fixture rejected the exact valid key"

    $invalidPrivateSwarmKey = $validPrivateSwarmKey.Replace(("a" * 64), ("A" + ("a" * 63)))
    [IO.File]::WriteAllText($temporaryKeyPath, $invalidPrivateSwarmKey, [Text.Encoding]::ASCII)
    & $shPath -c '. "$1"; validate_swarm_key "$2"' sh $PrivateSwarmEntrypointPath $temporaryKeyPath
    Assert-True ($LASTEXITCODE -ne 0) "Source-only private swarm key fixture accepted uppercase hex"

    $sourceOnlyCid = 'Qm' + ('c' * 44)
    $filteredLines = @(& $shPath -c '. "$1"; printf "%s\n" "Swarm key fingerprint: 0123456789abcdef0123456789abcdef" "ordinary 0123456789abcdef0123456789abcdef" "cid=$2" "Swarm key fingerprint: 0123456789abcdef0123456789abcdeF" | redact_swarm_fingerprint' sh $PrivateSwarmEntrypointPath $sourceOnlyCid)
    Assert-True (($filteredLines -join "`n") -ceq "Swarm key fingerprint: [redacted]`nordinary 0123456789abcdef0123456789abcdef`ncid=$sourceOnlyCid`nSwarm key fingerprint: 0123456789abcdef0123456789abcdeF") "Source-only fingerprint fixture must redact only the exact prefix while preserving ordinary 32-hex and content CIDs"

    foreach ($selection in @(
        [pscustomobject]@{ Daemon = 37; Filter = 0; Fifo = 0; Expected = 37 },
        [pscustomobject]@{ Daemon = 43; Filter = 0; Fifo = 0; Expected = 43 },
        [pscustomobject]@{ Daemon = 143; Filter = 0; Fifo = 0; Expected = 143 },
        [pscustomobject]@{ Daemon = 0; Filter = 1; Fifo = 0; Expected = 1 },
        [pscustomobject]@{ Daemon = 0; Filter = 0; Fifo = 1; Expected = 1 }
    )) {
        & $shPath -c '. "$1"; select_supervisor_exit "$2" "$3" "$4"' sh $PrivateSwarmEntrypointPath $selection.Daemon $selection.Filter $selection.Fifo
        Assert-True ($LASTEXITCODE -eq $selection.Expected) "Source-only supervisor selection returned an incorrect status"
    }
} finally {
    Remove-Item -LiteralPath $temporaryKeyPath -Force -ErrorAction SilentlyContinue
}

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
    "IPFS_S3_GATEWAY_PORT",
    "IPFS_S3_SWARM_KEY_FILE"
)) {
    Assert-Contains $Compose ('${' + $required + ':?') "Required interpolation missing: $required"
    Assert-NotContains $Compose ('${' + $required + ':-') "Required interpolation gained a default: $required"
    Assert-NotContains $Compose ('${' + $required + '-default') "Required interpolation gained an alternate default: $required"
}
Assert-NotMatches $Compose '(?i)\$\{(?:POSTGRES_PASSWORD|IPFS_S3_ACCESS_KEY_ID|IPFS_S3_SECRET_ACCESS_KEY|IPFS_S3_MASTER_KEY|IPFS_S3_CLUSTER_SECRET|IPFS_S3_GATEWAY_BIND|IPFS_S3_GATEWAY_PORT|IPFS_S3_SWARM_KEY_FILE)(?::-[^}]*)?\}' "Production secrets must not have development fallbacks"
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
    "remote"
)) {
    Assert-NotContains $Compose $forbidden "Forbidden Cluster production fragment: $forbidden"
}

$validationServices = Get-YamlBlock $Override "services" 0
$validationServiceNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):(?:\s*\{\})?\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedValidationServices = @($expectedServices + "kubo-c")
Assert-ExactSet $validationServiceNames $expectedValidationServices "Validation override service set changed"
$expectedMappings = @(
    "127.0.0.1:55435:5432",
    "127.0.0.1:55100:5001",
    "127.0.0.1:55101:5001",
    "127.0.0.1:55102:5001",
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
    "kubo-c" = @("127.0.0.1:55102:5001")
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
$wrongSwarmKey = Get-YamlBlock $Override "swarm_key_wrong" 2
Assert-True ($wrongSwarmKey.Trim() -ceq 'file: "${IPFS_S3_SWARM_KEY_WRONG_FILE:?IPFS_S3_SWARM_KEY_WRONG_FILE is required}"') "Validation wrong swarm key secret contract changed"
$kuboC = Get-YamlBlock $validationServices "kubo-c" 2
foreach ($fragment in @(
    '    profiles: ["private-swarm-validation"]',
    "    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0",
    "      - /data/ipfs",
    "      IPFS_PATH: /data/ipfs",
    "      IPFS_SWARM_KEY_FILE: /run/secrets/swarm_key_wrong",
    '      LIBP2P_FORCE_PNET: "1"',
    '      test: ["CMD", "ipfs", "id"]',
    "    restart: unless-stopped"
)) {
    Assert-Contains $kuboC $fragment "Validation Kubo C contract is missing: $fragment"
}
$kuboCSecrets = Get-YamlBlock $kuboC "secrets" 4
Assert-NotContains (Get-YamlBlock $kuboC "environment" 4) "IPFS_S3_SWARM_KEY_FILE" "Validation Kubo C must not expose the host secret-file interpolation variable"
Assert-True ($kuboCSecrets.Trim() -ceq "- source: swarm_key_wrong`n        target: swarm_key_wrong") "Validation Kubo C long secret target must be the wrong secret name"
Assert-NotMatches $kuboC '(?m)^    (?:build|volumes):\s*$' "Validation Kubo C must use the already-built image and a tmpfs repository"

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
Assert-True (([regex]::Matches($Rust, '(?m)^#\[tokio::test\]\s*$')).Count -eq 7) "Cluster target must define exactly seven Tokio tests"

foreach ($testName in @(
    "private_swarm_configuration_and_peering",
    "private_swarm_wrong_key_rejected",
    "cluster_topology_converges",
    "cluster_proxy_compatibility",
    "cluster_replication_and_retention",
    "cluster_peer_b_outage_contract",
    "cluster_peer_b_restart_recovery"
)) {
    Assert-True (([regex]::Matches($Rust, "(?m)^async fn $testName\(\) \{")).Count -eq 1) "Expected one live test named $testName"
}
Assert-InOrder $Rust @(
    "async fn private_swarm_configuration_and_peering()",
    "async fn private_swarm_wrong_key_rejected()",
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
    "IPFS_S3_CLUSTER_KUBO_C_URL",
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
    "pub struct KuboApiClient",
    "pub fn new(endpoint: &str) -> Result<Self>",
    "pub async fn config_probe",
    "pub async fn identity_probe",
    "pub async fn swarm_peers_probe",
    "pub async fn peering_probe",
    "pub async fn connect_probe",
    "pub struct KuboConfig",
    "pub struct KuboIdentity",
    "pub struct KuboSwarmPeers",
    "pub struct KuboPeeringPeers",
    "pub enum ConnectObservation",
    "pub struct PrivateSwarmEvidence",
    "pub async fn wait_for_private_swarm",
    "pub async fn prove_wrong_key_rejection",
    "fn validate_private_kubo_config",
    "fn validate_private_peering",
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
Assert-True (([regex]::Matches($RustSupport, '(?m)^#\[test\]\s*$')).Count -eq 4) "Cluster support must define exactly four deterministic plain unit tests"
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

$privateKuboConfigTest = Get-BracedBlock $RustSupport '(?m)^fn private_kubo_config_contract_rejects_open_discovery\(\) \{' 'private Kubo configuration unit test'
foreach ($fixture in @(
    '"Bootstrap": []',
    '"Type": "none"',
    '"Enabled": false',
    '"/ip4/0.0.0.0/tcp/4001"',
    '"Bootstrap": ["bootstrap"]',
    '"Type": "dht"',
    '"Enabled": true',
    '"/ip6/::/tcp/4001"'
)) {
    Assert-Contains $privateKuboConfigTest.Body $fixture "Private Kubo configuration fixture is missing: $fixture"
}
Assert-Contains $privateKuboConfigTest.Body "private_kubo_config_contract_invalid" "Private Kubo configuration must use a fixed terminal category"
foreach ($dtoDefaultContract in @(
    [pscustomobject]@{ Header = '(?m)^pub struct KuboConfig \{'; Label = 'KuboConfig'; Attribute = 'Bootstrap'; Field = 'bootstrap'; Type = 'String' },
    [pscustomobject]@{ Header = '(?m)^struct KuboAddresses \{'; Label = 'KuboAddresses'; Attribute = 'Swarm'; Field = 'swarm'; Type = 'String' },
    [pscustomobject]@{ Header = '(?m)^pub struct KuboPeeringPeers \{'; Label = 'KuboPeeringPeers'; Attribute = 'Peers'; Field = 'peers'; Type = 'KuboPeeringPeer' },
    [pscustomobject]@{ Header = '(?m)^struct KuboPeeringPeer \{'; Label = 'KuboPeeringPeer'; Attribute = 'Addrs'; Field = 'addrs'; Type = 'String' }
)) {
    $dtoBlock = Get-BracedBlock $RustSupport $dtoDefaultContract.Header $dtoDefaultContract.Label
    $attributePattern = '(?m)^\s*#\[serde\(rename = "' + $dtoDefaultContract.Attribute + '", default\)\]\s*$\r?\n^\s*' + $dtoDefaultContract.Field + ': Vec<' + $dtoDefaultContract.Type + '>,'
    Assert-Matches $dtoBlock.Body $attributePattern "$($dtoDefaultContract.Label).$($dtoDefaultContract.Field) must default only when the field is absent"
}
foreach ($fixture in @("missing_bootstrap", "missing_swarm")) {
    Assert-Contains $privateKuboConfigTest.Body $fixture "Private Kubo configuration missing-field fixture is missing: $fixture"
}
$kuboConfigDto = Get-BracedBlock $RustSupport '(?m)^pub struct KuboConfig \{' 'KuboConfig DTO'
Assert-Matches $kuboConfigDto.Body '(?m)^\s*#\[serde\(rename = "AutoConf"\)\]\s*$\r?\n^\s*auto_conf: KuboAutoConf,' "KuboConfig must require the exact-cased AutoConf DTO"
$kuboAutoConfDto = Get-BracedBlock $RustSupport '(?m)^struct KuboAutoConf \{' 'KuboAutoConf DTO'
Assert-Matches $kuboAutoConfDto.Body '(?m)^\s*#\[serde\(rename = "Enabled"\)\]\s*$\r?\n^\s*enabled: bool,' "KuboAutoConf must require the exact-cased Enabled boolean"
Assert-NotMatches $kuboAutoConfDto.Body '#\[serde\([^\]]*default' "KuboAutoConf must not default a missing runtime blocker"
Assert-Contains $RustSupport "!config.auto_conf.enabled" "Private Kubo validation must reject enabled AutoConf"
Assert-InOrder $RustSupport @(
    "validate_private_kubo_config(&a_config)?;",
    "let Some(evidence) = private_swarm_identities"
) "Private Kubo configuration must be validated before peer topology"
foreach ($fixture in @("auto_conf_enabled", "missing_auto_conf", "missing_auto_conf_enabled")) {
    Assert-Contains $privateKuboConfigTest.Body $fixture "AutoConf fixture is missing: $fixture"
}
Assert-Contains $privateKuboConfigTest.Body '"AutoConf": { "Enabled": false }' "Closed Kubo fixture must disable AutoConf"
Assert-Contains $privateKuboConfigTest.Body '"AutoConf": { "Enabled": true }' "Enabled AutoConf fixture is missing"
Assert-Matches $privateKuboConfigTest.Body 'missing_auto_conf[\s\S]*?is_err\(\)' "Missing AutoConf must fail deserialization"
Assert-Matches $privateKuboConfigTest.Body 'missing_auto_conf_enabled[\s\S]*?is_err\(\)' "Missing AutoConf.Enabled must fail deserialization"
Assert-Matches $kuboConfigDto.Body '(?m)^\s*#\[serde\(rename = "Swarm"\)\]\s*$\r?\n^\s*swarm: KuboSwarmConfig,' "KuboConfig must require the exact-cased Swarm configuration DTO"
$kuboSwarmConfigDto = Get-BracedBlock $RustSupport '(?m)^struct KuboSwarmConfig \{' 'KuboSwarmConfig DTO'
Assert-Matches $kuboSwarmConfigDto.Body '(?m)^\s*#\[serde\(rename = "AddrFilters"\)\]\s*$\r?\n^\s*addr_filters: Vec<String>,' "KuboSwarmConfig must require the exact-cased AddrFilters list"
Assert-NotMatches $kuboSwarmConfigDto.Body '#\[serde\([^\]]*default' "KuboSwarmConfig must not default missing AddrFilters"
Assert-Contains $RustSupport "config.swarm.addr_filters.is_empty()" "Private Kubo validation must reject nonempty Swarm AddrFilters"
foreach ($fixture in @("addr_filters_open", "missing_swarm_config", "missing_addr_filters")) {
    Assert-Contains $privateKuboConfigTest.Body $fixture "Swarm AddrFilters fixture is missing: $fixture"
}
Assert-Contains $privateKuboConfigTest.Body '"Swarm": { "AddrFilters": [] }' "Closed Kubo fixture must clear Swarm AddrFilters"
Assert-Contains $privateKuboConfigTest.Body '"/ip4/172.16.0.0/ipcidr/12"' "Nonempty Swarm AddrFilters fixture is missing"
Assert-Matches $privateKuboConfigTest.Body 'missing_swarm_config[\s\S]*?is_err\(\)' "Missing Swarm must fail deserialization"
Assert-Matches $privateKuboConfigTest.Body 'missing_addr_filters[\s\S]*?is_err\(\)' "Missing Swarm.AddrFilters must fail deserialization"

$privatePeeringTest = Get-BracedBlock $RustSupport '(?m)^fn private_peering_json_contract_matches_kubo_v0_43_addrinfo\(\) \{' 'private peering JSON unit test'
foreach ($fixture in @(
    '"Peers"',
    '"ID"',
    '"Addrs"',
    'extra-peer',
    'extra-address',
    'wrong-id',
    'wrong-address',
    'private_kubo_peering_contract_invalid'
)) {
    Assert-Contains $privatePeeringTest.Body $fixture "Private Kubo peering fixture is missing: $fixture"
}
Assert-NotContains $privatePeeringTest.Body "/p2p/" "Peering AddrInfo fixtures must not include a peer suffix"
foreach ($fixture in @(
    "empty_swarm_peers",
    "missing_swarm_peers",
    "empty_peering_peers",
    "missing_peering_peers",
    "missing_addrs"
)) {
    Assert-Contains $privatePeeringTest.Body $fixture "Private Kubo peer missing-field fixture is missing: $fixture"
}
$kuboSwarmPeersDto = Get-BracedBlock $RustSupport '(?m)^pub struct KuboSwarmPeers \{' 'KuboSwarmPeers DTO'
Assert-Matches $kuboSwarmPeersDto.Body '(?ms)^\s*#\[serde\(\s*rename = "Peers",\s*default,\s*deserialize_with = "deserialize_null_vec_as_empty"\s*\)\]\s*^\s*peers: Vec<KuboSwarmPeer>,' "Kubo swarm peers must map only null Peers to an empty list"
$kuboPeeringPeersDto = Get-BracedBlock $RustSupport '(?m)^pub struct KuboPeeringPeers \{' 'KuboPeeringPeers DTO'
Assert-Matches $kuboPeeringPeersDto.Body '(?m)^\s*#\[serde\(rename = "Peers", default\)\]\s*$\r?\n^\s*peers: Vec<KuboPeeringPeer>,' "Kubo peering peers must retain its ordinary missing-list behavior"
Assert-NotContains $kuboPeeringPeersDto.Body "deserialize_with" "Kubo peering peers must not accept null"
Assert-True (([regex]::Matches($RustSupport, '#\[serde\([^\]]*deserialize_with\s*=')).Count -eq 1) "Only Kubo swarm peers may use a custom null-list deserializer"
Assert-Matches $RustSupport 'fn deserialize_null_vec_as_empty<''de, D, T>\(\s*deserializer: D,?\s*\) -> std::result::Result<Vec<T>, D::Error>' "Null-list deserializer signature changed"
Assert-Matches $RustSupport 'Option::<Vec<T>>::deserialize\(deserializer\)\.map\(Option::unwrap_or_default\)' "Null-list deserializer must preserve arrays and default only null"
$nullSwarmPeersTest = Get-BracedBlock $RustSupport '(?m)^fn kubo_swarm_peers_null_is_empty_without_ndjson\(\) \{' 'null swarm peers unit test'
foreach ($fragment in @(
    '"Peers": null',
    '"Peers": [{ "Peer": "peer-a" }]',
    '"Peers": {}',
    'wrapped.peers.len() == 1',
    'wrapped.peers[0].peer == "peer-a"',
    'is_err()'
)) {
    Assert-Contains $nullSwarmPeersTest.Body $fragment "Null swarm peers fixture is missing: $fragment"
}
Assert-NotMatches $nullSwarmPeersTest.Body '(?i)(?:ndjson|\.lines\(\)|line splitting)' "Null swarm peers test must not use an NDJSON decoder"

$kuboApiClientStart = $RustSupport.IndexOf("impl KuboApiClient {", [StringComparison]::Ordinal)
$kuboApiClientEnd = $RustSupport.IndexOf("#[derive(Debug)]`nenum PeerView", [StringComparison]::Ordinal)
Assert-True ($kuboApiClientStart -ge 0 -and $kuboApiClientEnd -gt $kuboApiClientStart) "Kubo API client implementation boundaries are missing"
$kuboApiClientImpl = $RustSupport.Substring($kuboApiClientStart, $kuboApiClientEnd - $kuboApiClientStart)
foreach ($path in @(
    '"/api/v0/config/show"',
    '"/api/v0/id"',
    '"/api/v0/swarm/peers"',
    '"/api/v0/swarm/peering/ls"',
    '"/api/v0/swarm/connect"'
)) {
    Assert-Contains $kuboApiClientImpl $path "Kubo API client path is missing: $path"
}
Assert-Contains $kuboApiClientImpl ".post(" "Kubo API client must use POST"
Assert-NotContains $kuboApiClientImpl ".get(" "Kubo API client must not use GET"
Assert-Contains $kuboApiClientImpl 'query_pairs_mut().append_pair("arg"' "Kubo connect must encode its argument as a URL query pair"
Assert-Matches $kuboApiClientImpl 'connect_timeout\(Duration::from_secs\(5\)\)[\s\S]*?timeout\(CALL_TIMEOUT\)' "Kubo API client must bound loopback connect and call time"
Assert-NotMatches $kuboApiClientImpl '(?m)(?:anyhow!|ProbeError::(?:Transient|Terminal))\([^\r\n]*(?:\{|format!)' "Kubo API errors must use fixed categories only"
Assert-NotMatches $RustSupport '(?m)^#\[derive\(Debug[^\r\n]*\)\]\s*$\r?\n^pub struct KuboIdentity' "Kubo identity DTO must not derive Debug"
Assert-NotMatches $RustSupport '(?m)^#\[derive\([^\r\n]*Serialize[^\r\n]*\)\]\s*$\r?\n^pub struct KuboIdentity' "Kubo identity DTO must not derive Serialize"

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

foreach ($privateSwarmTestName in @("private_swarm_configuration_and_peering", "private_swarm_wrong_key_rejected")) {
    $privateSwarmTest = Get-BracedBlock $Rust ("(?m)^async fn " + $privateSwarmTestName + "\(\) \{") $privateSwarmTestName
    foreach ($forbiddenPrivateSwarmFragment in @(
        "std::fs",
        "std::process",
        "Command::new",
        "docker",
        "state_path",
        "RecoveryState",
        "create_bucket",
        "kubo_cat"
    )) {
        Assert-NotContains $privateSwarmTest.Body $forbiddenPrivateSwarmFragment "Private swarm live test must not use: $forbiddenPrivateSwarmFragment"
    }
    Assert-NotMatches $privateSwarmTest.Body '(?i)(?:panic!|assert!|println!|eprintln!)\([^\r\n]*(?:\{(?:id|peer|identity)|/p2p/)' "Private swarm live test must not emit identities or peer addresses"
}
$privateSwarmConfigurationTest = Get-BracedBlock $Rust '(?m)^async fn private_swarm_configuration_and_peering\(\) \{' 'private swarm configuration test'
Assert-Contains $privateSwarmConfigurationTest.Body 'println!("private_swarm_peers=2 wrong_key_peers=0");' "Private swarm configuration test output changed"
$privateSwarmWrongKeyTest = Get-BracedBlock $Rust '(?m)^async fn private_swarm_wrong_key_rejected\(\) \{' 'private swarm wrong-key test'
Assert-Contains $privateSwarmWrongKeyTest.Body 'println!("wrong_key_connect=rejected");' "Private swarm wrong-key test output changed"

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
Assert-True ($clusterEnvLines.Count -eq 19) "Cluster release job must contain exactly nineteen environment lines"
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
    "      IPFS_S3_CLUSTER_KUBO_C_URL: http://127.0.0.1:55102",
    '      IPFS_S3_SWARM_KEY_FILE: ${{ runner.temp }}/ipfs3-swarm-${{ github.run_id }}-${{ github.run_attempt }}.key',
    '      IPFS_S3_SWARM_KEY_WRONG_FILE: ${{ runner.temp }}/ipfs3-swarm-wrong-${{ github.run_id }}-${{ github.run_attempt }}.key',
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
) "Cluster release workflow order changed"

$pwshBlocks = Get-PwshRunBlocks $clusterJob
Assert-True ($pwshBlocks.Count -eq 17) "Expected exactly seventeen Cluster PowerShell run blocks"
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
$privateCausalitySource = $pwshBlocks[5]
$topologySource = $pwshBlocks[6]
$proxySource = $pwshBlocks[7]
$replicationSource = $pwshBlocks[8]
$preStopDiagnosticSource = $pwshBlocks[9]
$stopSource = $pwshBlocks[10]
$outageSource = $pwshBlocks[11]
$preRestartDiagnosticSource = $pwshBlocks[12]
$restartSource = $pwshBlocks[13]
$recoverySource = $pwshBlocks[14]
$finalDiagnosticSource = $pwshBlocks[15]
$cleanupSource = $pwshBlocks[16]
foreach ($blockContract in @(
    [pscustomobject]@{ Index = 0; Fragment = 'docker compose version'; Role = 'compose' },
    [pscustomobject]@{ Index = 1; Fragment = 'docker compose @compose config --quiet'; Role = 'configuration' },
    [pscustomobject]@{ Index = 2; Fragment = 'CLUSTER_PINSET_OWNED=true'; Role = 'ownership' },
    [pscustomobject]@{ Index = 3; Fragment = '--profile private-swarm-validation @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway'; Role = 'startup' },
    [pscustomobject]@{ Index = 4; Fragment = 'cluster_support::release_version_validator_accepts_exact_release_and_build_metadata'; Role = 'release-version unit' },
    [pscustomobject]@{ Index = 5; Fragment = 'private_swarm_configuration_and_peering'; Role = 'private swarm causality' },
    [pscustomobject]@{ Index = 6; Fragment = 'cluster_topology_converges'; Role = 'topology' },
    [pscustomobject]@{ Index = 7; Fragment = 'cluster_proxy_compatibility'; Role = 'proxy compatibility' },
    [pscustomobject]@{ Index = 8; Fragment = 'cluster_replication_and_retention'; Role = 'replication' },
    [pscustomobject]@{ Index = 9; Fragment = 'Pre-stop Cluster diagnostics failed'; Role = 'pre-stop diagnostics' },
    [pscustomobject]@{ Index = 10; Fragment = 'stop cluster-b kubo-b'; Role = 'peer stop' },
    [pscustomobject]@{ Index = 11; Fragment = 'cluster_peer_b_outage_contract'; Role = 'outage' },
    [pscustomobject]@{ Index = 12; Fragment = 'Stopped-peer diagnostics failed'; Role = 'pre-restart diagnostics' },
    [pscustomobject]@{ Index = 13; Fragment = 'restart --timeout 30 swarm-bootstrap'; Role = 'full swarm restart' },
    [pscustomobject]@{ Index = 14; Fragment = 'cluster_peer_b_restart_recovery'; Role = 'recovery' },
    [pscustomobject]@{ Index = 15; Fragment = 'Final Cluster diagnostics failed'; Role = 'final diagnostics' },
    [pscustomobject]@{ Index = 16; Fragment = '$errors = [Collections.Generic.List[string]]::new()'; Role = 'cleanup' }
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
    '55435, 55100, 55101, 55102, 59100, 59101, 59102, 59103',
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
    'docker compose @compose build kubo-a',
    '--profile private-swarm-validation @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway'
) "Cluster attempted marker must precede Kubo A build and exact private-swarm startup"
Assert-Contains $startupSource 'if ($LASTEXITCODE -ne 0) { throw "Cluster topology did not become healthy" }' "Cluster startup failure must be blocking"
Assert-InOrder $startupSource @(
    '"CLUSTER_PINSET_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'docker compose @compose build kubo-a',
    'IPFS_SWARM_KEY_FILE=/run/secrets/missing-swarm-key',
    'LIBP2P_FORCE_PNET=0',
    'Invoke-PrivateSwarmSupervisorProof -Mode normal -ExpectedExit 37',
    'Invoke-PrivateSwarmSupervisorProof -Mode handled -ExpectedExit 43',
    'Invoke-PrivateSwarmSupervisorProof -Mode unhandled -ExpectedExit 143',
    '--profile private-swarm-validation @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway'
) "Cluster startup must build, reject invalid private swarms, prove PID 1, then start the exact validation profile"

$expectedCargoCommands = @(
    'cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact',
    'cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact',
    'cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact',
    'cargo test --test cluster cluster_support::kubo_swarm_peers_null_is_empty_without_ndjson -- --exact',
    'cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster private_swarm_wrong_key_rejected -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1',
    'cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1'
)
Assert-True (([regex]::Matches($clusterJob, '(?m)^          cargo test --test cluster [^\r\n]+$')).Count -eq 12) "Cluster job must contain exactly twelve explicit Cluster cargo commands"
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
    'if ($env:CLUSTER_PRIVATE_SWARM_GREEN -ne "true") { throw "Private swarm GREEN receipt is required before topology" }',
    $expectedCargoCommands[6],
    'if ($LASTEXITCODE -ne 0) { throw "TOPOLOGY_CONVERGENCE_BLOCKER: exact v1.1.6 two-peer topology did not converge" }',
    '"CLUSTER_TOPOLOGY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Topology GREEN receipt must follow only a successful topology gate"
Assert-InOrder $proxySource @(
    'if ($env:CLUSTER_TOPOLOGY_GREEN -ne "true") { throw "Topology GREEN receipt is required before compatibility" }',
    $expectedCargoCommands[7],
    'if ($LASTEXITCODE -ne 0) { throw "PROXY_COMPATIBILITY_BLOCKER: stop and revise the approved design; do not add app fallback code" }',
    '"CLUSTER_PROXY_COMPATIBILITY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Proxy compatibility must require topology GREEN and write its receipt only after add-pin-cat success"
Assert-InOrder $replicationSource @(
    'if ($env:CLUSTER_PROXY_COMPATIBILITY_GREEN -ne "true") { throw "Add-pin-cat proxy GREEN receipt is required before replication" }',
    'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before replication" }',
    $expectedCargoCommands[8]
) "Replication must require proxy GREEN before its receipt-owned check"
$stateConsumerBlocks = @(
    [pscustomobject]@{ Index = 8; CargoCommand = $expectedCargoCommands[8] },
    [pscustomobject]@{ Index = 11; CargoCommand = $expectedCargoCommands[9] },
    [pscustomobject]@{ Index = 14; CargoCommand = $expectedCargoCommands[11] }
)
foreach ($stateConsumerBlock in $stateConsumerBlocks) {
    Assert-Contains $pwshBlocks[$stateConsumerBlock.Index] 'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true")' "Every state-consuming Cluster test must require the owned receipt"
    Assert-InOrder $pwshBlocks[$stateConsumerBlock.Index] @(
        'if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true")',
        $stateConsumerBlock.CargoCommand
    ) "Owned state receipt must be checked before its Cluster live test"
}
Assert-InOrder $clusterJob @(
    'logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway',
    'stop cluster-b kubo-b',
    'logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway',
    'stop cluster-a kubo-a',
    'start kubo-a kubo-b',
    'restart --timeout 30 swarm-bootstrap',
    'start cluster-a cluster-b',
    'logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway',
    'down --volumes --remove-orphans'
) "Cluster diagnostics, peer stop/restart, and cleanup order changed"

$workflowFingerprintRedactorSources = @($startupSource, $preStopDiagnosticSource, $preRestartDiagnosticSource, $finalDiagnosticSource)
Assert-True ($workflowFingerprintRedactorSources.Count -eq 4) "Cluster workflow must retain exactly four fingerprint redactor copies"
foreach ($workflowFingerprintRedactorSource in $workflowFingerprintRedactorSources) {
    Assert-Contains $workflowFingerprintRedactorSource '$safe = [regex]::Replace($safe, ''(Swarm key fingerprint: )[0-9a-f]{32}(?=\s*$)'', ''$1[redacted]'')' "Cluster workflow fingerprint redactor must retain Compose prefixes while replacing only the exact fingerprint"
    Assert-NotContains $workflowFingerprintRedactorSource '$safe = [regex]::Replace($safe, ''(?m)^Swarm key fingerprint: [0-9a-f]{32}\s*$'', ''Swarm key fingerprint: [redacted]'')' "Cluster workflow fingerprint redactor must not anchor the marker at line start"
}
$diagnosticSources = @($preStopDiagnosticSource, $preRestartDiagnosticSource, $finalDiagnosticSource)
Assert-True (([regex]::Matches($clusterJob, [regex]::Escape('logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway'))).Count -eq 3) "Cluster job must have exactly three sanitized diagnostics captures"
Assert-NotMatches $clusterJob '(?m)^\s*docker compose .*logs --no-color .*\|' "Cluster diagnostics must not stream raw logs through a pipeline"
foreach ($diagnosticSource in $diagnosticSources) {
    foreach ($fragment in @(
        'function Protect-ClusterDiagnosticLine',
        'function Write-SanitizedClusterDiagnostics',
        '$rawDiagnosticLines = @(',
        'docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway 2>&1',
        '$diagnosticExit = $LASTEXITCODE',
        '[Console]::Out.WriteLine((Protect-ClusterDiagnosticLine -Line "$rawLine"))',
        'if ($diagnosticExit -ne 0) { throw "$FailureMessage (exit=$diagnosticExit)" }'
    )) {
        Assert-Contains $diagnosticSource $fragment "Cluster diagnostics must sanitize before output: $fragment"
    }
    Assert-Matches $diagnosticSource '(?s)\$rawDiagnosticLines\s*=\s*@\(\s*docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway 2>&1\s*\)\s*\$diagnosticExit = \$LASTEXITCODE.*?\[Console\]::Out\.WriteLine\(\(Protect-ClusterDiagnosticLine -Line "\$rawLine"\)\).*?if \(\$diagnosticExit -ne 0\)' "Cluster diagnostics must capture, record exit, sanitize, then fail"
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
$ordinary32 = '0123456789abcdef0123456789abcdef'
$standalone64 = $ordinary32 + $ordinary32
foreach ($fixture in @(
    "Swarm key fingerprint: $ordinary32",
    '/key/swarm/psk/1.0.0/',
    '/base16/',
    "digest=$standalone64"
)) {
    $sanitized = Protect-ClusterDiagnosticLine $fixture
    Assert-NotContains $sanitized $ordinary32 "Cluster diagnostic redactor leaked private swarm material"
    Assert-NotContains $sanitized $standalone64 "Cluster diagnostic redactor leaked standalone 64-hex material"
}
Assert-True ((Protect-ClusterDiagnosticLine "ordinary $ordinary32") -ceq "ordinary $ordinary32") "Cluster diagnostic redactor must preserve ordinary 32-hex text"
$composePrefixedFingerprint = "kubo-a  | Swarm key fingerprint: $ordinary32"
Assert-True (
    (Protect-ClusterDiagnosticLine $composePrefixedFingerprint) -ceq "kubo-a  | Swarm key fingerprint: [redacted]"
) "Cluster diagnostic redactor must retain the Compose prefix while redacting only the swarm fingerprint"

Assert-InOrder $configurationSource @(
    'function New-PrivateSwarmHex',
    '[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)',
    '[Convert]::FromHexString($hex)',
    'do { $wrongSwarmHex = New-PrivateSwarmHex } while ($wrongSwarmHex -ceq $mainSwarmHex)',
    'Write-PrivateSwarmKey -Path $env:IPFS_S3_SWARM_KEY_FILE -Hex $mainSwarmHex',
    '"CLUSTER_SWARM_KEY_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'Write-PrivateSwarmKey -Path $env:IPFS_S3_SWARM_KEY_WRONG_FILE -Hex $wrongSwarmHex',
    '"CLUSTER_SWARM_KEY_WRONG_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'docker compose @compose config --quiet'
) "Private swarm key files must be independently generated and marked before Compose configuration"
foreach ($fragment in @(
    '$runnerTemp = [IO.Path]::GetFullPath($env:RUNNER_TEMP)',
    '[IO.Path]::GetFullPath($Path)',
    '[IO.FileMode]::CreateNew',
    '[IO.FileShare]::None',
    '$stream.Flush($true)',
    '"/key/swarm/psk/1.0.0/`n/base16/`n$Hex`n"',
    '$bytes.Length -ne 96',
    'if ($created) { Remove-Item -LiteralPath $canonicalPath -Force -ErrorAction SilentlyContinue }',
    '"IPFS_S3_SWARM_KEY_FILE"',
    '"IPFS_S3_SWARM_KEY_WRONG_FILE"'
)) {
    Assert-Contains $configurationSource $fragment "Private swarm file lifecycle contract is missing: $fragment"
}
Assert-NotMatches $configurationSource '(?im)^\s*(?:Write-Host|Write-Output|Write-Warning|\[Console\]::Out\.WriteLine).*?(?:\$mainSwarmHex|\$wrongSwarmHex|\$Hex)' "Private swarm generation must emit receipts, never key contents"

foreach ($fragment in @(
    'run --no-deps --rm @RunArgs kubo-a 2>&1',
    'IPFS_SWARM_KEY_FILE=/run/secrets/missing-swarm-key',
    'LIBP2P_FORCE_PNET=0',
    '$safeLines = @($rawLines | ForEach-Object { Protect-ClusterDiagnosticLine -Line "$_" })',
    "[Console]::Out.WriteLine('private swarm startup rejected')",
    'docker wait $name',
    "'{{.State.Pid}} {{.State.ExitCode}}'",
    '"0 $ExpectedExit"',
    'docker diff $name',
    "'(?im)(?:fifo|daemon-log)'",
    'ipfs3.cluster.private-swarm-proof=$Mode',
    '"--mount", "type=bind,src=$fullKeyPath,dst=/run/secrets/swarm_key,readonly"',
    '"--env", "IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key"',
    'Invoke-PrivateSwarmSupervisorProof -Mode normal -ExpectedExit 37',
    'Invoke-PrivateSwarmSupervisorProof -Mode handled -ExpectedExit 43',
    'Invoke-PrivateSwarmSupervisorProof -Mode unhandled -ExpectedExit 143',
    'docker stop --time 30 $name'
)) {
    Assert-Contains $startupSource $fragment "Private swarm wrapper/filter/PID 1 proof contract is missing: $fragment"
}
Assert-True (([regex]::Matches($startupSource, [regex]::Escape('Invoke-PrivateSwarmNegative -RunArgs'))).Count -eq 2) "Private swarm startup must run exactly two negative Compose probes"
Assert-NotContains $startupSource 'kill -0' "Private swarm proof must not use kill -0 polling"
Assert-Contains $startupSource '"--mount", "type=bind,src=$fullKeyPath,dst=/run/secrets/swarm_key,readonly"' "Supervisor proof must bind only the exact swarm key file at its approved secret path"
Assert-Contains $startupSource '$fullKeyPath = [IO.Path]::GetFullPath($env:IPFS_S3_SWARM_KEY_FILE)' "Supervisor proof must canonicalize the exact key-file path"
foreach ($forbiddenMountFragment in @('$keyParent', '$keyLeaf', 'dst=/run/ipfs3-swarm', 'src=$env:RUNNER_TEMP', 'IPFS_SWARM_KEY_FILE=/run/ipfs3-swarm/')) {
    Assert-NotContains $startupSource $forbiddenMountFragment "Supervisor proof must not mount a swarm-key directory or use a non-secret key path: $forbiddenMountFragment"
}
Assert-InOrder $startupSource @(
    '$sourceFixture = @''',
    '. /private-swarm-entrypoint.sh',
    'ordinary=0123456789abcdef0123456789abcdef',
    "cid=Qm`$(printf '%044d' 0 | tr 0 c)",
    'redact_swarm_fingerprint',
    'Swarm key fingerprint: [redacted]',
    'ordinary $ordinary',
    'cid=$cid',
    'select_supervisor_exit',
    'check_selector 37 37 1 1',
    'check_selector 1 0 1 0',
    'check_selector 1 0 0 1',
    'check_selector 0 0 0 0',
    'docker run --rm --entrypoint /bin/sh ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0 -ec $sourceFixture 2>&1',
    '$sourceFixtureOutput = $null',
    'if ($sourceFixtureExit -ne 0) { throw "Private swarm source-only filter and supervisor fixture failed" }',
    '"CLUSTER_PRIVATE_SWARM_FILTER_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV',
    'Invoke-PrivateSwarmSupervisorProof -Mode normal -ExpectedExit 37',
    'Invoke-PrivateSwarmSupervisorProof -Mode handled -ExpectedExit 43',
    'Invoke-PrivateSwarmSupervisorProof -Mode unhandled -ExpectedExit 143',
    '"CLUSTER_PRIVATE_SWARM_SUPERVISOR_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Source-only actual filter/selector fixture must pass before the disposable PID 1 supervisor proofs and their receipts"
Assert-InOrder $startupSource @(
    '$rawKuboLogs = @(docker compose @compose logs --no-color kubo-a kubo-b kubo-c 2>&1)',
    '$rawKuboText = $rawKuboLogs -join "`n"',
    'Direct Kubo logs exposed a raw swarm fingerprint',
    '$rawKuboRedactedFingerprintCount = ([regex]::Matches($rawKuboText, [regex]::Escape(''Swarm key fingerprint: [redacted]''))).Count',
    '$rawKuboRedactedFingerprintCount -lt 3',
    'PRIVATE_SWARM_DIRECT_KUBO_LOGS_CAPTURED',
    'PRIVATE_SWARM_DIRECT_KUBO_FINGERPRINT_ABSENT',
    'PRIVATE_SWARM_DIRECT_KUBO_MARKERS_VERIFIED',
    'CLUSTER_PRIVATE_SWARM_DIRECT_LOGS_GREEN=true',
    '$rawKuboLogs = $null',
    '$rawKuboText = $null',
    '$rawKuboRedactedFingerprintCount = $null'
) "Direct Kubo logs must be checked before any diagnostic redaction and emit fixed receipts only"
Assert-NotContains $startupSource '"Initializing daemon...", "Kubo version:", "Daemon is ready"' "Direct Kubo log gate must prove the redaction barrier, not generic daemon markers"
foreach ($fragment in @(
    'ps --all -q swarm-bootstrap',
    'com.docker.compose.project',
    'com.docker.compose.service',
    'swarm-bootstrap exited 0',
    'CLUSTER_PRIVATE_SWARM_BOOTSTRAP_GREEN=true',
    'stat -c "%a" "$IPFS_PATH/swarm.key"',
    'sha256sum "$IPFS_PATH/swarm.key"',
    "'^400 (?<digest>[0-9a-f]{64})$'",
    'CLUSTER_PRIVATE_SWARM_KEY_MATERIAL_GREEN=true',
    '$modeDigestA = $null',
    '$modeDigestB = $null'
)) {
    Assert-Contains $startupSource $fragment "Private swarm bootstrap/mode receipt contract is missing: $fragment"
}
Assert-NotMatches $startupSource '(?im)^\s*(?:Write-Host|Write-Output|Write-Warning|\[Console\]::Out\.WriteLine).*?(?:\$modeDigest|\$bootstrapId)' "Private swarm receipts must not emit digests or container IDs"
Assert-InOrder $privateCausalitySource @(
    'CLUSTER_PRIVATE_SWARM_WRAPPER_GREEN',
    'CLUSTER_PRIVATE_SWARM_FILTER_GREEN',
    'CLUSTER_PRIVATE_SWARM_SUPERVISOR_GREEN',
    'CLUSTER_PRIVATE_SWARM_DIRECT_LOGS_GREEN',
    'CLUSTER_PRIVATE_SWARM_BOOTSTRAP_GREEN',
    'CLUSTER_PRIVATE_SWARM_KEY_MATERIAL_GREEN',
    $expectedCargoCommands[1],
    $expectedCargoCommands[2],
    $expectedCargoCommands[3],
    $expectedCargoCommands[4],
    $expectedCargoCommands[5],
    '"CLUSTER_PRIVATE_SWARM_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV'
) "Private swarm units and live gates must complete before topology"
foreach ($diagnosticSource in $diagnosticSources) {
    foreach ($fragment in @(
        '''(Swarm key fingerprint: )[0-9a-f]{32}(?=\s*$)''',
        "'(?m)^/key/swarm/psk/1",
        "'(?m)^/base16/",
        "'[REDACTED_HEX]'"
    )) {
        Assert-Contains $diagnosticSource $fragment "Cluster diagnostic defense-in-depth redactor is missing: $fragment"
    }
}
Assert-InOrder $restartSource @(
    'stop cluster-a kubo-a',
    'start kubo-a kubo-b',
    'Wait-HealthyClusterService -Service kubo-a',
    'Wait-HealthyClusterService -Service kubo-b',
    'restart --timeout 30 swarm-bootstrap',
    'Wait-CompletedSwarmBootstrap',
    'start cluster-a cluster-b',
    'Wait-HealthyClusterService -Service cluster-a',
    'Wait-HealthyClusterService -Service cluster-b'
) "A/B recovery must re-establish the private swarm before restarting both Cluster peers"
Assert-InOrder $recoverySource @(
    $expectedCargoCommands[10],
    $expectedCargoCommands[11]
) "Post-restart private swarm proof must precede existing peer-B recovery"

foreach ($fragment in @(
    '$errors = [Collections.Generic.List[string]]::new()',
    '$topologyOwned = $env:CLUSTER_PINSET_OWNED -eq "true" -and $env:CLUSTER_PINSET_ATTEMPTED -eq "true"',
    '$proofsAttempted = $env:CLUSTER_PRIVATE_SWARM_PROOFS_ATTEMPTED -eq "true"',
    '$fileMarkersPresent = $env:CLUSTER_SWARM_KEY_OWNED -eq "true"',
    'if ($proofsAttempted)',
    'label=ipfs3.cluster.private-swarm-proof',
    'docker stop --time 30 $proofContainer',
    'docker rm $proofContainer',
    'if ($topologyOwned)',
    'docker compose --profile private-swarm-validation --project-name $project -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml down --volumes --remove-orphans',
    'down --volumes --remove-orphans',
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'Marker = "CLUSTER_SWARM_KEY_OWNED"; Path = $env:IPFS_S3_SWARM_KEY_FILE',
    'Marker = "CLUSTER_SWARM_KEY_WRONG_OWNED"; Path = $env:IPFS_S3_SWARM_KEY_WRONG_FILE',
    'Marker = "CLUSTER_STATE_RECEIPT_OWNED"; Path = $env:IPFS_S3_CLUSTER_STATE_PATH',
    'Remove-Item -LiteralPath $ownedFile.Path -ErrorAction Stop',
    'if ($errors.Count -ne 0) { throw ($errors -join "; ") }'
)) {
    Assert-Contains $cleanupSource $fragment "Cluster cleanup contract is missing: $fragment"
}
Assert-InOrder $cleanupSource @(
    'label=ipfs3.cluster.private-swarm-proof',
    'docker stop --time 30 $proofContainer',
    'docker rm $proofContainer',
    'docker compose --profile private-swarm-validation --project-name $project -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml down --volumes --remove-orphans',
    '$downExit = $LASTEXITCODE',
    '$containerExit = $LASTEXITCODE',
    '$networkExit = $LASTEXITCODE',
    '$volumeExit = $LASTEXITCODE',
    'foreach ($ownedFile in @(',
    'Remove-Item -LiteralPath $ownedFile.Path -ErrorAction Stop',
    'if ($errors.Count -ne 0) { throw ($errors -join "; ") }'
) "Cluster cleanup must clean proof residue, aggregate topology residual checks, then delete every marked temporary file"

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
    "exactly seven roles",
    "one-shot swarm-bootstrap",
    "same-host only",
    'shared Kubo `swarm.key` PSK',
    'separate from `IPFS_S3_CLUSTER_SECRET`',
    "Kubo A and Kubo B each retain one persistent Peering entry for the other as their sole peer",
    "AutoConf disabled",
    "public bootstrap is empty",
    'routing is `none`',
    "mDNS is disabled",
    'server-profile RFC1918 `Swarm.AddrFilters` are cleared solely for the PSK-gated internal Docker bridge',
    'Validation-only Kubo C on loopback `55102` uses a wrong key',
    "eventually reaches exact 2/2 allocations",
    "PSK membership and libp2p connection protection",
    "does not control container egress",
    "does not encrypt or authenticate REST",
    "does not provide high availability, multi-host discovery, online rotation, or member revocation",
    "Rotation requires coordinated downtime and is not automated",
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
    'Hosted job: `NOT RUN`',
    '$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"',
    '$env:IPFS_S3_GATEWAY_PORT = "9000"',
    'COMPOSE_DISABLE_ENV_FILE = "1"',
    "cryptographically random URL-safe",
    "32-byte master key",
    "32-byte Cluster secret",
    "lowercase hex",
    "32 random bytes",
    "exactly three LF-terminated lines",
    "persistent operator path outside the repository",
    'protect the directory and file with host ACLs',
    "Never commit the swarm-key file",
    'the exact path in `$env:IPFS_S3_SWARM_KEY_FILE`',
    "UTF-8 without a BOM",
    '`CreateNew`',
    "does not print the key or a digest",
    "Store these generated secrets before the first write and restore them unchanged",
    "Changing the master key breaks encrypted objects",
    "changing the Cluster secret breaks Cluster membership",
    "docker compose -f docker-compose.cluster.yml config --quiet",
    "Cluster Compose configuration failed",
    "docker compose -f docker-compose.cluster.yml up --detach --build --wait --wait-timeout 300",
    "Cluster profile did not become healthy",
    'base `docker-compose.cluster.yml` and its required environment paths',
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
    "same-host Compose mDNS only",
    "Private swarm remains unchecked",
    "down --volumes",
    "ipfs-cluster-ctl peers ls",
    "docker compose logs"
)) {
    Assert-NotContains $clusterReadme $forbidden "Cluster README must not contain: $forbidden"
}
Assert-NotContains $clusterReadme '$swarmKeyPath = Join-Path (Get-Location) "ipfs3-swarm.key"' "Cluster README must not write the swarm key into the repository"
Assert-NotMatches $clusterReadme '(?m)^\$swarmKey(?:Directory|Path)\s*=\s*Join-Path\s+(?:\(Get-Location\)|\(Resolve-Path\s+\.\)|\$PSScriptRoot|\$PSCommandPath|\$RepoRoot|\.)(?:\s|$)' "Cluster README must not use a repository-relative swarm-key path"
foreach ($fragment in @(
    '$swarmKeyDirectory = Join-Path $HOME ".ipfs3/secrets"',
    '[IO.Directory]::CreateDirectory($swarmKeyDirectory)',
    '$swarmKeyPath = Join-Path $swarmKeyDirectory "swarm.key"',
    '[IO.FileMode]::CreateNew',
    '$env:IPFS_S3_SWARM_KEY_FILE = $swarmKeyPath'
)) {
    Assert-Contains $clusterReadme $fragment "Cluster README persistent swarm-key contract is missing: $fragment"
}
Assert-NotMatches $clusterReadme '(?im)^.*(?:peer list|PeerID|peer_id|peer-id).*$' "Cluster README must not show peer-list commands or output"
Assert-NotMatches $clusterReadme '(?i)docker compose(?:\s+[^\r\n]+)?\s+logs\b' "Cluster README must not show raw Compose logs"
Assert-NotMatches $clusterReadme '(?i)0\.0\.0\.0[^\r\n]*59103|59103[^\r\n]*0\.0\.0\.0' "Cluster README must not show nonloopback proxy exposure"
Assert-NotMatches $clusterReadme '(?i)production[^\r\n]*(?:publish|expose)[^\r\n]*proxy|proxy[^\r\n]*(?:publish|expose)[^\r\n]*production' "Cluster README must not claim production proxy publication"
Assert-NotMatches $clusterReadme '(?i)\bdirect non-loopback(?: gateway)? (?:publication|access) (?:is )?supported\b' "Cluster README must not claim direct non-loopback publication support"
Assert-NotMatches $clusterReadme '(?i)hosted[^\r\n]*\bPASS\b|\bPASS\b[^\r\n]*hosted' "Cluster README must not claim hosted PASS"
Assert-NotMatches $clusterReadme '(?i)\bHA\b' "Cluster README must not claim HA"
Assert-NotMatches $clusterReadme '(?i)\b(?:supports|provides|enables|runs on|for) multi-host\b' "Cluster README must not claim multi-host operation"

$v05RoadmapSections = [regex]::Matches($Roadmap, '(?ms)^## v0\.5 — Multi-node\r?\n(?<body>.*?)(?=^## |\z)')
Assert-True ($v05RoadmapSections.Count -eq 1) "ROADMAP must contain exactly one v0.5 section"
$expectedV05Roadmap = @(
    "- [x] PostgreSQL production deployment",
    "- [x] Multiple gateway instances (horizontal scaling)",
    "- [x] IPFS Cluster for pinset replication",
    "- [x] Private swarm (swarm.key) for node-to-node communication"
) -join "`n"
Assert-True ($v05RoadmapSections[0].Groups["body"].Value.Trim() -ceq $expectedV05Roadmap) "ROADMAP v0.5 must change only the Private swarm checkbox"

Write-Host "cluster static contract tests: PASSED"
