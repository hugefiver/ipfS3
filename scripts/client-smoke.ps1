[CmdletBinding()]
param(
    [ValidateSet("All", "Rclone", "Mc", "Aws")]
    [string]$Client = "All",
    [switch]$Run,
    [switch]$CleanupVolumes
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path Env:IPFS_S3_MASTER_KEY)) { $env:IPFS_S3_MASTER_KEY = "" }
if (-not (Test-Path Env:CLOUDFLARE_TUNNEL_TOKEN)) { $env:CLOUDFLARE_TUNNEL_TOKEN = "" }
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\docker-compose.yml"))
$Stamp = Get-Date -Format "yyyyMMddHHmmss"
$RunRoot = Join-Path ([IO.Path]::GetTempPath()) "ipfs-s3-client-smoke-$Stamp"
$null = New-Item -ItemType Directory -Path $RunRoot -Force
$LogPath = Join-Path $RunRoot "client-smoke.log"
$FixturePath = Join-Path $RunRoot "file.txt"
$FixtureText = "ipfs-s3-client-smoke-$Stamp"
$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$Stamp"
$EvidenceRepoRoot = "<repo>"
$EvidenceLogPath = "client-smoke.log"
[IO.File]::WriteAllText($FixturePath, $FixtureText, [Text.UTF8Encoding]::new($false))

function Convert-ToDockerDesktopPath {
    param([Parameter(Mandatory)][string]$Path)
    $fullPath = [IO.Path]::GetFullPath($Path)
    $drive = $fullPath.Substring(0, 1).ToLowerInvariant()
    $rest = $fullPath.Substring(2).Replace("\", "/")
    return "/run/desktop/mnt/host/$drive$rest"
}

$PortablePathReplacements = @(
    [pscustomobject]@{ Actual = $RunRoot; Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = $RunRoot.Replace("\", "/"); Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = (Convert-ToDockerDesktopPath $RunRoot); Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = $RepoRoot; Portable = $EvidenceRepoRoot },
    [pscustomobject]@{ Actual = $RepoRoot.Replace("\", "/"); Portable = $EvidenceRepoRoot },
    [pscustomobject]@{ Actual = (Convert-ToDockerDesktopPath $RepoRoot); Portable = $EvidenceRepoRoot }
)

function Convert-ToPortableEvidence {
    param([AllowNull()][AllowEmptyString()][string]$Text)
    if ($null -eq $Text) { return "" }
    $portable = $Text
    foreach ($replacement in $PortablePathReplacements) {
        $portable = $portable.Replace($replacement.Actual, $replacement.Portable)
    }
    return $portable
}

function Convert-NativeOutputItem {
    param([AllowNull()][object]$Item)
    if ($null -eq $Item) { return $null }
    if ($Item -is [System.Management.Automation.ErrorRecord]) {
        $message = $Item.Exception.Message
        if (-not [string]::IsNullOrWhiteSpace($message) -and
            $message -ne "System.Management.Automation.RemoteException") {
            return $message
        }
    }
    $text = $Item.ToString()
    if ($text -eq "System.Management.Automation.RemoteException") { return $null }
    return $text
}

$Images = @{
    Rclone = "rclone/rclone:1.74.4"
    Mc = "minio/mc:latest"
    Aws = "amazon/aws-cli:latest"
}
$ServiceImages = @(
    "ghcr.io/hugefiver/ipfs3-kubo:latest",
    "ghcr.io/hugefiver/ipfs3:latest"
)
$StandardBuildImages = @(
    "ipfs/kubo:latest",
    "rust:latest",
    "debian:trixie-slim"
)
$Selected = if ($Client -eq "All") { @("Rclone", "Mc", "Aws") } else { @($Client) }

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $displayCommand = Convert-ToPortableEvidence ("docker " + ($Arguments -join " "))
    Write-Host $displayCommand
    $output = @(& docker @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $lines = @($output | ForEach-Object { Convert-NativeOutputItem $_ })
    $lines | ForEach-Object { Write-Host (Convert-ToPortableEvidence $_) }
    if ($exitCode -ne 0) {
        throw "docker exited ${exitCode}: $displayCommand"
    }
    return $lines
}

function Test-LocalImage {
    param([Parameter(Mandatory)][string]$Image)
    & docker image inspect $Image --format "{{.Id}}" *> $null
    return $LASTEXITCODE -eq 0
}

function Write-Result {
    param(
        [Parameter(Mandatory)][string]$Name,
        [ValidateSet("PASSED", "FAILED", "SKIPPED")][string]$Status,
        [Parameter(Mandatory)][string]$Detail,
        [ValidateSet("PASSED", "NOT_RUN")][string]$DualHead = "NOT_RUN"
    )
    $portableDetail = Convert-ToPortableEvidence $Detail
    Write-Host "[RESULT] client=$Name status=$Status dual_head=$DualHead detail=$portableDetail evidence=$EvidenceLogPath"
}

function Get-ImageEvidence {
    param([Parameter(Mandatory)][string]$Image)
    $lines = @(Invoke-Docker @("image", "inspect", $Image, "--format", "{{.Id}}"))
    if ($lines.Count -eq 0) { throw "docker image inspect returned no lines for $Image" }
    return $lines[-1]
}

function Get-ComposeNetwork {
    $lines = @(Invoke-Docker @("compose", "-f", $ComposeFile, "ps", "-q", "gateway"))
    if ($lines.Count -eq 0) { throw "docker compose ps returned no gateway lines" }
    $gatewayId = $lines[-1].Trim()
    if ([string]::IsNullOrWhiteSpace($gatewayId)) { throw "gateway container id is empty" }
    $lines = @(Invoke-Docker @("inspect", "--format", "{{json .NetworkSettings.Networks}}", $gatewayId))
    if ($lines.Count -eq 0) { throw "docker inspect returned no gateway network JSON" }
    $networks = $lines[-1] | ConvertFrom-Json
    $networkNames = @($networks.PSObject.Properties.Name)
    if ($networkNames.Count -eq 0) { throw "gateway network list is empty" }
    $network = $networkNames[0]
    if ([string]::IsNullOrWhiteSpace($network)) { throw "gateway network is empty" }
    return $network
}

function Wait-GatewayHealthy {
    $lines = @(Invoke-Docker @("compose", "-f", $ComposeFile, "ps", "-q", "gateway"))
    if ($lines.Count -eq 0) { throw "docker compose ps returned no gateway lines" }
    $gatewayId = $lines[-1].Trim()
    if ([string]::IsNullOrWhiteSpace($gatewayId)) { throw "gateway container id is empty" }
    for ($attempt = 0; $attempt -lt 36; $attempt++) {
        $lines = @(Invoke-Docker @("inspect", "--format", "{{.State.Health.Status}}", $gatewayId))
        if ($lines.Count -eq 0) { throw "docker inspect returned no health lines" }
        $health = $lines[-1].Trim()
        if ($health -eq "healthy") { return }
        if ($health -eq "unhealthy") { throw "gateway healthcheck is unhealthy" }
        Start-Sleep -Seconds 5
    }
    throw "gateway did not become healthy within 180 seconds"
}

function Invoke-OfflineGatewayBuild {
    $runtimeImage = "ghcr.io/hugefiver/ipfs3:latest"
    $vendorPath = Join-Path $RunRoot "vendor"
    $dockerfile = Join-Path $RunRoot "Dockerfile.gateway-runtime"
    Write-Host (Convert-ToPortableEvidence "cargo vendor --locked --offline $vendorPath")
    $vendorOutput = @(& cargo vendor --locked --offline $vendorPath 2>&1)
    $vendorExit = $LASTEXITCODE
    if ($vendorExit -ne 0) {
        $vendorOutput | ForEach-Object {
            $line = Convert-NativeOutputItem $_
            if ($null -ne $line) { Write-Host (Convert-ToPortableEvidence $line) }
        }
        throw "cargo vendor exited $vendorExit"
    }
    Write-Host "cargo vendor completed from the local cache"
    [IO.File]::WriteAllText(
        $dockerfile,
        @"
FROM rust:latest AS builder
WORKDIR /app
COPY --from=vendor . /vendor
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo --config 'source.crates-io.replace-with="vendored-sources"' --config 'source.vendored-sources.directory="/vendor"' build --release --locked --offline --bin ipfs-s3-gateway

FROM $runtimeImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @(
        "build", "--pull=false", "--network", "none", "--quiet",
        "--build-context", "vendor=$vendorPath", "--tag", $runtimeImage,
        "--file", $dockerfile, $RepoRoot
    ) | Out-Null
}

function BindMount {
    param([string]$Source, [string]$Target, [switch]$ReadOnly)
    $suffix = if ($ReadOnly) { ",readonly" } else { "" }
    return "type=bind,src=$Source,dst=$Target$suffix"
}

function Invoke-Rclone {
    param([string]$Network, [string]$ConfigPath, [string[]]$Arguments)
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "--mount", (BindMount $ConfigPath "/config/rclone.conf" -ReadOnly),
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        $Images.Rclone,
        "--config", "/config/rclone.conf"
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Invoke-Mc {
    param([string]$Network, [string]$ConfigDir, [string[]]$Arguments)
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "--mount", (BindMount $ConfigDir "/root/.mc"),
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        $Images.Mc
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Initialize-McAliases {
    param([string]$ConfigDir)
    $null = New-Item -ItemType Directory -Path $ConfigDir -Force
    $config = [ordered]@{
        version = "10"
        aliases = [ordered]@{
            network = [ordered]@{
                url = "http://gateway:9000"
                accessKey = "test"
                secretKey = "test"
                api = "S3v4"
                path = "on"
            }
            localhost = [ordered]@{
                url = "http://127.0.0.1:9000"
                accessKey = "test"
                secretKey = "test"
                api = "S3v4"
                path = "on"
            }
        }
    } | ConvertTo-Json -Depth 4
    [IO.File]::WriteAllText(
        (Join-Path $ConfigDir "config.json"),
        "$config$([Environment]::NewLine)",
        [Text.UTF8Encoding]::new($false)
    )
}

function Assert-McHead {
    param([string]$Network, [string]$ConfigDir, [string]$Alias, [string]$Bucket)
    $lines = @(Invoke-Mc $Network $ConfigDir @("stat", "--json", "$Alias/$Bucket/nested/path/file.txt"))
    $jsonLines = @($lines | Where-Object { $_.TrimStart().StartsWith("{") })
    if ($jsonLines.Count -eq 0) { throw "mc stat returned no JSON lines" }
    $jsonLine = $jsonLines[-1]
    $stat = $jsonLine | ConvertFrom-Json
    if ([int64]$stat.size -ne [Text.Encoding]::UTF8.GetByteCount($FixtureText)) {
        throw "HeadObject Content-Length mismatch: $($stat.size)"
    }
    $etag = ([string]$stat.etag).Trim('"')
    if ([string]::IsNullOrWhiteSpace($etag) -or $etag -notmatch "^(Qm|baf)") {
        throw "HeadObject ETag is not an IPFS CID: $etag"
    }
    return $etag
}

function Invoke-Aws {
    param([string]$Network, [string]$Endpoint, [string[]]$Arguments)
    $deletePath = Join-Path $RunRoot "delete.json"
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "AWS_DEFAULT_REGION=us-east-1",
        "-e", "AWS_EC2_METADATA_DISABLED=true",
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        "--mount", (BindMount $deletePath "/work/delete.json" -ReadOnly),
        $Images.Aws,
        "--endpoint-url", $Endpoint
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Assert-AwsHead {
    param([string]$Network, [string]$Endpoint, [string]$Bucket)
    $json = (Invoke-Aws $Network $Endpoint @(
        "s3api", "head-object", "--bucket", $Bucket, "--key", "nested/path/file.txt", "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if ([int64]$json.ContentLength -ne [Text.Encoding]::UTF8.GetByteCount($FixtureText)) {
        throw "AWS HeadObject ContentLength mismatch: $($json.ContentLength)"
    }
    $etag = ([string]$json.ETag).Trim('"')
    if ([string]::IsNullOrWhiteSpace($etag) -or $etag -notmatch "^(Qm|baf)") {
        throw "AWS HeadObject ETag is not an IPFS CID: $etag"
    }
    return $etag
}

function Invoke-RcloneSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-rclone-$Stamp"
    $config = Join-Path $RunRoot "rclone.conf"
    [IO.File]::WriteAllText($config, @"
[ipfs-s3]
type = s3
provider = Other
endpoint = http://gateway:9000
access_key_id = test
secret_access_key = test
region = us-east-1
force_path_style = true
list_version = 2
use_server_modtime = true
"@, [Text.UTF8Encoding]::new($false))
    Invoke-Docker @("run", "--rm", $Images.Rclone, "version") | Out-Null
    Write-Host "rclone effective options: list_version=2 use_server_modtime=true"
    Invoke-Rclone $Network $config @("mkdir", "ipfs-s3:$bucket") | Out-Null
    Invoke-Rclone $Network $config @("copy", "/work/file.txt", "ipfs-s3:$bucket/nested/path") | Out-Null
    $listed = (Invoke-Rclone $Network $config @("ls", "ipfs-s3:$bucket")) -join "`n"
    if (-not $listed.Contains("nested/path/file.txt")) { throw "rclone ls omitted nested object" }
    $cat = ((Invoke-Rclone $Network $config @("cat", "ipfs-s3:$bucket/nested/path/file.txt")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "rclone cat content mismatch" }

    Invoke-Rclone $Network $config @("deletefile", "ipfs-s3:$bucket/nested/path/file.txt") | Out-Null
    Invoke-Rclone $Network $config @("rmdir", "ipfs-s3:$bucket") | Out-Null
}

function Invoke-McSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-mc-$Stamp"
    $configDir = Join-Path $RunRoot "mc"
    Initialize-McAliases $configDir
    Invoke-Docker @("run", "--rm", $Images.Mc, "--version") | Out-Null
    Invoke-Mc $Network $configDir @("alias", "list", "network") | Out-Null
    Invoke-Mc "host" $configDir @("alias", "list", "localhost") | Out-Null
    Invoke-Mc $Network $configDir @("mb", "network/$bucket") | Out-Null
    Invoke-Mc $Network $configDir @("cp", "/work/file.txt", "network/$bucket/nested/path/file.txt") | Out-Null
    $listed = (Invoke-Mc $Network $configDir @("ls", "network/$bucket/nested/path/")) -join "`n"
    if (-not $listed.Contains("file.txt")) { throw "mc ls omitted file.txt" }
    $cat = ((Invoke-Mc $Network $configDir @("cat", "network/$bucket/nested/path/file.txt")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "mc cat content mismatch" }
    $networkEtag = Assert-McHead $Network $configDir "network" $bucket
    $localhostEtag = Assert-McHead "host" $configDir "localhost" $bucket
    if ($networkEtag -ne $localhostEtag) { throw "mc object ETag differs across endpoints" }
    $contentLength = [Text.Encoding]::UTF8.GetByteCount($FixtureText)
    Write-Host "[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000 etag=$networkEtag content_length=$contentLength"
    Invoke-Mc $Network $configDir @("rm", "network/$bucket/nested/path/file.txt") | Out-Null
    Invoke-Mc $Network $configDir @("rb", "network/$bucket") | Out-Null
}

function Invoke-AwsSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-aws-$Stamp"
    [IO.File]::WriteAllText(
        (Join-Path $RunRoot "delete.json"),
        '{"Objects":[{"Key":"a.txt"},{"Key":"nested/path/file.txt"},{"Key":"videos/file.txt"},{"Key":"missing"}],"Quiet":false}',
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @("run", "--rm", $Images.Aws, "--version") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "mb", "s3://$bucket") | Out-Null
    foreach ($key in @("a.txt", "nested/path/file.txt", "videos/file.txt")) {
        Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "/work/file.txt", "s3://$bucket/$key") | Out-Null
    }
    $listed = (Invoke-Aws $Network "http://gateway:9000" @("s3", "ls", "s3://$bucket", "--recursive")) -join "`n"
    if (-not $listed.Contains("nested/path/file.txt")) { throw "AWS s3 ls omitted nested object" }
    $cat = ((Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "s3://$bucket/nested/path/file.txt", "-")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "AWS s3 cp download mismatch" }

    $location = (Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "get-bucket-location", "--bucket", $bucket, "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if ($null -ne $location.LocationConstraint) { throw "us-east-1 LocationConstraint was not null" }

    $page = (Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "list-objects", "--bucket", $bucket, "--delimiter", "/", "--max-keys", "2", "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if (-not $page.IsTruncated -or [string]::IsNullOrWhiteSpace([string]$page.NextMarker)) {
        throw "AWS ListObjects v1 did not return a truncated page and NextMarker"
    }

    $networkEtag = Assert-AwsHead $Network "http://gateway:9000" $bucket
    $localhostEtag = Assert-AwsHead "host" "http://127.0.0.1:9000" $bucket
    if ($networkEtag -ne $localhostEtag) { throw "AWS object ETag differs across endpoints" }
    $contentLength = [Text.Encoding]::UTF8.GetByteCount($FixtureText)
    Write-Host "[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000 etag=$networkEtag content_length=$contentLength"

    Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "delete-objects", "--bucket", $bucket, "--delete", "file:///work/delete.json"
    ) | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "/work/file.txt", "s3://$bucket/cleanup.txt") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "rm", "s3://$bucket/cleanup.txt") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "rb", "s3://$bucket") | Out-Null
}

function Invoke-SmokeMain {
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker-compose.yml is missing" }
        return $false
    }
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker command is missing" }
        return $false
    }
    & docker info --format "{{.ServerVersion}}" *> $null
    if ($LASTEXITCODE -ne 0) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon is unavailable" }
        return $false
    }

    $missingClients = @{}
    foreach ($name in $Selected) {
        if (-not (Test-LocalImage $Images[$name])) {
            $missingClients[$name] = $Images[$name]
            Write-Host "Required command: docker pull $($Images[$name])"
        }
    }
    if (-not $Run) {
        foreach ($name in $Selected) {
            if ($missingClients.ContainsKey($name)) {
                Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
            } else {
                Write-Result $name "SKIPPED" "execution not requested; rerun with -Run after authorization"
            }
        }
        return $false
    }

    $missingServices = @($ServiceImages | Where-Object { -not (Test-LocalImage $_) })
    if ($missingServices.Count -gt 0) {
        $missingServices | ForEach-Object { Write-Host "Required command: docker pull $_" }
        Write-Host (Convert-ToPortableEvidence "Required startup: docker compose -f `"$ComposeFile`" up -d --build --pull never kubo gateway")
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "local service image missing: $($missingServices -join ', ')" }
        return $false
    }
    if (-not (Test-LocalImage "rust:latest")) {
        Write-Host "Required command: docker pull rust:latest"
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "local build image missing: rust:latest" }
        return $false
    }
    $missingStandardBuild = @($StandardBuildImages | Where-Object { -not (Test-LocalImage $_) })
    $useOfflineGatewayBuild = $missingStandardBuild.Count -gt 0

    $runnable = @($Selected | Where-Object { -not $missingClients.ContainsKey($_) })
    foreach ($name in $Selected | Where-Object { $missingClients.ContainsKey($_) }) {
        Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
    }
    if ($runnable.Count -eq 0) { return $false }

    try {
        if ($useOfflineGatewayBuild) {
            Write-Host "Standard Compose build images unavailable: $($missingStandardBuild -join ', ')"
            Write-Host "Building the current gateway source from an offline vendored dependency context."
            Invoke-OfflineGatewayBuild
            Invoke-Docker @("compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway") | Out-Null
        } else {
            Invoke-Docker @("compose", "-f", $ComposeFile, "up", "-d", "--build", "--pull", "never", "kubo", "gateway") | Out-Null
        }
        Wait-GatewayHealthy
        Invoke-Docker @("compose", "-f", $ComposeFile, "ps") | Out-Null
        $network = Get-ComposeNetwork
    } catch {
        $stackError = Convert-ToPortableEvidence $_.Exception.Message
        foreach ($name in $runnable) {
            Write-Result $name "FAILED" "stack setup failed: $stackError"
        }
        Write-Host "Stack setup failed before client execution; per-client smoke loop was not entered."
        Write-Host "Compose volumes preserved for diagnosis. Explicit cleanup requires -CleanupVolumes after the stack issue is resolved."
        return $true
    }
    $failed = $false

    foreach ($name in $runnable) {
        try {
            $imageId = Get-ImageEvidence $Images[$name]
            Write-Host "client=$name image=$($Images[$name]) image_id=$imageId"
            switch ($name) {
                "Rclone" { Invoke-RcloneSmoke $network }
                "Mc" { Invoke-McSmoke $network }
                "Aws" { Invoke-AwsSmoke $network }
            }
            $dualHead = if ($name -eq "Rclone") { "NOT_RUN" } else { "PASSED" }
            Write-Result -Name $name -Status "PASSED" -Detail "all commands and assertions completed" -DualHead $dualHead
        } catch {
            $failed = $true
            Write-Result $name "FAILED" (Convert-ToPortableEvidence $_.Exception.Message)
            Write-Host "Manual cleanup endpoint: http://127.0.0.1:9000 bucket prefix ipfs-s3-$($name.ToLower())-$Stamp"
        }
    }

    if ($CleanupVolumes) {
        Invoke-Docker @("compose", "-f", $ComposeFile, "down", "-v") | Out-Null
    } else {
        Write-Host "Compose volumes preserved. Explicit cleanup: pwsh -NoProfile -File scripts/client-smoke.ps1 -Run -CleanupVolumes"
    }
    return $failed
}

Start-Transcript -LiteralPath $LogPath -Force | Out-Null
try {
    $failed = Invoke-SmokeMain
} finally {
    Stop-Transcript | Out-Null
}
Write-Host "Evidence log: $EvidenceLogPath"
if ($failed) { exit 1 }
exit 0
