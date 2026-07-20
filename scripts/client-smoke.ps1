[CmdletBinding()]
param(
    [ValidateSet("All", "Rclone", "Mc", "Aws")]
    [string]$Client = "All",
    [switch]$Run
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path Env:IPFS_S3_MASTER_KEY)) { $env:IPFS_S3_MASTER_KEY = "" }
if (-not (Test-Path Env:CLOUDFLARE_TUNNEL_TOKEN)) { $env:CLOUDFLARE_TUNNEL_TOKEN = "" }
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\docker-compose.yml"))

function Convert-ToDockerDesktopPath {
    param([Parameter(Mandatory)][string]$Path)
    $fullPath = [IO.Path]::GetFullPath($Path)
    $drive = $fullPath.Substring(0, 1).ToLowerInvariant()
    $rest = $fullPath.Substring(2).Replace("\", "/")
    return "/run/desktop/mnt/host/$drive$rest"
}

$DockerCommandTimeout = [TimeSpan]::FromMinutes(5)
$CargoVendorTimeout = [TimeSpan]::FromMinutes(5)
$TarTimeout = [TimeSpan]::FromMinutes(5)
$OfflineBuildTimeout = [TimeSpan]::FromMinutes(30)

function New-SmokeRunId {
    $timestamp = [DateTime]::UtcNow.ToString(
        "yyyyMMddTHHmmssfffZ",
        [Globalization.CultureInfo]::InvariantCulture
    ).ToLowerInvariant()
    $guidSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 8).ToLowerInvariant()
    $runId = "$timestamp-$PID-$guidSuffix"
    if ($runId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Generated invalid RunId: $runId"
    }
    return $runId
}

function Assert-CanonicalChildPath {
    param(
        [Parameter(Mandatory)][string]$ParentPath,
        [Parameter(Mandatory)][string]$ChildPath
    )
    $canonicalParent = [IO.Path]::GetFullPath($ParentPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalChild = [IO.Path]::GetFullPath($ChildPath)
    $prefix = $canonicalParent + [IO.Path]::DirectorySeparatorChar
    if (-not $canonicalChild.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path is outside the owned root: $canonicalChild"
    }
    return $canonicalChild
}

function New-SmokeRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Invalid RunId for RunRoot: $RunId"
    }
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    if (-not (Test-Path -LiteralPath $canonicalTemp -PathType Container)) {
        throw "Temporary root must already exist: $canonicalTemp"
    }
    $runRoot = Assert-CanonicalChildPath `
        -ParentPath $canonicalTemp `
        -ChildPath (Join-Path $canonicalTemp "ipfs-s3-client-smoke-$RunId")
    if (-not [IO.Path]::GetDirectoryName($runRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot must be a direct child of the temporary root: $runRoot"
    }
    if (Test-Path -LiteralPath $runRoot) { throw "RunRoot collision: $runRoot" }
    $claimPath = Assert-CanonicalChildPath `
        -ParentPath $canonicalTemp `
        -ChildPath "$runRoot.creation-lock"
    $claim = $null
    try {
        $claim = [IO.File]::Open(
            $claimPath,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None
        )
    } catch {
        throw "RunRoot creation failed or collided: $runRoot; $($_.Exception.Message)"
    }
    try {
        if (Test-Path -LiteralPath $runRoot) { throw "RunRoot collision: $runRoot" }
        try {
            $created = New-Item -ItemType Directory -Path $runRoot -ErrorAction Stop
        } catch {
            throw "RunRoot creation failed or collided: $runRoot; $($_.Exception.Message)"
        }
        if (-not $created.FullName.Equals($runRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Created RunRoot differs from requested path: $($created.FullName)"
        }
        return $runRoot
    } finally {
        $claim.Dispose()
        if (Test-Path -LiteralPath $claimPath -PathType Leaf) {
            Remove-Item -LiteralPath $claimPath -Force
        }
    }
}

function New-SmokeBucketName {
    param(
        [Parameter(Mandatory)][string]$Prefix,
        [Parameter(Mandatory)][string]$RunId
    )
    $bucket = "$Prefix-$RunId"
    if ($bucket.Length -gt 63 -or $bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid S3 bucket name: $bucket"
    }
    return $bucket
}

$TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$RunId = New-SmokeRunId
$RunRoot = New-SmokeRunRoot -TempRoot $TempRoot -RunId $RunId
$LogPath = Join-Path $RunRoot "client-smoke.log"
$FixturePath = Join-Path $RunRoot "file.txt"
$FixtureText = "ipfs-s3-client-smoke-$RunId"
$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$RunId"
$EvidenceRepoRoot = "<repo>"
$EvidenceLogPath = "client-smoke.log"
[IO.File]::WriteAllText($FixturePath, $FixtureText, [Text.UTF8Encoding]::new($false))

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

function Convert-NativeTextToLines {
    param([AllowNull()][string]$Text)
    if ([string]::IsNullOrEmpty($Text)) { return @() }
    return @($Text -split "\r?\n" | Where-Object { $_.Length -gt 0 })
}

function Get-NativeDiagnostic {
    param([string[]]$StdOut, [string[]]$StdErr)
    $diagnostic = (@($StdErr) + @($StdOut) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join " | "
    if ($diagnostic.Length -gt 2000) { return $diagnostic.Substring($diagnostic.Length - 2000) }
    return $diagnostic
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][TimeSpan]$Timeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    if ($Timeout -le [TimeSpan]::Zero -or $Timeout.TotalMilliseconds -gt [int]::MaxValue) {
        throw "$Label has an invalid timeout: $Timeout"
    }
    if ($AllowedExitCodes.Count -eq 0) { throw "$Label requires at least one allowed exit code" }

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true
    foreach ($argument in $ArgumentList) {
        $null = $startInfo.ArgumentList.Add($argument)
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        try {
            if (-not $process.Start()) { throw "Process.Start returned false" }
        } catch {
            throw "$Label failed to start '$FilePath': $($_.Exception.Message)"
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timeoutMilliseconds = [int][Math]::Ceiling($Timeout.TotalMilliseconds)
        if (-not $process.WaitForExit($timeoutMilliseconds)) {
            try {
                $process.Kill($true)
            } catch {
                throw "$Label timed out after $($Timeout.TotalSeconds) seconds and Kill(true) failed: $($_.Exception.Message)"
            }
            if (-not $process.WaitForExit(10000)) {
                throw "$Label timed out after $($Timeout.TotalSeconds) seconds and its process tree did not terminate within 10 seconds"
            }
            $stdout = @(Convert-NativeTextToLines ($stdoutTask.GetAwaiter().GetResult()))
            $stderr = @(Convert-NativeTextToLines ($stderrTask.GetAwaiter().GetResult()))
            $diagnostic = Get-NativeDiagnostic -StdOut $stdout -StdErr $stderr
            $suffix = if ([string]::IsNullOrWhiteSpace($diagnostic)) { "" } else { "; $diagnostic" }
            throw "$Label timed out after $($Timeout.TotalSeconds) seconds; process tree terminated$suffix"
        }
        $stdout = @(Convert-NativeTextToLines ($stdoutTask.GetAwaiter().GetResult()))
        $stderr = @(Convert-NativeTextToLines ($stderrTask.GetAwaiter().GetResult()))
        $exitCode = $process.ExitCode
        if ($exitCode -notin $AllowedExitCodes) {
            $diagnostic = Get-NativeDiagnostic -StdOut $stdout -StdErr $stderr
            $suffix = if ([string]::IsNullOrWhiteSpace($diagnostic)) { "" } else { "; $diagnostic" }
            throw "$Label exited $exitCode$suffix"
        }
        return [pscustomobject]@{
            ExitCode = $exitCode
            StdOut = $stdout
            StdErr = $stderr
        }
    } finally {
        $process.Dispose()
    }
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
$Selected = if ($Client -eq "All") { @("Rclone", "Mc", "Aws") } else { @($Client) }

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [string]$Label = ""
    )
    $displayCommand = Convert-ToPortableEvidence ("docker " + ($Arguments -join " "))
    Write-Host $displayCommand
    if ([string]::IsNullOrWhiteSpace($Label)) { $Label = "docker $($Arguments[0])" }
    $result = Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList $Arguments `
        -Label $Label `
        -Timeout $Timeout
    foreach ($line in @($result.StdOut) + @($result.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    return @($result.StdOut)
}

function Test-LocalImage {
    param([Parameter(Mandatory)][string]$Image)
    $result = Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList @("image", "inspect", $Image, "--format", "{{.Id}}") `
        -Label "inspect local image $Image" `
        -Timeout $DockerCommandTimeout `
        -AllowedExitCodes @(0, 1)
    return $result.ExitCode -eq 0
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
    $archiveContext = Join-Path $RunRoot "vendor-archive-context"
    $vendorArchive = Join-Path $archiveContext "vendor.tar.gz"
    $dockerfile = Join-Path $RunRoot "Dockerfile.gateway-runtime"

    Write-Host (Convert-ToPortableEvidence "cargo vendor --locked --offline $vendorPath")
    $vendorResult = Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @("vendor", "--locked", "--offline", $vendorPath) `
        -Label "cargo vendor offline dependency tree" `
        -Timeout $CargoVendorTimeout
    foreach ($line in @($vendorResult.StdOut) + @($vendorResult.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    Write-Host "cargo vendor completed from the local cache"

    if (Test-Path -LiteralPath $archiveContext) {
        throw "archive context already exists: $(Convert-ToPortableEvidence $archiveContext)"
    }
    $null = New-Item -ItemType Directory -Path $archiveContext
    if ($null -eq (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        throw "installed tar.exe is required for offline vendor transport"
    }
    $tarCommand = Convert-ToPortableEvidence "tar.exe -czf $vendorArchive -C $vendorPath ."
    Write-Host $tarCommand
    $tarResult = Invoke-NativeCommand `
        -FilePath "tar.exe" `
        -ArgumentList @("-czf", $vendorArchive, "-C", $vendorPath, ".") `
        -Label "create offline vendor archive" `
        -Timeout $TarTimeout
    foreach ($line in @($tarResult.StdOut) + @($tarResult.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    $archiveItems = @(Get-ChildItem -LiteralPath $archiveContext -Force)
    if ($archiveItems.Count -ne 1 -or $archiveItems[0].PSIsContainer -or
        -not $archiveItems[0].FullName.Equals($vendorArchive, [StringComparison]::OrdinalIgnoreCase)) {
        throw "vendor archive context must contain only vendor.tar.gz"
    }
    Write-Host (Convert-ToPortableEvidence "vendor archive completed: $vendorArchive")

    [IO.File]::WriteAllText(
        $dockerfile,
        @"
FROM rust:latest AS builder
WORKDIR /app
COPY --from=vendor-archive vendor.tar.gz /tmp/vendor.tar.gz
RUN mkdir /vendor && tar -xzf /tmp/vendor.tar.gz -C /vendor && rm /tmp/vendor.tar.gz
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo --config 'source.crates-io.replace-with="vendored-sources"' --config 'source.vendored-sources.directory="/vendor"' build --release --locked --offline --bin ipfs-s3-gateway

FROM $runtimeImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker `
        -Arguments @(
            "build", "--pull=false", "--network", "none", "--quiet",
            "--build-context", "vendor-archive=$archiveContext", "--tag", $runtimeImage,
            "--file", $dockerfile, $RepoRoot
        ) `
        -Timeout $OfflineBuildTimeout `
        -Label "offline gateway image build" | Out-Null
}

function BindMount {
    param([string]$Source, [string]$Target, [switch]$ReadOnly)
    $suffix = if ($ReadOnly) { ",readonly" } else { "" }
    return "type=bind,src=$Source,dst=$Target$suffix"
}

function Invoke-Rclone {
    param([string]$Network, [string]$ConfigPath, [string[]]$Arguments)
    $dockerArgs = @(
        "run", "--rm", "--pull=never", "--network", $Network,
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
        "run", "--rm", "--pull=never", "--network", $Network,
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
        "run", "--rm", "--pull=never", "--network", $Network,
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
    $bucket = New-SmokeBucketName -Prefix "ipfs-s3-rclone" -RunId $RunId
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
    Invoke-Docker @("run", "--rm", "--pull=never", $Images.Rclone, "version") | Out-Null
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
    $bucket = New-SmokeBucketName -Prefix "ipfs-s3-mc" -RunId $RunId
    $configDir = Join-Path $RunRoot "mc"
    Initialize-McAliases $configDir
    Invoke-Docker @("run", "--rm", "--pull=never", $Images.Mc, "--version") | Out-Null
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
    $bucket = New-SmokeBucketName -Prefix "ipfs-s3-aws" -RunId $RunId
    [IO.File]::WriteAllText(
        (Join-Path $RunRoot "delete.json"),
        '{"Objects":[{"Key":"a.txt"},{"Key":"nested/path/file.txt"},{"Key":"videos/file.txt"},{"Key":"missing"}],"Quiet":false}',
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @("run", "--rm", "--pull=never", $Images.Aws, "--version") | Out-Null
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

function Test-PreflightMustFail {
    param(
        [Parameter(Mandatory)][bool]$RunRequested,
        [Parameter(Mandatory)][ValidateRange(0, 3)][int]$RunnableClientCount,
        [Parameter(Mandatory)][bool]$PrerequisiteUnavailable
    )
    if (-not $RunRequested) { return $false }
    return $PrerequisiteUnavailable -or $RunnableClientCount -eq 0
}

function Remove-OwnedBuildArtifacts {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalRunRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRunRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-client-smoke-$RunId"))
    if (-not $canonicalRunRoot.Equals($expectedRunRoot, [StringComparison]::OrdinalIgnoreCase) -or
        -not [IO.Path]::GetDirectoryName($canonicalRunRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Cleanup RunRoot is not the exact invocation root: $canonicalRunRoot"
    }

    $owned = @(
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "vendor"; Kind = "Container" },
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "vendor-archive-context"; Kind = "Container" },
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "Dockerfile.gateway-runtime"; Kind = "Leaf" }
    )
    foreach ($artifact in $owned) {
        $canonicalArtifact = Assert-CanonicalChildPath -ParentPath $canonicalRunRoot -ChildPath $artifact.Path
        if (-not $canonicalArtifact.Equals([IO.Path]::GetFullPath($artifact.Path), [StringComparison]::OrdinalIgnoreCase)) {
            throw "Owned artifact canonical path mismatch: $canonicalArtifact"
        }
        if (Test-Path -LiteralPath $canonicalArtifact -PathType Container) {
            if ($artifact.Kind -ne "Container") { throw "Owned artifact type mismatch: $canonicalArtifact" }
            Remove-Item -LiteralPath $canonicalArtifact -Recurse -Force
        } elseif (Test-Path -LiteralPath $canonicalArtifact -PathType Leaf) {
            if ($artifact.Kind -ne "Leaf") { throw "Owned artifact type mismatch: $canonicalArtifact" }
            Remove-Item -LiteralPath $canonicalArtifact -Force
        } elseif (Test-Path -LiteralPath $canonicalArtifact) {
            throw "Owned artifact has an unsupported filesystem type: $canonicalArtifact"
        }
    }
}

function Invoke-SmokeMain {
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker-compose.yml is missing" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker command is missing" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    try {
        $dockerInfo = Invoke-NativeCommand `
            -FilePath "docker" `
            -ArgumentList @("info", "--format", "{{.ServerVersion}}") `
            -Label "docker daemon preflight" `
            -Timeout $DockerCommandTimeout `
            -AllowedExitCodes @(0, 1)
    } catch {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon preflight failed: $($_.Exception.Message)" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    if ($dockerInfo.ExitCode -ne 0) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon is unavailable" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }

    $missingClients = @{}
    foreach ($name in $Selected) {
        if (-not (Test-LocalImage $Images[$name])) { $missingClients[$name] = $Images[$name] }
    }
    if (-not $Run) {
        foreach ($name in $Selected) {
            if ($missingClients.ContainsKey($name)) {
                Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
            } else {
                Write-Result $name "SKIPPED" "execution not requested; rerun with -Run"
            }
        }
        return $false
    }

    $runnable = @($Selected | Where-Object { -not $missingClients.ContainsKey($_) })
    foreach ($name in $Selected | Where-Object { $missingClients.ContainsKey($_) }) {
        Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
    }
    if ($runnable.Count -eq 0) {
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount 0 -PrerequisiteUnavailable $false)
    }

    $missingServices = @($ServiceImages | Where-Object { -not (Test-LocalImage $_) })
    if ($missingServices.Count -gt 0) {
        foreach ($name in $runnable) { Write-Result $name "SKIPPED" "local service image missing: $($missingServices -join ', ')" }
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount $runnable.Count -PrerequisiteUnavailable $true)
    }
    if (-not (Test-LocalImage "rust:latest")) {
        foreach ($name in $runnable) { Write-Result $name "SKIPPED" "local build image missing: rust:latest" }
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount $runnable.Count -PrerequisiteUnavailable $true)
    }

    try {
        Write-Host "Building the current gateway source from an archive-only offline vendored dependency context."
        Invoke-OfflineGatewayBuild
        Invoke-Docker @(
            "compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway"
        ) | Out-Null
        Wait-GatewayHealthy
        Invoke-Docker @("compose", "-f", $ComposeFile, "ps") | Out-Null
        $network = Get-ComposeNetwork
    } catch {
        $stackError = Convert-ToPortableEvidence $_.Exception.Message
        foreach ($name in $runnable) { Write-Result $name "FAILED" "stack setup failed: $stackError" }
        Write-Host "Stack setup failed before client execution; per-client smoke loop was not entered."
        Write-Host "Compose services and volumes preserved for diagnosis."
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
            Write-Host "Manual cleanup endpoint: http://127.0.0.1:9000 bucket prefix ipfs-s3-$($name.ToLower())-$RunId"
        }
    }
    Write-Host "Compose services and volumes preserved."
    return $failed
}

$exitCode = 1
$transcriptStarted = $false
try {
    Start-Transcript -LiteralPath $LogPath | Out-Null
    $transcriptStarted = $true
    $failed = Invoke-SmokeMain
    $exitCode = if ($failed) { 1 } else { 0 }
} catch {
    Write-Host "Runner failed: $(Convert-ToPortableEvidence $_.Exception.Message)"
    $exitCode = 1
} finally {
    if ($transcriptStarted) {
        try {
            Stop-Transcript | Out-Null
        } catch {
            Write-Host "Stop-Transcript failed: $($_.Exception.Message)"
            $exitCode = 1
        }
    }
    try {
        Remove-OwnedBuildArtifacts -TempRoot $TempRoot -RunRoot $RunRoot -RunId $RunId
    } catch {
        Write-Host "Owned build artifact cleanup failed: $($_.Exception.Message)"
        $exitCode = 1
    }
}
Write-Host "Evidence log: $EvidenceLogPath"
exit $exitCode
