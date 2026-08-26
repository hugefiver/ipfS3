[CmdletBinding()]
param(
    [switch]$Run
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $Run) {
    Write-Host "[RESULT] object-versioning-client=NOT RUN reason=execution-not-requested"
    exit 0
}

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\tests\compose.object-versioning-validation.yml"))
$ExpectedSpecSha256 = "56e53b0983ac9f244c5379a50454c6f7e64ab4105ceccc45f9c5ed5cf34ef489"
$GatewayRuntimeBaseImage = "ghcr.io/hugefiver/ipfs3:latest"
$AwsImage = "amazon/aws-cli:latest"
$VersioningPorts = @(55436, 55003, 59003)
$TouchedEnvironmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "IPFS_S3_OBJECT_VERSIONING_POSTGRES_PORT",
    "IPFS_S3_OBJECT_VERSIONING_KUBO_PORT",
    "IPFS_S3_OBJECT_VERSIONING_GATEWAY_PORT",
    "IPFS_S3_OBJECT_VERSION_IMAGE",
    "IPFS_S3_TEST_POSTGRES_URL",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL"
)
$RustEnvironmentNames = @(
    "IPFS_S3_TEST_POSTGRES_URL",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL"
)
$DockerCommandTimeout = [TimeSpan]::FromMinutes(5)
$ComposeStartupTimeout = [TimeSpan]::FromMinutes(6)
$CargoVendorTimeout = [TimeSpan]::FromMinutes(5)
$TarTimeout = [TimeSpan]::FromMinutes(5)
$OfflineBuildTimeout = [TimeSpan]::FromMinutes(30)

function Write-Evidence {
    param(
        [Parameter(Mandatory)][ValidateSet("metadata", "command", "assertion", "diagnostic", "cleanup", "result")][string]$Category,
        [Parameter(Mandatory)][string]$Value
    )
    if ($Value -notmatch '^[A-Za-z0-9._:=/ -]+$') {
        throw "Evidence value is not safely redacted"
    }
    Write-Host "[EVIDENCE] object-versioning category=$Category value=$Value"
}

function Set-VersioningStage {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("preflight", "config", "offline-build", "compose-up", "health", "network", "metadata", "postgres", "e2e", "aws", "cleanup")][string]$Stage
    )
    $State.Stage = $Stage
    Write-Evidence -Category "diagnostic" -Value "stage=$Stage"
}

function New-VersioningRunId {
    $timestamp = [DateTime]::UtcNow.ToString(
        "yyyyMMddTHHmmssfffZ",
        [Globalization.CultureInfo]::InvariantCulture
    ).ToLowerInvariant()
    $guidSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 8).ToLowerInvariant()
    $runId = "$timestamp-$PID-$guidSuffix"
    if ($runId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Generated invalid object-versioning RunId"
    }
    return $runId
}

function New-VersioningProjectName {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Invalid object-versioning RunId"
    }
    $project = "ipfs3-ver-$RunId"
    if ($project -cnotmatch '^[a-z0-9][a-z0-9_-]*$') {
        throw "Invalid object-versioning Compose project"
    }
    return $project
}

function New-VersioningBucketName {
    param([Parameter(Mandatory)][string]$RunId)
    $bucket = "ipfs3-ver-$RunId"
    if ($bucket.Length -gt 63 -or $bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid object-versioning bucket name"
    }
    return $bucket
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
        throw "Path is outside the owned root"
    }
    return $canonicalChild
}

function Get-CanonicalExistingDirectory {
    param([Parameter(Mandatory)][string]$Path)
    $canonicalPath = [IO.Path]::GetFullPath($Path).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    if (-not (Test-Path -LiteralPath $canonicalPath -PathType Container)) {
        throw "Working directory is unavailable"
    }
    return $canonicalPath
}

function New-VersioningRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Invalid object-versioning RunId for temporary root"
    }
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    if (-not (Test-Path -LiteralPath $canonicalTemp -PathType Container)) {
        throw "Temporary root is unavailable"
    }
    $runRoot = Assert-CanonicalChildPath `
        -ParentPath $canonicalTemp `
        -ChildPath (Join-Path $canonicalTemp "ipfs-s3-object-versioning-$RunId")
    if (-not [IO.Path]::GetDirectoryName($runRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot must be a direct child of the temporary root"
    }
    if (Test-Path -LiteralPath $runRoot) {
        throw "BLOCKED object-versioning RunRoot or receipt already exists"
    }
    try {
        $created = New-Item -ItemType Directory -Path $runRoot -ErrorAction Stop
    } catch {
        throw "BLOCKED object-versioning RunRoot creation collided"
    }
    if (-not $created.FullName.Equals($runRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot creation returned an unexpected location"
    }
    return $runRoot
}

function New-VersioningOwnershipReceipt {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $receipt = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "ownership-receipt")
    $stream = $null
    try {
        $stream = [IO.File]::Open(
            $receipt,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None
        )
        $bytes = [Text.Encoding]::UTF8.GetBytes("object-versioning-owned:$RunId")
        $stream.Write($bytes, 0, $bytes.Length)
    } catch {
        throw "BLOCKED object-versioning ownership receipt could not be claimed"
    } finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }
    return $receipt
}

function New-VersioningAwsConfig {
    param([Parameter(Mandatory)][string]$RunRoot)
    $configPath = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "aws-config")
    if (Test-Path -LiteralPath $configPath) {
        throw "Owned AWS config path unexpectedly exists"
    }
    $configContent = "[default]`nregion = us-east-1`ns3 =`n    addressing_style = path`n"
    [IO.File]::WriteAllText($configPath, $configContent, [Text.UTF8Encoding]::new($false))
    return $configPath
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][TimeSpan]$Timeout,
        [int[]]$AllowedExitCodes = @(0),
        [string]$WorkingDirectory
    )
    if ($Timeout -le [TimeSpan]::Zero -or $Timeout.TotalMilliseconds -gt [int]::MaxValue) {
        throw "$Label has an invalid timeout"
    }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true
    if (-not [string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $startInfo.WorkingDirectory = Get-CanonicalExistingDirectory -Path $WorkingDirectory
    }
    foreach ($argument in $ArgumentList) {
        $null = $startInfo.ArgumentList.Add($argument)
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw "$Label could not start" }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timeoutMilliseconds = [int][Math]::Ceiling($Timeout.TotalMilliseconds)
        if (-not $process.WaitForExit($timeoutMilliseconds)) {
            $process.Kill($true)
            if (-not $process.WaitForExit(10000)) { throw "$Label timed out and did not terminate" }
            throw "$Label timed out"
        }
        $stdout = @($stdoutTask.GetAwaiter().GetResult() -split "\r?\n" | Where-Object { $_.Length -gt 0 })
        $stderr = @($stderrTask.GetAwaiter().GetResult() -split "\r?\n" | Where-Object { $_.Length -gt 0 })
        if ($process.ExitCode -notin $AllowedExitCodes) { throw "$Label failed" }
        return [pscustomobject]@{ ExitCode = $process.ExitCode; StdOut = $stdout; StdErr = $stderr }
    } finally {
        $process.Dispose()
    }
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Label = "docker operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList $Arguments `
        -Label $Label `
        -Timeout $Timeout `
        -AllowedExitCodes $AllowedExitCodes
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory)][string]$Project,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Label = "Compose operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker `
        -Arguments (@("compose", "--project-name", $Project, "--file", $ComposeFile) + $Arguments) `
        -Label $Label `
        -Timeout $Timeout `
        -AllowedExitCodes $AllowedExitCodes
}

function Invoke-Cargo {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Label,
        [TimeSpan]$Timeout = $OfflineBuildTimeout
    )
    return Invoke-NativeCommand -FilePath "cargo" -ArgumentList $Arguments -Label $Label -Timeout $Timeout
}

function Test-LocalImage {
    param([Parameter(Mandatory)][string]$Image)
    $result = Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList @("image", "inspect", $Image, "--format", "{{.Id}}") `
        -Label "inspect local image" `
        -Timeout $DockerCommandTimeout `
        -AllowedExitCodes @(0, 1)
    if ($result.ExitCode -ne 0) { return [pscustomobject]@{ Exists = $false; ImageId = $null } }
    $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($ids.Count -ne 1 -or $ids[0] -notmatch '^sha256:[0-9a-f]{64}$') {
        throw "Local image inspection returned an invalid image identity"
    }
    return [pscustomobject]@{ Exists = $true; ImageId = $ids[0] }
}

function Assert-RequiredTools {
    foreach ($tool in @("pwsh", "docker", "cargo", "tar.exe")) {
        if ($null -eq (Get-Command $tool -ErrorAction SilentlyContinue)) {
            throw "Required local tool is unavailable"
        }
    }
}

function Assert-ComposeVersion {
    $result = Invoke-Docker `
        -Arguments @("compose", "version", "--short") `
        -Label "Docker Compose version preflight"
    $versionText = (@($result.StdOut) -join "`n")
    $composeVersionMatch = [regex]::Match($versionText, '^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$')
    $composeVersion = $null
    if (-not $composeVersionMatch.Success -or
        -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or
        $composeVersion -lt [Version]"2.23.1") {
        throw "Docker Compose 2.23.1 or newer is required"
    }
}

function Assert-RequiredLocalImages {
    $images = @(
        "postgres:17",
        "ghcr.io/hugefiver/ipfs3-kubo:latest",
        "ghcr.io/hugefiver/ipfs3:latest",
        "rust:latest",
        "amazon/aws-cli:latest"
    )
    $identities = @{}
    foreach ($image in $images) {
        $inspection = Test-LocalImage -Image $image
        if (-not $inspection.Exists) { throw "Required local image is unavailable" }
        $identities[$image] = $inspection.ImageId
    }
    return $identities
}

function Assert-ProjectResourcesAbsent {
    param([Parameter(Mandatory)][string]$Project)
    $queries = @(
        [pscustomobject]@{ Name = "containers"; Arguments = @("ps", "-aq", "--filter", "label=com.docker.compose.project=$Project") },
        [pscustomobject]@{ Name = "networks"; Arguments = @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project") },
        [pscustomobject]@{ Name = "volumes"; Arguments = @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project") }
    )
    $failures = [Collections.Generic.List[string]]::new()
    foreach ($query in $queries) {
        try {
            $result = Invoke-Docker -Arguments $query.Arguments -Label "project label preflight"
            $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            if ($ids.Count -ne 0) { $failures.Add($query.Name) }
        } catch {
            $failures.Add($query.Name)
        }
    }
    if ($failures.Count -ne 0) { throw "BLOCKED object-versioning project label preflight" }
}

function Test-ProjectResourcesExist {
    param([Parameter(Mandatory)][string]$Project)
    $found = $false
    foreach ($arguments in @(
        @("ps", "-aq", "--filter", "label=com.docker.compose.project=$Project"),
        @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project"),
        @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project")
    )) {
        $result = Invoke-Docker -Arguments $arguments -Label "project ownership probe"
        if (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) { $found = $true }
    }
    return $found
}

function Claim-VersioningProjectOwnership {
    param([Parameter(Mandatory)][hashtable]$State)
    if (Test-ProjectResourcesExist -Project $State.Project) {
        $State.ProjectOwned = $true
    }
}

function Assert-LoopbackPortsFree {
    param([Parameter(Mandatory)][int[]]$Ports)
    $listeners = [Collections.Generic.List[System.Net.Sockets.TcpListener]]::new()
    try {
        foreach ($port in $Ports) {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
            $listener.Start()
            $listeners.Add($listener)
        }
    } catch {
        throw "Required loopback port is unavailable"
    } finally {
        foreach ($listener in $listeners) { $listener.Stop() }
    }
}

function Save-EnvironmentState {
    param([Parameter(Mandatory)][string[]]$Names)
    $state = [Collections.Generic.List[object]]::new()
    foreach ($name in $Names) {
        $path = "Env:$name"
        $present = Test-Path -LiteralPath $path
        $state.Add([pscustomobject]@{
            Name = $name
            Present = $present
            Value = if ($present) { (Get-Item -LiteralPath $path).Value } else { $null }
        })
    }
    return @($state)
}

function Set-RunEnvironment {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Value
    )
    Set-Item -LiteralPath "Env:$Name" -Value $Value
}

function Restore-EnvironmentState {
    param(
        [Parameter(Mandatory)][object[]]$State,
        [string[]]$Names = @()
    )
    foreach ($entry in $State) {
        $name = [string]$entry.Name
        if ($Names.Count -ne 0 -and $name -notin $Names) { continue }
        if ([bool]$entry.Present) {
            Set-Item -LiteralPath "Env:$name" -Value ([string]$entry.Value)
        } elseif (Test-Path -LiteralPath "Env:$name") {
            Remove-Item -LiteralPath "Env:$name" -Force
        }
    }
}

function Invoke-OfflineGatewayBuild {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$GatewayImage
    )
    $vendorPath = Join-Path $RunRoot "vendor"
    $archiveContext = Join-Path $RunRoot "vendor-archive-context"
    $vendorArchive = Join-Path $archiveContext "vendor.tar.gz"
    $dockerfile = Join-Path $RunRoot "Dockerfile.gateway-runtime"
    if ((Test-Path -LiteralPath $vendorPath) -or (Test-Path -LiteralPath $archiveContext)) {
        throw "Owned offline build path unexpectedly exists"
    }
    Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @("vendor", "--locked", "--offline", $vendorPath) `
        -Label "offline Cargo vendoring" `
        -Timeout $CargoVendorTimeout `
        -WorkingDirectory $RepoRoot | Out-Null
    $null = New-Item -ItemType Directory -Path $archiveContext -ErrorAction Stop
    Invoke-NativeCommand `
        -FilePath "tar.exe" `
        -ArgumentList @("-czf", $vendorArchive, "-C", $vendorPath, ".") `
        -Label "offline vendor archive" `
        -Timeout $TarTimeout | Out-Null
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

FROM $GatewayRuntimeBaseImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker `
        -Arguments @(
            "build", "--pull=false", "--network", "none", "--quiet",
            "--build-context", "vendor-archive=$archiveContext", "--tag", $GatewayImage,
            "--file", $dockerfile, $RepoRoot
        ) `
        -Label "offline gateway image build" `
        -Timeout $OfflineBuildTimeout | Out-Null
}

function Get-ComposeGatewayContainer {
    param([Parameter(Mandatory)][string]$Project)
    $result = Invoke-Compose -Project $Project -Arguments @("ps", "-q", "gateway") -Label "gateway container lookup"
    $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($ids.Count -ne 1) { throw "Gateway container lookup was not unique" }
    return $ids[0].Trim()
}

function Wait-GatewayHealthy {
    param([Parameter(Mandatory)][string]$Project)
    $gatewayId = Get-ComposeGatewayContainer -Project $Project
    for ($attempt = 0; $attempt -lt 36; $attempt++) {
        $result = Invoke-Docker `
            -Arguments @("inspect", "--format", "{{.State.Health.Status}}", $gatewayId) `
            -Label "gateway health probe"
        $health = (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join "").Trim()
        if ($health -eq "healthy") { return }
        if ($health -eq "unhealthy") { throw "Gateway healthcheck is unhealthy" }
        Start-Sleep -Seconds 5
    }
    throw "Gateway did not become healthy"
}

function Get-ComposeNetwork {
    param([Parameter(Mandatory)][string]$Project)
    $gatewayId = Get-ComposeGatewayContainer -Project $Project
    $result = Invoke-Docker `
        -Arguments @("inspect", "--format", "{{json .NetworkSettings.Networks}}", $gatewayId) `
        -Label "gateway network lookup"
    $json = (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join "") | ConvertFrom-Json
    $networks = @($json.PSObject.Properties.Name)
    if ($networks.Count -ne 1) { throw "Gateway network lookup was not unique" }
    return $networks[0]
}

function Assert-RustSuiteExecuted {
    param(
        [Parameter(Mandatory)][object]$Result,
        [Parameter(Mandatory)][string]$Name
    )
    $output = (@($Result.StdOut) + @($Result.StdErr)) -join "`n"
    if ($output -notmatch '(?m)^running [1-9][0-9]* tests$' -or
        $output -notmatch 'test result: ok\.' -or
        $output -match '(?i)skipping PostgreSQL object-versioning test') {
        throw "$Name did not prove real, non-skipped Rust tests"
    }
}

function Invoke-RustEvidenceSuites {
    param([Parameter(Mandatory)][hashtable]$State)
    try {
        Set-VersioningStage -State $State -Stage "postgres"
        Set-RunEnvironment -Name "IPFS_S3_TEST_POSTGRES_URL" -Value "postgres://ipfs3:ipfs3@127.0.0.1:55436/ipfs3"
        Write-Evidence -Category "command" -Value "cargo test --test postgres_versioning -- --nocapture --test-threads=1"
        $postgresResult = Invoke-Cargo `
            -Arguments @("test", "--test", "postgres_versioning", "--", "--nocapture", "--test-threads=1") `
            -Label "PostgreSQL object-versioning test"
        Assert-RustSuiteExecuted -Result $postgresResult -Name "PostgreSQL object-versioning test"
        Write-Evidence -Category "result" -Value "postgres-versioning=passed"

        Set-VersioningStage -State $State -Stage "e2e"
        Set-RunEnvironment -Name "IPFS_S3_E2E_ENDPOINT" -Value "http://127.0.0.1:59003"
        Set-RunEnvironment -Name "IPFS_S3_E2E_KUBO_URL" -Value "http://127.0.0.1:55003"
        Write-Evidence -Category "command" -Value "cargo test --test e2e -- --nocapture --test-threads=1"
        $e2eResult = Invoke-Cargo `
            -Arguments @("test", "--test", "e2e", "--", "--nocapture", "--test-threads=1") `
            -Label "existing serial end-to-end test"
        Assert-RustSuiteExecuted -Result $e2eResult -Name "existing serial end-to-end test"
        Write-Evidence -Category "result" -Value "e2e=passed"
    } finally {
        Restore-EnvironmentState -State $State.EnvironmentState -Names $RustEnvironmentNames
        $State.RustEnvironmentRestored = $true
    }
}

function Invoke-Aws {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker `
        -Arguments (@(
            "run", "--rm", "--pull=never", "--network", $Network,
            "-e", "AWS_ACCESS_KEY_ID=test",
            "-e", "AWS_SECRET_ACCESS_KEY=test",
            "-e", "AWS_DEFAULT_REGION=us-east-1",
            "-e", "AWS_EC2_METADATA_DISABLED=true",
            "-e", "AWS_CONFIG_FILE=/work/aws-config",
            "--mount", "type=bind,src=$RunRoot,dst=/work",
            $AwsImage,
            "--endpoint-url", $Endpoint
        ) + $Arguments) `
        -Label "AWS CLI object-versioning assertion" `
        -AllowedExitCodes $AllowedExitCodes
}

function Invoke-AwsJson {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments,
        [ValidateSet("suspended-list")][string]$SafeReceipt = ""
    )
    if ($Arguments.Count -eq 0 -or $Arguments[0] -ne "s3api" -or
        @($Arguments | Where-Object { $_ -eq "--output" }).Count -ne 0) {
        throw "AWS JSON invocation requires one s3api command without an output option"
    }
    if ([string]::IsNullOrEmpty($SafeReceipt)) {
        $result = Invoke-Aws `
            -Network $Network `
            -RunRoot $RunRoot `
            -Endpoint $Endpoint `
            -Arguments (@("--output", "json") + $Arguments)
        $json = (@($result.StdOut) -join "`n")
        try {
            return $json | ConvertFrom-Json
        } catch {
            throw "AWS CLI returned non-JSON where JSON was required"
        }
    } else {
        if ($SafeReceipt -cne "suspended-list") { throw "AWS JSON receipt is not allowlisted" }
        $result = Invoke-Aws `
            -Network $Network `
            -RunRoot $RunRoot `
            -Endpoint $Endpoint `
            -Arguments (@("--output", "json") + $Arguments) `
            -AllowedExitCodes @(0, 1, 2, 252, 253, 254, 255)
        Write-Evidence -Category "diagnostic" -Value "aws-json-command-exit=$($result.ExitCode)"
        if ($result.ExitCode -ne 0) { throw "AWS JSON receipt request failed" }
        $json = (@($result.StdOut) -join "`n")
        try {
            $parsedJson = $json | ConvertFrom-Json -NoEnumerate
        } catch {
            Write-Evidence -Category "diagnostic" -Value "aws-json-parse=failed"
            throw "AWS CLI returned non-JSON where JSON was required"
        }
        Write-Evidence -Category "diagnostic" -Value "aws-json-parse=passed"
        [int]$stdoutLineCount = @($result.StdOut).Count
        [int]$stdoutCharacterCount = $json.Length
        if ($stdoutLineCount -lt 0 -or $stdoutCharacterCount -lt 0) { throw "AWS JSON receipt metrics were invalid" }
        Write-Evidence -Category "diagnostic" -Value "aws-json-stdout-lines=$stdoutLineCount"
        Write-Evidence -Category "diagnostic" -Value "aws-json-stdout-chars=$stdoutCharacterCount"
        $trimmedJson = $json.Trim()
        $shape = if ($trimmedJson -ceq "null") {
            "null"
        } elseif ($trimmedJson.StartsWith("{", [StringComparison]::Ordinal)) {
            "object"
        } elseif ($trimmedJson.StartsWith("[", [StringComparison]::Ordinal)) {
            "array"
        } else {
            "scalar"
        }
        Write-Evidence -Category "diagnostic" -Value "aws-json-shape=$shape"
        if ($shape -ceq "object") {
            $propertyNames = @($parsedJson.PSObject.Properties.Name)
            [int]$versionsPropertyCount = if ($propertyNames -ccontains "Versions") { 1 } else { 0 }
            [int]$markersPropertyCount = if ($propertyNames -ccontains "DeleteMarkers") { 1 } else { 0 }
            Write-Evidence -Category "diagnostic" -Value "aws-json-versions-property=$versionsPropertyCount"
            Write-Evidence -Category "diagnostic" -Value "aws-json-markers-property=$markersPropertyCount"
        }
        return $parsedJson
    }
}

function Assert-CanonicalOpaqueVersionId {
    param([Parameter(Mandatory)][string]$VersionId)
    if ($VersionId -ceq "null" -or $VersionId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') {
        throw "AWS VersionId is not a canonical opaque UUID"
    }
}

function Assert-CidEtag {
    param([Parameter(Mandatory)][string]$Etag)
    if ($Etag.Trim('"') -notmatch '^(Qm|baf)') { throw "AWS ETag is not CID-shaped" }
}

function Get-AwsCliErrorCode {
    param([Parameter(Mandatory)][string[]]$Stderr)
    $documentText = (@($Stderr) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($documentText) -or $documentText.Length -gt 8192) {
        throw "AWS CLI error document is absent or exceeds the bounded size"
    }
    try {
        $document = $documentText | ConvertFrom-Json -NoEnumerate
    } catch {
        throw "AWS CLI error document is not one JSON object"
    }
    if ($document -is [Array] -or $document -isnot [pscustomobject] -or
        @($document.PSObject.Properties.Match("Code")).Count -ne 1) {
        throw "AWS CLI error document does not have one top-level Code"
    }
    $code = $document.Code
    if ($code -isnot [string] -or $code -notin @("NoSuchKey", "404", "405")) {
        throw "AWS CLI error Code is not allowlisted"
    }
    return $code
}

function Assert-AwsExpectedFailure {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][ValidateSet("NoSuchKey", "404", "405")][string]$ExpectedCode
    )
    if ($Arguments.Count -eq 0 -or $Arguments[0] -ne "s3api" -or
        @($Arguments | Where-Object { $_ -eq "--cli-error-format" }).Count -ne 0) {
        throw "AWS expected failure requires one s3api command without a CLI error format option"
    }
    $result = Invoke-Aws `
        -Network $Network `
        -RunRoot $RunRoot `
        -Endpoint $Endpoint `
        -Arguments (@("--cli-error-format", "json") + $Arguments) `
        -AllowedExitCodes @(0, 1, 2, 252, 253, 254, 255)
    if ($result.ExitCode -eq 0) { throw "AWS request unexpectedly succeeded" }
    $code = Get-AwsCliErrorCode -Stderr $result.StdErr
    if ($code -cne $ExpectedCode) {
        throw "AWS failure did not have the expected error Code"
    }
    Write-Evidence -Category "assertion" -Value "aws-error-code=$code"
}

function Get-AwsEntries {
    param([AllowNull()][object]$Entries)
    return @($Entries | Where-Object { $null -ne $_ })
}

function Write-AwsSubstage {
    param([Parameter(Mandatory)][ValidateSet("cli-version", "create-bucket", "enable-versioning", "put-first", "assert-first", "put-second", "assert-second", "simple-delete", "assert-delete-marker", "list-enabled", "assert-enabled-order", "get-historical", "assert-historical", "current-get-nosuchkey", "current-head-404", "marker-head-405", "delete-marker-exact", "get-promoted", "assert-promoted", "suspend-versioning", "put-null-first", "assert-null-first", "put-null-second", "assert-null-second", "suspended-list", "suspended-list-assert", "exact-cleanup-delete", "empty-list", "assert-empty", "delete-bucket", "complete")][string]$Label)
    Write-Evidence -Category "assertion" -Value "aws-substage=$Label"
}

function Write-PostgresVersionSnapshot {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("after-second-enabled-put", "after-simple-marker", "after-exact-marker-delete-and-restored-get", "after-versioning-suspension", "after-first-null-put", "after-second-null-put", "after-suspended-list-parse")][string]$SnapshotStage,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$Key
    )
    if ($Bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Database snapshot bucket is invalid"
    }
    if ($Key -cnotmatch '^versioned-object\.txt$') {
        throw "Database snapshot key is invalid"
    }
    $query = @"
SELECT
    COUNT(*)::text,
    COUNT(*) FILTER (WHERE versions.version_id IS NOT NULL)::text,
    COUNT(*) FILTER (WHERE versions.version_id IS NULL)::text,
    COUNT(*) FILTER (WHERE versions.kind = 'delete_marker')::text,
    COUNT(*) FILTER (WHERE versions.is_latest)::text,
    COUNT(*) FILTER (WHERE versions.object_id IS NOT NULL)::text,
    (SELECT COUNT(*) FROM objects AS current_objects WHERE current_objects.bucket = :'bucket' AND current_objects.key = :'key')::text,
    COALESCE(string_agg(versions.sequence::text, '.' ORDER BY versions.sequence ASC), 'none')
FROM object_versions AS versions
LEFT JOIN objects AS linked ON linked.id = versions.object_id
WHERE versions.bucket = :'bucket' AND versions.key = :'key';
"@
    $result = Invoke-Compose -Project $State.Project -Arguments @(
        "exec", "-T", "postgres", "psql", "-X", "-U", "ipfs3", "-d", "ipfs3", "-v", "ON_ERROR_STOP=1", "-A", "-t", "-F", "|", "-v", "bucket=$Bucket", "-v", "key=$Key", "-c", $query
    ) -Label "versioning database snapshot" -Timeout ([TimeSpan]::FromSeconds(30))
    $rows = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($rows.Count -ne 1) { throw "Database snapshot did not return exactly one row" }
    $fields = $rows[0].Split('|')
    if ($fields.Count -ne 8) { throw "Database snapshot did not return eight fields" }
    $counts = [Collections.Generic.List[Int64]]::new()
    foreach ($index in 0..6) {
        [Int64]$parsedCount = 0
        if ($fields[$index] -cnotmatch '^[0-9]+$' -or
            -not [Int64]::TryParse($fields[$index], [Globalization.NumberStyles]::None, [Globalization.CultureInfo]::InvariantCulture, [ref]$parsedCount) -or
            $parsedCount -lt 0) {
            throw "Database snapshot has an invalid count"
        }
        $null = $counts.Add($parsedCount)
    }
    [Int64]$versionCount = $counts[0]
    [Int64]$opaqueCount = $counts[1]
    [Int64]$nullCount = $counts[2]
    [Int64]$markerCount = $counts[3]
    [Int64]$latestCount = $counts[4]
    [Int64]$linkedCount = $counts[5]
    [Int64]$objectCount = $counts[6]
    $sequence = $fields[7]
    if ($sequence -cne "none") {
        if ($sequence.Contains(',') -or $sequence -cnotmatch '^[1-9][0-9]*(?:\.[1-9][0-9]*)*$') {
            throw "Database snapshot has an invalid sequence"
        }
        [Int64]$previousSequence = 0
        foreach ($sequencePart in $sequence.Split([char]46)) {
            [Int64]$currentSequence = 0
            if (-not [Int64]::TryParse($sequencePart, [Globalization.NumberStyles]::None, [Globalization.CultureInfo]::InvariantCulture, [ref]$currentSequence) -or
                $currentSequence -le $previousSequence) {
                throw "Database snapshot has a non-increasing sequence"
            }
            $previousSequence = $currentSequence
        }
    }
    Write-Evidence -Category "diagnostic" -Value "db-snapshot-stage=$SnapshotStage"
    Write-Evidence -Category "diagnostic" -Value "db-total-version-rows=$versionCount"
    Write-Evidence -Category "diagnostic" -Value "db-opaque-rows=$opaqueCount"
    Write-Evidence -Category "diagnostic" -Value "db-null-rows=$nullCount"
    Write-Evidence -Category "diagnostic" -Value "db-delete-marker-rows=$markerCount"
    Write-Evidence -Category "diagnostic" -Value "db-latest-rows=$latestCount"
    Write-Evidence -Category "diagnostic" -Value "db-linked-object-rows=$linkedCount"
    Write-Evidence -Category "diagnostic" -Value "db-object-rows=$objectCount"
    Write-Evidence -Category "diagnostic" -Value "db-sequences=$sequence"
}

function Invoke-AwsVersioningSmoke {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $endpoint = "http://gateway:9000"
    $bucket = New-VersioningBucketName -RunId $RunId
    $key = "versioned-object.txt"
    $firstBody = "first-body-$RunId"
    $secondBody = "second-body-$RunId"
    $thirdBody = "third-body-$RunId"
    [IO.File]::WriteAllText((Join-Path $RunRoot "first.txt"), $firstBody, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText((Join-Path $RunRoot "second.txt"), $secondBody, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText((Join-Path $RunRoot "third.txt"), $thirdBody, [Text.UTF8Encoding]::new($false))

    Write-AwsSubstage -Label "cli-version"
    $awsCliVersion = Get-AwsCliVersion -Network $Network -RunRoot $RunRoot
    Write-Evidence -Category "metadata" -Value "aws-cli=$awsCliVersion"
    Write-Evidence -Category "command" -Value "aws-cli-versioning-configured"
    Write-AwsSubstage -Label "create-bucket"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Write-AwsSubstage -Label "enable-versioning"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled"
    ) | Out-Null
    Write-AwsSubstage -Label "put-first"
    $first = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/first.txt"
    )
    Write-AwsSubstage -Label "assert-first"
    if ($null -eq $first) { throw "First put did not return JSON" }
    $firstId = [string]$first.VersionId
    Assert-CanonicalOpaqueVersionId $firstId
    Assert-CidEtag ([string]$first.ETag)
    Write-AwsSubstage -Label "put-second"
    $second = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/second.txt"
    )
    if ($null -eq $second) { throw "Second put did not return JSON" }
    Write-AwsSubstage -Label "assert-second"
    $secondId = [string]$second.VersionId
    Assert-CanonicalOpaqueVersionId $secondId
    Assert-CidEtag ([string]$second.ETag)
    if ($firstId -ceq $secondId) { throw "Opaque object VersionIds must be distinct" }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-second-enabled-put" -Bucket $bucket -Key $key

    Write-AwsSubstage -Label "simple-delete"
    $delete = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "delete-object", "--bucket", $bucket, "--key", $key
    )
    if ($null -eq $delete) { throw "Simple delete did not return JSON" }
    Write-AwsSubstage -Label "assert-delete-marker"
    if (-not [bool]$delete.DeleteMarker) { throw "Simple delete did not return DeleteMarker=true" }
    $markerId = [string]$delete.VersionId
    Assert-CanonicalOpaqueVersionId $markerId
    if ($markerId -ceq $firstId -or $markerId -ceq $secondId) {
        throw "Delete marker VersionId must be a third opaque identity"
    }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-simple-marker" -Bucket $bucket -Key $key

    Write-AwsSubstage -Label "list-enabled"
    $enabledList = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "list-object-versions", "--bucket", $bucket
    )
    $enabledVersions = Get-AwsEntries $enabledList.Versions
    $enabledMarkers = Get-AwsEntries $enabledList.DeleteMarkers
    Write-AwsSubstage -Label "assert-enabled-order"
    if ($enabledVersions.Count -ne 2 -or $enabledMarkers.Count -ne 1 -or
        [string]$enabledMarkers[0].VersionId -cne $markerId -or
        [string]$enabledVersions[0].VersionId -cne $secondId -or
        [string]$enabledVersions[1].VersionId -cne $firstId -or
        @($enabledMarkers | Where-Object { [bool]$_.IsLatest }).Count -ne 1 -or
        @($enabledVersions | Where-Object { [bool]$_.IsLatest }).Count -ne 0) {
        throw "Version list is not the required descending marker/second/first state"
    }

    Write-AwsSubstage -Label "get-historical"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "get-object", "--bucket", $bucket, "--key", $key, "--version-id", $firstId, "/work/first-download.txt"
    ) | Out-Null
    Write-AwsSubstage -Label "assert-historical"
    if ([IO.File]::ReadAllText((Join-Path $RunRoot "first-download.txt")) -cne $firstBody) {
        throw "Explicit first-version get returned unexpected bytes"
    }
    Write-AwsSubstage -Label "current-get-nosuchkey"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "get-object", "--bucket", $bucket, "--key", $key, "/work/current-download.txt"
    ) -ExpectedCode "NoSuchKey"
    Write-AwsSubstage -Label "current-head-404"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "head-object", "--bucket", $bucket, "--key", $key
    ) -ExpectedCode "404"
    Write-AwsSubstage -Label "marker-head-405"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "head-object", "--bucket", $bucket, "--key", $key, "--version-id", $markerId
    ) -ExpectedCode "405"

    Write-AwsSubstage -Label "delete-marker-exact"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "delete-object", "--bucket", $bucket, "--key", $key, "--version-id", $markerId
    ) | Out-Null
    Write-AwsSubstage -Label "get-promoted"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "get-object", "--bucket", $bucket, "--key", $key, "/work/second-current.txt"
    ) | Out-Null
    Write-AwsSubstage -Label "assert-promoted"
    if ([IO.File]::ReadAllText((Join-Path $RunRoot "second-current.txt")) -cne $secondBody) {
        throw "Exact delete marker did not restore second body"
    }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-exact-marker-delete-and-restored-get" -Bucket $bucket -Key $key

    Write-AwsSubstage -Label "suspend-versioning"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Suspended"
    ) | Out-Null
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-versioning-suspension" -Bucket $bucket -Key $key
    Write-AwsSubstage -Label "put-null-first"
    $nullFirst = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/third.txt"
    )
    Write-AwsSubstage -Label "assert-null-first"
    if ([string]$nullFirst.VersionId -cne "null") { throw "Suspended upload did not return VersionId null" }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-first-null-put" -Bucket $bucket -Key $key
    Write-AwsSubstage -Label "put-null-second"
    $nullSecond = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/second.txt"
    )
    Write-AwsSubstage -Label "assert-null-second"
    if ([string]$nullSecond.VersionId -cne "null") { throw "Suspended null-slot overwrite did not return VersionId null" }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-second-null-put" -Bucket $bucket -Key $key
    Write-AwsSubstage -Label "suspended-list"
    $suspendedList = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -SafeReceipt "suspended-list" -Arguments @(
        "s3api", "list-object-versions", "--bucket", $bucket
    )
    if ($null -eq $suspendedList -or $suspendedList -isnot [pscustomobject]) { throw "Suspended list JSON was not an object" }
    $suspendedVersionProperties = @($suspendedList.PSObject.Properties.Match("Versions"))
    if ($suspendedVersionProperties.Count -ne 1) { throw "Suspended list JSON omitted Versions" }
    $suspendedMarkerProperties = @($suspendedList.PSObject.Properties.Match("DeleteMarkers"))
    $suspendedVersions = Get-AwsEntries $suspendedList.Versions
    if ($suspendedMarkerProperties.Count -eq 0) {
        $suspendedMarkers = @()
    } else {
        $suspendedMarkers = Get-AwsEntries $suspendedList.DeleteMarkers
    }
    Write-PostgresVersionSnapshot -State $State -SnapshotStage "after-suspended-list-parse" -Bucket $bucket -Key $key
    [int]$versionCount = $suspendedVersions.Count
    [int]$markerCount = $suspendedMarkers.Count
    [int]$nullCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq "null" }).Count
    [int]$firstMatchCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq $firstId }).Count
    [int]$secondMatchCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq $secondId }).Count
    Write-Evidence -Category "assertion" -Value "suspended-version-count=$versionCount"
    Write-Evidence -Category "assertion" -Value "suspended-marker-count=$markerCount"
    Write-Evidence -Category "assertion" -Value "suspended-null-count=$nullCount"
    Write-Evidence -Category "assertion" -Value "suspended-first-match-count=$firstMatchCount"
    Write-Evidence -Category "assertion" -Value "suspended-second-match-count=$secondMatchCount"
    Write-AwsSubstage -Label "suspended-list-assert"
    if ($versionCount -ne 3 -or
        $markerCount -ne 0 -or
        $nullCount -ne 1 -or
        $firstMatchCount -ne 1 -or
        $secondMatchCount -ne 1) {
        throw "Suspended version list did not retain one null and both opaque versions"
    }

    $finalVersions = $suspendedVersions
    $finalMarkers = $suspendedMarkers
    foreach ($entry in @($finalVersions + $finalMarkers)) {
        $versionId = [string]$entry.VersionId
        if ([string]::IsNullOrWhiteSpace($versionId)) { throw "Listed version entry has no VersionId" }
        Write-AwsSubstage -Label "exact-cleanup-delete"
        Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
            "s3api", "delete-object", "--bucket", $bucket, "--key", $key, "--version-id", $versionId
        ) | Out-Null
    }
    Write-AwsSubstage -Label "empty-list"
    $emptyList = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "list-object-versions", "--bucket", $bucket
    )
    if ($null -eq $emptyList -or $emptyList -isnot [pscustomobject]) { throw "Empty version list JSON was not an object" }
    $emptyVersionProperties = @($emptyList.PSObject.Properties.Match("Versions"))
    $emptyMarkerProperties = @($emptyList.PSObject.Properties.Match("DeleteMarkers"))
    if ($emptyVersionProperties.Count -eq 0) {
        $emptyVersions = @()
    } elseif ($emptyVersionProperties.Count -eq 1) {
        $emptyVersions = Get-AwsEntries $emptyList.Versions
    } else {
        throw "Empty version list JSON has duplicate Versions"
    }
    if ($emptyMarkerProperties.Count -eq 0) {
        $emptyMarkers = @()
    } elseif ($emptyMarkerProperties.Count -eq 1) {
        $emptyMarkers = Get-AwsEntries $emptyList.DeleteMarkers
    } else {
        throw "Empty version list JSON has duplicate DeleteMarkers"
    }
    Write-AwsSubstage -Label "assert-empty"
    if ($emptyVersions.Count -ne 0 -or $emptyMarkers.Count -ne 0) {
        throw "Version list was not empty after exact deletes"
    }
    Write-AwsSubstage -Label "delete-bucket"
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(
        "s3api", "delete-bucket", "--bucket", $bucket
    ) | Out-Null
    Write-AwsSubstage -Label "complete"
    Write-Evidence -Category "assertion" -Value "aws-versioning-assertions=complete"
}

function Get-AwsCliVersion {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot
    )
    $result = Invoke-Aws `
        -Network $Network `
        -RunRoot $RunRoot `
        -Endpoint "http://gateway:9000" `
        -Arguments @("--version")
    $text = (@($result.StdOut) + @($result.StdErr)) -join " "
    $match = [regex]::Match($text, 'aws-cli/[0-9A-Za-z._-]+')
    if (-not $match.Success) { throw "AWS CLI version output was not recognized" }
    return $match.Value
}

function Get-GitHead {
    $result = Invoke-NativeCommand `
        -FilePath "git" `
        -ArgumentList @("rev-parse", "HEAD") `
        -Label "Git revision evidence" `
        -Timeout $DockerCommandTimeout
    $head = (@($result.StdOut) -join "").Trim()
    if ($head -notmatch '^[0-9a-f]{40}$') { throw "Git revision evidence was not a commit hash" }
    return $head
}

function Get-VerifiedSpecSha256 {
    $specPath = Join-Path $RepoRoot "docs\superpowers\specs\2026-08-25-object-versioning-design.md"
    if (-not (Test-Path -LiteralPath $specPath -PathType Leaf)) { throw "Object-versioning spec is unavailable" }
    $hash = (Get-FileHash -LiteralPath $specPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($hash -cne $ExpectedSpecSha256) { throw "Object-versioning spec hash differs from the approved specification" }
    return $hash
}

function Capture-SanitizedDiagnostics {
    param([Parameter(Mandatory)][string]$Project)
    $psResult = Invoke-Compose -Project $Project -Arguments @("ps", "--format", "json") -Label "failure diagnostics compose ps"
    $psLineCount = @($psResult.StdOut + $psResult.StdErr | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    Write-Evidence -Category "diagnostic" -Value "compose-ps=redacted-lines-$psLineCount"
    $logsResult = Invoke-Compose -Project $Project -Arguments @("logs", "--no-color", "--tail", "200") -Label "failure diagnostics compose logs"
    $logLineCount = @($logsResult.StdOut + $logsResult.StdErr | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    Write-Evidence -Category "diagnostic" -Value "compose-logs=redacted-lines-$logLineCount"
}

function Remove-OwnedVersioningRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Receipt,
        [Parameter(Mandatory)][string]$RunId
    )
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-object-versioning-$RunId"))
    if (-not $canonicalRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase) -or
        -not [IO.Path]::GetDirectoryName($canonicalRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Owned RunRoot cleanup rejected a non-canonical direct child"
    }
    $canonicalReceipt = Assert-CanonicalChildPath -ParentPath $canonicalRoot -ChildPath $Receipt
    if (-not $canonicalReceipt.Equals((Join-Path $canonicalRoot "ownership-receipt"), [StringComparison]::OrdinalIgnoreCase) -or
        -not (Test-Path -LiteralPath $canonicalReceipt -PathType Leaf) -or
        [IO.File]::ReadAllText($canonicalReceipt) -cne "object-versioning-owned:$RunId") {
        throw "Owned RunRoot cleanup rejected an absent or foreign receipt"
    }
    Remove-Item -LiteralPath $canonicalRoot -Recurse -Force
}

function Remove-OwnedEmptyVersioningRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-object-versioning-$RunId"))
    if (-not $canonicalRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase) -or
        -not [IO.Path]::GetDirectoryName($canonicalRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Empty RunRoot cleanup rejected a non-canonical direct child"
    }
    if (-not (Test-Path -LiteralPath $canonicalRoot -PathType Container)) { return }
    if (@([IO.Directory]::EnumerateFileSystemEntries($canonicalRoot)).Count -ne 0) {
        throw "Empty RunRoot cleanup rejected non-empty content"
    }
    Remove-Item -LiteralPath $canonicalRoot -Force
}

function Test-VersioningCleanupResiduals {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    if ($State.Blocked -or [string]::IsNullOrWhiteSpace($State.Project)) {
        foreach ($name in @("containers", "networks", "volumes", "image")) {
            Write-Evidence -Category "cleanup" -Value "residual-$name=not-queried"
        }
        return @($errors)
    }
    foreach ($query in @(
        [pscustomobject]@{ Name = "containers"; Arguments = @("ps", "-aq", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "networks"; Arguments = @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "volumes"; Arguments = @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") }
    )) {
        try {
            $result = Invoke-Docker -Arguments $query.Arguments -Label "project cleanup residual query"
            $residualCount = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
            if ($residualCount -eq 0) {
                Write-Evidence -Category "cleanup" -Value "residual-$($query.Name)=zero"
            } else {
                Write-Evidence -Category "cleanup" -Value "residual-$($query.Name)=nonzero"
                $errors.Add("residual-$($query.Name)")
            }
        } catch {
            Write-Evidence -Category "cleanup" -Value "residual-$($query.Name)=query-failed"
            $errors.Add("residual-$($query.Name)")
        }
    }
    if ([string]::IsNullOrWhiteSpace($State.GatewayImage)) {
        Write-Evidence -Category "cleanup" -Value "residual-image=not-queried"
    } else {
        try {
            $imageResidual = Test-LocalImage -Image $State.GatewayImage
            if ($imageResidual.Exists) {
                Write-Evidence -Category "cleanup" -Value "residual-image=nonzero"
                $errors.Add("residual-image")
            } else {
                Write-Evidence -Category "cleanup" -Value "residual-image=zero"
            }
        } catch {
            Write-Evidence -Category "cleanup" -Value "residual-image=query-failed"
            $errors.Add("residual-image")
        }
    }
    return @($errors)
}

function Remove-OwnedVersioningResources {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    if (-not $State.Blocked -and -not [string]::IsNullOrWhiteSpace($State.Project) -and -not $State.ProjectOwned -and $State.ProjectOwnershipProbeFailed) {
        try {
            Claim-VersioningProjectOwnership -State $State
            if ($State.ProjectOwned) {
                Write-Evidence -Category "cleanup" -Value "project-ownership-retry=owned"
            } else {
                Write-Evidence -Category "cleanup" -Value "project-ownership-retry=absent"
            }
        } catch {
            Write-Evidence -Category "cleanup" -Value "project-ownership-retry=failed"
            $errors.Add("project-ownership-retry")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "project-ownership-retry=not-needed"
    }
    if ($State.ProjectOwned) {
        try {
            Invoke-Compose `
                -Project $State.Project `
                -Arguments @("down", "--volumes", "--remove-orphans") `
                -Label "owned Compose teardown" | Out-Null
            Write-Evidence -Category "cleanup" -Value "compose-down=passed"
        } catch {
            Write-Evidence -Category "cleanup" -Value "compose-down=failed"
            $errors.Add("compose-down")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "compose-down=not-owned"
    }
    if (-not $State.Blocked -and -not [string]::IsNullOrWhiteSpace($State.GatewayImage) -and -not $State.GatewayImageOwned) {
        try {
            $fallbackGatewayImage = Test-LocalImage -Image $State.GatewayImage
            if ($fallbackGatewayImage.Exists) {
                $State.GatewayImageOwned = $true
                Write-Evidence -Category "cleanup" -Value "image-ownership-probe=owned"
            } else {
                Write-Evidence -Category "cleanup" -Value "image-ownership-probe=absent"
            }
        } catch {
            Write-Evidence -Category "cleanup" -Value "image-ownership-probe=failed"
            $errors.Add("image-ownership-probe")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "image-ownership-probe=not-needed"
    }
    if ($State.GatewayImageOwned) {
        try {
            $ownedGatewayImage = Test-LocalImage -Image $State.GatewayImage
            if ($ownedGatewayImage.Exists) {
                Invoke-Docker -Arguments @("image", "rm", $State.GatewayImage) -Label "owned gateway image removal" | Out-Null
            }
            Write-Evidence -Category "cleanup" -Value "image-remove=passed"
        } catch {
            Write-Evidence -Category "cleanup" -Value "image-remove=failed"
            $errors.Add("image-remove")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "image-remove=not-owned"
    }
    foreach ($residualError in (Test-VersioningCleanupResiduals -State $State)) {
        $errors.Add($residualError)
    }
    if ($State.RunRootReceiptOwned) {
        try {
            Remove-OwnedVersioningRunRoot `
                -TempRoot $State.TempRoot `
                -RunRoot $State.RunRoot `
                -Receipt $State.RunRootReceipt `
                -RunId $State.RunId
            Write-Evidence -Category "cleanup" -Value "temp-remove=passed"
        } catch {
            Write-Evidence -Category "cleanup" -Value "temp-remove=failed"
            $errors.Add("run-root")
        }
    } elseif ($State.RunRootDirectoryOwned) {
        try {
            Remove-OwnedEmptyVersioningRunRoot `
                -TempRoot $State.TempRoot `
                -RunRoot $State.RunRoot `
                -RunId $State.RunId
            Write-Evidence -Category "cleanup" -Value "temp-remove=passed"
        } catch {
            Write-Evidence -Category "cleanup" -Value "temp-remove=failed"
            $errors.Add("run-root")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "temp-remove=not-owned"
    }
    return @($errors)
}

function Invoke-VersioningMain {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-VersioningStage -State $State -Stage "preflight"
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        throw "Object-versioning validation Compose file is unavailable"
    }
    Assert-RequiredTools
    Assert-ComposeVersion
    $imageIds = Assert-RequiredLocalImages

    $State.RunId = New-VersioningRunId
    $State.Project = New-VersioningProjectName -RunId $State.RunId
    $State.GatewayImage = "ipfs3-ver-gateway:$($State.RunId)"
    try {
        Assert-ProjectResourcesAbsent -Project $State.Project
        if ((Test-LocalImage -Image $State.GatewayImage).Exists) {
            throw "BLOCKED object-versioning gateway image already exists"
        }
    } catch {
        $State.Blocked = $true
        throw
    }
    Assert-LoopbackPortsFree -Ports $VersioningPorts

    try {
        $State.RunRoot = New-VersioningRunRoot -TempRoot $State.TempRoot -RunId $State.RunId
    } catch {
        $State.Blocked = $true
        throw
    }
    $State.RunRootDirectoryOwned = $true
    $State.RunRootReceipt = New-VersioningOwnershipReceipt -RunRoot $State.RunRoot -RunId $State.RunId
    $State.RunRootReceiptOwned = $true
    $State.AwsConfigPath = New-VersioningAwsConfig -RunRoot $State.RunRoot
    $State.EnvironmentState = Save-EnvironmentState -Names $TouchedEnvironmentNames
    $State.EnvironmentStateOwned = $true
    Set-RunEnvironment -Name "COMPOSE_DISABLE_ENV_FILE" -Value "1"
    Set-RunEnvironment -Name "IPFS_S3_OBJECT_VERSIONING_POSTGRES_PORT" -Value "55436"
    Set-RunEnvironment -Name "IPFS_S3_OBJECT_VERSIONING_KUBO_PORT" -Value "55003"
    Set-RunEnvironment -Name "IPFS_S3_OBJECT_VERSIONING_GATEWAY_PORT" -Value "59003"
    Set-RunEnvironment -Name "IPFS_S3_OBJECT_VERSION_IMAGE" -Value $State.GatewayImage

    Set-VersioningStage -State $State -Stage "config"
    Invoke-Compose -Project $State.Project -Arguments @("config", "--quiet") -Label "validation Compose config" | Out-Null
    Set-VersioningStage -State $State -Stage "offline-build"
    try {
        Invoke-OfflineGatewayBuild -RunRoot $State.RunRoot -GatewayImage $State.GatewayImage
    } finally {
        $postBuildGatewayImage = Test-LocalImage -Image $State.GatewayImage
        if ($postBuildGatewayImage.Exists) {
            $State.GatewayImageOwned = $true
        }
    }
    if (-not $State.GatewayImageOwned) { throw "Unique gateway image was not built" }

    Set-VersioningStage -State $State -Stage "compose-up"
    try {
        Invoke-Compose `
            -Project $State.Project `
            -Arguments @("up", "--detach", "--pull", "never", "--no-build", "--wait", "--wait-timeout", "300", "postgres", "kubo", "gateway") `
            -Label "validation Compose startup" `
            -Timeout $ComposeStartupTimeout | Out-Null
    } finally {
        try {
            Claim-VersioningProjectOwnership -State $State
        } catch {
            $State.ProjectOwnershipProbeFailed = $true
        }
    }
    if (-not $State.ProjectOwned) { throw "Compose startup did not claim project resources" }

    Set-VersioningStage -State $State -Stage "health"
    Wait-GatewayHealthy -Project $State.Project
    Set-VersioningStage -State $State -Stage "network"
    $network = Get-ComposeNetwork -Project $State.Project

    Set-VersioningStage -State $State -Stage "metadata"
    Write-Evidence -Category "metadata" -Value "package=0.1.0"
    Write-Evidence -Category "metadata" -Value "git-head=$(Get-GitHead)"
    Write-Evidence -Category "metadata" -Value "spec-sha256=$(Get-VerifiedSpecSha256)"
    Write-Evidence -Category "metadata" -Value "aws-image-id=$($imageIds[$AwsImage])"
    Write-Evidence -Category "metadata" -Value "gateway-image-id=$((Test-LocalImage -Image $State.GatewayImage).ImageId)"

    Invoke-RustEvidenceSuites -State $State
    Set-VersioningStage -State $State -Stage "aws"
    Invoke-AwsVersioningSmoke -State $State -Network $network -RunRoot $State.RunRoot -RunId $State.RunId
    Write-Evidence -Category "result" -Value "aws-versioning=passed"
}

$state = @{
    TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    RunId = $null
    Project = $null
    RunRoot = $null
    RunRootReceipt = $null
    AwsConfigPath = $null
    GatewayImage = $null
    EnvironmentState = @()
    Stage = "preflight"
    RunRootDirectoryOwned = $false
    RunRootReceiptOwned = $false
    GatewayImageOwned = $false
    ProjectOwned = $false
    ProjectOwnershipProbeFailed = $false
    EnvironmentStateOwned = $false
    RustEnvironmentRestored = $false
    Blocked = $false
}
$workSucceeded = $false
$failureReason = "execution-failed"
$cleanupErrors = [Collections.Generic.List[string]]::new()
try {
    Invoke-VersioningMain -State $state
    $workSucceeded = $true
} catch {
    Write-Evidence -Category "result" -Value "failed-stage=$($state.Stage)"
    if ($state.Blocked) {
        $failureReason = "blocked"
    }
} finally {
    Set-VersioningStage -State $state -Stage "cleanup"
    if (-not $workSucceeded -and $state.ProjectOwned) {
        Write-Evidence -Category "cleanup" -Value "diagnostics=attempted"
        try {
            Capture-SanitizedDiagnostics -Project $state.Project
        } catch {
            $cleanupErrors.Add("diagnostics")
        }
    } elseif (-not $workSucceeded) {
        Write-Evidence -Category "cleanup" -Value "diagnostics=not-owned"
    } else {
        Write-Evidence -Category "cleanup" -Value "diagnostics=not-required"
    }
    foreach ($cleanupError in (Remove-OwnedVersioningResources -State $state)) {
        $cleanupErrors.Add($cleanupError)
    }
    if ($state.EnvironmentStateOwned) {
        try {
            Restore-EnvironmentState -State $state.EnvironmentState
            Write-Evidence -Category "cleanup" -Value "environment-restore=passed"
        } catch {
            Write-Evidence -Category "cleanup" -Value "environment-restore=failed"
            $cleanupErrors.Add("environment")
        }
    } else {
        Write-Evidence -Category "cleanup" -Value "environment-restore=not-needed"
    }
    Write-Evidence -Category "cleanup" -Value "cleanup-errors=$($cleanupErrors.Count)"
}

if ($cleanupErrors.Count -ne 0) {
    $workSucceeded = $false
    $failureReason = "cleanup-failed"
}
if ($workSucceeded) {
    Write-Evidence -Category "cleanup" -Value "cleanup=complete"
    Write-Host "[RESULT] object-versioning-client=PASSED"
    exit 0
}
Write-Host "[RESULT] object-versioning-client=FAILED reason=$failureReason"
exit 1
