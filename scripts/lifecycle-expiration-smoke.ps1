[CmdletBinding()]
param(
    [switch]$Run,
    [switch]$DiagnoseMultiGateway,
    [switch]$DiagnoseLifecycleRaceExact,
    [switch]$DiagnoseLifecycleRaceStability,
    [switch]$DiagnoseLifecycleAws
)

$selectedModeCount = @(
    $Run.IsPresent,
    $DiagnoseMultiGateway.IsPresent,
    $DiagnoseLifecycleRaceExact.IsPresent,
    $DiagnoseLifecycleRaceStability.IsPresent,
    $DiagnoseLifecycleAws.IsPresent
).Where({ $_ }).Count
if ($selectedModeCount -gt 1) {
    throw "Lifecycle runner modes are mutually exclusive"
}
if ($selectedModeCount -eq 0) {
    Write-Host "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"
    exit 0
}

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\tests\compose.lifecycle-expiration-validation.yml"))
$ExpirationSpecPath = Join-Path $RepoRoot "docs\superpowers\specs\2026-08-26-lifecycle-expiration-design.md"
$ProgramSpecPath = Join-Path $RepoRoot "docs\superpowers\specs\2026-08-26-lifecycle-program-design.md"
$ExpectedExpirationSpecSha256 = "0c11f4df0b4f6e81e9834fde45368df5b330e1229dc2cdff4e8d8ab573f35742"
$ExpectedProgramSpecSha256 = "fdcfbb22447ea7c7bfae9c549b2722664a2e8df859076e7fd3c2d20b3b0e4574"
$GatewayRuntimeBaseImage = "ghcr.io/hugefiver/ipfs3:latest"
$AwsImage = "amazon/aws-cli:latest"
$LifecyclePorts = @(55437, 55004, 59004, 59005, 59006)
$TouchedEnvironmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "IPFS_S3_LIFECYCLE_POSTGRES_PORT",
    "IPFS_S3_LIFECYCLE_KUBO_PORT",
    "IPFS_S3_LIFECYCLE_GATEWAY_A_PORT",
    "IPFS_S3_LIFECYCLE_GATEWAY_B_PORT",
    "IPFS_S3_LIFECYCLE_LOAD_BALANCER_PORT",
    "IPFS_S3_LIFECYCLE_IMAGE",
    "IPFS_S3_TEST_POSTGRES_URL",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL",
    "IPFS_S3_MULTI_GATEWAY_A_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_B_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_KUBO_URL"
)
$DockerCommandTimeout = [TimeSpan]::FromMinutes(5)
$ComposeStartupTimeout = [TimeSpan]::FromMinutes(6)
$CargoVendorTimeout = [TimeSpan]::FromMinutes(5)
$TarTimeout = [TimeSpan]::FromMinutes(5)
$OfflineBuildTimeout = [TimeSpan]::FromMinutes(30)
$RustTestTimeout = [TimeSpan]::FromMinutes(20)
$AwsCommandTimeout = [TimeSpan]::FromMinutes(2)

function Write-LifecycleEvidence {
    param(
        [Parameter(Mandatory)][ValidateSet("metadata", "command", "assertion", "diagnostic", "cleanup", "result")][string]$Category,
        [Parameter(Mandatory)][string]$Value
    )
    if ($Value -notmatch '^[A-Za-z0-9._:=/ -]+$') {
        throw "Lifecycle evidence value is not safely redacted"
    }
    Write-Host "[EVIDENCE] lifecycle-expiration category=$Category value=$Value"
}

function Set-LifecycleStage {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("preflight", "config", "offline-build", "compose-up", "health", "network", "metadata", "postgres", "e2e", "multi-gateway", "integration", "aws", "cleanup")][string]$Stage
    )
    $State.Stage = $Stage
    Write-LifecycleEvidence -Category "diagnostic" -Value "stage=$Stage"
}

function New-LifecycleRunId {
    $timestamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ", [Globalization.CultureInfo]::InvariantCulture).ToLowerInvariant()
    $guidSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 8).ToLowerInvariant()
    $runId = "$timestamp-$PID-$guidSuffix"
    if ($runId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Generated invalid lifecycle RunId"
    }
    return $runId
}

function New-LifecycleProjectName {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') { throw "Invalid lifecycle RunId" }
    $project = "ipfs3-lifecycle-$RunId"
    if ($project -cnotmatch '^[a-z0-9][a-z0-9_-]*$') { throw "Invalid lifecycle Compose project" }
    return $project
}

function New-LifecycleBucketName {
    param(
        [Parameter(Mandatory)][string]$RunId,
        [string]$Prefix = "ipfs3-lifecycle"
    )
    if ($Prefix -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') { throw "Invalid lifecycle bucket prefix" }
    $bucket = "$Prefix-$RunId"
    if ($bucket.Length -gt 63 -or $bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid lifecycle bucket name"
    }
    return $bucket
}

function Assert-CanonicalChildPath {
    param(
        [Parameter(Mandatory)][string]$ParentPath,
        [Parameter(Mandatory)][string]$ChildPath
    )
    $canonicalParent = [IO.Path]::GetFullPath($ParentPath).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $canonicalChild = [IO.Path]::GetFullPath($ChildPath)
    $prefix = $canonicalParent + [IO.Path]::DirectorySeparatorChar
    if (-not $canonicalChild.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path is outside the owned root"
    }
    return $canonicalChild
}

function Get-CanonicalExistingDirectory {
    param([Parameter(Mandatory)][string]$Path)
    $canonicalPath = [IO.Path]::GetFullPath($Path).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $canonicalPath -PathType Container)) { throw "Working directory is unavailable" }
    return $canonicalPath
}

function New-LifecycleRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') { throw "Invalid lifecycle RunId for temporary root" }
    $canonicalTemp = Get-CanonicalExistingDirectory -Path $TempRoot
    $runRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath (Join-Path $canonicalTemp "ipfs-s3-lifecycle-expiration-$RunId")
    if (-not [IO.Path]::GetDirectoryName($runRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot must be a direct child of the temporary root"
    }
    if (Test-Path -LiteralPath $runRoot) { throw "BLOCKED lifecycle RunRoot already exists" }
    try {
        $created = New-Item -ItemType Directory -Path $runRoot -ErrorAction Stop
    } catch {
        throw "BLOCKED lifecycle RunRoot creation collided"
    }
    if (-not $created.FullName.Equals($runRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot creation returned an unexpected location"
    }
    return $runRoot
}

function New-LifecycleOwnershipReceipt {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $receipt = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "ownership-receipt")
    $stream = $null
    try {
        $stream = [IO.File]::Open($receipt, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $bytes = [Text.Encoding]::UTF8.GetBytes("lifecycle-expiration-owned:$RunId")
        $stream.Write($bytes, 0, $bytes.Length)
    } catch {
        throw "BLOCKED lifecycle ownership receipt could not be claimed"
    } finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }
    return $receipt
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
    if ($Timeout -le [TimeSpan]::Zero -or $Timeout.TotalMilliseconds -gt [int]::MaxValue -or $AllowedExitCodes.Count -eq 0) {
        throw "$Label has an invalid native-command bound"
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
    foreach ($argument in $ArgumentList) { $null = $startInfo.ArgumentList.Add($argument) }
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
        [string]$Label = "Docker operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-NativeCommand -FilePath "docker" -ArgumentList $Arguments -Label $Label -Timeout $Timeout -AllowedExitCodes $AllowedExitCodes
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory)][string]$Project,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Label = "Compose operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker -Arguments (@("compose", "--project-name", $Project, "--file", $ComposeFile) + $Arguments) -Label $Label -Timeout $Timeout -AllowedExitCodes $AllowedExitCodes
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
    if ($ids.Count -ne 1 -or $ids[0] -notmatch '^sha256:[0-9a-f]{64}$') { throw "Local image inspection returned an invalid identity" }
    return [pscustomobject]@{ Exists = $true; ImageId = $ids[0] }
}

function Assert-RequiredTools {
    foreach ($tool in @("pwsh", "docker", "cargo", "tar.exe", "git")) {
        if ($null -eq (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "Required local tool is unavailable" }
    }
}

function Assert-ComposeVersion {
    $result = Invoke-Docker -Arguments @("compose", "version", "--short") -Label "Docker Compose version preflight"
    $versionText = (@($result.StdOut) -join "`n")
    $composeVersionMatch = [regex]::Match($versionText, '^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$')
    $composeVersion = $null
    if (-not $composeVersionMatch.Success -or -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or $composeVersion -lt [Version]"2.23.1") {
        throw "Docker Compose 2.23.1 or newer is required"
    }
}

function Assert-RequiredLocalImages {
    $images = @(
        "postgres:17",
        "ghcr.io/hugefiver/ipfs3-kubo:latest",
        "ghcr.io/hugefiver/ipfs3:latest",
        "rust:latest",
        "amazon/aws-cli:latest",
        "nginx:1.28.0-alpine"
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
    foreach ($query in $queries) {
        $result = Invoke-Docker -Arguments $query.Arguments -Label "project label preflight"
        if (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
            throw "BLOCKED lifecycle project label preflight"
        }
    }
}

function Test-ProjectResourcesExist {
    param([Parameter(Mandatory)][string]$Project)
    foreach ($arguments in @(
        @("ps", "-aq", "--filter", "label=com.docker.compose.project=$Project"),
        @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project"),
        @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project")
    )) {
        $result = Invoke-Docker -Arguments $arguments -Label "project ownership probe"
        if (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) { return $true }
    }
    return $false
}

function Claim-LifecycleProjectOwnership {
    param([Parameter(Mandatory)][hashtable]$State)
    if (Test-ProjectResourcesExist -Project $State.Project) { $State.ProjectOwned = $true }
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
        $state.Add([pscustomobject]@{ Name = $name; Present = $present; Value = if ($present) { (Get-Item -LiteralPath $path).Value } else { $null } })
    }
    return @($state)
}

function Set-RunEnvironment {
    param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][string]$Value)
    Set-Item -LiteralPath "Env:$Name" -Value $Value
}

function Restore-EnvironmentState {
    param([Parameter(Mandatory)][object[]]$State)
    foreach ($entry in $State) {
        $name = [string]$entry.Name
        if ([bool]$entry.Present) {
            Set-Item -LiteralPath "Env:$name" -Value ([string]$entry.Value)
        } elseif (Test-Path -LiteralPath "Env:$name") {
            Remove-Item -LiteralPath "Env:$name" -Force
        }
    }
}

function New-LifecycleAwsConfig {
    param([Parameter(Mandatory)][string]$RunRoot)
    $configPath = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "aws-config")
    if (Test-Path -LiteralPath $configPath) { throw "Owned AWS config path unexpectedly exists" }
    [IO.File]::WriteAllText($configPath, "[default]`nregion = us-east-1`ns3 =`n    addressing_style = path`n", [Text.UTF8Encoding]::new($false))
    return $configPath
}

function Invoke-OfflineGatewayBuild {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$GatewayImage
    )
    $vendorPath = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "vendor")
    $archiveContext = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "vendor-archive-context")
    $vendorArchive = Assert-CanonicalChildPath -ParentPath $archiveContext -ChildPath (Join-Path $archiveContext "vendor.tar.gz")
    $dockerfile = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "Dockerfile.gateway-runtime")
    if ((Test-Path -LiteralPath $vendorPath) -or (Test-Path -LiteralPath $archiveContext) -or (Test-Path -LiteralPath $dockerfile)) {
        throw "Owned offline build path unexpectedly exists"
    }
    Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("vendor", "--locked", "--offline", $vendorPath) -Label "offline Cargo vendoring" -Timeout $CargoVendorTimeout -WorkingDirectory $RepoRoot | Out-Null
    $null = New-Item -ItemType Directory -Path $archiveContext -ErrorAction Stop
    Invoke-NativeCommand -FilePath "tar.exe" -ArgumentList @("-czf", $vendorArchive, "-C", $vendorPath, ".") -Label "offline vendor archive" -Timeout $TarTimeout | Out-Null
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
    Invoke-Docker -Arguments @(
        "build", "--pull=false", "--network", "none", "--quiet",
        "--build-context", "vendor-archive=$archiveContext", "--tag", $GatewayImage,
        "--file", $dockerfile, $RepoRoot
    ) -Label "offline lifecycle gateway image build" -Timeout $OfflineBuildTimeout | Out-Null
}

function Get-ComposeServiceContainer {
    param([Parameter(Mandatory)][string]$Project, [Parameter(Mandatory)][ValidateSet("postgres", "kubo", "gateway-a", "gateway-b", "load-balancer")][string]$Service)
    $result = Invoke-Compose -Project $Project -Arguments @("ps", "-q", $Service) -Label "Compose service container lookup"
    $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($ids.Count -ne 1 -or $ids[0].Trim() -notmatch '^[0-9a-f]{12,64}$') { throw "Compose service lookup was not unique" }
    return $ids[0].Trim()
}

function Wait-TopologyHealthy {
    param([Parameter(Mandatory)][string]$Project)
    foreach ($service in @("postgres", "kubo", "gateway-a", "gateway-b", "load-balancer")) {
        $container = Get-ComposeServiceContainer -Project $Project -Service $service
        $healthy = $false
        for ($attempt = 0; $attempt -lt 36; $attempt++) {
            $result = Invoke-Docker -Arguments @("inspect", "--format", "{{.State.Health.Status}}", $container) -Label "service health probe"
            $status = (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join "").Trim()
            if ($status -eq "healthy") { $healthy = $true; break }
            if ($status -eq "unhealthy") { throw "Topology healthcheck is unhealthy" }
            Start-Sleep -Seconds 5
        }
        if (-not $healthy) { throw "Topology did not become healthy" }
    }
}

function Get-ComposeNetwork {
    param([Parameter(Mandatory)][string]$Project)
    $container = Get-ComposeServiceContainer -Project $Project -Service "gateway-a"
    $result = Invoke-Docker -Arguments @("inspect", "--format", "{{json .NetworkSettings.Networks}}", $container) -Label "gateway network lookup"
    $jsonText = (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join "")
    try { $networks = $jsonText | ConvertFrom-Json } catch { throw "Gateway network lookup returned invalid JSON" }
    $names = @($networks.PSObject.Properties.Name)
    if ($names.Count -ne 1 -or $names[0] -notmatch '^[A-Za-z0-9_.-]+$') { throw "Gateway network lookup was not unique" }
    return $names[0]
}

function Set-LifecycleEndpointEnvironment {
    Set-RunEnvironment -Name "IPFS_S3_TEST_POSTGRES_URL" -Value "postgres://ipfs3:ipfs3@127.0.0.1:55437/ipfs3"
    Set-RunEnvironment -Name "IPFS_S3_E2E_ENDPOINT" -Value "http://127.0.0.1:59006"
    Set-RunEnvironment -Name "IPFS_S3_E2E_KUBO_URL" -Value "http://127.0.0.1:55004"
    Set-RunEnvironment -Name "IPFS_S3_MULTI_GATEWAY_A_ENDPOINT" -Value "http://127.0.0.1:59004"
    Set-RunEnvironment -Name "IPFS_S3_MULTI_GATEWAY_B_ENDPOINT" -Value "http://127.0.0.1:59005"
    Set-RunEnvironment -Name "IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT" -Value "http://127.0.0.1:59006"
    Set-RunEnvironment -Name "IPFS_S3_MULTI_GATEWAY_KUBO_URL" -Value "http://127.0.0.1:55004"
}

function Assert-RustSuiteExecuted {
    param([Parameter(Mandatory)][object]$Result, [Parameter(Mandatory)][string]$Name)
    $output = (@($Result.StdOut) + @($Result.StdErr)) -join "`n"
    $runningMatches = [regex]::Matches($output, '(?m)^running (?<running>[1-9][0-9]*) tests?$')
    $summaryMatches = [regex]::Matches($output, '(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$')
    if ($runningMatches.Count -ne 1 -or $summaryMatches.Count -ne 1) { throw "$Name did not prove real Rust test execution" }
    [long]$runningCount = 0
    [long]$passedCount = 0
    [decimal]$seconds = [decimal]0
    if (-not [Int64]::TryParse($runningMatches[0].Groups["running"].Value, [ref]$runningCount) -or
        -not [Int64]::TryParse($summaryMatches[0].Groups["passed"].Value, [ref]$passedCount) -or
        -not [decimal]::TryParse($summaryMatches[0].Groups["seconds"].Value, [Globalization.NumberStyles]::AllowDecimalPoint, [Globalization.CultureInfo]::InvariantCulture, [ref]$seconds) -or
        $seconds -lt [decimal]0 -or $seconds -gt [decimal]3600 -or $runningCount -ne $passedCount) {
        throw "$Name did not prove real Rust test execution"
    }
}

function Get-MultiGatewayFailureReceipt {
    param([Parameter(Mandatory)][object]$Result)

    $lines = @($Result.StdOut) + @($Result.StdErr)
    $output = $lines -join "`n"
    $runningMatches = [regex]::Matches($output, '(?m)^running (?<running>[1-9][0-9]*) tests?$')
    $summaryMatches = [regex]::Matches($output, '(?m)^test result: FAILED\. (?<passed>[0-9]+) passed; (?<failed>[1-9][0-9]*) failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$')
    $nameMatches = [regex]::Matches($output, '(?m)^test (?<name>[A-Za-z0-9_:]+) \.\.\. FAILED$')
    $stagePrefixMatches = [regex]::Matches(
        $output,
        '(?m)^\[LIFECYCLE-RACE-STAGE\].*\r?$'
    )
    $stageMatches = [regex]::Matches(
        $output,
        '(?m)^\[LIFECYCLE-RACE-STAGE\] test=multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome stage=(?<stage>bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-request-dispatched|successor-request-complete|observer-loop-entered|observer-get-response|observer-list-response|observer-list-status-ok|observer-successor-visible|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete)\r?$'
    )
    if ($runningMatches.Count -ne 1 -or $summaryMatches.Count -ne 1) {
        throw "Multi-gateway diagnostic receipt shape is invalid"
    }
    if ($stagePrefixMatches.Count -ne $stageMatches.Count) {
        throw "Multi-gateway diagnostic stage receipt is invalid"
    }

    [long]$running = 0
    [long]$passed = 0
    [long]$failed = 0
    [long]$ignored = 0
    [long]$measured = 0
    [long]$filtered = 0
    [decimal]$seconds = 0
    $parsed =
        [Int64]::TryParse($runningMatches[0].Groups['running'].Value, [ref]$running) -and
        [Int64]::TryParse($summaryMatches[0].Groups['passed'].Value, [ref]$passed) -and
        [Int64]::TryParse($summaryMatches[0].Groups['failed'].Value, [ref]$failed) -and
        [Int64]::TryParse($summaryMatches[0].Groups['ignored'].Value, [ref]$ignored) -and
        [Int64]::TryParse($summaryMatches[0].Groups['measured'].Value, [ref]$measured) -and
        [Int64]::TryParse($summaryMatches[0].Groups['filtered'].Value, [ref]$filtered) -and
        [decimal]::TryParse($summaryMatches[0].Groups['seconds'].Value, [Globalization.NumberStyles]::AllowDecimalPoint, [Globalization.CultureInfo]::InvariantCulture, [ref]$seconds)
    $names = @($nameMatches | ForEach-Object { $_.Groups['name'].Value })
    $lastStage = if ($stageMatches.Count -eq 0) {
        "not-reached"
    } else {
        $stageMatches[$stageMatches.Count - 1].Groups['stage'].Value
    }
    $raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
    if ($names -contains $raceTestName -and $lastStage -ceq "not-reached") {
        throw "Named lifecycle race failure omitted its safe stage receipt"
    }
    if (-not $parsed -or $seconds -lt 0 -or $seconds -gt 3600 -or
        $running -ne ($passed + $failed + $ignored + $measured) -or
        $names.Count -ne $failed -or @($names | Sort-Object -Unique).Count -ne $names.Count) {
        throw "Multi-gateway diagnostic receipt counts are invalid"
    }

    $categoryRules = [ordered]@{
        timeout = '(?i)\b(timed out|deadline has elapsed)\b'
        assertion = '(?i)\b(assertion|panicked at)\b'
        'http-status' = '(?i)\b(http|status code)\b'
        connection = '(?i)\b(connection|connect|refused)\b'
        database = '(?i)\b(database|postgres|sqlx)\b'
    }
    $category = 'process-exit'
    :lineScan foreach ($line in $lines) {
        foreach ($rule in $categoryRules.GetEnumerator()) {
            if ($line -match $rule.Value) {
                $category = $rule.Key
                break lineScan
            }
        }
    }
    return [pscustomobject]@{
        Running = $running
        Passed = $passed
        Failed = $failed
        Ignored = $ignored
        Measured = $measured
        Filtered = $filtered
        FailedNames = $names
        FirstErrorCategory = $category
        LastStage = $lastStage
    }
}

function Invoke-LifecycleMultiGatewayDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)

    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "multi-gateway"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test multi_gateway -- --nocapture --test-threads=1"
    $result = Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @("test", "--test", "multi_gateway", "--", "--nocapture", "--test-threads=1") `
        -Label "Owned multi-gateway diagnostic" `
        -Timeout $RustTestTimeout `
        -AllowedExitCodes @(0, 101) `
        -WorkingDirectory $RepoRoot
    if ($result.ExitCode -eq 0) {
        Assert-RustSuiteExecuted -Result $result -Name "Owned multi-gateway diagnostic"
        $State.DiagnosticOutcome = "not-reproduced"
        Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway=not-reproduced"
        return
    }

    $receipt = Get-MultiGatewayFailureReceipt -Result $result
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-running=$($receipt.Running)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-passed=$($receipt.Passed)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-failed=$($receipt.Failed)"
    foreach ($name in $receipt.FailedNames) {
        Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-failed-test=$name"
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-first-error-category=$($receipt.FirstErrorCategory)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-last-stage=$($receipt.LastStage)"
    $State.DiagnosticOutcome = "captured"
    throw "Owned multi-gateway diagnostic captured a safe failure receipt"
}

function Get-LifecycleRaceExactReceipt {
    param(
        [Parameter(Mandatory)][object]$Result,
        [Parameter(Mandatory)][ValidateSet(0, 101)][int]$ExitCode
    )

    $lines = @($Result.StdOut) + @($Result.StdErr)
    $output = $lines -join "`n"
    $raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
    $stageEnum = "bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-request-dispatched|successor-request-complete|observer-loop-entered|observer-get-response|observer-list-response|observer-list-status-ok|observer-successor-visible|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete"
    $broadStageMatches = [regex]::Matches($output, '(?m)\[LIFECYCLE-RACE-STAGE\][^\r\n]*')
    $strictStageMatches = [regex]::Matches(
        $output,
        "(?m)(?<!\S)\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=(?<stage>$stageEnum)(?=[ \t]*$)"
    )
    $stageRejected = $broadStageMatches.Count -ne $strictStageMatches.Count
    foreach ($broad in $broadStageMatches) {
        if ($broad.Value -cnotmatch "^\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=($stageEnum)[ \t]*$") {
            $stageRejected = $true
            break
        }
    }
    $lastStage = if ($strictStageMatches.Count -eq 0) {
        "not-reached"
    } else {
        $strictStageMatches[$strictStageMatches.Count - 1].Groups['stage'].Value
    }
    $seenStages = @($strictStageMatches | ForEach-Object { $_.Groups['stage'].Value })

    $runningMatches = [regex]::Matches($output, '(?m)^running (?<running>[1-9][0-9]*) tests?$')
    $successMatches = [regex]::Matches($output, '(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; [0-9]+ ignored; [0-9]+ measured; [0-9]+ filtered out; finished in [0-9]{1,4}(?:\.[0-9]{1,3})?s$')
    $failureMatches = [regex]::Matches($output, '(?m)^test result: FAILED\. (?<passed>[0-9]+) passed; (?<failed>[1-9][0-9]*) failed; [0-9]+ ignored; [0-9]+ measured; [0-9]+ filtered out; finished in [0-9]{1,4}(?:\.[0-9]{1,3})?s$')
    $countShape = "rejected"
    $running = -1
    $passed = -1
    $failed = -1
    if ($runningMatches.Count -eq 0 -and $successMatches.Count -eq 0 -and $failureMatches.Count -eq 0) {
        $countShape = "unavailable"
    } elseif ($runningMatches.Count -eq 1 -and (($successMatches.Count -eq 1) -xor ($failureMatches.Count -eq 1))) {
        $running = [int]$runningMatches[0].Groups['running'].Value
        if ($successMatches.Count -eq 1) {
            $passed = [int]$successMatches[0].Groups['passed'].Value
            $failed = 0
        } else {
            $passed = [int]$failureMatches[0].Groups['passed'].Value
            $failed = [int]$failureMatches[0].Groups['failed'].Value
        }
        if ($running -eq ($passed + $failed)) {
            $countShape = "available"
        }
    }

    $parserFailureCategory = if ($stageRejected) {
        "stage-rejected"
    } elseif ($lastStage -ceq "not-reached") {
        "stage-missing"
    } elseif ($countShape -ceq "rejected") {
        "output-shape-rejected"
    } else {
        "none"
    }
    $categoryRules = [ordered]@{
        timeout = '(?i)\b(timed out|deadline has elapsed)\b'
        assertion = '(?i)\b(assertion|panicked at)\b'
        'http-status' = '(?i)\b(http|status code)\b'
        connection = '(?i)\b(connection|connect|refused)\b'
        database = '(?i)\b(database|postgres|sqlx)\b'
    }
    $errorCategory = if ($ExitCode -eq 0) { "none" } else { "process-exit" }
    if ($ExitCode -eq 101) {
        :categoryScan foreach ($line in $lines) {
            foreach ($rule in $categoryRules.GetEnumerator()) {
                if ($line -match $rule.Value) {
                    $errorCategory = $rule.Key
                    break categoryScan
                }
            }
        }
    }
    $countsValid = $countShape -ceq "unavailable" -or
        ($ExitCode -eq 0 -and $running -eq 1 -and $passed -eq 1 -and $failed -eq 0) -or
        ($ExitCode -eq 101 -and $running -eq 1 -and $passed -eq 0 -and $failed -eq 1)
    if (-not $countsValid -and $parserFailureCategory -ceq "none") {
        $parserFailureCategory = "output-shape-rejected"
    }
    $outcome = if ($ExitCode -eq 0 -and $parserFailureCategory -ceq "none" -and $lastStage -ceq "bucket-delete") {
        "passed"
    } elseif ($ExitCode -eq 101) {
        "failed"
    } else {
        "unqualified"
    }
    return [pscustomobject]@{
        Outcome = $outcome
        CommandOutcome = if ($ExitCode -eq 0) { "allowed-exit-0" } else { "allowed-exit-101" }
        ParserFailureCategory = $parserFailureCategory
        CountShape = $countShape
        Running = $running
        Passed = $passed
        Failed = $failed
        FailedName = if ($ExitCode -eq 101) { $raceTestName } else { "none" }
        FirstErrorCategory = $errorCategory
        LastStage = $lastStage
        SuccessorRequestDispatchedSeen = $seenStages -ccontains "successor-request-dispatched"
        SuccessorRequestCompleteSeen = $seenStages -ccontains "successor-request-complete"
        ObserverGetResponseSeen = $seenStages -ccontains "observer-get-response"
        ObserverListResponseSeen = $seenStages -ccontains "observer-list-response"
        ObserverSuccessorVisibleSeen = $seenStages -ccontains "observer-successor-visible"
    }
}

function Invoke-LifecycleRaceExactDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)

    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "multi-gateway"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1"
    $receipt = $null
    try {
        $result = Invoke-NativeCommand `
            -FilePath "cargo" `
            -ArgumentList @(
                "test",
                "--test",
                "multi_gateway",
                "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
                "--",
                "--exact",
                "--nocapture",
                "--test-threads=1"
            ) `
            -Label "Owned exact lifecycle-race diagnostic" `
            -Timeout $RustTestTimeout `
            -AllowedExitCodes @(0, 101) `
            -WorkingDirectory $RepoRoot
        $receipt = Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode
    } catch {
        $fixedCommandOutcome = if ($_.Exception.Message -ceq "Owned exact lifecycle-race diagnostic timed out") {
            "timeout"
        } elseif ($_.Exception.Message -ceq "Owned exact lifecycle-race diagnostic failed") {
            "nonallowed-exit"
        } else {
            "command-error"
        }
        $receipt = [pscustomobject]@{
            Outcome = "unqualified"
            CommandOutcome = $fixedCommandOutcome
            ParserFailureCategory = "output-shape-rejected"
            CountShape = "unavailable"
            Running = -1
            Passed = -1
            Failed = -1
            FailedName = "none"
            FirstErrorCategory = "process-exit"
            LastStage = "not-reached"
            SuccessorRequestDispatchedSeen = $false
            SuccessorRequestCompleteSeen = $false
            ObserverGetResponseSeen = $false
            ObserverListResponseSeen = $false
            ObserverSuccessorVisibleSeen = $false
        }
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-running=$($receipt.Running)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-passed=$($receipt.Passed)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-failed=$($receipt.Failed)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-failed-test=$($receipt.FailedName)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-first-error-category=$($receipt.FirstErrorCategory)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-last-stage=$($receipt.LastStage)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-command-outcome=$($receipt.CommandOutcome)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-parser-failure-category=$($receipt.ParserFailureCategory)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-count-shape=$($receipt.CountShape)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-dispatched-seen=$($receipt.SuccessorRequestDispatchedSeen.ToString().ToLowerInvariant())"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-complete-seen=$($receipt.SuccessorRequestCompleteSeen.ToString().ToLowerInvariant())"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-get-response-seen=$($receipt.ObserverGetResponseSeen.ToString().ToLowerInvariant())"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-list-response-seen=$($receipt.ObserverListResponseSeen.ToString().ToLowerInvariant())"
    Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-successor-visible-seen=$($receipt.ObserverSuccessorVisibleSeen.ToString().ToLowerInvariant())"
    $State.DiagnosticOutcome = "exact-race-$($receipt.Outcome)"
    if ($receipt.Outcome -cne "passed") {
        throw "Owned exact lifecycle-race diagnostic captured a safe failure receipt"
    }
}

function Invoke-LifecycleRaceStabilityDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)

    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "multi-gateway"
    foreach ($iteration in 1..5) {
        Write-LifecycleEvidence -Category "command" -Value "cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1"
        try {
            $result = Invoke-NativeCommand `
                -FilePath "cargo" `
                -ArgumentList @(
                    "test",
                    "--test",
                    "multi_gateway",
                    "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
                    "--",
                    "--exact",
                    "--nocapture",
                    "--test-threads=1"
                ) `
                -Label "Owned lifecycle-race stability process" `
                -Timeout $RustTestTimeout `
                -AllowedExitCodes @(0, 101) `
                -WorkingDirectory $RepoRoot
            $receipt = Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode
        } catch {
            $receipt = [pscustomobject]@{
                Outcome = "unqualified"
                CommandOutcome = if ($_.Exception.Message -ceq "Owned lifecycle-race stability process timed out") { "timeout" } elseif ($_.Exception.Message -ceq "Owned lifecycle-race stability process failed") { "nonallowed-exit" } else { "command-error" }
                ParserFailureCategory = "output-shape-rejected"
                CountShape = "unavailable"
                Running = -1
                Passed = -1
                Failed = -1
                FailedName = "none"
                FirstErrorCategory = "process-exit"
                LastStage = "not-reached"
                SuccessorRequestDispatchedSeen = $false
                SuccessorRequestCompleteSeen = $false
                ObserverGetResponseSeen = $false
                ObserverListResponseSeen = $false
                ObserverSuccessorVisibleSeen = $false
            }
        }
        $isExactPass =
            $receipt.Outcome -ceq "passed" -and
            $receipt.CommandOutcome -ceq "allowed-exit-0" -and
            $receipt.ParserFailureCategory -ceq "none" -and
            $receipt.CountShape -ceq "available" -and
            $receipt.Running -eq 1 -and $receipt.Passed -eq 1 -and $receipt.Failed -eq 0 -and
            $receipt.FailedName -ceq "none" -and $receipt.FirstErrorCategory -ceq "none" -and
            $receipt.LastStage -ceq "bucket-delete" -and
            $receipt.SuccessorRequestDispatchedSeen -and $receipt.SuccessorRequestCompleteSeen -and
            $receipt.ObserverGetResponseSeen -and $receipt.ObserverListResponseSeen -and
            $receipt.ObserverSuccessorVisibleSeen
        if (-not $isExactPass) {
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-command-outcome=$($receipt.CommandOutcome)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-parser-failure-category=$($receipt.ParserFailureCategory)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-count-shape=$($receipt.CountShape)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-running=$($receipt.Running)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-passed=$($receipt.Passed)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-failed=$($receipt.Failed)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-failed-test=$($receipt.FailedName)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-first-error-category=$($receipt.FirstErrorCategory)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-last-stage=$($receipt.LastStage)"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-dispatched-seen=$($receipt.SuccessorRequestDispatchedSeen.ToString().ToLowerInvariant())"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-complete-seen=$($receipt.SuccessorRequestCompleteSeen.ToString().ToLowerInvariant())"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-get-response-seen=$($receipt.ObserverGetResponseSeen.ToString().ToLowerInvariant())"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-list-response-seen=$($receipt.ObserverListResponseSeen.ToString().ToLowerInvariant())"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-successor-visible-seen=$($receipt.ObserverSuccessorVisibleSeen.ToString().ToLowerInvariant())"
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-stability-iteration=$iteration result=failed"
            $State.DiagnosticOutcome = "exact-race-stability-failed"
            throw "Owned lifecycle-race stability receipt was not an exact pass"
        }
        Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-stability-iteration=$iteration command-outcome=allowed-exit-0 count-shape=available running=1 passed=1 failed=0 failed-name=none error-category=none parser=none last-stage=bucket-delete successor-request-dispatched-seen=true successor-request-complete-seen=true observer-get-response-seen=true observer-list-response-seen=true observer-successor-visible-seen=true result=passed"
    }
    $State.DiagnosticOutcome = "exact-race-stability-passed"
}

function Invoke-LifecycleRustSuites {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "postgres"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test postgres_versioning -- --nocapture --test-threads=1"
    $postgresVersioning = Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("test", "--test", "postgres_versioning", "--", "--nocapture", "--test-threads=1") -Label "Owned PostgreSQL versioning regression" -Timeout $RustTestTimeout -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $postgresVersioning -Name "Owned PostgreSQL versioning regression"
    Write-LifecycleEvidence -Category "assertion" -Value "postgres-versioning=passed"

    Write-LifecycleEvidence -Category "command" -Value "cargo test --test postgres_lifecycle -- --nocapture --test-threads=1"
    $postgresLifecycle = Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("test", "--test", "postgres_lifecycle", "--", "--nocapture", "--test-threads=1") -Label "Owned PostgreSQL lifecycle tests" -Timeout $RustTestTimeout -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $postgresLifecycle -Name "Owned PostgreSQL lifecycle tests"
    Write-LifecycleEvidence -Category "assertion" -Value "postgres-lifecycle=A-abort-B-reclaim-stale-CAS"

    Set-LifecycleStage -State $State -Stage "e2e"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test e2e -- --nocapture --test-threads=1"
    $e2e = Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("test", "--test", "e2e", "--", "--nocapture", "--test-threads=1") -Label "Owned lifecycle E2E regression" -Timeout $RustTestTimeout -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $e2e -Name "Owned lifecycle E2E regression"
    Write-LifecycleEvidence -Category "assertion" -Value "e2e=passed"

    Set-LifecycleStage -State $State -Stage "multi-gateway"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test multi_gateway -- --nocapture --test-threads=1"
    $multiGateway = Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("test", "--test", "multi_gateway", "--", "--nocapture", "--test-threads=1") -Label "Owned multi-gateway lifecycle regression" -Timeout $RustTestTimeout -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $multiGateway -Name "Owned multi-gateway lifecycle regression"
    Write-LifecycleEvidence -Category "assertion" -Value "cross-replica=one-terminal-successor-retained"

    Set-LifecycleStage -State $State -Stage "integration"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test integration lifecycle_expiration_invariants -- --nocapture --test-threads=1"
    $signedLifecycleInvariant = Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("test", "--test", "integration", "lifecycle_expiration_invariants", "--", "--nocapture", "--test-threads=1") -Label "Signed lifecycle pin-rm invariant" -Timeout $RustTestTimeout -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $signedLifecycleInvariant -Name "Signed lifecycle pin-rm invariant"
    Write-LifecycleEvidence -Category "assertion" -Value "pin-rm-request=zero-signed-integration"
}

function Invoke-Aws {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker -Arguments (@(
        "run", "--rm", "--pull=never", "--network", $Network,
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "AWS_DEFAULT_REGION=us-east-1",
        "-e", "AWS_EC2_METADATA_DISABLED=true",
        "-e", "AWS_CONFIG_FILE=/work/aws-config",
        "--mount", "type=bind,src=$RunRoot,dst=/work",
        $AwsImage,
        "--endpoint-url", $Endpoint
    ) + $Arguments) -Label "AWS CLI lifecycle assertion" -Timeout $AwsCommandTimeout -AllowedExitCodes $AllowedExitCodes
}

function Invoke-AwsJson {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments
    )
    if ($Arguments.Count -eq 0 -or $Arguments[0] -ne "s3api" -or @($Arguments | Where-Object { $_ -eq "--output" }).Count -ne 0) {
        throw "AWS JSON invocation requires one s3api command without an output option"
    }
    $result = Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments (@("--output", "json") + $Arguments)
    try { return ((@($result.StdOut) -join "`n") | ConvertFrom-Json -NoEnumerate) } catch { throw "AWS CLI returned non-JSON where JSON was required" }
}

function Get-AwsRequiredProperty {
    param(
        [Parameter(Mandatory)][object]$Document,
        [Parameter(Mandatory)][string]$PropertyName
    )
    if ($null -eq $Document -or $Document -isnot [pscustomobject]) { throw "AWS JSON document is not one object" }
    $properties = @($Document.PSObject.Properties.Match($PropertyName))
    if ($properties.Count -ne 1) { throw "AWS JSON required property does not occur exactly once" }
    return $properties[0].Value
}

function Get-AwsOptionalListEntries {
    param(
        [Parameter(Mandatory)][object]$Document,
        [Parameter(Mandatory)][ValidateSet("Versions", "DeleteMarkers", "Rules")][string]$PropertyName
    )
    if ($null -eq $Document -or $Document -isnot [pscustomobject]) { throw "AWS JSON document is not one object" }
    $properties = @($Document.PSObject.Properties.Match($PropertyName))
    if ($properties.Count -eq 0) { return @() }
    if ($properties.Count -ne 1) { throw "AWS JSON optional list property is duplicated" }
    $value = $properties[0].Value
    if ($null -eq $value) { return @() }
    $entries = @($value | Where-Object { $null -ne $_ })
    if ($entries.Count -gt 1000) { throw "AWS JSON optional list exceeds the bounded result size" }
    foreach ($entry in $entries) {
        if ($entry -isnot [pscustomobject]) { throw "AWS JSON optional list entry is not one object" }
    }
    return @($entries)
}

function Get-LifecycleVersionList {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string]$Bucket
    )
    $document = Invoke-AwsJson -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments @("s3api", "list-object-versions", "--bucket", $Bucket)
    return [pscustomobject]@{
        Versions = @(Get-AwsOptionalListEntries -Document $document -PropertyName "Versions")
        DeleteMarkers = @(Get-AwsOptionalListEntries -Document $document -PropertyName "DeleteMarkers")
    }
}

function Assert-LifecycleCurrentDaysState {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][ValidateSet("unversioned", "enabled", "suspended")][string]$Versioning
    )
    $list = Get-LifecycleVersionList -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Bucket $Bucket
    switch ($Versioning) {
        "unversioned" {
            if ($list.Versions.Count -ne 0 -or $list.DeleteMarkers.Count -ne 0) { throw "Unversioned lifecycle expiration retained a version entry" }
        }
        "enabled" {
            if ($list.Versions.Count -ne 1 -or $list.DeleteMarkers.Count -ne 1) { throw "Enabled lifecycle expiration did not retain one historical version and one marker" }
            if ([bool](Get-AwsRequiredProperty -Document $list.Versions[0] -PropertyName "IsLatest")) { throw "Enabled lifecycle historical version remained current" }
            if (-not [bool](Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "IsLatest")) { throw "Enabled lifecycle delete marker was not current" }
            if ([string](Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "VersionId") -cne "null") { return }
            throw "Enabled lifecycle delete marker used the suspended null identity"
        }
        "suspended" {
            if ($list.Versions.Count -ne 0 -or $list.DeleteMarkers.Count -ne 1) { throw "Suspended lifecycle expiration did not retain one null marker" }
            if (-not [bool](Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "IsLatest")) { throw "Suspended lifecycle delete marker was not current" }
            if ([string](Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "VersionId") -ceq "null") { return }
            throw "Suspended lifecycle delete marker did not use the null identity"
        }
    }
}

function Assert-LifecycleNoncurrentState {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][ValidateSet("content", "marker")][string]$Target
    )
    $list = Get-LifecycleVersionList -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Bucket $Bucket
    if ($list.Versions.Count -ne 1 -or $list.DeleteMarkers.Count -ne 0) { throw "Noncurrent lifecycle expiration did not leave one current content successor" }
    if (-not [bool](Get-AwsRequiredProperty -Document $list.Versions[0] -PropertyName "IsLatest")) { throw "Noncurrent lifecycle successor was not current" }
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments @("s3api", "head-object", "--bucket", $Bucket, "--key", $Key) | Out-Null
    Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-object", "--bucket", $Bucket, "--key", $Key, "/work/nve-successor-$Target.txt") | Out-Null
}

function Test-AwsAbsentOrNullProperty {
    param([Parameter(Mandatory)][object]$Document, [Parameter(Mandatory)][string]$PropertyName)
    if ($null -eq $Document -or $Document -isnot [pscustomobject]) { throw "AWS JSON document is not one object" }
    $properties = @($Document.PSObject.Properties.Match($PropertyName))
    if ($properties.Count -eq 0) { return $true }
    if ($properties.Count -ne 1) { throw "AWS JSON optional semantic property is duplicated" }
    return $null -eq $properties[0].Value
}

function Assert-LifecycleConfigurationShape {
    param(
        [Parameter(Mandatory)][object]$Document,
        [Parameter(Mandatory)][ValidateSet("current-days", "noncurrent")][string]$ExpectedKind
    )
    $rules = @(Get-AwsOptionalListEntries -Document $Document -PropertyName "Rules")
    if ($rules.Count -ne 1) { throw "Lifecycle GET did not return exactly one rule" }
    $rule = $rules[0]
    if ([string](Get-AwsRequiredProperty -Document $rule -PropertyName "Status") -cne "Enabled") { throw "Lifecycle GET rule status differed from canonical configuration" }
    switch ($ExpectedKind) {
        "current-days" {
            if ([string](Get-AwsRequiredProperty -Document $rule -PropertyName "ID") -cne "current-days") { throw "Lifecycle GET current rule identity differed" }
            $expiration = Get-AwsRequiredProperty -Document $rule -PropertyName "Expiration"
            if ($expiration -isnot [pscustomobject] -or [string](Get-AwsRequiredProperty -Document $expiration -PropertyName "Days") -cne "1") { throw "Lifecycle GET current expiration differed" }
            if (-not (Test-AwsAbsentOrNullProperty -Document $rule -PropertyName "NoncurrentVersionExpiration")) { throw "Lifecycle GET current rule retained a noncurrent action" }
        }
        "noncurrent" {
            if ([string](Get-AwsRequiredProperty -Document $rule -PropertyName "ID") -cne "noncurrent-days") { throw "Lifecycle GET noncurrent rule identity differed" }
            $noncurrentExpiration = Get-AwsRequiredProperty -Document $rule -PropertyName "NoncurrentVersionExpiration"
            if ($noncurrentExpiration -isnot [pscustomobject] -or [string](Get-AwsRequiredProperty -Document $noncurrentExpiration -PropertyName "NoncurrentDays") -cne "1") { throw "Lifecycle GET noncurrent expiration differed" }
            if (-not (Test-AwsAbsentOrNullProperty -Document $rule -PropertyName "Expiration")) { throw "Lifecycle GET noncurrent rule retained a current action" }
        }
    }
}

function Get-AwsCliErrorCode {
    param([Parameter(Mandatory)][string[]]$StdErr)
    $text = (@($StdErr) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text) -or $text.Length -gt 8192) { throw "AWS CLI error document is absent or oversized" }
    try { $document = $text | ConvertFrom-Json -NoEnumerate } catch { throw "AWS CLI error document is invalid" }
    if ($document -is [Array] -or $document -isnot [pscustomobject] -or @($document.PSObject.Properties.Match("Code")).Count -ne 1) {
        throw "AWS CLI error document has no one Code"
    }
    $code = [string]$document.Code
    if ($code -notin @("NoSuchKey", "NoSuchLifecycleConfiguration", "404", "405", "InvalidRequest")) { throw "AWS CLI error Code is not allowlisted" }
    return $code
}

function Assert-AwsExpectedFailure {
    param(
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][ValidateSet("NoSuchKey", "NoSuchLifecycleConfiguration", "404", "405", "InvalidRequest")][string]$ExpectedCode
    )
    $result = Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments (@("--cli-error-format", "json") + $Arguments) -AllowedExitCodes @(0, 1, 2, 252, 253, 254, 255)
    if ($result.ExitCode -eq 0) { throw "AWS request unexpectedly succeeded" }
    if ((Get-AwsCliErrorCode -StdErr $result.StdErr) -cne $ExpectedCode) { throw "AWS request failed with an unexpected code" }
}

function Write-LifecycleConfiguration {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][ValidateSet("current-days", "noncurrent", "timed-marker", "eodm", "unsupported-transition")][string]$Kind
    )
    $path = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "$Kind.json")
    $json = switch ($Kind) {
        "current-days" { '{"Rules":[{"ID":"current-days","Filter":{},"Status":"Enabled","Expiration":{"Days":1}}]}' }
        "noncurrent" { '{"Rules":[{"ID":"noncurrent-days","Filter":{},"Status":"Enabled","NoncurrentVersionExpiration":{"NoncurrentDays":1}}]}' }
        "timed-marker" { '{"Rules":[{"ID":"timed-marker","Filter":{},"Status":"Enabled","Expiration":{"Days":1}}]}' }
        "eodm" { '{"Rules":[{"ID":"eodm","Filter":{},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true}}]}' }
        "unsupported-transition" { '{"Rules":[{"ID":"unsupported","Filter":{},"Status":"Enabled","Transitions":[{"Days":1,"StorageClass":"GLACIER"}]}]}' }
    }
    [IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))
    return $path
}

function Invoke-LifecycleSql {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("current-age", "noncurrent-age", "marker-age", "sole-marker-age", "revision")][string]$Statement,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$Key
    )
    if ($Bucket.Length -gt 63 -or $Bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Lifecycle SQL bucket is invalid"
    }
    if ($Key.Length -gt 256 -or $Key -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$') {
        throw "Lifecycle SQL key is invalid"
    }
    $query = switch ($Statement) {
        "current-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest" }
        "noncurrent-age" { "UPDATE object_versions SET became_noncurrent_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND NOT is_latest" }
        "marker-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest AND kind = 'delete_marker'" }
        "sole-marker-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest AND kind = 'delete_marker'" }
        "revision" { "SELECT revision::text FROM bucket_lifecycle_configs WHERE bucket = '$Bucket'" }
    }
    $result = Invoke-Compose -Project $State.Project -Arguments @(
        "exec", "-T", "postgres", "psql", "-X", "-U", "ipfs3", "-d", "ipfs3", "-v", "ON_ERROR_STOP=1", "-A", "-t",
        "-c", $query
    ) -Label "targeted lifecycle database assertion" -Timeout ([TimeSpan]::FromSeconds(30)) -AllowedExitCodes @(0, 1)
    if ($result.ExitCode -ne 0) {
        $errorText = @($result.StdErr) -join "`n"
        $errorCategory = if ($errorText -match '(?i)relation .* does not exist') {
            "missing-relation"
        } elseif ($errorText -match '(?i)(connection|authentication|could not connect)') {
            "connection"
        } else {
            "other"
        }
        Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=failed"
        Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-error-category=$errorCategory"
        throw "Lifecycle SQL command failed"
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=passed"
    return @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-LifecycleRevision {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Bucket)
    $rows = @(Invoke-LifecycleSql -State $State -Statement "revision" -Bucket $Bucket -Key "lifecycle-control.txt")
    $rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }
    $shapeValid = $rows.Count -eq 1 -and $rows[0].Trim() -cmatch '^[1-9][0-9]*$'
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-row-count=$rowCountReceipt"
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-shape-valid=$($shapeValid.ToString().ToLowerInvariant())"
    if (-not $shapeValid) { throw "Lifecycle revision receipt is invalid" }
    return $rows[0].Trim()
}

function Wait-LifecycleCondition {
    param(
        [Parameter(Mandatory)][scriptblock]$Condition,
        [Parameter(Mandatory)][string]$Name
    )
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Lifecycle assertion timed out"
}

function Test-KeyAbsent {
    param([Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$RunRoot, [Parameter(Mandatory)][string]$Endpoint, [Parameter(Mandatory)][string]$Bucket, [Parameter(Mandatory)][string]$Key)
    try {
        Assert-AwsExpectedFailure -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments @("s3api", "head-object", "--bucket", $Bucket, "--key", $Key) -ExpectedCode "404"
        return $true
    } catch { return $false }
}

function Invoke-LifecycleCurrentDaysScenario {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string]$Endpoint,
        [Parameter(Mandatory)][ValidateSet("unversioned", "enabled", "suspended")][string]$Versioning
    )
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-$Versioning") -Prefix "ipfs3-lc"
    $key = "current-$Versioning.txt"
    $config = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "current-days"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    if ($Versioning -eq "enabled") {
        Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled") | Out-Null
    } elseif ($Versioning -eq "suspended") {
        Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled") | Out-Null
        Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Suspended") | Out-Null
    }
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/current-days.json") | Out-Null
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "$Versioning.txt"), "lifecycle-$Versioning", [Text.UTF8Encoding]::new($false))
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/$Versioning.txt") | Out-Null
    Invoke-LifecycleSql -State $State -Statement "current-age" -Bucket $bucket -Key $key | Out-Null
    Wait-LifecycleCondition -Name "current expiration" -Condition {
        if (-not (Test-KeyAbsent -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Key $key)) { return $false }
        try {
            Assert-LifecycleCurrentDaysState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Versioning $Versioning
            return $true
        } catch { return $false }
    }
    Assert-LifecycleCurrentDaysState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Versioning $Versioning
    Write-LifecycleEvidence -Category "assertion" -Value "current-days-$Versioning=passed"
}

function Invoke-LifecycleNoncurrentScenario {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$Endpoint, [Parameter(Mandatory)][ValidateSet("content", "marker")][string]$Target)
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-nve-$Target") -Prefix "ipfs3-lc"
    $key = "nve-$Target.txt"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "noncurrent"
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "nve-first.txt"), "nve-first", [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "nve-second.txt"), "nve-second", [Text.UTF8Encoding]::new($false))
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled") | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/noncurrent.json") | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/nve-first.txt") | Out-Null
    if ($Target -eq "marker") {
        Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-object", "--bucket", $bucket, "--key", $key) | Out-Null
    }
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/nve-second.txt") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=setup-complete"
    Invoke-LifecycleSql -State $State -Statement "noncurrent-age" -Bucket $bucket -Key $key | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=age-applied"
    Wait-LifecycleCondition -Name "noncurrent expiration" -Condition {
        try {
            Assert-LifecycleNoncurrentState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Key $key -Target $Target
            return $true
        } catch { return $false }
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=wait-complete"
    Assert-LifecycleNoncurrentState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Key $key -Target $Target
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=final-assert-passed"
    Write-LifecycleEvidence -Category "assertion" -Value "nve-$Target=passed"
}

function Invoke-LifecycleMarkerScenario {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$Endpoint, [Parameter(Mandatory)][ValidateSet("timed", "eodm")][string]$Mode)
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-marker-$Mode") -Prefix "ipfs3-lc"
    $key = "marker-$Mode.txt"
    $kind = if ($Mode -eq "timed") { "timed-marker" } else { "eodm" }
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind $kind
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "marker.txt"), "marker", [Text.UTF8Encoding]::new($false))
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled") | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/$kind.json") | Out-Null
    $put = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/marker.txt")
    $delete = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-object", "--bucket", $bucket, "--key", $key)
    if ([string]::IsNullOrWhiteSpace([string]$put.VersionId) -or [string]::IsNullOrWhiteSpace([string]$delete.VersionId)) { throw "Lifecycle marker setup did not return version identities" }
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-object", "--bucket", $bucket, "--key", $key, "--version-id", ([string]$put.VersionId)) | Out-Null
    if ($Mode -eq "timed") { Invoke-LifecycleSql -State $State -Statement "sole-marker-age" -Bucket $bucket -Key $key | Out-Null }
    Wait-LifecycleCondition -Name "sole marker cleanup" -Condition {
        try {
            $list = Get-LifecycleVersionList -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket
            return $list.Versions.Count -eq 0 -and $list.DeleteMarkers.Count -eq 0
        } catch { return $false }
    }
    $finalList = Get-LifecycleVersionList -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket
    if ($finalList.Versions.Count -ne 0 -or $finalList.DeleteMarkers.Count -ne 0) { throw "Sole marker lifecycle cleanup retained public entries" }
    Write-LifecycleEvidence -Category "assertion" -Value "sole-marker-$Mode=passed"
}

function Invoke-LifecycleControlPlaneEvidence {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$Endpoint)
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-control") -Prefix "ipfs3-lc"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=files-written"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "current-days"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "noncurrent"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "unsupported-transition"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=files-written-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=bucket-created"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=bucket-created-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-put"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/current-days.json") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-put-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-get-shape"
    $initialConfiguration = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket)
    Assert-LifecycleConfigurationShape -Document $initialConfiguration -ExpectedKind "current-days"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-get-shape-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-revision"
    $initialRevision = Get-LifecycleRevision -State $State -Bucket $bucket
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-revision-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=unsupported-rejected"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/unsupported-transition.json") -ExpectedCode "InvalidRequest"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=unsupported-rejected-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=revision-unchanged"
    if ((Get-LifecycleRevision -State $State -Bucket $bucket) -cne $initialRevision) { throw "Unsupported lifecycle action changed the revision" }
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=revision-unchanged-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-put"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/noncurrent.json") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-put-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-get-shape"
    $replacementConfiguration = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket)
    Assert-LifecycleConfigurationShape -Document $replacementConfiguration -ExpectedKind "noncurrent"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-get-shape-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=lifecycle-deleted"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-bucket-lifecycle", "--bucket", $bucket) | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=lifecycle-deleted-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=absent-get-verified"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket) -ExpectedCode "NoSuchLifecycleConfiguration"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=absent-get-verified-passed"
    Write-LifecycleEvidence -Category "assertion" -Value "lifecycle-put-get-replace-delete=passed"
    Write-LifecycleEvidence -Category "assertion" -Value "unsupported-revision-unchanged=passed"
}

function Invoke-LifecycleAwsEvidence {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network)
    $endpoint = "http://load-balancer:9000"
    Write-LifecycleEvidence -Category "command" -Value "aws-cli-path-style-sigv4-lifecycle-actions"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=control-plane"
    Invoke-LifecycleControlPlaneEvidence -State $State -Network $Network -Endpoint $endpoint
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=control-plane-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-unversioned"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "unversioned"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-unversioned-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-enabled"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "enabled"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-enabled-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-suspended"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "suspended"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-suspended-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-content"
    Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "content"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-content-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-marker"
    Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "marker"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-marker-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-timed"
    Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "timed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-timed-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-eodm"
    Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "eodm"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-eodm-passed"
    Write-LifecycleEvidence -Category "result" -Value "lifecycle-actions=passed"
}

function Get-GitHead {
    $result = Invoke-NativeCommand -FilePath "git" -ArgumentList @("rev-parse", "HEAD") -Label "Git revision evidence" -Timeout $DockerCommandTimeout -WorkingDirectory $RepoRoot
    $head = (@($result.StdOut) -join "").Trim()
    if ($head -notmatch '^[0-9a-f]{40}$') { throw "Git revision evidence was invalid" }
    return $head
}

function Get-VerifiedSpecSha256 {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$ExpectedHash)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "Approved lifecycle specification is unavailable" }
    $hash = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($hash -cne $ExpectedHash) { throw "Approved lifecycle specification hash differs" }
    return $hash
}

function Capture-SanitizedDiagnostics {
    param([Parameter(Mandatory)][string]$Project)
    $ps = Invoke-Compose -Project $Project -Arguments @("ps", "--format", "json") -Label "failure diagnostics compose ps"
    $psLines = @($ps.StdOut + $ps.StdErr | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    Write-LifecycleEvidence -Category "diagnostic" -Value "compose-ps=redacted-lines-$psLines"
    $logs = Invoke-Compose -Project $Project -Arguments @("logs", "--no-color", "--tail", "200") -Label "failure diagnostics compose logs"
    $logLines = @($logs.StdOut + $logs.StdErr | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    Write-LifecycleEvidence -Category "diagnostic" -Value "compose-logs=redacted-lines-$logLines"
}

function Remove-OwnedLifecycleRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$Receipt,
        [Parameter(Mandatory)][string]$RunId
    )
    $canonicalTemp = Get-CanonicalExistingDirectory -Path $TempRoot
    $canonicalRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-lifecycle-expiration-$RunId"))
    if (-not $canonicalRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase) -or -not [IO.Path]::GetDirectoryName($canonicalRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Lifecycle root cleanup rejected a noncanonical direct child"
    }
    $canonicalReceipt = Assert-CanonicalChildPath -ParentPath $canonicalRoot -ChildPath $Receipt
    if (-not $canonicalReceipt.Equals((Join-Path $canonicalRoot "ownership-receipt"), [StringComparison]::OrdinalIgnoreCase) -or -not (Test-Path -LiteralPath $canonicalReceipt -PathType Leaf) -or [IO.File]::ReadAllText($canonicalReceipt) -cne "lifecycle-expiration-owned:$RunId") {
        throw "Lifecycle root cleanup rejected an absent or foreign receipt"
    }
    Remove-Item -LiteralPath $canonicalRoot -Recurse -Force
}

function Remove-OwnedEmptyLifecycleRunRoot {
    param([Parameter(Mandatory)][string]$TempRoot, [Parameter(Mandatory)][string]$RunRoot, [Parameter(Mandatory)][string]$RunId)
    $canonicalTemp = Get-CanonicalExistingDirectory -Path $TempRoot
    $canonicalRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-lifecycle-expiration-$RunId"))
    if (-not $canonicalRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase) -or -not [IO.Path]::GetDirectoryName($canonicalRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Empty lifecycle root cleanup rejected a noncanonical direct child"
    }
    if (-not (Test-Path -LiteralPath $canonicalRoot -PathType Container)) { return }
    if (@([IO.Directory]::EnumerateFileSystemEntries($canonicalRoot)).Count -ne 0) { throw "Empty lifecycle root cleanup rejected nonempty content" }
    Remove-Item -LiteralPath $canonicalRoot -Force
}

function Test-LifecycleCleanupResiduals {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    if ($State.Blocked -or [string]::IsNullOrWhiteSpace($State.Project)) {
        foreach ($name in @("containers", "networks", "volumes", "image")) { Write-LifecycleEvidence -Category "cleanup" -Value "residual-$name=not-queried" }
        return @($errors)
    }
    foreach ($query in @(
        [pscustomobject]@{ Name = "containers"; Arguments = @("ps", "-aq", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "networks"; Arguments = @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "volumes"; Arguments = @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") }
    )) {
        try {
            $result = Invoke-Docker -Arguments $query.Arguments -Label "project cleanup residual query"
            $count = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
            if ($count -eq 0) { Write-LifecycleEvidence -Category "cleanup" -Value "residual-$($query.Name)=zero" } else { Write-LifecycleEvidence -Category "cleanup" -Value "residual-$($query.Name)=nonzero"; $errors.Add("residual-$($query.Name)") }
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "residual-$($query.Name)=query-failed"
            $errors.Add("residual-$($query.Name)")
        }
    }
    if ([string]::IsNullOrWhiteSpace($State.GatewayImage)) {
        Write-LifecycleEvidence -Category "cleanup" -Value "residual-image=not-queried"
    } else {
        try {
            if ((Test-LocalImage -Image $State.GatewayImage).Exists) { Write-LifecycleEvidence -Category "cleanup" -Value "residual-image=nonzero"; $errors.Add("residual-image") } else { Write-LifecycleEvidence -Category "cleanup" -Value "residual-image=zero" }
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "residual-image=query-failed"
            $errors.Add("residual-image")
        }
    }
    return @($errors)
}

function Remove-OwnedLifecycleResources {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    if (-not $State.Blocked -and -not [string]::IsNullOrWhiteSpace($State.Project) -and -not $State.ProjectOwned -and $State.ProjectOwnershipProbeFailed) {
        try {
            Claim-LifecycleProjectOwnership -State $State
            if ($State.ProjectOwned) { Write-LifecycleEvidence -Category "cleanup" -Value "project-ownership-retry=owned" } else { Write-LifecycleEvidence -Category "cleanup" -Value "project-ownership-retry=absent" }
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "project-ownership-retry=failed"
            $errors.Add("project-ownership-retry")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "project-ownership-retry=not-needed"
    }
    if ($State.ProjectOwned) {
        try {
            Invoke-Compose -Project $State.Project -Arguments @("down", "--volumes", "--remove-orphans") -Label "owned Compose teardown" | Out-Null
            Write-LifecycleEvidence -Category "cleanup" -Value "compose-down=passed"
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "compose-down=failed"
            $errors.Add("compose-down")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "compose-down=not-owned"
    }
    if (-not $State.Blocked -and $State.GatewayImagePreflightAbsent -and -not $State.GatewayImageOwned) {
        try {
            if ((Test-LocalImage -Image $State.GatewayImage).Exists) { $State.GatewayImageOwned = $true; Write-LifecycleEvidence -Category "cleanup" -Value "image-ownership-probe=owned" } else { Write-LifecycleEvidence -Category "cleanup" -Value "image-ownership-probe=absent" }
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "image-ownership-probe=failed"
            $errors.Add("image-ownership-probe")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "image-ownership-probe=not-needed"
    }
    if ($State.GatewayImageOwned) {
        try {
            if ((Test-LocalImage -Image $State.GatewayImage).Exists) { Invoke-Docker -Arguments @("image", "rm", $State.GatewayImage) -Label "owned lifecycle gateway image removal" | Out-Null }
            Write-LifecycleEvidence -Category "cleanup" -Value "image-remove=passed"
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "image-remove=failed"
            $errors.Add("image-remove")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "image-remove=not-owned"
    }
    foreach ($errorName in (Test-LifecycleCleanupResiduals -State $State)) { $errors.Add($errorName) }
    if ($State.RunRootReceiptOwned) {
        try {
            Remove-OwnedLifecycleRunRoot -TempRoot $State.TempRoot -RunRoot $State.RunRoot -Receipt $State.RunRootReceipt -RunId $State.RunId
            Write-LifecycleEvidence -Category "cleanup" -Value "temp-remove=passed"
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "temp-remove=failed"
            $errors.Add("run-root")
        }
    } elseif ($State.RunRootDirectoryOwned) {
        try {
            Remove-OwnedEmptyLifecycleRunRoot -TempRoot $State.TempRoot -RunRoot $State.RunRoot -RunId $State.RunId
            Write-LifecycleEvidence -Category "cleanup" -Value "temp-remove=passed"
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "temp-remove=failed"
            $errors.Add("run-root")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "temp-remove=not-owned"
    }
    return @($errors)
}

function Invoke-LifecycleMain {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-LifecycleStage -State $State -Stage "preflight"
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) { throw "Lifecycle validation Compose file is unavailable" }
    Assert-RequiredTools
    Assert-ComposeVersion
    $imageIds = Assert-RequiredLocalImages
    $State.RunId = New-LifecycleRunId
    $State.Project = New-LifecycleProjectName -RunId $State.RunId
    $State.GatewayImage = "ipfs3-lifecycle-gateway:$($State.RunId)"
    $State.Bucket = New-LifecycleBucketName -RunId $State.RunId
    try {
        Assert-ProjectResourcesAbsent -Project $State.Project
        if ((Test-LocalImage -Image $State.GatewayImage).Exists) { throw "BLOCKED lifecycle gateway image already exists" }
        $State.GatewayImagePreflightAbsent = $true
        Assert-LoopbackPortsFree -Ports $LifecyclePorts
    } catch {
        $State.Blocked = $true
        throw
    }
    try {
        $State.RunRoot = New-LifecycleRunRoot -TempRoot $State.TempRoot -RunId $State.RunId
    } catch {
        $State.Blocked = $true
        throw
    }
    $State.RunRootDirectoryOwned = $true
    $State.RunRootReceipt = New-LifecycleOwnershipReceipt -RunRoot $State.RunRoot -RunId $State.RunId
    $State.RunRootReceiptOwned = $true
    $null = New-LifecycleAwsConfig -RunRoot $State.RunRoot
    $State.EnvironmentState = Save-EnvironmentState -Names $TouchedEnvironmentNames
    $State.EnvironmentStateOwned = $true
    Set-RunEnvironment -Name "COMPOSE_DISABLE_ENV_FILE" -Value "1"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_POSTGRES_PORT" -Value "55437"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_KUBO_PORT" -Value "55004"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_GATEWAY_A_PORT" -Value "59004"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_GATEWAY_B_PORT" -Value "59005"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_LOAD_BALANCER_PORT" -Value "59006"
    Set-RunEnvironment -Name "IPFS_S3_LIFECYCLE_IMAGE" -Value $State.GatewayImage

    Set-LifecycleStage -State $State -Stage "config"
    Invoke-Compose -Project $State.Project -Arguments @("config", "--quiet") -Label "lifecycle validation Compose config" | Out-Null
    Set-LifecycleStage -State $State -Stage "offline-build"
    try {
        Invoke-OfflineGatewayBuild -RunRoot $State.RunRoot -GatewayImage $State.GatewayImage
    } finally {
        if ((Test-LocalImage -Image $State.GatewayImage).Exists) { $State.GatewayImageOwned = $true }
    }
    if (-not $State.GatewayImageOwned) { throw "Unique lifecycle gateway image was not built" }

    Set-LifecycleStage -State $State -Stage "compose-up"
    try {
        Invoke-Compose -Project $State.Project -Arguments @("up", "--detach", "--pull", "never", "--no-build", "--wait", "--wait-timeout", "300", "postgres", "kubo", "gateway-a", "gateway-b", "load-balancer") -Label "lifecycle validation Compose startup" -Timeout $ComposeStartupTimeout | Out-Null
    } finally {
        try { Claim-LifecycleProjectOwnership -State $State } catch { $State.ProjectOwnershipProbeFailed = $true }
    }
    if (-not $State.ProjectOwned) { throw "Compose startup did not claim lifecycle project resources" }
    Set-LifecycleStage -State $State -Stage "health"
    Wait-TopologyHealthy -Project $State.Project
    Set-LifecycleStage -State $State -Stage "network"
    $network = Get-ComposeNetwork -Project $State.Project
    Set-LifecycleStage -State $State -Stage "metadata"
    Write-LifecycleEvidence -Category "metadata" -Value "package=0.1.0"
    Write-LifecycleEvidence -Category "metadata" -Value "git-head=$(Get-GitHead)"
    Write-LifecycleEvidence -Category "metadata" -Value "expiration-spec-sha256=$(Get-VerifiedSpecSha256 -Path $ExpirationSpecPath -ExpectedHash $ExpectedExpirationSpecSha256)"
    Write-LifecycleEvidence -Category "metadata" -Value "program-spec-sha256=$(Get-VerifiedSpecSha256 -Path $ProgramSpecPath -ExpectedHash $ExpectedProgramSpecSha256)"
    Write-LifecycleEvidence -Category "metadata" -Value "aws-image-id=$($imageIds[$AwsImage])"
    if ($DiagnoseLifecycleAws) {
        Set-LifecycleStage -State $State -Stage "aws"
        Invoke-LifecycleAwsEvidence -State $State -Network $network
        $State.DiagnosticOutcome = "aws-diagnostic-passed"
    } elseif ($DiagnoseLifecycleRaceStability) {
        Invoke-LifecycleRaceStabilityDiagnostic -State $State
    } elseif ($DiagnoseLifecycleRaceExact) {
        Invoke-LifecycleRaceExactDiagnostic -State $State
    } elseif ($DiagnoseMultiGateway) {
        Invoke-LifecycleMultiGatewayDiagnostic -State $State
    } else {
        Invoke-LifecycleRustSuites -State $State
        Set-LifecycleStage -State $State -Stage "aws"
        Invoke-LifecycleAwsEvidence -State $State -Network $network
    }
}

$state = @{
    TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    RunId = $null
    Project = $null
    Bucket = $null
    RunRoot = $null
    RunRootReceipt = $null
    GatewayImage = $null
    EnvironmentState = @()
    Stage = "preflight"
    RunRootDirectoryOwned = $false
    RunRootReceiptOwned = $false
    GatewayImagePreflightAbsent = $false
    GatewayImageOwned = $false
    ProjectOwned = $false
    ProjectOwnershipProbeFailed = $false
    EnvironmentStateOwned = $false
    Blocked = $false
    DiagnosticOutcome = $null
}
$workSucceeded = $false
$failureReason = "execution-failed"
$cleanupErrors = [Collections.Generic.List[string]]::new()
try {
    Invoke-LifecycleMain -State $state
    $workSucceeded = $true
} catch {
    Write-LifecycleEvidence -Category "result" -Value "failed-stage=$($state.Stage)"
    if ($state.Blocked) { $failureReason = "blocked" }
} finally {
    Set-LifecycleStage -State $state -Stage "cleanup"
    if (-not $workSucceeded -and $state.ProjectOwned) {
        Write-LifecycleEvidence -Category "cleanup" -Value "diagnostics=attempted"
        try { Capture-SanitizedDiagnostics -Project $state.Project } catch { $cleanupErrors.Add("diagnostics") }
    } elseif (-not $workSucceeded) {
        Write-LifecycleEvidence -Category "cleanup" -Value "diagnostics=not-owned"
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "diagnostics=not-required"
    }
    foreach ($cleanupError in (Remove-OwnedLifecycleResources -State $state)) { $cleanupErrors.Add($cleanupError) }
    if ($state.EnvironmentStateOwned) {
        try {
            Restore-EnvironmentState -State $state.EnvironmentState
            Write-LifecycleEvidence -Category "cleanup" -Value "environment-restore=passed"
        } catch {
            Write-LifecycleEvidence -Category "cleanup" -Value "environment-restore=failed"
            $cleanupErrors.Add("environment")
        }
    } else {
        Write-LifecycleEvidence -Category "cleanup" -Value "environment-restore=not-needed"
    }
    Write-LifecycleEvidence -Category "cleanup" -Value "cleanup-errors=$($cleanupErrors.Count)"
}

if ($cleanupErrors.Count -ne 0) {
    $workSucceeded = $false
    $failureReason = "cleanup-failed"
}
if ($state.DiagnosticOutcome -eq "captured") {
    Write-Host "[RESULT] lifecycle-expiration=FAILED reason=multi-gateway-diagnostic-captured"
    exit 1
}
if ($state.DiagnosticOutcome -eq "exact-race-failed") {
    Write-Host "[RESULT] lifecycle-expiration=FAILED reason=exact-race-diagnostic-captured"
    exit 1
}
if ($state.DiagnosticOutcome -eq "exact-race-stability-failed") {
    Write-Host "[RESULT] lifecycle-expiration=FAILED reason=exact-race-stability-captured"
    exit 1
}
if ($workSucceeded) {
    Write-LifecycleEvidence -Category "cleanup" -Value "cleanup=complete"
    if ($state.DiagnosticOutcome -eq "aws-diagnostic-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=aws-passed"
    } elseif ($state.DiagnosticOutcome -eq "exact-race-stability-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-stability-passed"
    } elseif ($state.DiagnosticOutcome -eq "exact-race-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-passed"
    } elseif ($state.DiagnosticOutcome -eq "not-reproduced") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=not-reproduced"
    } else {
        Write-Host "[RESULT] lifecycle-expiration=PASSED"
    }
    exit 0
}
Write-Host "[RESULT] lifecycle-expiration=FAILED reason=$failureReason"
exit 1
